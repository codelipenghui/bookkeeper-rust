# io_uring Optimization for Rust BookKeeper Bookie

**Date:** 2026-04-09
**Status:** Approved design, pending implementation

## Goal

Replace all blocking disk I/O in the Rust BookKeeper bookie with Linux
`io_uring` to reduce write latency, increase flush throughput, and lower
read tail latency. Provide a transparent runtime fallback to the current
`std::fs` / `BufWriter` code on macOS and older Linux kernels.

## Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Scope | Journal + entry log flush + entry log reads | Full coverage; journal is highest-value but flush and reads also benefit from syscall batching |
| Platform strategy | Runtime fallback via `IoEngine` trait | macOS dev experience stays working; Linux gets io_uring automatically; old kernels degrade gracefully |
| Crate | `io-uring` (low-level) | No runtime conflict with tokio; manual ring control matches our dedicated-thread architecture; production-proven (Databend, RisingWave) |
| Ring topology | 2 rings: journal + ledger | Journal latency is isolated from bulk flush traffic; simpler than 3 rings while avoiding the single-ring contention problem |
| Journal pipeline | Single io thread replaces writer + fsync threads | SQE chaining (WRITEV → linked FSYNC) eliminates the inter-thread hop and reduces 4+ syscalls to 1 `io_uring_enter()` |

## Architecture

### IoEngine trait

```rust
use std::io;
use std::os::fd::RawFd;

pub trait IoEngine: Send + Sync + 'static {
    fn journal(&self) -> &dyn JournalIo;
    fn ledger(&self) -> &dyn LedgerIo;
}

pub trait JournalIo: Send + Sync {
    /// Write entries + fsync as an atomic batch. Returns after fsync.
    /// Each &[u8] is one framed journal entry (size-prefix + payload).
    fn write_and_sync(&self, fd: RawFd, entries: &[&[u8]]) -> io::Result<()>;
}

pub trait LedgerIo: Send + Sync {
    /// Batch-write entries to entry log at the given offset.
    /// Each &[u8] is one framed entry log entry (size-prefix + payload).
    /// Returns the starting byte offset of each entry written.
    fn write_batch(&self, fd: RawFd, offset: u64, entries: &[&[u8]]) -> io::Result<Vec<u64>>;
    /// Read `len` bytes at `offset` from the given fd.
    fn read_at(&self, fd: RawFd, offset: u64, len: usize) -> io::Result<Vec<u8>>;
}
```

Two implementations:
- `UringEngine`: two `io_uring` rings on dedicated threads (Linux 5.6+)
- `StdEngine`: wraps the existing `BufWriter<fs::File>` + `File::sync_data()` code

Runtime selection at startup via `uring_available()` probe (attempts
`io_uring_setup()` with minimal params).

### Journal ring

Single `journal-io` thread owns an `io_uring` instance (SQ depth 64).

**Write flow:**
1. Client threads send entries via bounded `mpsc::sync_channel` (unchanged API)
2. Ring thread drains channel, batches entries
3. Submits 2 SQEs per batch:
   - `WRITEV` with scatter-gather iovec of all entries (including V5+ padding)
   - `FSYNC` linked to the WRITEV via `IO_LINK` flag
4. `io_uring_enter(submit=2, wait=2)` — single syscall, kernel-ordered
5. On FSYNC completion: ACK all clients in the batch

**Journal rotation:** between batches, when file size exceeds
`max_file_size`. Close current fd, open new `.txn` file, update
registered fd. Never mid-batch.

**Fixed file descriptors:** current journal fd registered via
`io_uring_register_files()` to avoid per-SQE fd resolution.

**StdEngine fallback:** keeps existing 2-thread architecture
(`journal-writer` + `journal-fsync`) with `BufWriter` + `sync_data()`.

### Ledger ring

Single `ledger-io` thread owns an `io_uring` instance (SQ depth 256).
Multiplexes two workloads:

**Entry log flush writes (throughput-critical):**
- `DbLedgerStorage::flush()` builds iovec from sorted write cache
- Single `WRITEV` SQE at known file offset
- No FSYNC needed (journal provides durability)
- On completion: record offsets, call `entry_location_index.put_batch()`

**Entry log reads (latency-sensitive, concurrent):**
- `read_entry_from_log()` sends `ReadRequest` to ring thread
- `READ` SQE with registered fd and pre-registered buffer
- Multiple reads in-flight simultaneously
- Each CQE carries user_data linking back to caller's reply channel

**Pre-registered resources:**
- Fixed file table: 256 slots for open entry log files, refreshed on
  rotation/GC
- Fixed buffer pool: 64 buffers x 64KB = 4MB for `READ_FIXED` SQEs

**StdEngine fallback:** `write_batch()` does current `BufWriter::write_all`
loop + `flush()`. `read_at()` does `File::open` + `seek` + `read_exact`.

## Integration points

### What changes

| Component | Change |
|---|---|
| `BookieServer::start()` | Calls `select_engine()`, passes `Arc<dyn IoEngine>` to Journal and DbLedgerStorage |
| `Journal::start()` | Takes `Arc<dyn JournalIo>`. UringJournalIo: 1 ring thread. StdJournalIo: 2 threads (unchanged). |
| `DbLedgerStorage::open()` | Takes `Arc<dyn LedgerIo>`. `flush()` and `read_entry_from_log()` delegate to engine. |
| `DbLedgerStorage::write_entry_to_log()` | Used by GC compaction. Delegates to `engine.ledger().write_batch()` with a single entry. |

### What does NOT change

- Wire protocol (V2/V3)
- RocksDB index reads/writes (RocksDB manages its own I/O)
- Write cache and read cache (in-memory)
- ZK client, HTTP API, auto-recovery
- On-disk formats (entry log, journal, lastMark, cookie)
- `bk serve` CLI and config file format
- All test assertions (behavior identical, only syscall paths differ)

## File layout

```
crates/bookkeeper-storage/
    src/
        io_engine/
            mod.rs            IoEngine, JournalIo, LedgerIo traits + select_engine()
            std_engine.rs     StdEngine wrapping current BufWriter/fs::File code
            uring_engine.rs   UringEngine: two rings (cfg linux only)
            uring_journal.rs  Journal ring implementation
            uring_ledger.rs   Ledger ring implementation
        journal/
            writer.rs         Refactored to use dyn JournalIo
        db_ledger_storage.rs  Refactored to use dyn LedgerIo
```

`uring_*.rs` files gated behind `#[cfg(target_os = "linux")]`.

## Dependencies

```toml
# Cargo.toml (workspace)
[target.'cfg(target_os = "linux")'.dependencies]
io-uring = "0.7"
```

## Error handling

| Failure | Behavior |
|---|---|
| WRITE CQE short write | Retry remaining bytes as new SQE |
| FSYNC CQE returns EIO | Log FATAL, stop ACKing (clients timeout), bookie goes read-only. Matches Java's crash-on-fsync semantics. |
| READ CQE returns EIO | Return `StorageError::Io` to caller (same as current `read_exact` failure) |
| Ring SQ full | Ring thread blocks on `io_uring_enter(GETEVENTS)` until CQEs are reaped. Back-pressures channel senders naturally. |
| `io_uring_setup()` fails at startup | Fall back to `StdEngine` with warning log. NOT fatal. |
| Kernel too old | Detected by `uring_available()` probe → `StdEngine` fallback |

## Ring sizing

| Ring | SQ depth | CQ depth | Fixed files | Fixed buffers |
|---|---|---|---|---|
| Journal | 64 | 128 | 1 slot (current .txn fd) | None |
| Ledger | 256 | 512 | 256 slots (open .log files) | 64 x 64KB = 4MB |

## Testing strategy

**Unit tests:** `IoEngine` trait enables mock injection.
- `StdEngine`: all existing 476+ tests run unchanged (default engine)
- `UringEngine`: `#[cfg(target_os = "linux")]` tests covering journal
  write+fsync+replay, ledger write+read round-trip, and fallback detection

**Integration tests:** existing suite runs on `StdEngine` by default.
Separate Linux CI job runs same suite with `BOOKKEEPER_IO_ENGINE=uring`
env override.

**Benchmark:** extend `bk bench-bookie` with `--io-engine uring|std`
flag. Key metrics: p50/p99/p999 add_entry latency, sustained write
throughput MB/s, read IOPS on cache miss.

## Expected performance impact

| Path | Current syscalls | With io_uring | Estimated improvement |
|---|---|---|---|
| Journal write+fsync | 4+ per batch + 1 thread hop | 1 `io_uring_enter()`, 0 thread hops | 2-3x p99 write latency reduction on NVMe |
| Entry log flush | N `write()` per flush | 1 WRITEV per flush | 1.5-2x flush throughput |
| Entry log read | 3 per read (open, read, close) | 1 READ via registered fd + buffer | Lower p99 read latency, eliminates open/close overhead |
| Context switches | 2 per journal batch | 0 extra | Reduced kernel scheduling overhead |
