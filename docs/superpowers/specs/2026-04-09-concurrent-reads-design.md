# Concurrent io_uring Reads for Ledger Ring

**Date:** 2026-04-09
**Status:** Approved design, pending implementation
**Depends on:** io_uring optimization (implemented)

## Goal

Replace the current serialized one-read-at-a-time `Mutex<IoUring>` in
`UringLedgerIo` with a dedicated `ledger-io` ring thread that batches
concurrent read requests from multiple tokio blocking threads into a
single `io_uring_enter()` call. Also add an LRU file-descriptor cache
with io_uring fixed-file registration to eliminate per-read file
open/close syscalls.

## Current bottleneck

```
50 concurrent reads → Mutex<IoUring> → 1 READ SQE → wait → 1 READ SQE → wait → ...
= 50 sequential io_uring_enter() calls, each waiting for 1 CQE
```

Each read also opens the entry log file fresh (`File::open`) and closes
it on drop — 2 extra syscalls per read.

## Target

```
50 concurrent reads → channel → ledger-io thread drains all 50
  → submit 50 READ SQEs in 1 io_uring_enter(submit=50, wait=50)
  → dispatch 50 CQEs back to callers via oneshot channels
= 1 io_uring_enter() call, kernel parallelizes reads across NVMe queues
```

File descriptors are cached in an LRU (256 entries) and registered with
the ring via `register_files()`. SQEs use `types::Fixed(index)` for
zero-overhead fd resolution.

## Architecture

### Request flow

```
tokio blocking thread (caller)
  │
  ├── send ReadRequest { log_id, offset, len, reply_tx } to channel
  │
  ├── block on reply_rx.recv()
  │
  ▼
crossbeam bounded channel (capacity=256, backpressure)
  │
  ▼
ledger-io thread (owns the ring)
  │
  ├── drain channel → Vec<ReadRequest>
  ├── for each: resolve log_id → Fixed(fd_index) via LRU cache
  ├── submit N READ SQEs (user_data = request index)
  ├── io_uring_enter(submit=N, wait=N)
  ├── for each CQE: requests[user_data].reply_tx.send(data)
  └── loop
```

### LedgerIo trait addition

```rust
pub trait LedgerIo: Send + Sync {
    // Existing (used by write path):
    fn write_batch(&self, fd: RawFd, offset: u64, entries: &[&[u8]]) -> io::Result<Vec<u64>>;
    fn read_at(&self, fd: RawFd, offset: u64, len: usize) -> io::Result<Vec<u8>>;

    // New (used by read path — engine manages fds internally):
    fn read_entry(&self, entry_log_dir: &Path, log_id: u32, offset: u64, len: usize) -> io::Result<Vec<u8>>;
}
```

`StdEngine::read_entry` opens the file, seeks, reads, closes (same as
today). `UringLedgerIo::read_entry` sends a request to the ring thread
and blocks on the oneshot reply.

`DbLedgerStorage::read_entry_from_log()` switches from:
```rust
let file = fs::File::open(&path)?;
let fd = file.as_raw_fd();
let size_buf = self.ledger_io.read_at(fd, pos - 4, 4)?;
let entry = self.ledger_io.read_at(fd, pos, size)?;
```
to:
```rust
let size_buf = self.ledger_io.read_entry(&self.entry_log_dir, loc.log_id(), pos - 4, 4)?;
let entry = self.ledger_io.read_entry(&self.entry_log_dir, loc.log_id(), pos, size)?;
```

### LRU fd cache

- Capacity: 256 entries (matching io_uring fixed-file table size)
- Key: `(entry_log_dir, log_id)` → Value: `(fs::File, fixed_index)`
- On cache miss: `File::open()`, register in next free fixed-file slot
- On eviction: close file, mark slot as free
- On GC file deletion: explicit eviction before `fs::remove_file()`
- Registration: `submitter.register_files(&fds)` at startup (256 slots,
  initially -1/empty), `register_files_update(index, &[new_fd])` on
  cache insert

### Ring thread lifecycle

- Created in `UringLedgerIo::new()`
- Stopped when `UringLedgerIo` is dropped (channel disconnects, thread
  exits on `Disconnected`)
- Handles both reads AND writes (flush writes are sent as `WriteRequest`
  variants on the same channel)

### Request types

```rust
enum LedgerRequest {
    Read {
        entry_log_dir: PathBuf,
        log_id: u32,
        offset: u64,
        len: usize,
        reply: oneshot::Sender<io::Result<Vec<u8>>>,
    },
    Write {
        fd: RawFd,
        offset: u64,
        entries: Vec<Vec<u8>>,
        reply: oneshot::Sender<io::Result<Vec<u64>>>,
    },
}
```

### Batching strategy

The ring thread uses `recv_timeout` (similar to the journal):
1. Block on `channel.recv()` for the first request
2. Drain all immediately-available requests via `try_recv()`
3. Submit all SQEs in one `io_uring_enter()`
4. Wait for all CQEs
5. Dispatch replies

Under low load (1 read at a time), this degrades to the current
1-SQE-at-a-time behavior with channel overhead (~200ns). Under high
load (50+ concurrent reads), it batches into 1 syscall.

## What changes

| Component | Change |
|---|---|
| `LedgerIo` trait | Add `read_entry(dir, log_id, offset, len)` method |
| `UringLedgerIo` | Replace `Mutex<IoUring>` with ring thread + channel + LRU fd cache |
| `StdEngine` (StdLedgerIo) | Implement `read_entry` as open+seek+read (same as today) |
| `DbLedgerStorage::read_entry_from_log` | Call `read_entry` instead of opening file + calling `read_at` |
| `GarbageCollector` | Notify engine to evict fd before deleting entry log file |

## What does NOT change

- `LedgerIo::write_batch` — still used for flush, still takes raw fd
- `LedgerIo::read_at` — still exists for backward compat, used by write path
- Journal ring — completely independent, unaffected
- On-disk format — unchanged
- StdEngine behavior — unchanged (just adds a trivial `read_entry` impl)

## Error handling

| Failure | Behavior |
|---|---|
| Channel full (backpressure) | Caller blocks on `channel.send()` until space. Bounded at 256. |
| Ring thread panics | Channel disconnects, all callers get `BrokenPipe` error |
| READ CQE error | `reply_tx.send(Err(...))` — caller sees io::Error |
| Cache miss + open fails | `reply_tx.send(Err(...))` — same as today's File::open failure |
| GC deletes a cached file | Explicit `evict(log_id)` call before delete |

## Performance estimate

| Metric | Current | With concurrent reads |
|---|---|---|
| Syscalls per read (cache miss) | 3 (open + read + close) | 0 extra (fd cached, READ via ring) |
| io_uring_enter per N reads | N | 1 |
| Max concurrent in-flight reads | 1 (Mutex) | 256 (SQ depth) |
| fd resolution per SQE | kernel fd-table lookup | zero (Fixed index) |

## Dependencies

- `crossbeam-channel` for bounded MPMC channel (or `std::sync::mpsc`)
- `oneshot` crate for reply channels (or `tokio::sync::oneshot` if available on blocking threads)

## Testing

- StdEngine: existing tests pass unchanged (read_entry is a trivial wrapper)
- UringEngine: Linux-only test that spawns N threads, each reading a
  different entry, all through the ring thread. Verify all N reads
  return correct data. Verify via metrics/logs that batching occurred.
- fd cache: test that reading the same log_id twice doesn't open the
  file twice (check open count via a mock or by counting cache hits).
