# Fully Async io_uring Architecture Design

## Overview

Replace all blocking I/O in the Rust BookKeeper bookie with async io_uring operations — both disk (journal writes, ledger reads/writes) and network (TCP accept/recv/send). Keep tokio as the async task scheduler; io_uring handles the actual syscalls.

**Scope:** Full end-state architecture designed upfront, implemented in three phases. Each phase ships independently and delivers standalone value.

**Runtime model:** Tokio multi-threaded runtime for task scheduling and work-stealing. Dedicated OS threads own io_uring rings. Tokio tasks communicate with ring threads via `tokio::sync::mpsc` (requests) and `tokio::sync::oneshot` (completions).

**Fallback:** Non-Linux platforms (and Linux without io_uring) use `StdEngine` with `spawn_blocking` for disk I/O and tokio's epoll-based networking. Selection happens at startup via `uring_available()` probe + `BOOKKEEPER_IO_ENGINE` env var override.

---

## Phase 1: Async Storage Traits + Journal ACK

**Goal:** Make all storage I/O non-blocking from tokio's perspective. Remove every `spawn_blocking` in the hot path.

### Trait changes

All I/O traits become async via `#[async_trait]`:

```rust
#[async_trait]
pub trait JournalIo: Send + Sync {
    async fn write_and_sync(&self, fd: RawFd, entries: &[&[u8]]) -> io::Result<()>;
}

#[async_trait]
pub trait LedgerIo: Send + Sync {
    async fn write_batch(&self, fd: RawFd, offset: u64, entries: &[&[u8]]) -> io::Result<Vec<u64>>;
    async fn read_at(&self, fd: RawFd, offset: u64, len: usize) -> io::Result<Vec<u8>>;
    async fn read_entry(&self, entry_log_dir: &Path, log_id: u32, offset: u64, len: usize) -> io::Result<Vec<u8>>;
    fn evict_fd(&self, log_id: u32);
}
```

`evict_fd` stays synchronous — it's a cache operation, not I/O.

### StdEngine implementation

Wraps blocking calls in `tokio::task::spawn_blocking` internally:

```rust
#[async_trait]
impl JournalIo for StdEngine {
    async fn write_and_sync(&self, fd: RawFd, entries: &[&[u8]]) -> io::Result<()> {
        let owned: Vec<Vec<u8>> = entries.iter().map(|e| e.to_vec()).collect();
        tokio::task::spawn_blocking(move || {
            with_file(fd, |file| {
                for entry in &owned {
                    file.write_all(entry)?;
                }
                file.sync_data()
            })
        }).await.unwrap()
    }
}
```

Same pattern for `LedgerIo` methods. The data copy (`entries` → `Vec<Vec<u8>>`) is necessary to satisfy the `'static` bound on `spawn_blocking`. For StdEngine this is acceptable — the copy overhead is dwarfed by the syscall.

### UringJournalIo / UringLedgerIo implementation

Ring threads are unchanged. The async boundary moves from the caller doing `spawn_blocking` to the io struct sending a request via channel and awaiting a oneshot reply:

```rust
#[async_trait]
impl JournalIo for UringJournalIo {
    async fn write_and_sync(&self, fd: RawFd, entries: &[&[u8]]) -> io::Result<()> {
        let owned: Vec<Vec<u8>> = entries.iter().map(|e| e.to_vec()).collect();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender.send(JournalRequest::WriteSync { fd, data: owned, reply: tx })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "journal ring gone"))?;
        rx.await.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "journal ring dropped"))?
    }
}
```

### Journal ACK mechanism

Replace `std::sync::mpsc` blocking ACK with `tokio::sync::oneshot`:

```rust
// Journal add_entry becomes async:
pub async fn add_entry(&self, entry: JournalEntry) -> Result<()> {
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    self.sender.send((entry, ack_tx))?;
    ack_rx.await.map_err(|_| anyhow!("journal thread gone"))??;
    Ok(())
}
```

The journal-io ring thread sends the ACK after WRITEV+FSYNC completes. This means the bookie server's write handler can `add_entry(...).await` without blocking a tokio worker thread.

### DbLedgerStorage

All public methods that do I/O become async: `read_entry`, `flush`, `checkpoint`. The `LedgerStorage` trait (used by the server) becomes async accordingly.

### Performance note

`#[async_trait]` adds one heap allocation per call (boxed future, ~40ns). For journal writes where FSYNC is 50-200us, this is <0.1% overhead. If profiling later shows it matters, individual hot-path methods can be converted to manual `Future` impls.

### What stays the same

- Ring thread architecture (journal-io thread, ledger-io thread) — unchanged internally
- FdCache — unchanged
- Wire protocol / server layer — unchanged (that's Phase 2)
- io_uring SQE/CQE submission logic inside ring threads — unchanged

---

## Phase 2: io_uring Network I/O

**Goal:** Replace tokio's epoll-based TCP accept/recv/send with io_uring operations for better performance at high connection counts.

### Network ring thread

A dedicated OS thread owns an io_uring ring for all network operations:

```
                    tokio tasks
                        |
            +-----------+-----------+
            v           v           v
      UringListener  UringStream  UringStream
            |           |           |
            v           v           v
     +-------------------------------------+
     |   tokio::sync::mpsc  (request ch)   |
     +------------------+------------------+
                        v
              +------------------+
              |  network-io ring |  (OS thread)
              |   ACCEPT / RECV  |
              |   SEND / CLOSE   |
              +------------------+
                        |
                        v oneshot reply
                   tokio task resumes
```

Request types:

```rust
enum NetRequest {
    Accept { listener_fd: RawFd, reply: oneshot::Sender<io::Result<RawFd>> },
    Recv { fd: RawFd, len: usize, reply: oneshot::Sender<io::Result<Vec<u8>>> },
    Send { fd: RawFd, data: Vec<u8>, reply: oneshot::Sender<io::Result<usize>> },
    Close { fd: RawFd },
}
```

Ring thread loop:
1. Drain inbound channel -> build SQEs (tagged with `user_data` for reply routing)
2. `submit_and_wait(1)` with timeout
3. Reap CQEs -> dispatch results via oneshot senders
4. For `ACCEPT`: immediately resubmit another ACCEPT SQE (keep-accepting)

### Async adapters

Thin wrappers replacing `tokio::net::TcpListener` / `TcpStream`:

```rust
pub struct UringTcpListener {
    fd: RawFd,
    sender: mpsc::Sender<NetRequest>,
}

impl UringTcpListener {
    pub async fn accept(&self) -> io::Result<UringTcpStream> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(NetRequest::Accept { listener_fd: self.fd, reply: tx }).await?;
        let client_fd = rx.await??;
        Ok(UringTcpStream::new(client_fd, self.sender.clone()))
    }
}

pub struct UringTcpStream {
    fd: RawFd,
    sender: mpsc::Sender<NetRequest>,
}

impl UringTcpStream {
    pub async fn read(&self, len: usize) -> io::Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(NetRequest::Recv { fd: self.fd, len, reply: tx }).await?;
        rx.await?
    }
    pub async fn write(&self, data: Vec<u8>) -> io::Result<usize> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(NetRequest::Send { fd: self.fd, data, reply: tx }).await?;
        rx.await?
    }
}
```

### Server integration

Minimal change to the accept loop:

```rust
// Before (tokio):
let listener = TcpListener::bind(addr).await?;
loop {
    let (stream, _) = listener.accept().await?;
    tokio::spawn(handle_connection(stream));
}

// After (uring):
let listener = UringTcpListener::bind(addr, &net_ring_sender)?;
loop {
    let stream = listener.accept().await?;
    tokio::spawn(handle_connection(stream));
}
```

`handle_connection` uses `stream.read()`/`stream.write()` — same signatures, different backend.

### TLS compatibility

`UringTcpStream` implements `tokio::io::AsyncRead + AsyncWrite` so rustls can wrap it transparently. The `poll_read`/`poll_write` implementations internally poll the oneshot receiver.

### Multishot RECV optimization

io_uring's multishot RECV resubmits automatically after each completion, avoiding per-read SQE resubmission:

```rust
opcode::RecvMulti::new(fd, buf_group_id)
    .build()
    .flags(Flags::BUFFER_SELECT)
```

Requires a provided buffer ring (`IORING_REGISTER_PBUF_RING`) set up at ring init. This is an optimization within Phase 2.

### Fallback

On non-Linux or when io_uring is unavailable, the server uses `tokio::net` directly. Selection at startup:

```rust
let net_ring = if uring_available() {
    Some(NetworkRing::start()?)
} else {
    None
};
```

Server code abstracts over both via an enum or trait object.

---

## Phase 3: Unified Ring Architecture

**Goal:** Consolidate separate ring threads into fewer unified rings for better resource utilization, fewer context switches, and cross-domain I/O linking.

### Ring topology

```
  Core 0              Core 1              Core 2              Core 3
+-----------+    +-----------+    +-----------+    +-----------+
| journal   |    | general   |    | general   |    | general   |
| ring      |    | ring      |    | ring      |    | ring      |
|           |    |           |    |           |    |           |
| - WRITEV  |    | - RECV    |    | - RECV    |    | - RECV    |
| - FSYNC   |    | - SEND    |    | - SEND    |    | - SEND    |
|           |    | - READ    |    | - READ    |    | - READ    |
|           |    | - WRITEV  |    | - WRITEV  |    | - WRITEV  |
|           |    |  (ledger) |    |  (ledger) |    |  (ledger) |
+-----------+    +-----------+    +-----------+    +-----------+
```

**Journal stays dedicated.** FSYNC latency is the single most important metric for write throughput. Mixing journal SQEs with read/network traffic would add head-of-line blocking. The journal ring runs on a pinned core.

**General rings** handle both ledger I/O and network I/O. Each socket and ledger fd is assigned to a specific ring via hash (fd affinity).

### Fd affinity routing

```rust
pub struct RingPool {
    journal_ring: Sender<JournalRequest>,
    general_rings: Vec<Sender<GeneralRequest>>,
}

impl RingPool {
    fn route(&self, fd: RawFd) -> &Sender<GeneralRequest> {
        let idx = (fd as usize) % self.general_rings.len();
        &self.general_rings[idx]
    }

    pub fn submit_read(&self, fd: RawFd, ...) { self.route(fd).send(...) }
    pub fn submit_send(&self, fd: RawFd, ...) { self.route(fd).send(...) }
    pub fn submit_journal(&self, ...) { self.journal_ring.send(...) }
}
```

Benefits of fd affinity:
- No contention — a given fd is always processed by the same ring
- Connection locality — all RECVs/SENDs for a client stay on one core
- Read locality — ledger reads for a given entry log stay on one core
- Cache-friendly — data for a connection stays in one core's L1/L2

### Unified request type

```rust
enum GeneralRequest {
    // Network
    Accept { listener_fd: RawFd, reply: oneshot::Sender<io::Result<RawFd>> },
    Recv { fd: RawFd, len: usize, reply: oneshot::Sender<io::Result<Vec<u8>>> },
    Send { fd: RawFd, data: Vec<u8>, reply: oneshot::Sender<io::Result<usize>> },
    // Ledger
    ReadAt { fd: RawFd, offset: u64, len: usize, reply: oneshot::Sender<io::Result<Vec<u8>>> },
    WriteBatch { fd: RawFd, offset: u64, data: Vec<Vec<u8>>, reply: oneshot::Sender<io::Result<Vec<u64>>> },
    // Lifecycle
    Close { fd: RawFd },
}
```

### Cross-domain optimization: read-respond linking

With disk and network on the same ring, a ledger read request can chain READ -> SEND via `IO_LINK`:

```rust
let read_sqe = opcode::Read::new(ledger_fd, buf, len)
    .offset(offset)
    .build()
    .flags(Flags::IO_LINK);  // link to next SQE
let send_sqe = opcode::Send::new(socket_fd, buf, len)
    .build();
// SEND executes only after READ completes — no round-trip through channel
```

This eliminates one channel hop and one context switch per read-serve operation. This optimization is impossible when reads and network are on separate rings.

### Journal SQPOLL

The journal ring uses `IORING_SETUP_SQPOLL` — a kernel thread polls the submission queue, eliminating the `io_uring_enter` syscall:

```rust
let ring = IoUring::builder()
    .setup_sqpoll(2000)  // 2ms idle before kernel thread sleeps
    .build(256)?;
```

Saves ~1-2us per journal write. Only applied to journal because SQPOLL burns CPU when active.

### Registered resources

All rings register fd sets and buffer pools at init:

```rust
// Fixed file table — avoids per-op fd lookup in kernel
ring.submitter().register_files(&fds)?;

// Provided buffer ring — kernel picks buffers for RECV
ring.submitter().register_buf_ring(buf_ring, bgid)?;
```

Fixed fds are critical for SQPOLL (avoids fd table locking). Provided buffers eliminate the "allocate before RECV" round-trip.

### Scaling

- **Default:** 1 journal ring + (num_cores - 1) general rings
- **Small deployments (2 cores or fewer):** 1 journal ring + 1 general ring
- **Config override:** `ring_thread_count` in `ServerConfig`

### Fallback

`RingPool` has a `StdPool` implementation for non-Linux: delegates disk I/O to `spawn_blocking`, network to tokio TCP. Same interface via trait object or enum dispatch.

---

## Phase Summary

| Phase | Scope | Key Deliverable | Independent Value |
|-------|-------|-----------------|-------------------|
| 1 | Async storage traits + oneshot ACK | Tokio workers never block on I/O | Removes spawn_blocking overhead, enables async callers |
| 2 | Network io_uring | TCP via ACCEPT/RECV/SEND SQEs | Better performance at high connection counts (10K+) |
| 3 | Unified rings + fd affinity | Single ring per core for disk+network | Cross-domain IO_LINK, fewer threads, cache locality |

## Dependencies

- `io-uring` crate v0.7 (already in use)
- `async-trait` crate (already in use)
- `tokio` with `sync` feature (already in use)
- No new external dependencies required

## Testing Strategy

- **Phase 1:** Existing integration tests run with async traits. Add benchmarks comparing oneshot ACK latency vs blocking ACK.
- **Phase 2:** Network adapter unit tests (mock ring). Integration tests with real io_uring on Linux (docker-compose-test.yml).
- **Phase 3:** Benchmark suite comparing separate rings vs unified rings. Stress tests at 10K+ connections.
- **All phases:** `docker-compose-test.yml` runs the full suite with `BOOKKEEPER_IO_ENGINE=uring` on Linux.

## Non-Goals

- Replacing tokio's async runtime (we keep tokio for task scheduling)
- `io_uring` for filesystem metadata operations (mkdir, stat, rename) — these are infrequent and fine with std
- Supporting `io_uring` on kernels older than 5.11 (minimum for linked SQEs and multishot)
- Thread-per-core architecture (we keep tokio's work-stealing model)
