# Unified Entry Cache — Design

## Context

`DbLedgerStorage` currently holds three independent caches on the read path:

1. **`write_cache: RwLock<WriteCache>`** — 256 MB bump-allocator `Vec<u8>` with a `DashMap<(i64,i64), (usize,usize)>` index. Receives writes.
2. **`write_cache_being_flushed: RwLock<WriteCache>`** — same type; the staging buffer `std::mem::swap`'d in during `flush()` so new adds never block behind the flush worker.
3. **`read_cache: Arc<ReadCache>`** — 64 MB ring of 4 × 16 MB segments, each with its own `DashMap` index. Populated by reads that missed the write caches and came from disk.

Every `get_entry` call hashes the `(ledger_id, entry_id)` tuple up to **six times** in the worst case: once for the active write cache, once for the flushing cache, and up to four times while scanning each `ReadCache` segment from newest back. Flamegraph analysis from the preceding catch-up-read perf session (commit `07b55eb0 "storage+server: catch-up read perf fixes ..."`) puts `DefaultHasher::write_u64` + `DashMap::find_inner` + scheduler-glue `wrapping_add` collectively around 14 % of on-CPU time at 5,600 reads/s/core.

## Goal

**One hash, one bucket probe, one shard-lock acquire per `get_entry` call**, across all caches, in every workload — not just catch-up-read. This is the B1 option from the brainstorming session: full unification, preserves the O(1) flush swap, no wire-format or public-API change.

Non-goals (out of scope for this spec):
- Switching the hasher (ahash / FxHash) — tracked as a separate follow-up.
- Changing cache sizes or eviction policies.
- Zero-copy `Bytes` returns from the cache (would let us skip the per-`get` `memcpy`).
- Moving flushed entries into the read cache instead of dropping them (opportunistic warm-up).

## Architecture

Replace the three cache fields with one `UnifiedCache`:

```
┌──────────────────────────────────────────────────────────────┐
│ UnifiedCache                                                 │
├──────────────────────────────────────────────────────────────┤
│  index: DashMap<(i64, i64), CacheEntry>    ← single index   │
│  active_write: AtomicU8                    ← 0 or 1         │
│                                                              │
│  write_bufs: [Arc<WriteBuffer>; 2]  (256 MB each)           │
│    • one currently "active"  (receiving writes)             │
│    • the other "flushing" or reset                          │
│                                                              │
│  read_segments: [Arc<ReadSegment>; 4]  (16 MB each)         │
│  current_segment: AtomicUsize                                │
│                                                              │
│  last_entry: DashMap<i64, i64>    ← ledger_id → max entry   │
└──────────────────────────────────────────────────────────────┘
```

Each of the 6 storage regions is a fixed-capacity, never-moved `Vec<u8>` with its own bump-allocator offset. What moves is the **role** each buffer plays, which is a single `AtomicU8` for the write side and `AtomicUsize` for the read-cache ring.

Reader flow: hash `(ledger,entry)` once → DashMap lookup → dispatch on the region tag stored in the `CacheEntry` value → copy bytes from that region's `Vec<u8>`.

## Data Structures

```rust
/// One entry's location in the unified cache.
/// Packed to 9 bytes (or 12 with `repr(C)` padding — a 1.33× reduction vs the
/// `(usize, usize)` = 16-byte values used today).
#[derive(Clone, Copy)]
pub struct CacheEntry {
    /// Packed: top bit clear (0x00–0x7F) = WriteBuffer, top bit set (0x80–0xFF) = ReadSegment.
    /// Low 3 bits = index within the relevant array (0..=1 for write, 0..=3 for read).
    pub region_tag: u8,
    pub offset: u32,
    pub size: u32,
}

/// Fixed-capacity append-only buffer. Never moves once constructed.
struct WriteBuffer {
    data: UnsafeCell<Vec<u8>>,          // allocated once
    offset: AtomicUsize,                // bump allocator head
    max_size: usize,                    // aligned to 64B
    // Per-region tracking list for enumeration during drain. Held briefly
    // on append and drain; never across I/O.
    entries: parking_lot::Mutex<Vec<(i64, i64)>>,
}

struct ReadSegment { /* same shape as WriteBuffer; 16 MB */ }

pub struct UnifiedCache {
    index: DashMap<(i64, i64), CacheEntry>,

    write_bufs: [Arc<WriteBuffer>; 2],
    active_write: AtomicU8,              // 0 or 1

    read_segments: [Arc<ReadSegment>; READ_CACHE_SEGMENTS],
    current_segment: AtomicUsize,

    last_entry: DashMap<i64, i64>,       // ledger_id → max entry_id
}
```

`DbLedgerStorage` collapses its three cache fields into:

```rust
pub struct DbLedgerStorage {
    // ...
    unified_cache: Arc<UnifiedCache>,    // replaces write_cache,
                                         //          write_cache_being_flushed,
                                         //          read_cache
    flush_mutex: tokio::sync::Mutex<()>, // unchanged
    // ...
}
```

## Core Operations

### `get(l, e) -> Option<Vec<u8>>`

```rust
// MUST do the copy while holding the DashMap Ref — see Concurrency Invariants.
self.index.get(&(l, e)).map(|entry| {
    let (region, offset, size) = entry.value().unpack();
    let buf: &[u8] = match region {
        Region::Write(i)       => unsafe { &(*self.write_bufs[i].data.get()) },
        Region::ReadSegment(i) => unsafe { &(*self.read_segments[i].data.get()) },
    };
    buf[offset..offset + size].to_vec()
})
```

One hash, one bucket probe.

### `put_write(l, e, data)` — new write from `add_entry`

Return type stays `Result<(), StorageError>` to match the existing
`WriteCache::put` signature (caller code does not change). `StorageError::StorageFull`
remains the "buffer full — flush and retry" signal, same as today.

```rust
pub fn put_write(&self, l: i64, e: i64, data: &[u8]) -> Result<(), StorageError> {
    let i = self.active_write.load(Ordering::Acquire) as usize;
    let buf = &self.write_bufs[i];
    let aligned = align64(data.len());               // helper preserved from WriteCache
    // CAS loop: reserve a non-overlapping range or bail if the buffer is full.
    let offset = loop {
        let cur = buf.offset.load(Ordering::SeqCst);
        if cur + aligned > buf.max_size {
            return Err(StorageError::StorageFull);
        }
        if buf.offset.compare_exchange(cur, cur + aligned,
                                       Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            break cur;
        }
    };
    unsafe { (*buf.data.get())[offset..offset + data.len()].copy_from_slice(data); }
    buf.entries.lock().push((l, e));
    self.index.insert((l, e), CacheEntry::write(i as u8, offset as u32, data.len() as u32));
    self.last_entry.entry(l).and_modify(|v| if e > *v { *v = e; }).or_insert(e);
    Ok(())
}
```

### `put_read(l, e, data)` — populate from disk read

```rust
// or_insert_with: if a concurrent writer beat us, DO NOT overwrite their newer version.
self.index.entry((l, e)).or_insert_with(|| {
    let (seg_idx, offset) = self.read_ring_append(data);
    CacheEntry::read(seg_idx as u8, offset as u32, data.len() as u32)
});
```

### `start_flush() -> FlushGuard` (under `flush_mutex`)

```rust
let old = self.active_write.load(Ordering::Acquire);
self.active_write.store(old ^ 1, Ordering::Release);
FlushGuard { flushing_idx: old }
```

O(1). New writes now go to the other buffer; entries still in the old buffer remain indexed and readable.

### `drain(guard)` — flush worker walks the flushing buffer

```rust
let buf = &self.write_bufs[guard.flushing_idx as usize];
let to_flush: Vec<(i64, i64)> = buf.entries.lock().drain(..).collect();
for (l, e) in &to_flush {
    // 1. Read the entry's bytes (using the index lookup, holding Ref for the copy).
    // 2. Write to entry log + RocksDB location index.
    // 3. Remove from the unified index.
    self.index.remove(&(*l, *e));    // <-- BEFORE offset reset
}
buf.offset.store(0, Ordering::Release); // buffer now reusable
```

### `read_ring_append` — ring rotation inside `put_read`

```rust
let mut cur = self.current_segment.load(Ordering::Acquire);
loop {
    let seg = &self.read_segments[cur];
    let offset = seg.offset.fetch_add(align64(data.len()), Ordering::AcqRel);
    if offset + data.len() <= seg.max_size {
        unsafe { (*seg.data.get())[offset..offset+data.len()].copy_from_slice(data); }
        seg.entries.lock().push((l, e));
        return (cur, offset);
    }
    // Segment full: CAS to rotate. The winning thread evicts the next segment.
    let next = (cur + 1) % READ_CACHE_SEGMENTS;
    if self.current_segment.compare_exchange(cur, next, Ordering::AcqRel, Ordering::Acquire).is_ok() {
        let evicted = self.read_segments[next].entries.lock().drain(..).collect::<Vec<_>>();
        for (l, e) in &evicted {
            self.index.remove(&(*l, *e));     // <-- BEFORE offset reset
        }
        self.read_segments[next].offset.store(0, Ordering::Release);
    }
    cur = self.current_segment.load(Ordering::Acquire);
}
```

## Concurrency Invariants

Three rules the implementation **must** enforce — each testable.

### I1. Read copies bytes under the DashMap `Ref`

The unsafe byte copy in `get()` runs inside the closure passed to `DashMap::get(..).map(...)`. This holds the shard's read lock for the copy's duration, blocking concurrent `index.remove()` calls from drain/rotation. Releasing the `Ref` before the copy would let drain run, reset the region offset, and let a new writer overwrite the bytes mid-copy.

**Test:** `get_holds_ref_across_data_copy` — spawn a reader in a tight loop; start a drain that would reset the buffer. Assert reader never observes torn bytes (e.g., by embedding a ledger_id in the payload and checking every read).

### I2. `put_read` uses `entry(..).or_insert_with(..)`, never `insert`

A reader that missed the cache goes to disk, comes back, and wants to populate the read cache. Meanwhile a writer wrote the same `(l, e)` into the active write buffer. If `put_read` used plain `insert`, it would clobber the writer's newer entry with a stale disk-read version pointing into the read segment.

**Test:** `put_read_does_not_clobber_concurrent_write` — race a reader's `put_read` against a writer's `put_write` for the same key; assert the final `CacheEntry` has region = `Write(_)`.

### I3. `index.remove()` precedes `buf.offset.store(0)` during drain and rotation

If we reset the offset first, a reader that has already captured a `(region, offset, size)` tuple from the DashMap would try to copy bytes from a region the writer is already overwriting. The remove-first ordering plus I1 (read under Ref) together ensure no reader can hold stale coordinates into a reset buffer.

**Test:** `drain_removes_from_index_before_offset_reset` — with a `#[cfg(test)]` sync point between `remove` and `store(0)`, have a reader try to look up a flushed key and assert `None` is returned.

## Testing

### Preserved (no change expected to pass)

All `db_ledger_storage_test.rs` tests go through the public `DbLedgerStorage` API and should pass unchanged after the refactor:

- `add_and_get_entry`, `get_missing_entry`, `last_add_confirmed`
- `read_ahead_populates_cache`, `read_ahead_stops_at_ledger_boundary`
- `add_entry_overflow_flush_with_platform_engine`
- `add_entry_rejects_when_throttle_timeout_elapses`
- `fence_ledger`

### New — unit tests against `UnifiedCache`

Under `tests/cache_test.rs` (rewritten):

1. `put_get_round_trip` — basic
2. `write_overflow_triggers_flush_signal` — active WriteBuffer full → `put_write` returns `BufferFull`
3. `swap_preserves_old_readability` — entries written to buffer A remain readable after `start_flush` flips active to buffer B
4. `drain_clears_index_and_offset` — after `drain(guard)`, flushing region's entries are gone from index and offset is 0
5. `read_ring_rotation` — fill 4 segments, 5th insert evicts the oldest segment
6. `CacheEntry_round_trip` — packing/unpacking for all 6 valid region tags
7. `empty_cache_get_returns_none`
8. `put_read_preserves_writer_version` (I2 regression)

### New — concurrency tests

9. `get_holds_ref_across_data_copy` (I1)
10. `put_read_does_not_clobber_concurrent_write` (I2)
11. `drain_removes_from_index_before_offset_reset` (I3)

### Optional — loom

12. `loom_drain_vs_read` under `#[cfg(loom)]` with a 2-entry cache. Explores atomic-op interleavings that stress the ordering of remove/offset-reset. ~50 LOC; catches subtle `Ordering::Relaxed` bugs that integration tests miss.

## Migration

**Big-bang rewrite of the cache modules — no feature flag.** A runtime toggle would force branches on every read/write call, defeating the measurable win, and would double the test matrix. The public `DbLedgerStorage` surface is preserved, so rollback is revert + rebuild.

### Files changed

- **New:** `crates/bookkeeper-storage/src/cache/unified_cache.rs`
- **Delete:** `crates/bookkeeper-storage/src/cache/read_cache.rs`, `crates/bookkeeper-storage/src/cache/write_cache.rs`
- **Modify:** `crates/bookkeeper-storage/src/cache/mod.rs` — re-export `UnifiedCache` only
- **Modify:** `crates/bookkeeper-storage/src/db_ledger_storage.rs` — collapse 3 cache fields into 1; rewrite `get_entry`, `add_entry`, `swap_caches`, `drain_flushing`, and the readahead spawn block
- **Rewrite:** `crates/bookkeeper-storage/tests/cache_test.rs` against `UnifiedCache`
- **No change:** `crates/bookkeeper-storage/tests/db_ledger_storage_test.rs`
- **Wire up:** `crates/bookkeeper-storage/src/storage_metrics.rs` — the existing `bookie_read_cache_*` and `bookie_write_cache_*` gauges (declared but never updated today) get populated from `UnifiedCache`. Free win.

### Implementation sequence

1. Build `UnifiedCache` standalone with its tests (1–12). Doesn't touch `DbLedgerStorage`.
2. Swap `DbLedgerStorage` fields + callers. Single commit with the diff; public-API tests confirm correctness.
3. Delete `read_cache.rs` and `write_cache.rs`.
4. Wire up the previously-stub cache gauges.

Step 1 proves the new code against a test harness before it becomes load-bearing in step 2.

### Rollout gate — bench reproduction

Before merging, rerun the catch-up-read test on the compose stack with backlog ≥ 3 M messages. Acceptance criteria:

| Metric | Target |
|---|---|
| `bookie_BOOKIE_READ_ENTRY_count{success="false"}` | stays at 0 (no new EIO) |
| `bookie_io_uring_sq_full_total` | stays at 0 |
| `bookie_io_uring_buf_pool_exhausted_total` | stays at 0 |
| Pulsar `msgRateOut` | ≥ current baseline |
| Flamegraph DashMap hash+probe cost (self time) | drops from ~14 % to ≲ 5 % |

If any row regresses, revert before merge.

## Open Questions / Deferred

- **Hasher switch (ahash)** — separate 1-line PR, do after this lands so the wins can be measured independently.
- **Zero-copy returns via `Bytes`** — would eliminate the per-`get` `memcpy` (~5–7 % of CPU). Requires reference-counting the storage regions so we can't reset them while a reader still holds a slice. Material refactor; separate spec.
- **Warming the read cache from drained flush entries** — the drain path already has the bytes in memory and knows they're on disk. Moving them to the read cache instead of dropping them would avoid the first-miss-after-flush disk read for each entry. Separate spec.
