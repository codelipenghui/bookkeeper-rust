use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

use bookkeeper_protocol::digest::{DigestManager, DigestType};
use bookkeeper_protocol::entry::EntryBuilder;
use bookkeeper_storage::db_ledger_storage::{DbLedgerStorage, CURRENT_DIR};
use bookkeeper_storage::io_engine::{select_engine, LedgerIo};
use bookkeeper_storage::journal::writer::JournalConfig;
use bookkeeper_storage::ledger_storage::LedgerStorage;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn best_engine() -> (Arc<dyn LedgerIo>, Arc<dyn bookkeeper_storage::io_engine::JournalIo>) {
    let engines = select_engine();
    (engines.ledger, engines.journal)
}

fn random_payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i & 0xFF) as u8).collect()
}

fn build_entry(size: usize, ledger_id: i64, entry_id: i64) -> Vec<u8> {
    let dm = DigestManager::new(DigestType::Crc32c);
    let payload = random_payload(size);
    EntryBuilder::new(&dm)
        .ledger_id(ledger_id)
        .entry_id(entry_id)
        .lac(entry_id.saturating_sub(1))
        .length(size as i64)
        .payload(bytes::Bytes::copy_from_slice(&payload))
        .build()
        .to_vec()
}

/// Open storage WITH journal — matches the real bookie write path.
fn open_storage_with_journal(dir: &Path) -> DbLedgerStorage {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (ledger_io, journal_io) = best_engine();
    let mut storage = DbLedgerStorage::open(dir, ledger_io).unwrap();

    let journal_dir = dir.join(CURRENT_DIR).join("journal");
    std::fs::create_dir_all(&journal_dir).unwrap();
    let config = JournalConfig {
        journal_dir: journal_dir.clone(),
        max_file_size: 128 * 1024 * 1024,
        sync_data: true,
        max_group_wait_ms: 1,
        buffered_writes_threshold: 512 * 1024, // 512KB, matches Java default
        buffered_entries_threshold: 0,
        alignment_size: 0,
        format_version: 6,
    };
    rt.block_on(storage.init_journal(&journal_dir, config, journal_io, None))
        .unwrap();
    storage
}

/// Open storage WITH journal but WITHOUT fsync (journalSyncData=false).
fn open_storage_journal_no_sync(dir: &Path) -> DbLedgerStorage {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (ledger_io, journal_io) = best_engine();
    let mut storage = DbLedgerStorage::open(dir, ledger_io).unwrap();

    let journal_dir = dir.join(CURRENT_DIR).join("journal");
    std::fs::create_dir_all(&journal_dir).unwrap();
    let config = JournalConfig {
        journal_dir: journal_dir.clone(),
        max_file_size: 128 * 1024 * 1024,
        sync_data: false,
        max_group_wait_ms: 1,
        buffered_writes_threshold: 512 * 1024,
        buffered_entries_threshold: 0,
        alignment_size: 0,
        format_version: 6,
    };
    rt.block_on(storage.init_journal(&journal_dir, config, journal_io, None))
        .unwrap();
    storage
}

/// Open storage WITHOUT journal — pure write-cache performance.
fn open_storage_no_journal(dir: &Path) -> DbLedgerStorage {
    let (ledger_io, _) = best_engine();
    DbLedgerStorage::open(dir, ledger_io).unwrap()
}

// ---------------------------------------------------------------------------
// add_entry WITHOUT journal (write cache only — in-memory baseline)
// ---------------------------------------------------------------------------

fn bench_add_entry_no_journal(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("add_entry_no_journal");

    for &size in &[128, 1024, 4096] {
        let dir = TempDir::new().unwrap();
        let storage = open_storage_no_journal(dir.path());
        storage.validate_for_write(1, &[0u8; 20]).unwrap();

        group.throughput(Throughput::Bytes(size as u64));
        let mut eid = 0i64;
        group.bench_with_input(BenchmarkId::new("cache_only", size), &size, |b, _| {
            b.iter(|| {
                let entry = build_entry(size, 1, eid);
                rt.block_on(storage.add_entry(&entry)).unwrap();
                eid += 1;
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// add_entry WITH journal (write cache + journal write + fsync)
// This is the real-world hot path. Measures end-to-end write latency.
// ---------------------------------------------------------------------------

fn bench_add_entry_with_journal(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("add_entry_journaled");
    group.sample_size(50); // fewer samples — fsync is slow

    for &size in &[128, 1024, 4096] {
        let dir = TempDir::new().unwrap();
        let storage = open_storage_with_journal(dir.path());
        storage.validate_for_write(1, &[0u8; 20]).unwrap();

        group.throughput(Throughput::Bytes(size as u64));
        let mut eid = 0i64;
        group.bench_with_input(BenchmarkId::new("journaled", size), &size, |b, _| {
            b.iter(|| {
                let entry = build_entry(size, 1, eid);
                rt.block_on(storage.add_entry(&entry)).unwrap();
                // Journal the entry — this is the fsync-bound step
                let journal = storage.journal().unwrap();
                rt.block_on(journal.log_add_entry(&entry)).unwrap();
                eid += 1;
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// get_entry benchmark (write cache hit — no disk I/O)
// ---------------------------------------------------------------------------

fn bench_get_entry_cached(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("get_entry_cached");

    for &size in &[128, 1024, 4096] {
        let dir = TempDir::new().unwrap();
        let storage = open_storage_no_journal(dir.path());
        storage.validate_for_write(1, &[0u8; 20]).unwrap();

        for i in 0..100 {
            let entry = build_entry(size, 1, i);
            rt.block_on(storage.add_entry(&entry)).unwrap();
        }

        group.throughput(Throughput::Bytes(size as u64));
        let mut read_id = 0i64;
        group.bench_with_input(BenchmarkId::new("write_cache", size), &size, |b, _| {
            b.iter(|| {
                let _ = rt.block_on(storage.get_entry(1, read_id % 100));
                read_id += 1;
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// get_entry benchmark (from entry log after flush — exercises LedgerIo)
// ---------------------------------------------------------------------------

fn bench_get_entry_disk(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("get_entry_flushed");

    for &size in &[1024, 4096] {
        let dir = TempDir::new().unwrap();
        let storage = open_storage_no_journal(dir.path());
        storage.validate_for_write(1, &[0u8; 20]).unwrap();

        // Write 1000 entries and flush to entry log
        for i in 0..1000 {
            let entry = build_entry(size, 1, i);
            rt.block_on(storage.add_entry(&entry)).unwrap();
        }
        rt.block_on(storage.flush()).unwrap();

        // Entries are now in entry log + read cache.
        // This measures read_cache hit + RocksDB index lookup.
        group.throughput(Throughput::Bytes(size as u64));
        let mut read_id = 0i64;
        group.bench_with_input(BenchmarkId::new("entry_log", size), &size, |b, _| {
            b.iter(|| {
                let _ = rt.block_on(storage.get_entry(1, read_id % 1000));
                read_id += 1;
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// add_entry WITH journal but WITHOUT fsync (journalSyncData=false)
// Data reaches OS page cache but not forced to disk.
// ---------------------------------------------------------------------------

fn bench_add_entry_journal_no_sync(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("add_entry_journal_no_sync");

    for &size in &[128, 1024, 4096] {
        let dir = TempDir::new().unwrap();
        let storage = open_storage_journal_no_sync(dir.path());
        storage.validate_for_write(1, &[0u8; 20]).unwrap();

        group.throughput(Throughput::Bytes(size as u64));
        let mut eid = 0i64;
        group.bench_with_input(BenchmarkId::new("no_sync", size), &size, |b, _| {
            b.iter(|| {
                let entry = build_entry(size, 1, eid);
                rt.block_on(storage.add_entry(&entry)).unwrap();
                let journal = storage.journal().unwrap();
                rt.block_on(journal.log_add_entry(&entry)).unwrap();
                eid += 1;
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Concurrent journaled writes — measures per-entry latency distribution.
// N tokio tasks write simultaneously; each entry's latency is measured
// from send to journal ACK. Reports P50/P99/P999/max.
// ---------------------------------------------------------------------------

fn run_concurrent_bench(
    group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
    label: &str,
    storage: Arc<DbLedgerStorage>,
    concurrency: usize,
) {
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::Instant;

    let entries_per_writer = 100;
    let total_entries = concurrency * entries_per_writer;
    group.throughput(Throughput::Elements(total_entries as u64));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let eid = Arc::new(AtomicI64::new(0));
    let last_latencies: Arc<std::sync::Mutex<Vec<std::time::Duration>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let last_lats = last_latencies.clone();
    group.bench_function(format!("{label}/{concurrency}writers"), |b| {
        b.iter(|| {
            let latencies = rt.block_on(async {
                let latencies = Arc::new(std::sync::Mutex::new(Vec::with_capacity(total_entries)));
                let mut handles = Vec::with_capacity(concurrency);
                for _ in 0..concurrency {
                    let storage = storage.clone();
                    let eid = eid.clone();
                    let latencies = latencies.clone();
                    handles.push(tokio::spawn(async move {
                        for _ in 0..entries_per_writer {
                            let id = eid.fetch_add(1, Ordering::Relaxed);
                            let entry = build_entry(1024, 1, id);
                            let start = Instant::now();
                            storage.add_entry(&entry).await.unwrap();
                            let journal = storage.journal().unwrap();
                            journal.log_add_entry(&entry).await.unwrap();
                            let elapsed = start.elapsed();
                            latencies.lock().unwrap().push(elapsed);
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
                let mut lats = latencies.lock().unwrap().clone();
                lats.sort();
                lats
            });
            *last_lats.lock().unwrap() = latencies;
        });
    });
    let lats = last_latencies.lock().unwrap();
    if !lats.is_empty() {
        let len = lats.len();
        let p50 = lats[len * 50 / 100];
        let p99 = lats[len * 99 / 100];
        let p999 = lats[len.saturating_sub(1).min((len as f64 * 0.999) as usize)];
        let max = lats[len - 1];
        eprintln!(
            "  [{label} {concurrency}w x {entries_per_writer}e] \
             P50={p50:?} P99={p99:?} P999={p999:?} max={max:?}"
        );
    }
}

fn bench_concurrent_journaled_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("journal_group_commit");
    group.sample_size(10);

    for &concurrency in &[1, 10, 50] {
        // Journal + sync
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(open_storage_with_journal(dir.path()));
        storage.validate_for_write(1, &[0u8; 20]).unwrap();
        run_concurrent_bench(&mut group, "sync", storage, concurrency);

        // Journal without sync
        let dir2 = TempDir::new().unwrap();
        let storage2 = Arc::new(open_storage_journal_no_sync(dir2.path()));
        storage2.validate_for_write(1, &[0u8; 20]).unwrap();
        run_concurrent_bench(&mut group, "no_sync", storage2, concurrency);
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// flush benchmark (write cache → entry log + RocksDB index)
// ---------------------------------------------------------------------------

fn bench_flush(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("flush");

    let dir = TempDir::new().unwrap();
    let storage = open_storage_no_journal(dir.path());
    storage.validate_for_write(1, &[0u8; 20]).unwrap();

    let mut eid = 0i64;
    group.bench_function("1000_entries_1kb", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let entry = build_entry(1024, 1, eid);
                rt.block_on(storage.add_entry(&entry)).unwrap();
                eid += 1;
            }
            rt.block_on(storage.flush()).unwrap();
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// RocksDB index operations (pure index, no storage layer)
// ---------------------------------------------------------------------------

fn bench_rocksdb_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("rocksdb_index");

    let dir = TempDir::new().unwrap();
    let index = bookkeeper_storage::index::entry_location::EntryLocationIndex::open(
        &dir.path().join("locations"),
        None,
    )
    .unwrap();

    let batch: Vec<(i64, i64, u64)> = (0..10000)
        .map(|i| (1i64, i as i64, ((i as u64) << 32) | 1024))
        .collect();
    index.put_batch(&batch).unwrap();
    index.flush().unwrap();

    group.bench_function("put_single", |b| {
        let mut eid = 10000i64;
        b.iter(|| {
            index.put(1, eid, (eid as u64) << 32 | 1024).unwrap();
            eid += 1;
        });
    });

    group.bench_function("get_single", |b| {
        let mut eid = 0i64;
        b.iter(|| {
            let _ = index.get(1, eid % 10000);
            eid += 1;
        });
    });

    group.bench_function("put_batch_100", |b| {
        let mut base = 20000i64;
        b.iter(|| {
            let batch: Vec<(i64, i64, u64)> = (0..100)
                .map(|i| (1i64, base + i as i64, ((base as u64 + i) << 32) | 1024))
                .collect();
            index.put_batch(&batch).unwrap();
            base += 100;
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_add_entry_no_journal,
    bench_add_entry_with_journal,
    bench_add_entry_journal_no_sync,
    bench_concurrent_journaled_writes,
    bench_get_entry_cached,
    bench_get_entry_disk,
    bench_flush,
    bench_rocksdb_index,
);
criterion_main!(benches);
