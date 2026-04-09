use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
#[allow(unused_imports)]
use std::path::Path;
use tempfile::TempDir;

use bookkeeper_protocol::digest::{DigestManager, DigestType};
use bookkeeper_protocol::entry::EntryBuilder;
use std::sync::Arc;
use bookkeeper_storage::db_ledger_storage::DbLedgerStorage;
use bookkeeper_storage::io_engine::std_engine::StdEngine;
use bookkeeper_storage::ledger_storage::LedgerStorage;

fn random_payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i & 0xFF) as u8).collect()
}

fn build_entry(size: usize, ledger_id: i64, entry_id: i64) -> Vec<u8> {
    let dm = DigestManager::new(DigestType::Crc32c);
    let payload = random_payload(size);
    let entry = EntryBuilder::new(&dm)
        .ledger_id(ledger_id)
        .entry_id(entry_id)
        .lac(entry_id.saturating_sub(1))
        .length(size as i64)
        .payload(bytes::Bytes::copy_from_slice(&payload))
        .build();
    entry.to_vec()
}

// ---------------------------------------------------------------------------
// add_entry benchmark
// ---------------------------------------------------------------------------

fn bench_add_entry(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_add_entry");

    for &size in &[128, 1024, 4096] {
        let dir = TempDir::new().unwrap();
        let storage = DbLedgerStorage::open(dir.path(), Arc::new(StdEngine) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>).unwrap();
        // Pre-create ledger
        storage.validate_for_write(1, &[0u8; 20]).unwrap();

        group.throughput(Throughput::Bytes(size as u64));
        let mut eid = 0i64;
        group.bench_with_input(BenchmarkId::new("add", size), &size, |b, _| {
            b.iter(|| {
                let entry = build_entry(size, 1, eid);
                storage.add_entry(&entry).unwrap();
                eid += 1;
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// get_entry benchmark (cache hit)
// ---------------------------------------------------------------------------

fn bench_get_entry_cached(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_get_entry_cached");

    for &size in &[128, 1024, 4096] {
        let dir = TempDir::new().unwrap();
        let storage = DbLedgerStorage::open(dir.path(), Arc::new(StdEngine) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>).unwrap();
        storage.validate_for_write(1, &[0u8; 20]).unwrap();

        // Write entries into write cache
        for i in 0..100 {
            let entry = build_entry(size, 1, i);
            storage.add_entry(&entry).unwrap();
        }

        group.throughput(Throughput::Bytes(size as u64));
        let mut read_id = 0i64;
        group.bench_with_input(BenchmarkId::new("get_cached", size), &size, |b, _| {
            b.iter(|| {
                let _ = storage.get_entry(1, read_id % 100);
                read_id += 1;
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// get_entry benchmark (from disk after flush)
// ---------------------------------------------------------------------------

fn bench_get_entry_disk(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_get_entry_disk");

    for &size in &[1024, 4096] {
        let dir = TempDir::new().unwrap();
        let storage = DbLedgerStorage::open(dir.path(), Arc::new(StdEngine) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>).unwrap();
        storage.validate_for_write(1, &[0u8; 20]).unwrap();

        // Write and flush to disk
        for i in 0..1000 {
            let entry = build_entry(size, 1, i);
            storage.add_entry(&entry).unwrap();
        }
        storage.flush().unwrap();

        group.throughput(Throughput::Bytes(size as u64));
        let mut read_id = 0i64;
        group.bench_with_input(BenchmarkId::new("get_disk", size), &size, |b, _| {
            b.iter(|| {
                let _ = storage.get_entry(1, read_id % 1000);
                read_id += 1;
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// flush benchmark
// ---------------------------------------------------------------------------

fn bench_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_flush");

    let dir = TempDir::new().unwrap();
    let storage = DbLedgerStorage::open(dir.path(), Arc::new(StdEngine) as Arc<dyn bookkeeper_storage::io_engine::LedgerIo>).unwrap();
    storage.validate_for_write(1, &[0u8; 20]).unwrap();

    let mut eid = 0i64;
    group.bench_function("flush_1000_entries_1kb", |b| {
        b.iter(|| {
            // Fill write cache with 1000 entries
            for _ in 0..1000 {
                let entry = build_entry(1024, 1, eid);
                storage.add_entry(&entry).unwrap();
                eid += 1;
            }
            // Flush to disk
            storage.flush().unwrap();
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// RocksDB index operations
// ---------------------------------------------------------------------------

fn bench_rocksdb_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("rocksdb_index");

    let dir = TempDir::new().unwrap();
    let index = bookkeeper_storage::index::entry_location::EntryLocationIndex::open(
        &dir.path().join("locations"),
    )
    .unwrap();

    // Populate some entries
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
    bench_add_entry,
    bench_get_entry_cached,
    bench_get_entry_disk,
    bench_flush,
    bench_rocksdb_index,
);
criterion_main!(benches);
