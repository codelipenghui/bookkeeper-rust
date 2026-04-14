use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use bytes::Bytes;
use bookkeeper_protocol::digest::{DigestManager, DigestType, generate_master_key};
use bookkeeper_protocol::entry::{EntryBuilder, parse_entry};
use bookkeeper_protocol::framing::BookieCodec;
use bookkeeper_protocol::v2::request::{encode_request, V2Request};
#[allow(unused_imports)]
use bookkeeper_protocol::constants::*;
use bytes::BytesMut;
use tokio_util::codec::{Encoder, Decoder};

fn random_payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i & 0xFF) as u8).collect()
}

// ---------------------------------------------------------------------------
// Digest benchmarks
// ---------------------------------------------------------------------------

fn bench_digest(c: &mut Criterion) {
    let mut group = c.benchmark_group("digest");

    for &size in &[128, 1024, 4096, 65536] {
        let payload = random_payload(size);
        let mut metadata = [0u8; 32];
        // Fill metadata with realistic values
        metadata[0..8].copy_from_slice(&1i64.to_be_bytes()); // ledger_id
        metadata[8..16].copy_from_slice(&0i64.to_be_bytes()); // entry_id
        metadata[16..24].copy_from_slice(&(-1i64).to_be_bytes()); // lac
        metadata[24..32].copy_from_slice(&(size as i64).to_be_bytes()); // length

        // CRC32
        let dm_crc32 = DigestManager::new(DigestType::Crc32);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("crc32", size), &size, |b, _| {
            b.iter(|| dm_crc32.compute(&metadata, &payload));
        });

        // CRC32C
        let dm_crc32c = DigestManager::new(DigestType::Crc32c);
        group.bench_with_input(BenchmarkId::new("crc32c", size), &size, |b, _| {
            b.iter(|| dm_crc32c.compute(&metadata, &payload));
        });

        // HMAC-SHA1
        let dm_hmac = DigestManager::new(DigestType::HmacSha1).with_password(b"bench");
        group.bench_with_input(BenchmarkId::new("hmac_sha1", size), &size, |b, _| {
            b.iter(|| dm_hmac.compute(&metadata, &payload));
        });

        // Dummy
        let dm_dummy = DigestManager::new(DigestType::Dummy);
        group.bench_with_input(BenchmarkId::new("dummy", size), &size, |b, _| {
            b.iter(|| dm_dummy.compute(&metadata, &payload));
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Entry codec benchmarks
// ---------------------------------------------------------------------------

fn bench_entry_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("entry_codec");

    for &size in &[128, 1024, 4096, 65536] {
        let payload = random_payload(size);
        let dm = DigestManager::new(DigestType::Crc32c);

        // Build
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("build", size), &size, |b, _| {
            b.iter(|| {
                EntryBuilder::new(&dm)
                    .ledger_id(1)
                    .entry_id(0)
                    .lac(-1)
                    .length(size as i64)
                    .payload(&payload)
                    .build()
            });
        });

        // Parse (build first, then parse)
        let entry_bytes = EntryBuilder::new(&dm)
            .ledger_id(1)
            .entry_id(0)
            .lac(-1)
            .length(size as i64)
            .payload(&payload)
            .build()
            .freeze();

        group.bench_with_input(BenchmarkId::new("parse", size), &size, |b, _| {
            b.iter(|| parse_entry(&dm, entry_bytes.clone()));
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Frame codec benchmarks
// ---------------------------------------------------------------------------

fn bench_frame_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_codec");

    for &size in &[128, 1024, 4096, 65536] {
        let payload = random_payload(size);
        let dm = DigestManager::new(DigestType::Crc32c);
        let master_key = generate_master_key(b"bench");

        let entry_bytes = EntryBuilder::new(&dm)
            .ledger_id(1)
            .entry_id(0)
            .lac(-1)
            .length(size as i64)
            .payload(&payload)
            .build()
            .freeze();

        // Build a V2Request and encode it
        let req = V2Request::AddEntry {
            flags: 0,
            master_key,
            entry_data: entry_bytes.clone(), deferred_sync: false,
        };
        let req_bytes = encode_request(&req).freeze();

        // Encode frame
        let mut codec = BookieCodec::new(5 * 1024 * 1024);
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("encode_frame", size), &size, |b, _| {
            let mut out = BytesMut::with_capacity(size + 256);
            b.iter(|| {
                out.clear();
                let _ = codec.encode(req_bytes.clone(), &mut out);
            });
        });

        // Decode frame
        let mut encoded = BytesMut::with_capacity(size + 256);
        let _ = codec.encode(req_bytes.clone(), &mut encoded);

        group.bench_with_input(BenchmarkId::new("decode_frame", size), &size, |b, _| {
            b.iter(|| {
                let mut buf = encoded.clone();
                let _ = codec.decode(&mut buf);
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Master key derivation benchmark
// ---------------------------------------------------------------------------

fn bench_master_key(c: &mut Criterion) {
    c.bench_function("master_key_derivation", |b| {
        b.iter(|| generate_master_key(b"benchmark-password"));
    });
}

criterion_group!(benches, bench_digest, bench_entry_codec, bench_frame_codec, bench_master_key);
criterion_main!(benches);
