use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use hdrhistogram::Histogram;
use tokio::sync::Semaphore;

use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_protocol::digest::{DigestManager, DigestType, generate_master_key};
use bookkeeper_protocol::entry::EntryBuilder;

/// `bk bench-bookie` — Direct single-bookie benchmark.
///
/// Matches Java's BenchBookie: sends raw AddEntry/ReadEntry protocol messages
/// to a single bookie, measuring latency and throughput.
pub async fn cmd_bench_bookie(
    bookie_addr: &str,
    entry_size: usize,
    warmup: u64,
    latency_count: u64,
    throughput_count: u64,
    max_outstanding: usize,
    digest_str: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let digest_type = match digest_str {
        "crc32" => DigestType::Crc32,
        "crc32c" => DigestType::Crc32c,
        "hmac" => DigestType::HmacSha1,
        "dummy" => DigestType::Dummy,
        other => return Err(format!("unknown digest type: {other}").into()),
    };

    let addr: SocketAddr = bookie_addr.parse()?;
    let password = b"bench-password";
    let master_key = generate_master_key(password);
    let dm = DigestManager::new(digest_type).with_password(password);

    // Generate random payload
    let payload = {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut buf = vec![0u8; entry_size];
        rng.fill(&mut buf[..]);
        buf
    };

    println!("=== Bookie Benchmark ===");
    println!("  Target:     {bookie_addr}");
    println!("  Entry size: {} bytes", entry_size);
    println!("  Digest:     {digest_str}");
    println!();

    // Ledger ID for the benchmark
    let ledger_id: i64 = 999_999;

    // --- WARMUP ---
    println!("Warmup: {} entries...", warmup);
    {
        let conn = Arc::new(BookieConnection::connect(addr).await?);
        for i in 0..warmup {
            let entry = build_entry(&dm, ledger_id, i as i64, &payload);
            conn.send_add_entry(0, master_key, entry).await?;
        }
    }
    println!("Warmup complete.");
    println!();

    // --- WRITE LATENCY (sequential) ---
    println!("Write Latency: {} entries (sequential)...", latency_count);
    let write_latency = {
        let conn = Arc::new(BookieConnection::connect(addr).await?);
        let mut hist = Histogram::<u64>::new(3)?;
        let start_id = warmup;

        for i in 0..latency_count {
            let entry_id = (start_id + i) as i64;
            let entry = build_entry(&dm, ledger_id, entry_id, &payload);

            let t0 = Instant::now();
            conn.send_add_entry(0, master_key, entry).await?;
            let elapsed_us = t0.elapsed().as_micros() as u64;
            let _ = hist.record(elapsed_us);
        }
        hist
    };
    print_latency_report("Write Latency", &write_latency, entry_size);

    // --- WRITE THROUGHPUT (async) ---
    println!("Write Throughput: {} entries (async, max_outstanding={})...",
        throughput_count, max_outstanding);
    let (write_dur, write_count) = {
        let conn = Arc::new(BookieConnection::connect(addr).await?);
        let sem = Arc::new(Semaphore::new(max_outstanding));
        let start_id = warmup + latency_count;
        let t0 = Instant::now();

        let mut handles = Vec::with_capacity(throughput_count as usize);
        for i in 0..throughput_count {
            let entry_id = (start_id + i) as i64;
            let entry = build_entry(&dm, ledger_id, entry_id, &payload);
            let conn = conn.clone();
            let sem = sem.clone();
            let mk = master_key;

            let permit = sem.acquire_owned().await?;
            handles.push(tokio::spawn(async move {
                let _ = conn.send_add_entry(0, mk, entry).await;
                drop(permit);
            }));
        }

        for h in handles {
            let _ = h.await;
        }
        (t0.elapsed(), throughput_count)
    };
    print_throughput_report("Write Throughput", write_dur, write_count, entry_size);

    // --- READ LATENCY (sequential) ---
    let total_written = warmup + latency_count + throughput_count;
    let read_start = 0i64;
    let read_end = std::cmp::min(latency_count, total_written) as i64;

    println!("Read Latency: {} entries (sequential)...", read_end);
    let read_latency = {
        let conn = Arc::new(BookieConnection::connect(addr).await?);
        let mut hist = Histogram::<u64>::new(3)?;

        for i in read_start..read_end {
            let t0 = Instant::now();
            let _ = conn.send_read_entry(0, ledger_id, i, Some(master_key)).await;
            let elapsed_us = t0.elapsed().as_micros() as u64;
            let _ = hist.record(elapsed_us);
        }
        hist
    };
    print_latency_report("Read Latency", &read_latency, entry_size);

    // --- READ THROUGHPUT (async) ---
    println!("Read Throughput: {} entries (async, max_outstanding={})...",
        throughput_count, max_outstanding);
    let (read_dur, read_count) = {
        let conn = Arc::new(BookieConnection::connect(addr).await?);
        let sem = Arc::new(Semaphore::new(max_outstanding));
        let t0 = Instant::now();

        let entries_to_read = std::cmp::min(throughput_count, total_written);
        let mut handles = Vec::with_capacity(entries_to_read as usize);

        for i in 0..entries_to_read {
            let entry_id = i as i64;
            let conn = conn.clone();
            let sem = sem.clone();
            let mk = master_key;

            let permit = sem.acquire_owned().await?;
            handles.push(tokio::spawn(async move {
                let _ = conn.send_read_entry(0, ledger_id, entry_id, Some(mk)).await;
                drop(permit);
            }));
        }

        for h in handles {
            let _ = h.await;
        }
        (t0.elapsed(), entries_to_read)
    };
    print_throughput_report("Read Throughput", read_dur, read_count, entry_size);

    Ok(())
}

fn build_entry(
    dm: &DigestManager,
    ledger_id: i64,
    entry_id: i64,
    payload: &[u8],
) -> bytes::Bytes {
    EntryBuilder::new(dm)
        .ledger_id(ledger_id)
        .entry_id(entry_id)
        .lac(entry_id.saturating_sub(1))
        .length(payload.len() as i64)
        .payload(bytes::Bytes::copy_from_slice(payload))
        .build()
        .freeze()
}

fn print_latency_report(title: &str, hist: &Histogram<u64>, entry_size: usize) {
    println!();
    println!("=== {} ({} byte entries) ===", title, entry_size);
    println!("  Entries: {}", hist.len());
    println!("  p50:    {:.2} ms", hist.value_at_quantile(0.50) as f64 / 1000.0);
    println!("  p95:    {:.2} ms", hist.value_at_quantile(0.95) as f64 / 1000.0);
    println!("  p99:    {:.2} ms", hist.value_at_quantile(0.99) as f64 / 1000.0);
    println!("  p999:   {:.2} ms", hist.value_at_quantile(0.999) as f64 / 1000.0);
    println!("  max:    {:.2} ms", hist.max() as f64 / 1000.0);
    println!();
}

fn print_throughput_report(title: &str, duration: std::time::Duration, count: u64, entry_size: usize) {
    let secs = duration.as_secs_f64();
    let ops_per_sec = count as f64 / secs;
    let mb_per_sec = (count as f64 * entry_size as f64) / (secs * 1024.0 * 1024.0);

    println!();
    println!("=== {} ({} byte entries) ===", title, entry_size);
    println!("  Entries:    {}", count);
    println!("  Duration:   {:.2} s", secs);
    println!("  Throughput: {:.0} entries/s", ops_per_sec);
    println!("  Bandwidth:  {:.2} MB/s", mb_per_sec);
    println!();
}
