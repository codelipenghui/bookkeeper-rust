use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use tokio::sync::Semaphore;

use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_protocol::digest::{DigestManager, DigestType, generate_master_key};
use bookkeeper_protocol::entry::EntryBuilder;

fn parse_digest(s: &str) -> Result<DigestType, Box<dyn std::error::Error>> {
    s.parse::<DigestType>().map_err(|e| e.into())
}

fn random_payload(size: usize) -> Vec<u8> {
    use rand::Rng;
    let mut buf = vec![0u8; size];
    rand::thread_rng().fill(&mut buf[..]);
    buf
}

fn build_entry(dm: &DigestManager, ledger_id: i64, entry_id: i64, payload: &[u8]) -> bytes::Bytes {
    EntryBuilder::new(dm)
        .ledger_id(ledger_id)
        .entry_id(entry_id)
        .lac(entry_id.saturating_sub(1))
        .length(payload.len() as i64)
        .payload(bytes::Bytes::copy_from_slice(payload))
        .build()
        .freeze()
}

fn print_latency(title: &str, hist: &Histogram<u64>, entry_size: usize) {
    println!();
    println!("=== {} ({} byte entries) ===", title, entry_size);
    println!("  Entries: {}", hist.len());
    println!("  p50:    {:.2} ms", hist.value_at_quantile(0.50) as f64 / 1000.0);
    println!("  p75:    {:.2} ms", hist.value_at_quantile(0.75) as f64 / 1000.0);
    println!("  p95:    {:.2} ms", hist.value_at_quantile(0.95) as f64 / 1000.0);
    println!("  p99:    {:.2} ms", hist.value_at_quantile(0.99) as f64 / 1000.0);
    println!("  p999:   {:.2} ms", hist.value_at_quantile(0.999) as f64 / 1000.0);
    println!("  max:    {:.2} ms", hist.max() as f64 / 1000.0);
    println!();
}

fn print_throughput(title: &str, dur: Duration, count: u64, entry_size: usize) {
    let secs = dur.as_secs_f64();
    let ops = count as f64 / secs;
    let mb = (count as f64 * entry_size as f64) / (secs * 1024.0 * 1024.0);
    println!();
    println!("=== {} ({} byte entries) ===", title, entry_size);
    println!("  Entries:    {}", count);
    println!("  Duration:   {:.2} s", secs);
    println!("  Throughput: {:.0} entries/s", ops);
    println!("  Bandwidth:  {:.2} MB/s", mb);
    println!();
}

// ---------------------------------------------------------------------------
// bk bench-bookie
// ---------------------------------------------------------------------------

pub async fn cmd_bench_bookie(
    bookie_addr: &str,
    entry_size: usize,
    warmup: u64,
    latency_count: u64,
    throughput_count: u64,
    max_outstanding: usize,
    digest_str: &str,
    duration_secs: u64,
    read_pct: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let digest_type = parse_digest(digest_str)?;
    let addr: SocketAddr = bookie_addr.parse()?;
    let password = b"bench-password";
    let master_key = generate_master_key(password);
    let dm = DigestManager::new(digest_type).with_password(password);
    let payload = random_payload(entry_size);
    let ledger_id: i64 = 999_999;

    println!("=== Bookie Benchmark ===");
    println!("  Target:     {bookie_addr}");
    println!("  Entry size: {} bytes", entry_size);
    println!("  Digest:     {digest_str}");
    if duration_secs > 0 {
        println!("  Duration:   {}s (sustained)", duration_secs);
    }
    if read_pct > 0 {
        println!("  Read mix:   {}%", read_pct);
    }
    println!();

    // --- WARMUP ---
    println!("Warmup: {} entries...", warmup);
    {
        let conn = Arc::new(BookieConnection::connect(addr).await?);
        for i in 0..warmup {
            let entry = build_entry(&dm, ledger_id, i as i64, &payload);
            conn.send_add_entry(0, master_key, entry).await?;
        }
    }
    println!("Warmup complete.\n");

    // --- WRITE LATENCY (sequential) ---
    println!("Write Latency: {} entries (sequential)...", latency_count);
    {
        let conn = Arc::new(BookieConnection::connect(addr).await?);
        let mut hist = Histogram::<u64>::new(3)?;
        for i in 0..latency_count {
            let eid = (warmup + i) as i64;
            let entry = build_entry(&dm, ledger_id, eid, &payload);
            let t0 = Instant::now();
            conn.send_add_entry(0, master_key, entry).await?;
            let _ = hist.record(t0.elapsed().as_micros() as u64);
        }
        print_latency("Write Latency", &hist, entry_size);
    }

    // --- WRITE THROUGHPUT (async) ---
    let total_write = if duration_secs > 0 {
        println!("Write Throughput: sustained for {}s (max_outstanding={})...", duration_secs, max_outstanding);
        let conn = Arc::new(BookieConnection::connect(addr).await?);
        let sem = Arc::new(Semaphore::new(max_outstanding));
        let counter = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let base_id = warmup + latency_count;
        let t0 = Instant::now();

        let stop_c = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(duration_secs)).await;
            stop_c.store(true, Ordering::SeqCst);
        });

        let mut id = 0u64;
        while !stop.load(Ordering::Relaxed) {
            let eid = (base_id + id) as i64;
            let entry = build_entry(&dm, ledger_id, eid, &payload);
            let conn = conn.clone();
            let sem = sem.clone();
            let mk = master_key;
            let cnt = counter.clone();
            let permit = sem.acquire_owned().await?;
            tokio::spawn(async move {
                let _ = conn.send_add_entry(0, mk, entry).await;
                cnt.fetch_add(1, Ordering::Relaxed);
                drop(permit);
            });
            id += 1;
        }
        // Drain outstanding
        let _ = sem.acquire_many(max_outstanding as u32).await;
        let count = counter.load(Ordering::SeqCst);
        print_throughput("Write Throughput (sustained)", t0.elapsed(), count, entry_size);
        count
    } else {
        println!("Write Throughput: {} entries (async, max_outstanding={})...", throughput_count, max_outstanding);
        let conn = Arc::new(BookieConnection::connect(addr).await?);
        let sem = Arc::new(Semaphore::new(max_outstanding));
        let base_id = warmup + latency_count;
        let t0 = Instant::now();
        let mut handles = Vec::with_capacity(throughput_count as usize);
        for i in 0..throughput_count {
            let eid = (base_id + i) as i64;
            let entry = build_entry(&dm, ledger_id, eid, &payload);
            let conn = conn.clone();
            let sem = sem.clone();
            let mk = master_key;
            let permit = sem.acquire_owned().await?;
            handles.push(tokio::spawn(async move {
                let _ = conn.send_add_entry(0, mk, entry).await;
                drop(permit);
            }));
        }
        for h in handles { let _ = h.await; }
        print_throughput("Write Throughput", t0.elapsed(), throughput_count, entry_size);
        throughput_count
    };

    let total_written = warmup + latency_count + total_write;

    // --- MIXED READ/WRITE or READ-ONLY ---
    if read_pct > 0 {
        let mix_count = throughput_count;
        println!("Mixed Workload: {} ops ({}% read, {}% write, max_outstanding={})...",
            mix_count, read_pct, 100 - read_pct, max_outstanding);

        let conn = Arc::new(BookieConnection::connect(addr).await?);
        let sem = Arc::new(Semaphore::new(max_outstanding));
        let write_hist = Arc::new(parking_lot::Mutex::new(Histogram::<u64>::new(3)?));
        let read_hist = Arc::new(parking_lot::Mutex::new(Histogram::<u64>::new(3)?));
        let write_counter = Arc::new(AtomicU64::new(0));
        let read_counter = Arc::new(AtomicU64::new(0));
        let mut next_write_id = total_written;
        let t0 = Instant::now();

        let mut handles = Vec::with_capacity(mix_count as usize);
        for i in 0..mix_count {
            let is_read = (i % 100) < (read_pct as u64);
            let conn = conn.clone();
            let sem = sem.clone();
            let mk = master_key;
            let permit = sem.acquire_owned().await?;

            if is_read {
                let read_eid = (i % total_written) as i64;
                let rh = read_hist.clone();
                let rc = read_counter.clone();
                handles.push(tokio::spawn(async move {
                    let t = Instant::now();
                    let _ = conn.send_read_entry(0, ledger_id, read_eid, Some(mk)).await;
                    let _ = rh.lock().record(t.elapsed().as_micros() as u64);
                    rc.fetch_add(1, Ordering::Relaxed);
                    drop(permit);
                }));
            } else {
                let eid = next_write_id as i64;
                next_write_id += 1;
                let entry = build_entry(&dm, ledger_id, eid, &payload);
                let wh = write_hist.clone();
                let wc = write_counter.clone();
                handles.push(tokio::spawn(async move {
                    let t = Instant::now();
                    let _ = conn.send_add_entry(0, mk, entry).await;
                    let _ = wh.lock().record(t.elapsed().as_micros() as u64);
                    wc.fetch_add(1, Ordering::Relaxed);
                    drop(permit);
                }));
            }
        }
        for h in handles { let _ = h.await; }

        let dur = t0.elapsed();
        let wc = write_counter.load(Ordering::SeqCst);
        let rc = read_counter.load(Ordering::SeqCst);
        println!();
        println!("=== Mixed Workload ({} byte entries) ===", entry_size);
        println!("  Total ops:  {}", wc + rc);
        println!("  Writes:     {} ({:.0} ops/s)", wc, wc as f64 / dur.as_secs_f64());
        println!("  Reads:      {} ({:.0} ops/s)", rc, rc as f64 / dur.as_secs_f64());
        println!("  Duration:   {:.2} s", dur.as_secs_f64());
        println!("  Combined:   {:.0} ops/s", (wc + rc) as f64 / dur.as_secs_f64());
        print_latency("  Write Latency", &write_hist.lock(), entry_size);
        print_latency("  Read Latency", &read_hist.lock(), entry_size);
    } else {
        // --- READ LATENCY ---
        let read_count = std::cmp::min(latency_count, total_written);
        println!("Read Latency: {} entries (sequential)...", read_count);
        {
            let conn = Arc::new(BookieConnection::connect(addr).await?);
            let mut hist = Histogram::<u64>::new(3)?;
            for i in 0..read_count {
                let t0 = Instant::now();
                let _ = conn.send_read_entry(0, ledger_id, i as i64, Some(master_key)).await;
                let _ = hist.record(t0.elapsed().as_micros() as u64);
            }
            print_latency("Read Latency", &hist, entry_size);
        }

        // --- READ THROUGHPUT ---
        let read_tp_count = std::cmp::min(throughput_count, total_written);
        println!("Read Throughput: {} entries (async, max_outstanding={})...", read_tp_count, max_outstanding);
        {
            let conn = Arc::new(BookieConnection::connect(addr).await?);
            let sem = Arc::new(Semaphore::new(max_outstanding));
            let t0 = Instant::now();
            let mut handles = Vec::with_capacity(read_tp_count as usize);
            for i in 0..read_tp_count {
                let conn = conn.clone();
                let sem = sem.clone();
                let mk = master_key;
                let permit = sem.acquire_owned().await?;
                handles.push(tokio::spawn(async move {
                    let _ = conn.send_read_entry(0, ledger_id, i as i64, Some(mk)).await;
                    drop(permit);
                }));
            }
            for h in handles { let _ = h.await; }
            print_throughput("Read Throughput", t0.elapsed(), read_tp_count, entry_size);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// bk bench-e2e
// ---------------------------------------------------------------------------

pub async fn cmd_bench_e2e(
    zk_connect: &str,
    ledger_root: &str,
    entry_size: usize,
    duration_secs: u64,
    ensemble: i32,
    write_quorum: i32,
    ack_quorum: i32,
    num_ledgers: usize,
    max_outstanding: usize,
    warmup: u64,
    digest_str: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_client::zk_client::ZkBookKeeper;

    let digest_type = parse_digest(digest_str)?;
    let password = b"bench-e2e-password";
    let payload = random_payload(entry_size);

    println!("=== End-to-End Benchmark ===");
    println!("  ZK:         {zk_connect}");
    println!("  Ensemble:   E={ensemble}, W={write_quorum}, A={ack_quorum}");
    println!("  Entry size: {} bytes", entry_size);
    println!("  Ledgers:    {num_ledgers}");
    println!("  Duration:   {}s", duration_secs);
    println!("  Throttle:   {max_outstanding}");
    println!("  Digest:     {digest_str}");
    println!();

    // Setup: connect and create ledgers
    let mut bk = ZkBookKeeper::new(zk_connect, ledger_root).await?;
    let mut ledger_handles = Vec::new();
    let mut ledger_conns = Vec::new();

    for i in 0..num_ledgers {
        let (wh, conns) = bk
            .create_ledger(ensemble, write_quorum, ack_quorum, digest_type, password)
            .await?;
        println!("  Created ledger {} (id={})", i, wh.ledger_id());
        ledger_handles.push(wh);
        ledger_conns.push(conns);
    }

    // Warmup
    if warmup > 0 {
        println!("\nWarmup: {} entries...", warmup);
        for i in 0..warmup {
            let idx = (i as usize) % num_ledgers;
            ledger_handles[idx]
                .append(&ledger_conns[idx], &payload)
                .await?;
        }
        println!("Warmup complete.");
    }

    // Benchmark: time-based write with throttling
    println!("\nRunning benchmark for {}s...", duration_secs);
    let sem = Arc::new(Semaphore::new(max_outstanding));
    let counter = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let hist = Arc::new(parking_lot::Mutex::new(Histogram::<u64>::new(3)?));

    let stop_c = stop.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(duration_secs)).await;
        stop_c.store(true, Ordering::SeqCst);
    });

    let t0 = Instant::now();
    let mut op_id = 0u64;

    while !stop.load(Ordering::Relaxed) {
        let idx = (op_id as usize) % num_ledgers;
        let permit = sem.clone().acquire_owned().await?;
        let cnt = counter.clone();
        let h = hist.clone();

        // Use append_async for non-blocking sends
        match ledger_handles[idx].append_async(&ledger_conns[idx], &payload) {
            Ok(pending) => {
                tokio::spawn(async move {
                    let t = Instant::now();
                    let _ = pending.wait().await;
                    let _ = h.lock().record(t.elapsed().as_micros() as u64);
                    cnt.fetch_add(1, Ordering::Relaxed);
                    drop(permit);
                });
            }
            Err(_) => {
                drop(permit);
            }
        }
        op_id += 1;
    }

    // Drain outstanding
    let _ = sem.acquire_many(max_outstanding as u32).await;
    let elapsed = t0.elapsed();
    let total = counter.load(Ordering::SeqCst);

    // Report
    let hist_locked = hist.lock();
    println!();
    println!("=== End-to-End Write Results ===");
    println!("  Config:     E={ensemble}, W={write_quorum}, A={ack_quorum}, {} byte entries, {} ledger(s)",
        entry_size, num_ledgers);
    println!("  Duration:   {:.2} s", elapsed.as_secs_f64());
    println!("  Entries:    {}", total);
    println!("  Throughput: {:.0} entries/s", total as f64 / elapsed.as_secs_f64());
    println!("  Bandwidth:  {:.2} MB/s",
        (total as f64 * entry_size as f64) / (elapsed.as_secs_f64() * 1024.0 * 1024.0));
    println!();
    println!("  Latency:");
    println!("    p50:    {:.2} ms", hist_locked.value_at_quantile(0.50) as f64 / 1000.0);
    println!("    p75:    {:.2} ms", hist_locked.value_at_quantile(0.75) as f64 / 1000.0);
    println!("    p95:    {:.2} ms", hist_locked.value_at_quantile(0.95) as f64 / 1000.0);
    println!("    p99:    {:.2} ms", hist_locked.value_at_quantile(0.99) as f64 / 1000.0);
    println!("    p999:   {:.2} ms", hist_locked.value_at_quantile(0.999) as f64 / 1000.0);
    println!("    max:    {:.2} ms", hist_locked.max() as f64 / 1000.0);
    println!();
    drop(hist_locked);

    // Close ledgers
    for wh in &ledger_handles {
        let _ = bk.close_ledger(wh).await;
    }
    println!("All ledgers closed.");

    Ok(())
}
