use clap::{Parser, Subcommand};

mod bench;

#[derive(Parser)]
#[command(name = "bk", about = "BookKeeper CLI tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new BookKeeper cluster in ZooKeeper
    Initnewcluster {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
    },
    /// List registered bookies
    Listbookies {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// List writable bookies
        #[arg(long)]
        rw: bool,
        /// List read-only bookies
        #[arg(long)]
        ro: bool,
        /// List all bookies
        #[arg(short, long)]
        all: bool,
    },
    /// Start a bookie server (long-running)
    Serve {
        /// Path to bk_server.conf configuration file
        #[arg(long, short = 'c')]
        conf: Option<String>,
        /// Override: bookie port
        #[arg(long)]
        port: Option<u16>,
        /// Override: data directory
        #[arg(long)]
        data_dir: Option<String>,
        /// Override: ZooKeeper connection string
        #[arg(long)]
        zk: Option<String>,
        /// Override: ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
    },
    /// Run a sanity test: start bookie, write entries, read back, verify
    Bookiesanity {
        /// Number of entries to write
        #[arg(long, default_value = "10")]
        entries: i64,
        /// Data directory (temporary if not specified)
        #[arg(long)]
        data_dir: Option<String>,
    },
    /// Benchmark a single bookie (direct protocol, no ZK)
    BenchBookie {
        /// Bookie address (ip:port)
        #[arg(long, default_value = "127.0.0.1:3181")]
        bookie: String,
        /// Entry payload size in bytes
        #[arg(long, default_value = "1024")]
        entry_size: usize,
        /// Warmup entries (not measured)
        #[arg(long, default_value = "1000")]
        warmup: u64,
        /// Sequential entries for latency measurement
        #[arg(long, default_value = "5000")]
        latency_count: u64,
        /// Async entries for throughput measurement
        #[arg(long, default_value = "50000")]
        throughput_count: u64,
        /// Max concurrent in-flight requests
        #[arg(long, default_value = "1000")]
        max_outstanding: usize,
        /// Digest type: crc32, crc32c, hmac, dummy
        #[arg(long, default_value = "crc32c")]
        digest: String,
        /// Time-based sustained load duration in seconds (0 = count-based only)
        #[arg(long, default_value = "0")]
        duration: u64,
        /// Mixed read/write: percentage of reads (0-100, 0 = write-only)
        #[arg(long, default_value = "0")]
        read_pct: u8,
    },
    /// End-to-end benchmark with ZK, quorum writes, multi-ledger
    BenchE2e {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Entry payload size in bytes
        #[arg(long, default_value = "1024")]
        entry_size: usize,
        /// Benchmark duration in seconds
        #[arg(long, default_value = "60")]
        duration: u64,
        /// Ensemble size
        #[arg(long, default_value = "3")]
        ensemble: i32,
        /// Write quorum
        #[arg(long, default_value = "2")]
        write_quorum: i32,
        /// Ack quorum
        #[arg(long, default_value = "2")]
        ack_quorum: i32,
        /// Number of concurrent ledgers
        #[arg(long, default_value = "1")]
        ledgers: usize,
        /// Max concurrent in-flight requests
        #[arg(long, default_value = "10000")]
        max_outstanding: usize,
        /// Warmup entries
        #[arg(long, default_value = "10000")]
        warmup: u64,
        /// Digest type: crc32, crc32c, hmac, dummy
        #[arg(long, default_value = "crc32c")]
        digest: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Commands::Initnewcluster { zk, ledger_root } => {
            cmd_initnewcluster(&zk, &ledger_root).await
        }
        Commands::Listbookies { zk, ledger_root, rw, ro, all } => {
            cmd_listbookies(&zk, &ledger_root, rw, ro, all).await
        }
        Commands::Serve { conf, port, data_dir, zk, ledger_root } => {
            cmd_serve(conf, port, data_dir, zk, ledger_root).await
        }
        Commands::Bookiesanity { entries, data_dir } => {
            cmd_bookiesanity(entries, data_dir).await
        }
        Commands::BenchBookie { bookie, entry_size, warmup, latency_count, throughput_count, max_outstanding, digest, duration, read_pct } => {
            bench::cmd_bench_bookie(&bookie, entry_size, warmup, latency_count, throughput_count, max_outstanding, &digest, duration, read_pct).await
        }
        Commands::BenchE2e { zk, ledger_root, entry_size, duration, ensemble, write_quorum, ack_quorum, ledgers, max_outstanding, warmup, digest } => {
            bench::cmd_bench_e2e(&zk, &ledger_root, entry_size, duration, ensemble, write_quorum, ack_quorum, ledgers, max_outstanding, warmup, &digest).await
        }
    };
    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

async fn cmd_initnewcluster(zk: &str, ledger_root: &str) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_metadata::zk::registration::ZkRegistrationManager;
    use bookkeeper_metadata::traits::{LedgerLayout, RegistrationManager};

    println!("Initializing new cluster at {zk}{ledger_root}...");
    let mgr = ZkRegistrationManager::new(zk, ledger_root).await?;
    let layout = LedgerLayout {
        manager_factory_class: "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory"
            .to_string(),
        manager_version: 1,
    };
    let instance_id = uuid::Uuid::new_v4().to_string();
    mgr.init_new_cluster(&layout, &instance_id).await?;
    println!("Cluster initialized successfully.");
    println!("  Instance ID: {instance_id}");
    println!("  Layout: HierarchicalLedgerManagerFactory v2");
    Ok(())
}

async fn cmd_listbookies(
    zk: &str,
    ledger_root: &str,
    rw: bool,
    ro: bool,
    all: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_metadata::zk::registration::ZkRegistrationManager;
    use bookkeeper_metadata::traits::RegistrationManager;

    let mgr = ZkRegistrationManager::new(zk, ledger_root).await?;
    let show_rw = rw || all || (!rw && !ro);
    let show_ro = ro || all;

    if show_rw {
        let bookies = mgr.get_writable_bookies().await?;
        if bookies.is_empty() {
            println!("No writable bookies found.");
        } else {
            println!("Writable bookies:");
            for b in &bookies {
                println!("  {b}");
            }
        }
    }
    if show_ro {
        let bookies = mgr.get_readonly_bookies().await?;
        if bookies.is_empty() {
            println!("No read-only bookies found.");
        } else {
            println!("Read-only bookies:");
            for b in &bookies {
                println!("  {b}");
            }
        }
    }
    Ok(())
}

async fn cmd_bookiesanity(
    num_entries: i64,
    data_dir: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_client::client::BookKeeperClient;
    use bookkeeper_config::server::ServerConfig;
    use bookkeeper_protocol::digest::DigestType;
    use bookkeeper_server::server::BookieServer;

    // Start a temporary bookie
    let _temp_dir;
    let dir_path = match &data_dir {
        Some(d) => d.clone(),
        None => {
            _temp_dir = tempfile::TempDir::new()?;
            _temp_dir.path().to_string_lossy().to_string()
        }
    };

    let mut config = ServerConfig::default();
    config.bookie_port = 0;
    config.ledger_directories = vec![dir_path.clone()];
    config.journal_directory = dir_path.clone();
    let server = BookieServer::start(config).await?;
    let addr = server.local_addr();
    println!("Started temporary bookie at {addr}");

    // Create client and establish connections
    let mut client = BookKeeperClient::new(vec![addr.to_string()]);
    client.ensure_connections().await?;

    // Write entries
    println!("Writing {num_entries} entries...");
    let ledger_id = 1;
    let mut wh = client
        .create_ledger(ledger_id, 1, 1, 1, DigestType::Crc32c, b"sanity-test")
        .await?;
    for i in 0..num_entries {
        let data = format!("sanity-entry-{i}");
        wh.append(client.connections(), data.as_bytes()).await?;
    }
    println!("  Wrote {num_entries} entries. LAC = {}", wh.last_add_confirmed());

    // Read entries back
    println!("Reading {num_entries} entries...");
    let rh = client.open_ledger(ledger_id, 1, 1, 1, DigestType::Crc32c, b"sanity-test")?;
    for i in 0..num_entries {
        let data = rh.read_entry(client.connections(), i).await?;
        let expected = format!("sanity-entry-{i}");
        if data != expected.as_bytes() {
            eprintln!(
                "MISMATCH at entry {i}: expected '{expected}', got '{}'",
                String::from_utf8_lossy(&data)
            );
            server.shutdown().await;
            return Err("sanity test failed".into());
        }
    }

    println!("All {num_entries} entries verified successfully.");
    println!("Sanity test PASSED.");

    server.shutdown().await;
    Ok(())
}

async fn cmd_serve(
    conf: Option<String>,
    port: Option<u16>,
    data_dir: Option<String>,
    zk: Option<String>,
    ledger_root: String,
) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_config::server::ServerConfig;
    use bookkeeper_server::server::BookieServer;

    // Load config from file or use defaults
    let mut config = match conf {
        Some(ref path) => {
            println!("Loading config from {path}");
            ServerConfig::from_file(std::path::Path::new(path))?
        }
        None => ServerConfig::default(),
    };

    // Apply command-line overrides
    if let Some(p) = port { config.bookie_port = p; }
    if let Some(ref d) = data_dir {
        config.ledger_directories = vec![d.clone()];
        config.journal_directory = d.clone();
    }
    if let Some(ref z) = zk { config.zk_servers = Some(z.clone()); }
    config.zk_ledger_root = ledger_root;

    // Ensure directories exist
    for dir in &config.ledger_directories {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::create_dir_all(&config.journal_directory)?;

    let server = BookieServer::start(config).await?;
    let addr = server.local_addr();
    println!("Bookie started at {addr}");
    if let Some(http) = server.http_addr() {
        println!("HTTP admin at {http}");
    }
    println!("Press Ctrl+C to stop.");

    // Handle both SIGINT (Ctrl+C) and SIGTERM (kill/systemd)
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\nReceived SIGINT, shutting down...");
            }
            _ = sigterm.recv() => {
                println!("\nReceived SIGTERM, shutting down...");
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        println!("\nShutting down...");
    }

    server.shutdown().await;
    Ok(())
}
