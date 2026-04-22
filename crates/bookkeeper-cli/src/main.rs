use clap::{Parser, Subcommand};

mod admin;
mod bench;

#[derive(Parser)]
#[command(name = "bk", about = "BookKeeper CLI tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new BookKeeper cluster
    Initnewcluster {
        /// ZooKeeper connection string (ignored if --metadata-service-uri is set)
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path (ignored if --metadata-service-uri is set)
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Full metadata-service URI (e.g. `oxia://host:6648/default/ledgers`,
        /// `zk+hierarchical://host:2181/ledgers`). Takes precedence over
        /// `--zk` + `--ledger-root` when set.
        #[arg(long)]
        metadata_service_uri: Option<String>,
    },
    /// List registered bookies
    Listbookies {
        /// ZooKeeper connection string (ignored if --metadata-service-uri is set)
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path (ignored if --metadata-service-uri is set)
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Full metadata-service URI (e.g. `oxia://host:6648/default/ledgers`).
        /// Takes precedence over `--zk` + `--ledger-root` when set.
        #[arg(long)]
        metadata_service_uri: Option<String>,
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
        /// Override: the bookie ID advertised in ZK (e.g., "rust-bookie-1").
        /// Required when running multiple bookies behind a shared config file.
        #[arg(long)]
        advertised_address: Option<String>,
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
        /// I/O engine override: auto, std, uring
        #[arg(long, default_value = "auto")]
        io_engine: String,
    },
    /// List all ledgers in the cluster
    Listledgers {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Show metadata for each ledger
        #[arg(long)]
        meta: bool,
        /// Filter by bookie ID (show only ledgers on this bookie)
        #[arg(long)]
        bookieid: Option<String>,
    },
    /// Show metadata for a specific ledger
    Ledgerinfo {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Ledger ID to inspect
        #[arg(long)]
        ledger_id: i64,
    },
    /// List under-replicated ledgers
    Listunderreplicated {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Filter: only show ledgers missing this replica
        #[arg(long)]
        missingreplica: Option<String>,
        /// Filter: exclude ledgers missing this replica
        #[arg(long)]
        excludingmissingreplica: Option<String>,
        /// Print missing replicas for each ledger
        #[arg(long)]
        printmissingreplica: bool,
        /// Only display the count
        #[arg(long)]
        onlydisplayledgercount: bool,
    },
    /// Show the current auditor
    Whoisauditor {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
    },
    /// Enable auto-recovery
    Autorecovery {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Enable auto-recovery
        #[arg(long)]
        enable: bool,
        /// Disable auto-recovery
        #[arg(long)]
        disable: bool,
    },
    /// Show cluster info: bookies, auditor, replication status
    Clusterinfo {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
    },
    /// Delete a ledger's metadata
    Deleteledger {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Ledger ID to delete
        #[arg(long)]
        ledger_id: i64,
        /// Confirm deletion
        #[arg(long)]
        force: bool,
    },
    /// Read entries from a ledger
    Readledger {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Ledger ID to read
        #[arg(long)]
        ledger_id: i64,
        /// First entry ID to read
        #[arg(long, default_value = "0")]
        first_entry: i64,
        /// Last entry ID to read (defaults to last entry in ledger)
        #[arg(long)]
        last_entry: Option<i64>,
        /// Read from a specific bookie (ip:port)
        #[arg(long)]
        bookie: Option<String>,
    },
    /// Get a bookie's cookie from ZooKeeper
    CookieGet {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Bookie ID (host:port)
        #[arg(long)]
        bookie_id: String,
    },
    /// Delete a bookie's cookie from ZooKeeper
    CookieDelete {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Bookie ID (host:port)
        #[arg(long)]
        bookie_id: String,
        /// Confirm deletion
        #[arg(long)]
        force: bool,
    },
    /// Recover ledgers from a failed bookie or a specific ledger
    Recover {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Recover a specific ledger
        #[arg(long)]
        ledger_id: Option<i64>,
        /// Source bookie to recover data from (for full-bookie recovery)
        #[arg(long)]
        bookie_src: Option<String>,
        /// Dry run: show what would be done without making changes
        #[arg(long)]
        dry_run: bool,
    },
    /// Decommission a bookie (replicate data away, delete cookie)
    Decommissionbookie {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Bookie ID to decommission (host:port)
        #[arg(long)]
        bookie_id: String,
    },
    /// Check local bookie storage consistency
    Localconsistencycheck {
        /// Bookie data directory
        #[arg(long)]
        data_dir: String,
    },
    /// Get or set lostBookieRecoveryDelay (seconds)
    Lostbookierecoverydelay {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Set the delay in seconds (omit to read current value)
        #[arg(long)]
        set: Option<i32>,
    },
    /// Trigger an immediate audit cycle
    Triggeraudit {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
    },
    /// Wipe all BookKeeper metadata from ZooKeeper
    Metaformat {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Confirm destructive operation
        #[arg(long)]
        force: bool,
    },
    /// Print the last journal checkpoint mark
    Lastmark {
        /// Data directory (searches for lastMark)
        #[arg(long)]
        data_dir: String,
    },
    /// Scan and dump entries from a journal file
    Readjournal {
        /// Path to journal file (.txn)
        #[arg(long)]
        file: String,
    },
    /// Scan and dump entries from an entry log file
    Readlog {
        /// Path to entry log file (.log)
        #[arg(long)]
        file: String,
    },
    /// List journal, entry log, and index files on disk
    Listfilesondisc {
        /// Bookie data directory
        #[arg(long)]
        data_dir: String,
        /// Journal directory (defaults to data_dir)
        #[arg(long)]
        journal_dir: Option<String>,
    },
    /// Replace a bookie address in all ledger metadata (re-IP)
    Updatebookieinledger {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Old bookie address to replace
        #[arg(long)]
        old_bookie: String,
        /// New bookie address
        #[arg(long)]
        new_bookie: String,
        /// Show what would be done without making changes
        #[arg(long)]
        dry_run: bool,
    },
    /// Print the cluster instance ID
    Whatisinstanceid {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
    },
    /// Format (wipe) local bookie data directories
    Bookieformat {
        /// Bookie data directory
        #[arg(long)]
        data_dir: String,
        /// Journal directory (defaults to data_dir)
        #[arg(long)]
        journal_dir: Option<String>,
        /// Confirm destructive operation
        #[arg(long)]
        force: bool,
    },
    /// Delete all cluster metadata and state (nuclear option)
    Nukeexistingcluster {
        /// ZooKeeper connection string
        #[arg(long, default_value = "localhost:2181")]
        zk: String,
        /// ZooKeeper ledger root path
        #[arg(long, default_value = "/ledgers")]
        ledger_root: String,
        /// Confirm destructive operation
        #[arg(long)]
        force: bool,
        /// Validate instance ID before nuking (alternative to --force)
        #[arg(long)]
        instanceid: Option<String>,
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

fn main() {
    // Initialize structured logging.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    // Determine tokio worker thread count. For the serve command, read
    // serverNumIOThreads from the config file. For other commands, use
    // the default (number of CPU cores).
    let num_threads = match &cli.command {
        Commands::Serve { conf, .. } => {
            let config = conf.as_ref().map(|path| {
                bookkeeper_config::server::ServerConfig::from_file(std::path::Path::new(path))
            });
            match config {
                Some(Ok(c)) if c.server_num_io_threads > 0 => c.server_num_io_threads,
                _ => std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4),
            }
        }
        _ => std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4),
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_threads)
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async {
    let result = match cli.command {
        Commands::Initnewcluster { zk, ledger_root, metadata_service_uri } => {
            cmd_initnewcluster(&zk, &ledger_root, metadata_service_uri.as_deref()).await
        }
        Commands::Listbookies { zk, ledger_root, metadata_service_uri, rw, ro, all } => {
            cmd_listbookies(&zk, &ledger_root, metadata_service_uri.as_deref(), rw, ro, all).await
        }
        Commands::Serve { conf, port, data_dir, zk, ledger_root, advertised_address } => {
            cmd_serve(conf, port, data_dir, zk, ledger_root, advertised_address).await
        }
        Commands::Bookiesanity { entries, data_dir } => {
            cmd_bookiesanity(entries, data_dir).await
        }
        Commands::Listledgers { zk, ledger_root, meta, bookieid } => {
            admin::cmd_listledgers(&zk, &ledger_root, meta, bookieid.as_deref()).await
        }
        Commands::Ledgerinfo { zk, ledger_root, ledger_id } => {
            admin::cmd_ledgerinfo(&zk, &ledger_root, ledger_id).await
        }
        Commands::Listunderreplicated { zk, ledger_root, missingreplica, excludingmissingreplica, printmissingreplica, onlydisplayledgercount } => {
            admin::cmd_listunderreplicated(&zk, &ledger_root, missingreplica.as_deref(), excludingmissingreplica.as_deref(), printmissingreplica, onlydisplayledgercount).await
        }
        Commands::Whoisauditor { zk, ledger_root } => {
            admin::cmd_whoisauditor(&zk, &ledger_root).await
        }
        Commands::Autorecovery { zk, ledger_root, enable, disable } => {
            if enable == disable {
                Err("specify exactly one of --enable or --disable".into())
            } else {
                admin::cmd_toggle_recovery(&zk, &ledger_root, enable).await
            }
        }
        Commands::Clusterinfo { zk, ledger_root } => {
            admin::cmd_clusterinfo(&zk, &ledger_root).await
        }
        Commands::Deleteledger { zk, ledger_root, ledger_id, force } => {
            admin::cmd_deleteledger(&zk, &ledger_root, ledger_id, force).await
        }
        Commands::Readledger { zk, ledger_root, ledger_id, first_entry, last_entry, bookie } => {
            admin::cmd_readledger(&zk, &ledger_root, ledger_id, first_entry, last_entry, bookie.as_deref()).await
        }
        Commands::CookieGet { zk, ledger_root, bookie_id } => {
            admin::cmd_cookie_get(&zk, &ledger_root, &bookie_id).await
        }
        Commands::CookieDelete { zk, ledger_root, bookie_id, force } => {
            admin::cmd_cookie_delete(&zk, &ledger_root, &bookie_id, force).await
        }
        Commands::Recover { zk, ledger_root, ledger_id, bookie_src, dry_run } => {
            admin::cmd_recover(&zk, &ledger_root, ledger_id, bookie_src.as_deref(), dry_run).await
        }
        Commands::Decommissionbookie { zk, ledger_root, bookie_id } => {
            admin::cmd_decommissionbookie(&zk, &ledger_root, &bookie_id).await
        }
        Commands::Localconsistencycheck { data_dir } => {
            admin::cmd_localconsistencycheck(&data_dir).await
        }
        Commands::Lostbookierecoverydelay { zk, ledger_root, set } => {
            admin::cmd_lostbookierecoverydelay(&zk, &ledger_root, set).await
        }
        Commands::Triggeraudit { zk, ledger_root } => {
            admin::cmd_triggeraudit(&zk, &ledger_root).await
        }
        Commands::Metaformat { zk, ledger_root, force } => {
            admin::cmd_metaformat(&zk, &ledger_root, force).await
        }
        Commands::Lastmark { data_dir } => {
            admin::cmd_lastmark(&data_dir).await
        }
        Commands::Readjournal { file } => {
            admin::cmd_readjournal(&file).await
        }
        Commands::Readlog { file } => {
            admin::cmd_readlog(&file).await
        }
        Commands::Listfilesondisc { data_dir, journal_dir } => {
            admin::cmd_listfilesondisc(&data_dir, journal_dir.as_deref()).await
        }
        Commands::Updatebookieinledger { zk, ledger_root, old_bookie, new_bookie, dry_run } => {
            admin::cmd_updatebookieinledger(&zk, &ledger_root, &old_bookie, &new_bookie, dry_run).await
        }
        Commands::Whatisinstanceid { zk, ledger_root } => {
            admin::cmd_whatisinstanceid(&zk, &ledger_root).await
        }
        Commands::Bookieformat { data_dir, journal_dir, force } => {
            admin::cmd_bookieformat(&data_dir, journal_dir.as_deref(), force).await
        }
        Commands::Nukeexistingcluster { zk, ledger_root, force, instanceid } => {
            admin::cmd_nukeexistingcluster(&zk, &ledger_root, force, instanceid.as_deref()).await
        }
        Commands::BenchBookie { bookie, entry_size, warmup, latency_count, throughput_count, max_outstanding, digest, duration, read_pct, io_engine } => {
            bench::cmd_bench_bookie(&bookie, entry_size, warmup, latency_count, throughput_count, max_outstanding, &digest, duration, read_pct, &io_engine).await
        }
        Commands::BenchE2e { zk, ledger_root, entry_size, duration, ensemble, write_quorum, ack_quorum, ledgers, max_outstanding, warmup, digest } => {
            bench::cmd_bench_e2e(&zk, &ledger_root, entry_size, duration, ensemble, write_quorum, ack_quorum, ledgers, max_outstanding, warmup, &digest).await
        }
    };
    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
    }); // rt.block_on
}

/// Resolve a metadata-service URI from either an explicit
/// `--metadata-service-uri` or the ZK-style `--zk` + `--ledger-root`
/// fallback. Returns the parsed enum + a human-readable source
/// description for logging.
fn resolve_init_uri(
    zk: &str,
    ledger_root: &str,
    metadata_service_uri: Option<&str>,
) -> Result<
    (bookkeeper_metadata::metadata_paths::ParsedMetadataUri, String),
    Box<dyn std::error::Error>,
> {
    if let Some(uri) = metadata_service_uri {
        let parsed = bookkeeper_config::parse_metadata_service_uri(uri)
            .ok_or_else(|| format!("unrecognised metadataServiceUri: {uri}"))?;
        Ok((parsed, uri.to_string()))
    } else {
        Ok((
            bookkeeper_metadata::metadata_paths::ParsedMetadataUri::Zk {
                servers: zk.to_string(),
                ledgers_root: ledger_root.to_string(),
            },
            format!("zk+hierarchical://{zk}{ledger_root}"),
        ))
    }
}

async fn cmd_initnewcluster(
    zk: &str,
    ledger_root: &str,
    metadata_service_uri: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_metadata::backend::open_metadata_backend;
    use bookkeeper_metadata::traits::LedgerLayout;

    let (uri, display) = resolve_init_uri(zk, ledger_root, metadata_service_uri)?;
    println!("Initializing new cluster at {display}...");
    let backend = open_metadata_backend(&uri, None, None).await?;
    let mgr = backend.registration_manager;
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
    metadata_service_uri: Option<&str>,
    rw: bool,
    ro: bool,
    all: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_metadata::backend::open_metadata_backend;

    let (uri, _display) = resolve_init_uri(zk, ledger_root, metadata_service_uri)?;
    let backend = open_metadata_backend(&uri, None, None).await?;
    let mgr = backend.registration_manager;
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
    advertised_address: Option<String>,
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
    if let Some(a) = advertised_address { config.advertised_address = Some(a); }
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
