use clap::{Parser, Subcommand};

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
        /// Bookie port (0 = OS-assigned)
        #[arg(long, default_value = "3181")]
        port: u16,
        /// Data directory
        #[arg(long)]
        data_dir: Option<String>,
        /// ZooKeeper connection string
        #[arg(long)]
        zk: Option<String>,
        /// ZooKeeper ledger root path
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
        Commands::Serve { port, data_dir, zk, ledger_root } => {
            cmd_serve(port, data_dir, zk, ledger_root).await
        }
        Commands::Bookiesanity { entries, data_dir } => {
            cmd_bookiesanity(entries, data_dir).await
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
    use bookkeeper_protocol::digest::DigestType;
    use bookkeeper_server::server::BookieServer;
    use bookkeeper_server::config::BookieConfig;

    // Start a temporary bookie
    let _temp_dir;
    let dir_path = match &data_dir {
        Some(d) => d.clone(),
        None => {
            _temp_dir = tempfile::TempDir::new()?;
            _temp_dir.path().to_string_lossy().to_string()
        }
    };

    let config = BookieConfig {
        port: 0,
        ledger_dirs: vec![dir_path.clone()],
        journal_dir: dir_path.clone(),
        max_frame_size: 5 * 1024 * 1024,
        zk_connect: None,
        zk_ledger_root: "/ledgers".to_string(),
        bookie_id: None,
        http_port: None,
    };
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
    port: u16,
    data_dir: Option<String>,
    zk: Option<String>,
    ledger_root: String,
) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_server::server::BookieServer;
    use bookkeeper_server::config::BookieConfig;

    let _temp_dir;
    let dir_path = match &data_dir {
        Some(d) => d.clone(),
        None => {
            _temp_dir = tempfile::TempDir::new()?;
            _temp_dir.path().to_string_lossy().to_string()
        }
    };

    let config = BookieConfig {
        port,
        ledger_dirs: vec![dir_path.clone()],
        journal_dir: dir_path.clone(),
        max_frame_size: 5 * 1024 * 1024,
        zk_connect: zk,
        zk_ledger_root: ledger_root,
        bookie_id: None,
        http_port: None,
    };

    let server = BookieServer::start(config).await?;
    let addr = server.local_addr();
    println!("Bookie started at {addr}");
    println!("Press Ctrl+C to stop.");

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;
    println!("\nShutting down...");
    server.shutdown().await;
    println!("Bookie stopped.");
    Ok(())
}
