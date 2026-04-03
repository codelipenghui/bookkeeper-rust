# Phase 10: CLI Tools Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a `bk` CLI binary with essential commands for cluster initialization, bookie listing, and sanity testing — providing operational tooling for the Rust BookKeeper implementation.

**Architecture:** New `bookkeeper-cli` binary crate using `clap` for argument parsing. Commands delegate to existing crate APIs: `ZkRegistrationManager` for cluster init and bookie listing, `BookKeeperClient` + `BookieServer` for sanity testing. Focus on the 3 most useful commands: `initnewcluster`, `listbookies`, `bookiesanity`.

**Tech Stack:** `clap` (CLI parsing), `tokio` (async runtime), existing crates

**Spec reference:** `docs/superpowers/specs/10-cli-tools.md`

---

## File Structure

```
crates/bookkeeper-cli/
├── Cargo.toml
└── src/
    └── main.rs            # CLI entry point with clap subcommands
```

---

### Task 1: CLI Binary with initnewcluster, listbookies, bookiesanity

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Create: `crates/bookkeeper-cli/Cargo.toml`
- Create: `crates/bookkeeper-cli/src/main.rs`

- [ ] **Step 1: Add crate to workspace**

Add `"crates/bookkeeper-cli"` to members. Add to workspace.dependencies:
```toml
bookkeeper-cli = { path = "crates/bookkeeper-cli" }
clap = { version = "4", features = ["derive"] }
```

- [ ] **Step 2: Create Cargo.toml**

```toml
[package]
name = "bookkeeper-cli"
version.workspace = true
edition.workspace = true
license.workspace = true

[[bin]]
name = "bk"
path = "src/main.rs"

[dependencies]
bookkeeper-metadata = { workspace = true }
bookkeeper-client = { workspace = true }
bookkeeper-server = { workspace = true }
bookkeeper-protocol = { workspace = true }
bookkeeper-config = { workspace = true }
clap = { workspace = true }
tokio = { workspace = true }
uuid = { workspace = true }
```

- [ ] **Step 3: Implement main.rs**

```rust
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
        manager_factory_class: "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory".to_string(),
        manager_version: 2,
    };
    let instance_id = uuid::Uuid::new_v4().to_string();
    mgr.init_new_cluster(&layout, &instance_id).await?;
    println!("Cluster initialized successfully.");
    println!("  Instance ID: {instance_id}");
    println!("  Layout: HierarchicalLedgerManagerFactory v2");
    Ok(())
}

async fn cmd_listbookies(zk: &str, ledger_root: &str, rw: bool, ro: bool, all: bool) -> Result<(), Box<dyn std::error::Error>> {
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

async fn cmd_bookiesanity(num_entries: i64, data_dir: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_client::client::BookKeeperClient;
    use bookkeeper_protocol::digest::DigestType;
    use bookkeeper_server::server::BookieServer;
    use bookkeeper_server::config::BookieConfig;

    // Start a temporary bookie
    let temp_dir;
    let dir_path = match &data_dir {
        Some(d) => d.clone(),
        None => {
            temp_dir = tempfile::TempDir::new()?;
            temp_dir.path().to_string_lossy().to_string()
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

    // Create client
    let mut client = BookKeeperClient::new(vec![addr.to_string()]);

    // Write entries
    println!("Writing {num_entries} entries...");
    let ledger_id = 1;
    let mut wh = client.create_ledger(ledger_id, 1, 1, 1, DigestType::Crc32c, b"sanity-test").await?;
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
            eprintln!("MISMATCH at entry {i}: expected '{expected}', got '{}'", String::from_utf8_lossy(&data));
            server.shutdown().await;
            return Err("sanity test failed".into());
        }
    }

    println!("All {num_entries} entries verified successfully.");
    println!("Sanity test PASSED.");

    server.shutdown().await;
    Ok(())
}
```

- [ ] **Step 4: Add tempfile dependency**

In crates/bookkeeper-cli/Cargo.toml:
```toml
tempfile = "3"
```

- [ ] **Step 5: Verify it compiles and runs**

```bash
cargo build -p bookkeeper-cli
./target/debug/bk --help
./target/debug/bk bookiesanity --entries 5
```

- [ ] **Step 6: Commit**

```bash
git commit -m "feat: add bk CLI with initnewcluster, listbookies, and bookiesanity commands

The final phase: a runnable CLI binary for cluster administration.
- initnewcluster: initialize ZK metadata for a new BookKeeper cluster
- listbookies: list writable and read-only bookies from ZK
- bookiesanity: start a temp bookie, write+read+verify entries end-to-end

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Spec Coverage

| Command | Covered | Notes |
|---|---|---|
| initnewcluster | Yes | Via ZkRegistrationManager.init_new_cluster |
| metaformat | Deferred | Needs layout delete + recreate |
| bookieformat | Deferred | Needs directory cleanup |
| nukeexistingcluster | Deferred | Dangerous, needs safety checks |
| listbookies | Yes | -rw, -ro, -a flags |
| bookieinfo | Deferred | Needs bookie connection |
| bookiesanity | Yes | Full write+read+verify cycle |
| decommissionbookie | Deferred | Complex orchestration |
| readjournal | Deferred | Needs journal scanner integration |
| readlog | Deferred | Needs entry log reader |
| localconsistencycheck | Deferred | Needs storage internals |
