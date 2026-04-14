//! Admin CLI commands for BookKeeper cluster management.
//! Matches Java's BookieShell and admin tools.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use bookkeeper_client::bookie_connection::BookieConnection;
use bookkeeper_metadata::traits::{LedgerManager, RegistrationManager};
use bookkeeper_metadata::zk::ledger_manager::ZkLedgerManager;
use bookkeeper_metadata::zk::registration::ZkRegistrationManager;
use bookkeeper_metadata::zk_path::ZkPaths;
use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::digest::{DigestManager, DigestType as ProtoDigestType};
use bookkeeper_protocol::entry::parse_entry;

fn convert_digest_type(
    dt: Option<bookkeeper_metadata::ledger_metadata::DigestType>,
) -> ProtoDigestType {
    match dt {
        Some(bookkeeper_metadata::ledger_metadata::DigestType::Crc32) => ProtoDigestType::Crc32,
        Some(bookkeeper_metadata::ledger_metadata::DigestType::Crc32c) => ProtoDigestType::Crc32c,
        Some(bookkeeper_metadata::ledger_metadata::DigestType::Hmac) => ProtoDigestType::HmacSha1,
        Some(bookkeeper_metadata::ledger_metadata::DigestType::Dummy) => ProtoDigestType::Dummy,
        None => ProtoDigestType::Crc32c,
    }
}

/// List all ledgers in the cluster with optional metadata.
/// Supports --bookieid filter matching Java's ListLedgersCommand.
pub async fn cmd_listledgers(
    zk: &str,
    ledger_root: &str,
    show_meta: bool,
    bookie_id_filter: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = zookeeper_client::Client::connect(zk).await?;
    let paths = ZkPaths::new(ledger_root);
    let lm = ZkLedgerManager::new(client, paths);

    let ledgers = lm.get_ledger_ranges().await?;

    // If filtering by bookie, we need to read metadata for each to check ensembles
    let filtered: Vec<i64> = if let Some(bookie_filter) = bookie_id_filter {
        let mut result = Vec::new();
        for lid in &ledgers {
            match lm.read_ledger_metadata(*lid).await {
                Ok(versioned) => {
                    if versioned.value().all_ensembles().values()
                        .any(|e| e.iter().any(|b| b == bookie_filter))
                    {
                        result.push(*lid);
                    }
                }
                Err(_) => continue,
            }
        }
        result
    } else {
        ledgers
    };

    println!("Ledgers ({} total):", filtered.len());

    for lid in &filtered {
        if show_meta {
            match lm.read_ledger_metadata(*lid).await {
                Ok(versioned) => {
                    let md = versioned.value();
                    println!(
                        "  Ledger {}: E={} W={} A={} state={:?} lastEntry={:?} length={:?}",
                        lid,
                        md.ensemble_size(),
                        md.write_quorum_size(),
                        md.ack_quorum_size(),
                        md.state(),
                        md.last_entry_id(),
                        md.length(),
                    );
                    for (first_entry, ensemble) in md.all_ensembles() {
                        println!("    Segment @{}: {:?}", first_entry, ensemble);
                    }
                }
                Err(e) => println!("  Ledger {}: error reading metadata: {e}", lid),
            }
        } else {
            println!("  {}", lid);
        }
    }
    Ok(())
}

/// Show metadata for a specific ledger.
pub async fn cmd_ledgerinfo(
    zk: &str,
    ledger_root: &str,
    ledger_id: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = zookeeper_client::Client::connect(zk).await?;
    let paths = ZkPaths::new(ledger_root);
    let lm = ZkLedgerManager::new(client, paths);

    let versioned = lm.read_ledger_metadata(ledger_id).await?;
    let md = versioned.value();

    println!("Ledger {} (version {})", ledger_id, versioned.version());
    println!("  State:       {:?}", md.state());
    println!("  Ensemble:    {}", md.ensemble_size());
    println!("  WriteQuorum: {}", md.write_quorum_size());
    println!("  AckQuorum:   {}", md.ack_quorum_size());
    println!("  LastEntryId: {:?}", md.last_entry_id());
    println!("  Length:       {:?}", md.length());
    if let Some(dt) = md.digest_type() {
        println!("  DigestType:  {:?}", dt);
    }
    println!("  HasPassword: {}", md.password().is_some());
    println!("  Ctime:       {}", md.ctime());
    println!("  Segments:");
    for (first_entry, ensemble) in md.all_ensembles() {
        println!("    @{}: {:?}", first_entry, ensemble);
    }
    Ok(())
}

/// List under-replicated ledgers from ZK.
/// Matches Java's ListUnderReplicatedCommand with filter flags.
pub async fn cmd_listunderreplicated(
    zk: &str,
    ledger_root: &str,
    missing_replica: Option<&str>,
    exclude_replica: Option<&str>,
    print_missing: bool,
    count_only: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = zookeeper_client::Client::connect(zk).await?;
    let paths = ZkPaths::new(ledger_root);

    let ur_path = format!("{}/ledgers", paths.underreplication());
    let mut results: Vec<(i64, Vec<String>)> = Vec::new();

    scan_underreplicated_full(&client, &ur_path, &mut results).await;

    // Apply filters
    let filtered: Vec<&(i64, Vec<String>)> = results.iter().filter(|(_, replicas)| {
        if let Some(mr) = missing_replica {
            if !replicas.iter().any(|r| r == mr) {
                return false;
            }
        }
        if let Some(er) = exclude_replica {
            if replicas.iter().any(|r| r == er) {
                return false;
            }
        }
        true
    }).collect();

    if count_only {
        println!("{}", filtered.len());
        return Ok(());
    }

    if filtered.is_empty() {
        println!("No under-replicated ledgers found.");
    } else {
        for (lid, replicas) in &filtered {
            if print_missing && !replicas.is_empty() {
                println!("  Under-replicated ledger: {lid}  missing: {}", replicas.join(", "));
            } else {
                println!("  Under-replicated ledger: {lid}");
            }
        }
        println!("\nTotal under-replicated ledgers: {}", filtered.len());
    }
    Ok(())
}

async fn scan_underreplicated_full(
    client: &zookeeper_client::Client,
    path: &str,
    results: &mut Vec<(i64, Vec<String>)>,
) {
    let children = match client.get_children(path).await {
        Ok((c, _)) => c,
        Err(zookeeper_client::Error::NoNode) => return,
        Err(e) => {
            eprintln!("  Warning: failed to read {path}: {e}");
            return;
        }
    };
    for child in &children {
        if let Some(suffix) = child.strip_prefix("urL") {
            if let Ok(lid) = suffix.parse::<i64>() {
                // Read the znode data to extract missing replicas
                let replicas = match client.get_data(&format!("{}/{}", path, child)).await {
                    Ok((data, _)) => {
                        match bookkeeper_replication::text_format::parse_underreplicated_ledger(&data) {
                            Ok(ur) => ur.replica,
                            Err(_) => Vec::new(),
                        }
                    }
                    Err(_) => Vec::new(),
                };
                results.push((lid, replicas));
                continue;
            }
        }
        let child_path = format!("{}/{}", path, child);
        Box::pin(scan_underreplicated_full(client, &child_path, results)).await;
    }
}

/// Show who is the current auditor.
pub async fn cmd_whoisauditor(
    zk: &str,
    ledger_root: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = zookeeper_client::Client::connect(zk).await?;
    let paths = ZkPaths::new(ledger_root);

    let election_path = format!("{}/auditorelection", paths.underreplication());
    let (children, _) = client.get_children(&election_path).await?;

    let mut votes: Vec<String> = children.into_iter().filter(|c| c.starts_with("V_")).collect();
    votes.sort();

    if let Some(leader) = votes.first() {
        let vote_path = format!("{}/{}", election_path, leader);
        let (data, _) = client.get_data(&vote_path).await?;
        let auditor_id = bookkeeper_replication::text_format::parse_auditor_vote(&data)
            .ok()
            .and_then(|v| v.bookie_id)
            .unwrap_or_else(|| String::from_utf8_lossy(&data).trim().to_string());
        println!("Auditor: {auditor_id}");
    } else {
        println!("No auditor elected.");
    }
    Ok(())
}

/// Enable or disable auto-recovery.
pub async fn cmd_toggle_recovery(
    zk: &str,
    ledger_root: &str,
    enable: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = zookeeper_client::Client::connect(zk).await?;
    let paths = ZkPaths::new(ledger_root);
    let disable_path = format!("{}/disable", paths.underreplication());

    if enable {
        // Delete the disable node to enable
        match client.delete(&disable_path, None).await {
            Ok(()) => println!("Auto-recovery ENABLED"),
            Err(zookeeper_client::Error::NoNode) => println!("Auto-recovery already enabled"),
            Err(e) => return Err(format!("Failed to enable: {e}").into()),
        }
    } else {
        // Create the disable node
        let options = zookeeper_client::CreateMode::Persistent
            .with_acls(zookeeper_client::Acls::anyone_all());
        match client.create(&disable_path, &[], &options).await {
            Ok(_) => println!("Auto-recovery DISABLED"),
            Err(zookeeper_client::Error::NodeExists) => println!("Auto-recovery already disabled"),
            Err(e) => return Err(format!("Failed to disable: {e}").into()),
        }
    }
    Ok(())
}

/// Show cluster info: bookie count, auditor, replication status.
pub async fn cmd_clusterinfo(
    zk: &str,
    ledger_root: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mgr = ZkRegistrationManager::new(zk, ledger_root).await?;

    let writable = mgr.get_writable_bookies().await?;
    let readonly = mgr.get_readonly_bookies().await?;

    println!("Cluster Info:");
    println!("  Writable bookies: {}", writable.len());
    for b in &writable {
        println!("    {b}");
    }
    println!("  Read-only bookies: {}", readonly.len());
    for b in &readonly {
        println!("    {b}");
    }

    let zk_client = mgr.zk_client();
    let paths = mgr.paths();

    // Auto-recovery status
    let disable_path = format!("{}/disable", paths.underreplication());
    let replication_enabled = zk_client.check_stat(&disable_path).await?.is_none();
    println!("  Auto-recovery: {}", if replication_enabled { "ENABLED" } else { "DISABLED" });

    // Auditor info
    let election_path = format!("{}/auditorelection", paths.underreplication());
    match zk_client.get_children(&election_path).await {
        Ok((children, _)) => {
            let mut votes: Vec<String> = children.into_iter()
                .filter(|c| c.starts_with("V_")).collect();
            votes.sort();
            if let Some(leader) = votes.first() {
                let vote_path = format!("{}/{}", election_path, leader);
                if let Ok((data, _)) = zk_client.get_data(&vote_path).await {
                    let auditor_id = bookkeeper_replication::text_format::parse_auditor_vote(&data)
                        .ok()
                        .and_then(|v| v.bookie_id)
                        .unwrap_or_else(|| String::from_utf8_lossy(&data).trim().to_string());
                    println!("  Auditor:     {auditor_id}");
                } else {
                    println!("  Auditor:     (elected but data unreadable)");
                }
            } else {
                println!("  Auditor:     (none elected)");
            }
        }
        Err(_) => {
            println!("  Auditor:     (election path not found)");
        }
    }

    // Under-replication status
    let ur_path = format!("{}/ledgers", paths.underreplication());
    let has_ur = has_underreplicated_ledgers(zk_client, &ur_path).await;
    println!("  Under-replicated: {}", if has_ur { "YES" } else { "no" });

    // Instance ID
    if let Ok(Some(id)) = mgr.get_cluster_instance_id().await {
        println!("  Instance ID: {id}");
    }

    Ok(())
}

async fn has_underreplicated_ledgers(
    client: &zookeeper_client::Client,
    path: &str,
) -> bool {
    let children = match client.get_children(path).await {
        Ok((c, _)) => c,
        Err(_) => return false,
    };
    for child in &children {
        if child.starts_with("urL") {
            return true;
        }
        let child_path = format!("{}/{}", path, child);
        if Box::pin(has_underreplicated_ledgers(client, &child_path)).await {
            return true;
        }
    }
    false
}

/// Delete a ledger's metadata from the metadata store.
/// Matches Java's DeleteLedgerCommand.
pub async fn cmd_deleteledger(
    zk: &str,
    ledger_root: &str,
    ledger_id: i64,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = zookeeper_client::Client::connect(zk).await?;
    let paths = ZkPaths::new(ledger_root);
    let lm = ZkLedgerManager::new(client, paths);

    // Read metadata first to show what we're deleting
    let versioned = lm.read_ledger_metadata(ledger_id).await?;
    let md = versioned.value();

    if !force {
        println!("Ledger {} — state={:?} E={} W={} A={}",
            ledger_id, md.state(), md.ensemble_size(),
            md.write_quorum_size(), md.ack_quorum_size());
        println!("WARNING: This will permanently delete ledger metadata.");
        println!("Use --force to confirm deletion.");
        return Ok(());
    }

    // Use version -1 (ANY) to match Java's unconditional delete behavior
    lm.remove_ledger_metadata(ledger_id, -1).await?;
    println!("Ledger {ledger_id} deleted successfully.");
    Ok(())
}

/// Read entries from a ledger via a specific bookie or auto-discover from metadata.
/// Matches Java's ReadLedgerCommand.
pub async fn cmd_readledger(
    zk: &str,
    ledger_root: &str,
    ledger_id: i64,
    first_entry: i64,
    last_entry: Option<i64>,
    bookie_addr: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = zookeeper_client::Client::connect(zk).await?;
    let paths = ZkPaths::new(ledger_root);
    let lm = ZkLedgerManager::new(client, paths);

    let versioned = lm.read_ledger_metadata(ledger_id).await?;
    let md = versioned.value();
    let digest_type = convert_digest_type(md.digest_type());
    let password = md.password().unwrap_or(&[]);
    let dm = DigestManager::new(digest_type).with_password(password);

    let last = last_entry.unwrap_or_else(|| md.last_entry_id().unwrap_or(0));
    println!("Reading ledger {ledger_id} entries [{first_entry}..{last}]");
    println!("  State={:?} E={} W={} A={}", md.state(),
        md.ensemble_size(), md.write_quorum_size(), md.ack_quorum_size());

    if let Some(addr) = bookie_addr {
        // Read directly from a specific bookie.
        // Java uses LongStream.range (exclusive end) for bookie-direct reads.
        let socket_addr: SocketAddr = addr.parse()?;
        let conn = Arc::new(BookieConnection::connect(socket_addr).await?);
        for eid in first_entry..last {
            read_and_print_entry(&conn, &dm, ledger_id, eid).await;
        }
    } else {
        // Auto-discover bookies from metadata ensemble
        let mut conn_cache: HashMap<String, Arc<BookieConnection>> = HashMap::new();
        for eid in first_entry..=last {
            let ensemble = match md.ensemble_at(eid) {
                Some(e) => e,
                None => {
                    println!("  Entry {eid}: no ensemble segment found");
                    continue;
                }
            };
            let mut read_ok = false;
            for bookie in ensemble {
                let conn = match conn_cache.get(bookie) {
                    Some(c) => c.clone(),
                    None => {
                        let addr: SocketAddr = match bookie.parse() {
                            Ok(a) => a,
                            Err(_) => continue,
                        };
                        match BookieConnection::connect(addr).await {
                            Ok(c) => {
                                let c = Arc::new(c);
                                conn_cache.insert(bookie.clone(), c.clone());
                                c
                            }
                            Err(_) => continue,
                        }
                    }
                };
                if read_and_print_entry(&conn, &dm, ledger_id, eid).await {
                    read_ok = true;
                    break;
                }
            }
            if !read_ok {
                println!("  Entry {eid}: NOT FOUND on any bookie");
            }
        }
    }
    Ok(())
}

async fn read_and_print_entry(
    conn: &BookieConnection,
    dm: &DigestManager,
    ledger_id: i64,
    entry_id: i64,
) -> bool {
    use bookkeeper_protocol::v2::response::V2Response;
    match conn.send_read_entry(FLAG_NONE, ledger_id, entry_id, None).await {
        Ok(V2Response::Read { error_code, data, .. }) if error_code == V2_EOK => {
            if let Some(raw) = data {
                match parse_entry(dm, raw.clone()) {
                    Ok(entry) => {
                        let preview = if entry.payload.len() <= 64 {
                            format!("{:?}", &entry.payload[..])
                        } else {
                            format!("{:?}... ({} bytes)", &entry.payload[..64], entry.payload.len())
                        };
                        println!("  Entry {}: lac={} length={} payload={preview}",
                            entry_id, entry.lac, entry.length);
                        return true;
                    }
                    Err(e) => {
                        println!("  Entry {entry_id}: digest error: {e}");
                        return false;
                    }
                }
            }
            println!("  Entry {entry_id}: empty data");
            false
        }
        Ok(V2Response::Read { error_code, .. }) => {
            println!("  Entry {entry_id}: error code {error_code}");
            false
        }
        _ => false,
    }
}

/// Get a bookie's cookie from ZooKeeper.
/// Matches Java's GetCookieCommand.
pub async fn cmd_cookie_get(
    zk: &str,
    ledger_root: &str,
    bookie_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mgr = ZkRegistrationManager::new(zk, ledger_root).await?;
    match mgr.read_cookie(bookie_id).await? {
        Some((data, version)) => {
            let text = String::from_utf8_lossy(&data);
            println!("Cookie for {bookie_id} (version {version}):");
            println!("{text}");
        }
        None => {
            println!("No cookie found for {bookie_id}");
        }
    }
    Ok(())
}

/// Delete a bookie's cookie from ZooKeeper.
/// Matches Java's DeleteCookieCommand.
pub async fn cmd_cookie_delete(
    zk: &str,
    ledger_root: &str,
    bookie_id: &str,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mgr = ZkRegistrationManager::new(zk, ledger_root).await?;
    match mgr.read_cookie(bookie_id).await? {
        Some((data, version)) => {
            if !force {
                let text = String::from_utf8_lossy(&data);
                println!("Cookie for {bookie_id} (version {version}):");
                println!("{text}");
                println!("\nWARNING: This will permanently delete the cookie.");
                println!("Use --force to confirm deletion.");
                return Ok(());
            }
            mgr.delete_cookie(bookie_id, version).await?;
            println!("Cookie for {bookie_id} deleted successfully.");
        }
        None => {
            println!("No cookie found for {bookie_id}");
        }
    }
    Ok(())
}

/// Recover ledgers from a failed bookie, or recover a specific ledger.
/// Matches Java's RecoverCommand: --bokiesrc for full-bookie recovery,
/// --ledger-id for single-ledger recovery.
pub async fn cmd_recover(
    zk: &str,
    ledger_root: &str,
    ledger_id: Option<i64>,
    bookie_src: Option<&str>,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(src) = bookie_src {
        return cmd_recover_bookie(zk, ledger_root, src, dry_run).await;
    }
    let ledger_id = ledger_id.ok_or("specify --ledger-id or --bookie-src")?;
    cmd_recover_ledger(zk, ledger_root, ledger_id, dry_run).await
}

/// Recover all ledgers from a failed bookie using fragment-level replication.
/// Matches Java's BookKeeperAdmin.recoverBookieData().
async fn cmd_recover_bookie(
    zk: &str,
    ledger_root: &str,
    bookie_src: &str,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = zookeeper_client::Client::connect(zk).await?;
    let paths = ZkPaths::new(ledger_root);
    let lm = ZkLedgerManager::new(client.clone(), paths.clone());

    let all_ledgers = lm.get_ledger_ranges().await?;
    println!("Scanning {} ledgers for bookie {bookie_src}...", all_ledgers.len());

    let mut affected = Vec::new();
    for lid in &all_ledgers {
        match lm.read_ledger_metadata(*lid).await {
            Ok(versioned) => {
                let md = versioned.value();
                if md.all_ensembles().values().any(|e| e.iter().any(|b| b == bookie_src)) {
                    affected.push(*lid);
                }
            }
            Err(_) => continue,
        }
    }
    println!("Found {} affected ledgers.", affected.len());

    if dry_run {
        for lid in &affected {
            let versioned = lm.read_ledger_metadata(*lid).await?;
            let md = versioned.value();
            println!("  Ledger {lid}: state={:?} E={} W={} A={}",
                md.state(), md.ensemble_size(), md.write_quorum_size(), md.ack_quorum_size());
        }
        println!("[dry-run] Would recover {} ledgers.", affected.len());
        return Ok(());
    }

    // Get available bookies for replacement
    let mgr = ZkRegistrationManager::new(zk, ledger_root).await?;
    let available = mgr.get_writable_bookies().await?;

    let mut recovered = 0;
    let mut errors = 0;

    for lid in &affected {
        match recover_ledger_fragments(&lm, *lid, bookie_src, &available).await {
            Ok(()) => {
                println!("  Ledger {lid}: recovered");
                recovered += 1;
            }
            Err(e) => {
                eprintln!("  Ledger {lid}: failed — {e}");
                errors += 1;
            }
        }
    }

    println!("\n{recovered} ledgers recovered, {errors} errors.");
    Ok(())
}

/// Recover a single ledger by finding fragments on a bad bookie, copying to replacements,
/// and updating ensemble metadata. Works for both open and closed ledgers.
async fn recover_ledger_fragments(
    lm: &ZkLedgerManager,
    ledger_id: i64,
    bad_bookie: &str,
    available: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
    use std::collections::HashSet;

    let versioned = lm.read_ledger_metadata(ledger_id).await?;
    let md = versioned.value();
    let version = versioned.version();

    let password = md.password().unwrap_or(&[]);
    let digest_type = convert_digest_type(md.digest_type());
    let master_key = bookkeeper_protocol::digest::generate_master_key(password);

    let ensemble_size = md.ensemble_size() as usize;
    let write_quorum = md.write_quorum_size() as usize;
    let ack_quorum = md.ack_quorum_size() as usize;
    let schedule = RoundRobinDistributionSchedule::new(ensemble_size, write_quorum, ack_quorum);

    // If open and bad bookie is in last segment, fence the ledger first
    let md = if md.state() == bookkeeper_metadata::ledger_metadata::LedgerState::Open {
        let ensembles = md.all_ensembles();
        let (last_seg_entry, last_ensemble) = ensembles.iter().next_back()
            .ok_or("no ensemble segments found")?;
        if last_ensemble.iter().any(|b| b == bad_bookie) {
            use bookkeeper_client::recovery::RecoveryHandle;
            let dm = DigestManager::new(digest_type).with_password(password);
            let mut conns: Vec<Arc<BookieConnection>> = Vec::new();
            for addr in last_ensemble {
                match BookieConnection::connect(addr.parse()?).await {
                    Ok(c) => conns.push(Arc::new(c)),
                    Err(_) => {
                        // Bad bookie may be down — create a dummy that will fail reads
                        return Err(format!("cannot connect to {addr} for fencing").into());
                    }
                }
            }
            let mut recovery = RecoveryHandle::new(
                ledger_id, last_ensemble.clone(), schedule.clone(), dm, master_key,
            );
            let (last_entry, total_length) = recovery.recover(&conns, *last_seg_entry).await?;
            let closed_md = bookkeeper_metadata::ledger_metadata::LedgerMetadataBuilder::from_existing(md)
                .with_closed_state(last_entry, total_length)
                .build()?;
            let new_versioned = lm.write_ledger_metadata(ledger_id, &closed_md, version).await?;
            let _version = new_versioned.version();
            new_versioned.into_value()
        } else {
            md.clone()
        }
    } else {
        md.clone()
    };

    let last_entry_id = md.last_entry_id().unwrap_or(-1);
    if last_entry_id < 0 {
        return Ok(()); // empty ledger
    }

    let ensembles = md.all_ensembles();
    let keys: Vec<i64> = ensembles.keys().copied().collect();
    let mut conn_cache: HashMap<String, Arc<BookieConnection>> = HashMap::new();

    for (i, &seg_start) in keys.iter().enumerate() {
        let seg_end = if i + 1 < keys.len() { keys[i + 1] - 1 } else { last_entry_id };
        let ensemble = &ensembles[&seg_start];

        // Find the bad bookie's index in this ensemble
        let bad_idx = match ensemble.iter().position(|b| b == bad_bookie) {
            Some(idx) => idx,
            None => continue,
        };

        // Pick a replacement bookie
        let exclude: HashSet<&str> = ensemble.iter().map(|s| s.as_str()).collect();
        let replacement = available.iter()
            .find(|b| !exclude.contains(b.as_str()))
            .ok_or("no available replacement bookie")?;

        // Connect to good bookies and replacement
        for addr in ensemble.iter().chain(std::iter::once(replacement)) {
            if !conn_cache.contains_key(addr) {
                if let Ok(conn) = BookieConnection::connect(addr.parse()?).await {
                    conn_cache.insert(addr.clone(), Arc::new(conn));
                }
            }
        }

        // Copy entries that belong to the bad bookie
        for eid in seg_start..=seg_end {
            let write_set = schedule.write_set(eid);
            if !write_set.contains(&bad_idx) {
                continue; // this entry wasn't on the bad bookie
            }

            // Read from a good replica
            let mut entry_data = None;
            for &bookie_idx in &write_set {
                if bookie_idx == bad_idx || bookie_idx >= ensemble.len() {
                    continue;
                }
                if let Some(conn) = conn_cache.get(&ensemble[bookie_idx]) {
                    use bookkeeper_protocol::v2::response::V2Response;
                    match conn.send_read_entry(FLAG_NONE, ledger_id, eid, Some(master_key)).await {
                        Ok(V2Response::Read { error_code, data, .. }) if error_code == V2_EOK => {
                            entry_data = data;
                            break;
                        }
                        _ => continue,
                    }
                }
            }

            let data = entry_data.ok_or_else(||
                format!("cannot read entry {eid} from any good replica"))?;

            // Write to replacement
            if let Some(conn) = conn_cache.get(replacement) {
                use bookkeeper_protocol::v2::response::V2Response;
                let resp = conn.send_add_entry(
                    FLAG_RECOVERY_ADD | FLAG_HIGH_PRIORITY, master_key, data,
                ).await?;
                match resp {
                    V2Response::Add { error_code, .. } if error_code == V2_EOK => {}
                    V2Response::Add { error_code, .. } => {
                        return Err(format!("write to replacement failed: error {error_code}").into());
                    }
                    _ => return Err("unexpected response from replacement".into()),
                }
            }
        }

        // CAS retry: re-read current metadata and apply targeted replacement
        for _ in 0..5 {
            let fresh = lm.read_ledger_metadata(ledger_id).await?;
            let current = fresh.value();
            let cur_ensemble = match current.all_ensembles().get(&seg_start) {
                Some(e) => e.clone(),
                None => break, // segment removed
            };
            if cur_ensemble.get(bad_idx).map(|b| b.as_str()) != Some(bad_bookie) {
                break; // already replaced by another process
            }
            let mut new_ens = cur_ensemble;
            new_ens[bad_idx] = replacement.clone();

            let mut builder = bookkeeper_metadata::ledger_metadata::LedgerMetadataBuilder::from_existing(current);
            builder.clear_ensembles();
            for (fe, ens) in current.all_ensembles() {
                if *fe == seg_start {
                    builder = builder.new_ensemble_entry(*fe, new_ens.clone());
                } else {
                    builder = builder.new_ensemble_entry(*fe, ens.clone());
                }
            }
            let new_md = builder.build()?;
            match lm.write_ledger_metadata(ledger_id, &new_md, fresh.version()).await {
                Ok(_) => break,
                Err(bookkeeper_metadata::ledger_metadata::MetadataError::VersionConflict { .. }) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    Ok(())
}

/// Recover a single specific ledger: fence, forward-read, re-replicate, close.
async fn cmd_recover_ledger(
    zk: &str,
    ledger_root: &str,
    ledger_id: i64,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_client::distribution::RoundRobinDistributionSchedule;
    use bookkeeper_client::recovery::RecoveryHandle;

    let client = zookeeper_client::Client::connect(zk).await?;
    let paths = ZkPaths::new(ledger_root);
    let lm = ZkLedgerManager::new(client, paths);

    let versioned = lm.read_ledger_metadata(ledger_id).await?;
    let md = versioned.value();

    println!("Recovering ledger {ledger_id}:");
    println!("  State:  {:?}", md.state());
    println!("  E={} W={} A={}", md.ensemble_size(), md.write_quorum_size(), md.ack_quorum_size());

    if md.state() == bookkeeper_metadata::ledger_metadata::LedgerState::Closed {
        println!("  Ledger is already CLOSED, nothing to recover.");
        return Ok(());
    }

    if dry_run {
        println!("  [dry-run] Would fence and recover this ledger.");
        for (first_entry, ensemble) in md.all_ensembles() {
            println!("    Segment @{first_entry}: {ensemble:?}");
        }
        return Ok(());
    }

    // Get the last ensemble segment for recovery
    let (last_segment_entry, ensemble) = md.all_ensembles().iter().next_back()
        .ok_or("no ensemble segments found")?;

    let digest_type = convert_digest_type(md.digest_type());
    let password = md.password().unwrap_or(&[]);
    let dm = DigestManager::new(digest_type).with_password(password);
    let master_key = bookkeeper_protocol::digest::generate_master_key(password);

    let schedule = RoundRobinDistributionSchedule::new(
        md.ensemble_size() as usize,
        md.write_quorum_size() as usize,
        md.ack_quorum_size() as usize,
    );

    // Connect to all bookies in the ensemble
    let mut conns: Vec<Arc<BookieConnection>> = Vec::new();
    for bookie in ensemble {
        let addr: SocketAddr = bookie.parse()?;
        let conn = BookieConnection::connect(addr).await?;
        conns.push(Arc::new(conn));
    }

    let mut recovery = RecoveryHandle::new(
        ledger_id,
        ensemble.clone(),
        schedule,
        dm,
        master_key,
    );

    println!("  Fencing ledger...");
    let (last_entry, total_length) = recovery.recover(&conns, *last_segment_entry).await?;

    println!("  Recovery complete: lastEntry={last_entry} length={total_length}");

    // Update metadata to CLOSED state
    let closed_md = bookkeeper_metadata::ledger_metadata::LedgerMetadataBuilder::from_existing(md)
        .with_closed_state(last_entry, total_length)
        .build()?;
    lm.write_ledger_metadata(ledger_id, &closed_md, versioned.version()).await?;
    println!("  Ledger {ledger_id} closed in metadata store.");

    Ok(())
}

/// Decommission a bookie: mark all its ledgers as under-replicated, wait for
/// replication to complete, then delete its cookie.
/// Matches Java's DecommissionCommand.
pub async fn cmd_decommissionbookie(
    zk: &str,
    ledger_root: &str,
    bookie_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mgr = ZkRegistrationManager::new(zk, ledger_root).await?;

    // Step 1: Verify the bookie is not still registered (must be shut down first)
    let writable = mgr.get_writable_bookies().await?;
    let readonly = mgr.get_readonly_bookies().await?;
    if writable.contains(&bookie_id.to_string()) || readonly.contains(&bookie_id.to_string()) {
        return Err(format!(
            "Bookie {bookie_id} is still registered. Shut it down before decommissioning."
        ).into());
    }
    println!("Bookie {bookie_id} is not registered (good — it's shut down).");

    // Step 2: Trigger audit to kick the auditor (matches Java's decommission flow)
    {
        let trigger_client = zookeeper_client::Client::connect(zk).await?;
        let trigger_paths = ZkPaths::new(ledger_root);
        let delay_path = format!("{}/lostBookieRecoveryDelay", trigger_paths.underreplication());
        let current = match trigger_client.get_data(&delay_path).await {
            Ok((data, _)) => data,
            Err(_) => b"0".to_vec(),
        };
        let _ = trigger_client.set_data(&delay_path, &current, None).await;
        println!("Triggered audit.");
    }

    // Step 3: Find all ledgers that have this bookie in their ensemble
    let zk_client = mgr.zk_client().clone();
    let paths = ZkPaths::new(ledger_root);
    let lm = ZkLedgerManager::new(zk_client.clone(), paths.clone());

    let all_ledgers = lm.get_ledger_ranges().await?;
    let mut affected_ledgers: Vec<i64> = Vec::new();

    println!("Scanning {} ledgers for bookie {bookie_id}...", all_ledgers.len());
    for lid in &all_ledgers {
        match lm.read_ledger_metadata(*lid).await {
            Ok(versioned) => {
                let md = versioned.value();
                let has_bookie = md.all_ensembles().values().any(|ensemble| {
                    ensemble.iter().any(|b| b == bookie_id)
                });
                if has_bookie {
                    affected_ledgers.push(*lid);
                }
            }
            Err(_) => continue,
        }
    }

    if affected_ledgers.is_empty() {
        println!("No ledgers found on bookie {bookie_id}.");
    } else {
        println!("Found {} ledgers on bookie {bookie_id}.", affected_ledgers.len());

        // Step 4: Mark all affected ledgers as under-replicated
        let ur_mgr = bookkeeper_replication::underreplication::ZkUnderreplicationManager::new(
            zk_client.clone(),
            paths.clone(),
            "admin-cli".to_string(),
        ).await?;

        for lid in &affected_ledgers {
            match ur_mgr.mark_ledger_underreplicated(*lid, &[bookie_id.to_string()]).await {
                Ok(()) => {}
                Err(e) => eprintln!("  Warning: failed to mark ledger {lid}: {e}"),
            }
        }
        println!("Marked {} ledgers as under-replicated.", affected_ledgers.len());

        // Step 5: Wait for all affected ledgers to be replicated away
        println!("Waiting for replication to complete...");
        let mut remaining = affected_ledgers.clone();
        loop {
            if remaining.is_empty() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

            let mut still_remaining = Vec::new();
            for lid in &remaining {
                match lm.read_ledger_metadata(*lid).await {
                    Ok(versioned) => {
                        let md = versioned.value();
                        let still_has_bookie = md.all_ensembles().values().any(|ensemble| {
                            ensemble.iter().any(|b| b == bookie_id)
                        });
                        if still_has_bookie {
                            still_remaining.push(*lid);
                        }
                    }
                    Err(_) => {
                        // Ledger deleted or error — consider it done
                    }
                }
            }
            if still_remaining.len() != remaining.len() {
                println!("  Progress: {}/{} ledgers remaining",
                    still_remaining.len(), affected_ledgers.len());
            }
            remaining = still_remaining;
        }
        println!("All ledgers replicated away from {bookie_id}.");
    }

    // Step 6: Delete the cookie
    match mgr.read_cookie(bookie_id).await? {
        Some((_, version)) => {
            mgr.delete_cookie(bookie_id, version).await?;
            println!("Cookie for {bookie_id} deleted.");
        }
        None => {
            println!("No cookie found for {bookie_id} (already deleted).");
        }
    }

    println!("Bookie {bookie_id} decommissioned successfully.");
    Ok(())
}

/// Validate local bookie storage: iterate all indexed entries and verify
/// they exist at the correct location in entry log files.
/// Matches Java's LedgerStorage.localConsistencyCheck().
pub async fn cmd_localconsistencycheck(
    data_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_storage::entry_log::format::EntryLocation;
    use bookkeeper_storage::index::entry_location::EntryLocationIndex;
    use bookkeeper_storage::index::ledger_index::LedgerMetadataIndex;
    use std::collections::HashMap;
    use std::io::{Read as _, Seek, SeekFrom};

    let base = Path::new(data_dir);
    let locations_path = base.join("locations");
    let ledgers_path = base.join("ledgers");
    let entry_log_dir = base.join("entry_logs");

    if !locations_path.exists() {
        return Err(format!("Locations index not found at {}", locations_path.display()).into());
    }
    if !ledgers_path.exists() {
        return Err(format!("Ledgers index not found at {}", ledgers_path.display()).into());
    }

    let ledger_index = LedgerMetadataIndex::open(&ledgers_path, None)?;
    let location_index = EntryLocationIndex::open(&locations_path, None)?;

    let all_ledgers = ledger_index.list_all_ledgers()?;
    println!("Checking {} ledgers in local storage...", all_ledgers.len());

    let mut errors = 0u64;
    let mut entries_checked = 0u64;
    let mut log_file_cache: HashMap<u32, Option<std::fs::File>> = HashMap::new();

    for ledger_id in &all_ledgers {
        // Verify ledger metadata exists
        match ledger_index.get(*ledger_id)? {
            Some(data) => {
                if !data.exists {
                    println!("  Ledger {ledger_id}: metadata says does not exist");
                    errors += 1;
                    continue;
                }
            }
            None => {
                println!("  Ledger {ledger_id}: in list but metadata missing");
                errors += 1;
                continue;
            }
        }

        // Iterate ALL entries for this ledger in the location index
        let entries = location_index.iter_ledger_entries(*ledger_id)?;

        for (entry_id, location) in &entries {
            if *location == 0 {
                println!("  ({ledger_id}, {entry_id}): zero location");
                errors += 1;
                continue;
            }

            let loc = EntryLocation::from_u64(*location);
            let log_id = loc.log_id();
            let position = loc.position() as u64;

            // Open entry log file (cached)
            let file = log_file_cache.entry(log_id).or_insert_with(|| {
                let log_path = entry_log_dir.join(format!("{:x}.log", log_id));
                std::fs::File::open(&log_path).ok()
            });

            match file {
                None => {
                    println!("  ({ledger_id}, {entry_id}): entry log {:x}.log not found", log_id);
                    errors += 1;
                }
                Some(f) => {
                    // position is an absolute file offset; read size(4)+ledgerId(8)+entryId(8)
                    let offset = position - 4; // back up 4 bytes to include size prefix
                    if f.seek(SeekFrom::Start(offset)).is_err() {
                        println!("  ({ledger_id}, {entry_id}): seek to offset {offset} failed in {:x}.log", log_id);
                        errors += 1;
                        continue;
                    }
                    let mut header_buf = [0u8; 20]; // 4 size + 8 ledgerId + 8 entryId
                    if f.read_exact(&mut header_buf).is_err() {
                        println!("  ({ledger_id}, {entry_id}): read failed at offset {offset} in {:x}.log", log_id);
                        errors += 1;
                        continue;
                    }
                    let stored_ledger_id = i64::from_be_bytes(header_buf[4..12].try_into().unwrap());
                    let stored_entry_id = i64::from_be_bytes(header_buf[12..20].try_into().unwrap());

                    if stored_ledger_id != *ledger_id {
                        println!("  ({ledger_id}, {entry_id}): ledger ID mismatch in entry log — got {stored_ledger_id}");
                        errors += 1;
                    } else if stored_entry_id != *entry_id {
                        println!("  ({ledger_id}, {entry_id}): entry ID mismatch in entry log — got {stored_entry_id}");
                        errors += 1;
                    }
                }
            }
            entries_checked += 1;
        }
    }

    println!("\nConsistency check complete:");
    println!("  Ledgers checked: {}", all_ledgers.len());
    println!("  Entries verified: {entries_checked}");
    if errors == 0 {
        println!("  Result: PASSED (no inconsistencies found)");
    } else {
        println!("  Result: FAILED ({errors} inconsistencies found)");
    }
    Ok(())
}

/// Get or set the lostBookieRecoveryDelay (seconds).
/// Matches Java's LostBookieRecoveryDelayCommand.
pub async fn cmd_lostbookierecoverydelay(
    zk: &str,
    ledger_root: &str,
    set_value: Option<i32>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = zookeeper_client::Client::connect(zk).await?;
    let paths = ZkPaths::new(ledger_root);
    let delay_path = format!("{}/lostBookieRecoveryDelay", paths.underreplication());

    match set_value {
        Some(seconds) => {
            let data = seconds.to_string().into_bytes();
            let options = zookeeper_client::CreateMode::Persistent
                .with_acls(zookeeper_client::Acls::anyone_all());
            match client.set_data(&delay_path, &data, None).await {
                Ok(_) => {
                    println!("LostBookieRecoveryDelay set to {seconds} seconds.");
                }
                Err(zookeeper_client::Error::NoNode) => {
                    // Create the node
                    client.create(&delay_path, &data, &options).await?;
                    println!("LostBookieRecoveryDelay set to {seconds} seconds.");
                }
                Err(e) => return Err(format!("Failed to set: {e}").into()),
            }
        }
        None => {
            match client.get_data(&delay_path).await {
                Ok((data, _)) => {
                    let text = String::from_utf8_lossy(&data);
                    println!("LostBookieRecoveryDelay: {text} seconds");
                }
                Err(zookeeper_client::Error::NoNode) => {
                    println!("LostBookieRecoveryDelay: not set (using default)");
                }
                Err(e) => return Err(format!("Failed to read: {e}").into()),
            }
        }
    }
    Ok(())
}

/// Trigger an immediate audit cycle by resetting the lostBookieRecoveryDelay.
/// Matches Java's TriggerAuditCommand: checks replication is enabled and
/// auditor is elected before triggering.
pub async fn cmd_triggeraudit(
    zk: &str,
    ledger_root: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = zookeeper_client::Client::connect(zk).await?;
    let paths = ZkPaths::new(ledger_root);

    // Pre-check 1: Verify auto-recovery is enabled
    let disable_path = format!("{}/disable", paths.underreplication());
    if client.check_stat(&disable_path).await?.is_some() {
        return Err("Auto-recovery is disabled. Enable it before triggering audit.".into());
    }

    // Pre-check 2: Verify an auditor is elected
    let election_path = format!("{}/auditorelection", paths.underreplication());
    let has_auditor = match client.get_children(&election_path).await {
        Ok((children, _)) => children.iter().any(|c| c.starts_with("V_")),
        Err(_) => false,
    };
    if !has_auditor {
        return Err("No auditor is currently elected. Cannot trigger audit.".into());
    }

    // Read current value, then re-set it to trigger an immediate audit
    let delay_path = format!("{}/lostBookieRecoveryDelay", paths.underreplication());
    let current = match client.get_data(&delay_path).await {
        Ok((data, _)) => data,
        Err(zookeeper_client::Error::NoNode) => b"0".to_vec(),
        Err(e) => return Err(format!("Failed to read recovery delay: {e}").into()),
    };

    match client.set_data(&delay_path, &current, None).await {
        Ok(_) => println!("Audit triggered (reset lostBookieRecoveryDelay)."),
        Err(zookeeper_client::Error::NoNode) => {
            let options = zookeeper_client::CreateMode::Persistent
                .with_acls(zookeeper_client::Acls::anyone_all());
            client.create(&delay_path, &current, &options).await?;
            println!("Audit triggered (created lostBookieRecoveryDelay).");
        }
        Err(e) => return Err(format!("Failed to trigger audit: {e}").into()),
    }
    Ok(())
}

/// Format (wipe and reinitialize) cluster metadata in ZooKeeper.
/// Matches Java's MetaFormatCommand: deletes existing metadata then
/// recreates a clean structure ready for use.
pub async fn cmd_metaformat(
    zk: &str,
    ledger_root: &str,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !force {
        println!("WARNING: This will FORMAT all BookKeeper metadata at {ledger_root}.");
        println!("All ledger data references will be lost.");
        println!("Use --force to confirm.");
        return Ok(());
    }

    let client = zookeeper_client::Client::connect(zk).await?;

    // Step 1: Recursively delete all children under ledger_root
    delete_zk_recursive(&client, ledger_root).await?;
    println!("Old metadata deleted.");

    // Step 2: Reinitialize the cluster structure (like Java's format)
    let mgr = ZkRegistrationManager::new(zk, ledger_root).await?;
    let layout = bookkeeper_metadata::traits::LedgerLayout {
        manager_factory_class: "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory"
            .to_string(),
        manager_version: 1,
    };
    let instance_id = uuid::Uuid::new_v4().to_string();
    mgr.init_new_cluster(&layout, &instance_id).await?;
    println!("Metadata reformatted successfully.");
    println!("  New instance ID: {instance_id}");
    Ok(())
}

async fn delete_zk_recursive(
    client: &zookeeper_client::Client,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let children = match client.get_children(path).await {
        Ok((c, _)) => c,
        Err(zookeeper_client::Error::NoNode) => return Ok(()),
        Err(e) => return Err(format!("Failed to list {path}: {e}").into()),
    };
    for child in &children {
        let child_path = format!("{}/{}", path, child);
        Box::pin(delete_zk_recursive(client, &child_path)).await?;
    }
    match client.delete(path, None).await {
        Ok(()) => Ok(()),
        Err(zookeeper_client::Error::NoNode) => Ok(()),
        Err(e) => Err(format!("Failed to delete {path}: {e}").into()),
    }
}

/// Print the last journal checkpoint mark.
/// Java reads lastMark from ALL ledger directories and takes the MAX.
/// Also checks journal directories for lastMark files.
/// Matches Java's LastMarkCommand.
pub async fn cmd_lastmark(
    data_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_storage::journal::format::LastMark;

    // Search for lastMark in the given directory and common subdirectories
    let base = Path::new(data_dir);
    // Search for both Rust (.lastMark) and Java (lastMark, current/lastMark) filenames
    let candidates = [
        base.join(".lastMark"),
        base.join("lastMark"),
        base.join("current").join("lastMark"),
    ];

    let mut best_mark: Option<LastMark> = None;
    let mut found_path: Option<std::path::PathBuf> = None;

    for path in &candidates {
        if path.exists() {
            let data = std::fs::read(path)?;
            if let Ok(mark) = LastMark::from_bytes(&data) {
                let is_better = match &best_mark {
                    None => true,
                    Some(prev) => mark.log_id() > prev.log_id()
                        || (mark.log_id() == prev.log_id() && mark.offset() > prev.offset()),
                };
                if is_better {
                    best_mark = Some(mark);
                    found_path = Some(path.clone());
                }
            }
        }
    }

    match best_mark {
        Some(mark) => {
            println!("Last journal mark (from {}):", found_path.unwrap().display());
            println!("  Journal Id - {}({:x}.txn), Pos - {}",
                mark.log_id(), mark.log_id(), mark.offset());
        }
        None => {
            println!("No lastMark file found in {}", base.display());
        }
    }
    Ok(())
}

/// Scan and dump entries from a journal file.
/// Matches Java's ReadJournalCommand.
pub async fn cmd_readjournal(
    journal_file: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_storage::journal::format::{JournalHeader, scan_journal_entries};

    let data = std::fs::read(journal_file)?;
    if data.len() < 8 {
        return Err("journal file too short".into());
    }

    let header = JournalHeader::from_bytes(&data)?;
    println!("Journal file: {journal_file}");
    println!("  Version: {}", header.version());
    println!("  Header size: {} bytes", header.header_size());

    let body = &data[header.header_size()..];
    let entries = scan_journal_entries(body, header.version());
    println!("  Entries: {}", entries.len());

    for (i, entry) in entries.iter().enumerate() {
        if entry.len() >= 16 {
            let ledger_id = i64::from_be_bytes(entry[0..8].try_into().unwrap());
            let entry_id = i64::from_be_bytes(entry[8..16].try_into().unwrap());
            // Check for meta-entries
            if entry_id == -0x1000 {
                println!("  [{i}] MASTER_KEY ledger={ledger_id} ({} bytes)", entry.len());
            } else if entry_id == -0x2000 {
                println!("  [{i}] FENCE_KEY ledger={ledger_id} ({} bytes)", entry.len());
            } else {
                let lac = if entry.len() >= 24 {
                    i64::from_be_bytes(entry[16..24].try_into().unwrap())
                } else { -1 };
                println!("  [{i}] ledger={ledger_id} entry={entry_id} lac={lac} ({} bytes)",
                    entry.len());
            }
        } else {
            println!("  [{i}] ({} bytes — too short to parse)", entry.len());
        }
    }
    Ok(())
}

/// Scan and dump entries from an entry log file.
/// Matches Java's ReadLogCommand.
pub async fn cmd_readlog(
    entry_log_file: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use bookkeeper_storage::entry_log::format::{EntryLogHeader, LOGFILE_HEADER_SIZE,
        scan_entry_log_entries};

    let data = std::fs::read(entry_log_file)?;
    if data.len() < LOGFILE_HEADER_SIZE {
        return Err("entry log file too short".into());
    }

    let header = EntryLogHeader::from_bytes(&data[..LOGFILE_HEADER_SIZE])?;
    println!("Entry log file: {entry_log_file}");
    println!("  Version:        {}", header.version());
    println!("  Ledger map at:  {}", header.ledgers_map_offset());
    println!("  Ledger count:   {}", header.ledgers_count());

    let body = &data[LOGFILE_HEADER_SIZE..];
    let entries = scan_entry_log_entries(body);
    println!("  Entries: {}", entries.len());

    for (i, entry) in entries.iter().enumerate() {
        if entry.len() >= 16 {
            let ledger_id = i64::from_be_bytes(entry[0..8].try_into().unwrap());
            let entry_id = i64::from_be_bytes(entry[8..16].try_into().unwrap());
            let lac = if entry.len() >= 24 {
                i64::from_be_bytes(entry[16..24].try_into().unwrap())
            } else { -1 };
            println!("  [{i}] ledger={ledger_id} entry={entry_id} lac={lac} ({} bytes)",
                entry.len());
        } else {
            println!("  [{i}] ({} bytes — too short)", entry.len());
        }
        if i >= 1000 {
            println!("  ... (truncated after 1000 entries)");
            break;
        }
    }
    Ok(())
}

/// List journal, entry log, and index files on disk.
/// Matches Java's ListFilesOnDiscCommand.
pub async fn cmd_listfilesondisc(
    data_dir: &str,
    journal_dir: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let base = Path::new(data_dir);
    let j_dir = journal_dir.map(Path::new).unwrap_or(base);

    // Journal files
    println!("Journal files ({}):", j_dir.display());
    if let Ok(entries) = std::fs::read_dir(j_dir) {
        let mut journal_files: Vec<_> = entries
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name();
                let name = name.to_string_lossy();
                name.ends_with(".txn") || name == "lastMark" || name == ".lastMark"
            })
            .collect();
        journal_files.sort_by_key(|e| e.file_name());
        for f in &journal_files {
            let meta = f.metadata()?;
            println!("  {} ({} bytes)", f.file_name().to_string_lossy(), meta.len());
        }
        if journal_files.is_empty() {
            println!("  (none)");
        }
    }

    // Entry log files
    let entry_log_dir = base.join("entry_logs");
    println!("\nEntry log files ({}):", entry_log_dir.display());
    if entry_log_dir.exists() {
        if let Ok(entries) = std::fs::read_dir(&entry_log_dir) {
            let mut log_files: Vec<_> = entries
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().ends_with(".log"))
                .collect();
            log_files.sort_by_key(|e| e.file_name());
            for f in &log_files {
                let meta = f.metadata()?;
                println!("  {} ({} bytes)", f.file_name().to_string_lossy(), meta.len());
            }
            if log_files.is_empty() {
                println!("  (none)");
            }
        }
    } else {
        println!("  (directory not found)");
    }

    // Index directories
    let locations_dir = base.join("locations");
    let ledgers_dir = base.join("ledgers");
    println!("\nIndex directories:");
    if locations_dir.exists() {
        let size = dir_size(&locations_dir);
        println!("  locations/ ({size} bytes)");
    } else {
        println!("  locations/ (not found)");
    }
    if ledgers_dir.exists() {
        let size = dir_size(&ledgers_dir);
        println!("  ledgers/   ({size} bytes)");
    } else {
        println!("  ledgers/   (not found)");
    }

    Ok(())
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() {
                    total += meta.len();
                } else if meta.is_dir() {
                    total += dir_size(&entry.path());
                }
            }
        }
    }
    total
}

/// Replace a bookie address in all ledger metadata.
/// Matches Java's UpdateBookieInLedgerCommand with CAS retry loop.
pub async fn cmd_updatebookieinledger(
    zk: &str,
    ledger_root: &str,
    old_bookie: &str,
    new_bookie: &str,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Pre-check: verify old bookie is NOT still registered (matches Java)
    let mgr = ZkRegistrationManager::new(zk, ledger_root).await?;
    let writable = mgr.get_writable_bookies().await?;
    let readonly = mgr.get_readonly_bookies().await?;
    if writable.contains(&old_bookie.to_string()) || readonly.contains(&old_bookie.to_string()) {
        return Err(format!(
            "Bookie {old_bookie} is still registered. Shut it down before updating ledger metadata."
        ).into());
    }

    let zk_client = mgr.zk_client().clone();
    let paths = ZkPaths::new(ledger_root);
    let lm = ZkLedgerManager::new(zk_client, paths);

    let all_ledgers = lm.get_ledger_ranges().await?;
    println!("Scanning {} ledgers for bookie {old_bookie}...", all_ledgers.len());

    let mut updated = 0;
    let mut errors = 0;
    let max_retries = 5;

    for lid in &all_ledgers {
        let mut retries = 0;
        loop {
            let versioned = match lm.read_ledger_metadata(*lid).await {
                Ok(v) => v,
                Err(_) => break,
            };
            let md = versioned.value();

            let has_old = md.all_ensembles().values().any(|ensemble| {
                ensemble.iter().any(|b| b == old_bookie)
            });
            if !has_old {
                break;
            }

            if dry_run {
                println!("  Ledger {lid}: would replace {old_bookie} -> {new_bookie}");
                for (first_entry, ensemble) in md.all_ensembles() {
                    if ensemble.contains(&old_bookie.to_string()) {
                        println!("    Segment @{first_entry}: {ensemble:?}");
                    }
                }
                updated += 1;
                break;
            }

            // Build updated metadata with replaced bookie
            let mut builder = bookkeeper_metadata::ledger_metadata::LedgerMetadataBuilder::from_existing(md);
            builder.clear_ensembles();
            for (first_entry, ensemble) in md.all_ensembles() {
                let new_ensemble: Vec<String> = ensemble.iter().map(|b| {
                    if b == old_bookie { new_bookie.to_string() } else { b.clone() }
                }).collect();
                builder = builder.new_ensemble_entry(*first_entry, new_ensemble);
            }

            match builder.build() {
                Ok(new_md) => {
                    match lm.write_ledger_metadata(*lid, &new_md, versioned.version()).await {
                        Ok(_) => {
                            println!("  Ledger {lid}: updated");
                            updated += 1;
                            break;
                        }
                        Err(bookkeeper_metadata::ledger_metadata::MetadataError::VersionConflict { .. }) => {
                            retries += 1;
                            if retries >= max_retries {
                                eprintln!("  Ledger {lid}: version conflict after {max_retries} retries");
                                errors += 1;
                                break;
                            }
                            // CAS retry: re-read and try again
                            continue;
                        }
                        Err(e) => {
                            eprintln!("  Ledger {lid}: failed to update: {e}");
                            errors += 1;
                            break;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("  Ledger {lid}: failed to build metadata: {e}");
                    errors += 1;
                    break;
                }
            }
        }
    }

    println!("\n{} ledgers {}, {} errors",
        updated, if dry_run { "would be updated" } else { "updated" }, errors);
    Ok(())
}

/// Print the cluster instance ID.
/// Matches Java's WhatIsInstanceIdCommand.
pub async fn cmd_whatisinstanceid(
    zk: &str,
    ledger_root: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mgr = ZkRegistrationManager::new(zk, ledger_root).await?;
    match mgr.get_cluster_instance_id().await? {
        Some(id) => println!("Instance ID: {id}"),
        None => println!("No instance ID set."),
    }
    Ok(())
}

/// Format (wipe) local bookie data directories.
/// Matches Java's BookieFormatCommand.
pub async fn cmd_bookieformat(
    data_dir: &str,
    journal_dir: Option<&str>,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !force {
        println!("WARNING: This will DELETE all bookie data in {data_dir}.");
        if let Some(j) = journal_dir {
            println!("  Journal dir: {j}");
        }
        println!("Use --force to confirm.");
        return Ok(());
    }

    let base = Path::new(data_dir);

    // Remove all contents of the data directory (matching Java's cleanDir)
    if let Ok(entries) = std::fs::read_dir(base) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                std::fs::remove_dir_all(&path)?;
                println!("  Removed {}", path.display());
            } else {
                std::fs::remove_file(&path)?;
                println!("  Removed {}", path.display());
            }
        }
    }

    // Remove all contents of journal directory if separate
    let j_dir = journal_dir.map(Path::new);
    if let Some(j_dir) = j_dir {
        if j_dir != base {
            if let Ok(entries) = std::fs::read_dir(j_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        std::fs::remove_dir_all(&path)?;
                    } else {
                        std::fs::remove_file(&path)?;
                    }
                    println!("  Removed {}", path.display());
                }
            }
        }
    }

    println!("Bookie data formatted successfully.");
    Ok(())
}

/// Nuke (delete) all cluster metadata and state.
/// Matches Java's NukeExistingClusterCommand.
/// Requires either --force or --instanceid (not both) as a safety check.
pub async fn cmd_nukeexistingcluster(
    zk: &str,
    ledger_root: &str,
    force: bool,
    expected_instance_id: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    if !force && expected_instance_id.is_none() {
        println!("WARNING: This will DELETE all BookKeeper metadata at {ledger_root}.");
        println!("This is a DESTRUCTIVE operation that cannot be undone.");
        println!("Use --force or --instanceid <id> to confirm.");
        return Ok(());
    }

    if force && expected_instance_id.is_some() {
        return Err("Specify either --force or --instanceid, not both.".into());
    }

    let mgr = ZkRegistrationManager::new(zk, ledger_root).await?;
    let actual_instance_id = mgr.get_cluster_instance_id().await?;

    // Validate instance ID if provided
    if let Some(expected) = expected_instance_id {
        match &actual_instance_id {
            Some(actual) if actual == expected => {
                println!("Instance ID verified: {actual}");
            }
            Some(actual) => {
                return Err(format!(
                    "Instance ID mismatch: expected '{expected}', actual '{actual}'"
                ).into());
            }
            None => {
                return Err("Cluster has no instance ID set.".into());
            }
        }
    } else if let Some(ref id) = actual_instance_id {
        println!("Nuking cluster with instance ID: {id}");
    }

    // Check no bookies are registered
    let writable = mgr.get_writable_bookies().await?;
    let readonly = mgr.get_readonly_bookies().await?;
    if !writable.is_empty() || !readonly.is_empty() {
        return Err(format!(
            "Cannot nuke: {} writable and {} read-only bookies still registered. Shut them down first.",
            writable.len(), readonly.len()
        ).into());
    }

    // Wipe metadata
    let client = zookeeper_client::Client::connect(zk).await?;
    delete_zk_recursive(&client, ledger_root).await?;
    println!("Cluster metadata at {ledger_root} nuked successfully.");
    Ok(())
}
