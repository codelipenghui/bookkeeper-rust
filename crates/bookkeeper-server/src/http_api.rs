use std::path::{Path, PathBuf};
use std::sync::Arc;
use axum::{Router, Json, extract::State, http::StatusCode, response::IntoResponse};
use axum::extract::Query;
use serde::{Deserialize, Serialize};
use bookkeeper_storage::db_ledger_storage::CURRENT_DIR;
use crate::state::{BookieState, BookieStateManager};

/// Shared GC status for the gc_details HTTP endpoint.
/// Updated by the GC thread, read by the HTTP handler.
/// Matches Java's GarbageCollectionStatus fields.
///
/// Wrapped in a single `Arc` (one heap allocation) rather than per-field
/// `Arc<AtomicXxx>` (seven allocations). All fields are plain atomics —
/// safe for concurrent access through the shared `Arc`.
#[derive(Clone)]
pub struct GcStatus(Arc<GcStatusInner>);

pub struct GcStatusInner {
    pub force_compacting: std::sync::atomic::AtomicBool,
    pub major_compacting: std::sync::atomic::AtomicBool,
    pub minor_compacting: std::sync::atomic::AtomicBool,
    pub last_major_compaction_time: std::sync::atomic::AtomicI64,
    pub last_minor_compaction_time: std::sync::atomic::AtomicI64,
    pub major_compaction_counter: std::sync::atomic::AtomicI64,
    pub minor_compaction_counter: std::sync::atomic::AtomicI64,
}

impl Default for GcStatus {
    fn default() -> Self {
        Self(Arc::new(GcStatusInner {
            force_compacting: std::sync::atomic::AtomicBool::new(false),
            major_compacting: std::sync::atomic::AtomicBool::new(false),
            minor_compacting: std::sync::atomic::AtomicBool::new(false),
            last_major_compaction_time: std::sync::atomic::AtomicI64::new(0),
            last_minor_compaction_time: std::sync::atomic::AtomicI64::new(0),
            major_compaction_counter: std::sync::atomic::AtomicI64::new(0),
            minor_compaction_counter: std::sync::atomic::AtomicI64::new(0),
        }))
    }
}

impl std::ops::Deref for GcStatus {
    type Target = GcStatusInner;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Clone)]
pub struct HttpState {
    pub state_manager: Arc<BookieStateManager>,
    pub storage: Option<Arc<dyn bookkeeper_storage::ledger_storage::LedgerStorage>>,
    pub ledger_dirs: Vec<String>,
    pub journal_dir: String,
    pub zk_client: Option<zookeeper_client::Client>,
    pub zk_paths: Option<bookkeeper_metadata::zk_path::ZkPaths>,
    pub bookie_id: Option<String>,
    /// Shared GC suspension flags (shared with the GC loop in server.rs).
    pub gc_suspend_major: Arc<std::sync::atomic::AtomicBool>,
    pub gc_suspend_minor: Arc<std::sync::atomic::AtomicBool>,
    /// Shared GC force flags — set to true to trigger immediate GC on next loop iteration.
    pub gc_force_major: Arc<std::sync::atomic::AtomicBool>,
    pub gc_force_minor: Arc<std::sync::atomic::AtomicBool>,
    /// Shared GC status for the gc_details endpoint.
    pub gc_status: GcStatus,
}

pub fn create_router(http_state: HttpState) -> Router {
    let router = Router::new()
        // Existing
        .route("/heartbeat", axum::routing::get(heartbeat))
        .route("/api/v1/bookie/is_ready", axum::routing::get(is_ready))
        .route("/api/v1/bookie/state", axum::routing::get(bookie_state))
        .route("/metrics", axum::routing::get(prometheus_metrics))
        // Bookie state and info
        .route("/api/v1/bookie/state/readonly",
            axum::routing::get(get_readonly).put(set_readonly))
        .route("/api/v1/bookie/info", axum::routing::get(bookie_info))
        .route("/api/v1/bookie/last_log_mark", axum::routing::get(last_log_mark))
        .route("/api/v1/bookie/list_disk_file", axum::routing::get(list_disk_files))
        .route("/api/v1/bookie/sanity", axum::routing::get(bookie_sanity))
        .route("/api/v1/bookie/gc_details", axum::routing::get(gc_details))
        // GC and compaction
        .route("/api/v1/bookie/gc", axum::routing::put(trigger_gc))
        .route("/api/v1/bookie/gc/suspend_compaction",
            axum::routing::get(get_suspend_compaction).put(suspend_compaction))
        .route("/api/v1/bookie/gc/resume_compaction",
            axum::routing::put(resume_compaction))
        .route("/api/v1/bookie/entry_location_compact",
            axum::routing::put(trigger_entry_location_compact))
        .route("/api/v1/bookie/expand_storage",
            axum::routing::put(expand_storage_endpoint))
        .route("/api/v1/bookie/list_bookie_info",
            axum::routing::get(list_bookie_info_endpoint))
        // Configuration
        .route("/api/v1/config/server_config", axum::routing::get(get_server_config))
        // Cluster info (requires ZK)
        .route("/api/v1/bookie/cluster_info", axum::routing::get(cluster_info))
        .route("/api/v1/bookie/list_bookies", axum::routing::get(list_bookies_endpoint))
        // Ledger operations
        .route("/api/v1/ledger/list", axum::routing::get(list_ledgers_endpoint))
        .route("/api/v1/ledger/delete", axum::routing::delete(delete_ledger_endpoint))
        .route("/api/v1/ledger/metadata", axum::routing::get(ledger_metadata_endpoint))
        .route("/api/v1/ledger/read", axum::routing::get(read_ledger_endpoint))
        // Autorecovery (requires ZK)
        .route("/api/v1/autorecovery/who_is_auditor",
            axum::routing::get(who_is_auditor_endpoint))
        .route("/api/v1/autorecovery/status",
            axum::routing::get(get_autorecovery_status).put(set_autorecovery_status))
        .route("/api/v1/autorecovery/lost_bookie_recovery_delay",
            axum::routing::get(get_recovery_delay).put(set_recovery_delay))
        .route("/api/v1/autorecovery/trigger_audit",
            axum::routing::put(trigger_audit_endpoint))
        .route("/api/v1/autorecovery/list_under_replicated_ledger",
            axum::routing::get(list_under_replicated_endpoint))
        .route("/api/v1/autorecovery/bookie",
            axum::routing::put(recover_bookie_endpoint))
        .route("/api/v1/autorecovery/decommission",
            axum::routing::put(decommission_endpoint));

    // Profiling endpoint — only present in builds compiled with the
    // `profiling` feature (e.g. the debug-runtime Docker image).
    #[cfg(feature = "profiling")]
    let router = router.route(
        "/debug/pprof/flamegraph",
        axum::routing::get(pprof_flamegraph),
    );

    router.with_state(http_state)
}

// ---------------------------------------------------------------------------
// Existing endpoints
// ---------------------------------------------------------------------------

async fn heartbeat() -> &'static str {
    "OK"
}

async fn is_ready(State(state): State<HttpState>) -> impl IntoResponse {
    if state.state_manager.is_readable() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[derive(Serialize)]
struct BookieStateResponse {
    running: bool,
    #[serde(rename = "readOnly")]
    read_only: bool,
    #[serde(rename = "shuttingDown")]
    shutting_down: bool,
    #[serde(rename = "availableForHighPriorityWrites")]
    available_for_high_priority_writes: bool,
}

async fn bookie_state(State(state): State<HttpState>) -> Json<BookieStateResponse> {
    let current = state.state_manager.state();
    Json(BookieStateResponse {
        running: current == BookieState::Running,
        read_only: current == BookieState::ReadOnly,
        shutting_down: current == BookieState::ShuttingDown,
        available_for_high_priority_writes: current == BookieState::Running,
    })
}

async fn prometheus_metrics() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        crate::metrics::encode_metrics(),
    )
}

// ---------------------------------------------------------------------------
// /api/v1/bookie/state/readonly — GET/PUT read-only mode
// Matches Java's BookieStateReadOnlyService
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ReadOnlyResponse {
    #[serde(rename = "readOnly")]
    read_only: bool,
}

#[derive(Deserialize)]
struct ReadOnlyRequest {
    #[serde(rename = "readOnly")]
    read_only: bool,
}

async fn get_readonly(State(state): State<HttpState>) -> Json<ReadOnlyResponse> {
    Json(ReadOnlyResponse {
        read_only: state.state_manager.state() == BookieState::ReadOnly,
    })
}

async fn set_readonly(
    State(state): State<HttpState>,
    Json(body): Json<ReadOnlyRequest>,
) -> impl IntoResponse {
    if body.read_only {
        // Manual read-only: set the flag so the disk monitor won't auto-recover
        // while the operator intends the bookie to stay read-only.
        // Matches Java's BookieImpl.doTransitionToReadOnlyMode(forceReadOnly=true).
        state.state_manager.transition_to_read_only(true);
        (StatusCode::OK, Json(serde_json::json!({"readOnly": true})))
    } else {
        // Clearing manual read-only: also clear the manually_read_only flag so
        // the disk monitor can resume auto-recovery behavior on disk space events.
        // Matches Java's BookieStateManager where setReadOnly(false) clears
        // forceReadOnly and allows LedgerDirsMonitor to manage state again.
        state.state_manager.clear_manual_read_only();
        state.state_manager.transition_to(BookieState::Running);
        (StatusCode::OK, Json(serde_json::json!({"readOnly": false})))
    }
}

// ---------------------------------------------------------------------------
// /api/v1/bookie/info — disk space info
// Matches Java's BookieInfoService
// ---------------------------------------------------------------------------

async fn bookie_info(State(state): State<HttpState>) -> Json<serde_json::Value> {
    let mut total_space: u64 = 0;
    let mut free_space: u64 = 0;

    for dir in &state.ledger_dirs {
        if let Ok(stats) = disk_stats(dir) {
            total_space += stats.0;
            free_space += stats.1;
        }
    }

    // Java BookieInfoService field names: freeSpace, totalSpace
    Json(serde_json::json!({
        "freeSpace": free_space,
        "totalSpace": total_space,
    }))
}

#[cfg(unix)]
fn disk_stats(path: &str) -> Result<(u64, u64), std::io::Error> {
    use std::ffi::CString;
    let c_path = CString::new(path).map_err(|e| std::io::Error::other(e))?;
    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let total = stat.f_blocks as u64 * stat.f_frsize as u64;
        let free = stat.f_bavail as u64 * stat.f_frsize as u64;
        Ok((total, free))
    }
}

#[cfg(not(unix))]
fn disk_stats(_path: &str) -> Result<(u64, u64), std::io::Error> {
    Ok((0, 0))
}

// ---------------------------------------------------------------------------
// /api/v1/bookie/last_log_mark — journal checkpoint position
// Matches Java's GetLastLogMarkService
// ---------------------------------------------------------------------------

async fn last_log_mark(State(state): State<HttpState>) -> Json<serde_json::Value> {
    use bookkeeper_storage::checkpoint;

    // Search for .lastMark in ledger dirs and journal dir
    let mut dirs: Vec<PathBuf> = state.ledger_dirs.iter().map(PathBuf::from).collect();
    dirs.push(PathBuf::from(&state.journal_dir));

    match checkpoint::find_latest_last_mark(&dirs) {
        Ok(mark) => {
            // Java format: key starts with "LastLogMark: "
            let key = format!("LastLogMark: Journal Id - {}({:x}.txn)",
                mark.log_id(), mark.log_id());
            let value = format!("Pos - {}", mark.offset());
            Json(serde_json::json!({ key: value }))
        }
        Err(e) => {
            Json(serde_json::json!({"error": format!("{e}")}))
        }
    }
}

// ---------------------------------------------------------------------------
// /api/v1/bookie/list_disk_file — list files on disk
// Matches Java's ListDiskFilesService
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ListDiskFilesQuery {
    file_type: Option<String>,
}

async fn list_disk_files(
    State(state): State<HttpState>,
    Query(query): Query<ListDiskFilesQuery>,
) -> Json<serde_json::Value> {
    let show_all = query.file_type.is_none();
    let ft = query.file_type.as_deref().unwrap_or("");

    let mut result = serde_json::Map::new();

    // Java separator is tab, not comma
    if show_all || ft == "journal" {
        // Java filter: only *.txn files
        let journal_files = list_files_in_dir(&state.journal_dir, |name| {
            name.ends_with(".txn")
        });
        result.insert("journal files".into(), serde_json::Value::String(
            journal_files.join("\t")
        ));
    }

    if show_all || ft == "entrylog" {
        if let Some(ref storage) = state.storage {
            if let Some(log_dir) = storage.entry_log_dir() {
                let log_files = list_files_in_dir(
                    &log_dir.to_string_lossy(), |name| name.ends_with(".log")
                );
                result.insert("entrylog files".into(), serde_json::Value::String(
                    log_files.join("\t")
                ));
            }
        }
    }

    if show_all || ft == "index" {
        // Aggregate index info across ALL ledger dirs (not just last one).
        let mut all_index_info = Vec::new();
        for dir in &state.ledger_dirs {
            let current = Path::new(dir).join(CURRENT_DIR);
            let locations = current.join("locations");
            let ledgers = current.join("ledgers");
            if locations.exists() {
                all_index_info.push(format!("{}/locations ({} bytes)", dir, dir_size(&locations)));
            }
            if ledgers.exists() {
                all_index_info.push(format!("{}/ledgers ({} bytes)", dir, dir_size(&ledgers)));
            }
        }
        result.insert("index files".into(), serde_json::Value::String(
            all_index_info.join("\t")
        ));
    }

    Json(serde_json::Value::Object(result))
}

fn list_files_in_dir(dir: &str, filter: impl Fn(&str) -> bool) -> Vec<String> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if filter(&name) {
                files.push(name);
            }
        }
    }
    // Also check current/ subdirectory (Java convention)
    let current = Path::new(dir).join(CURRENT_DIR);
    if let Ok(entries) = std::fs::read_dir(&current) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if filter(&name) {
                files.push(format!("current/{name}"));
            }
        }
    }
    files.sort();
    files
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

// ---------------------------------------------------------------------------
// /api/v1/bookie/gc — trigger garbage collection
// Matches Java's TriggerGCService
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
struct TriggerGcRequest {
    #[serde(rename = "forceMajor", default)]
    force_major: Option<bool>,
    #[serde(rename = "forceMinor", default)]
    force_minor: Option<bool>,
}

async fn trigger_gc(
    State(state): State<HttpState>,
    body: Option<Json<TriggerGcRequest>>,
) -> impl IntoResponse {
    use std::sync::atomic::Ordering;
    let req = body.map(|Json(b)| b).unwrap_or_default();
    // No body or both unset = full force (Java's forceGC() behavior)
    let force_major = req.force_major.unwrap_or(true);
    let force_minor = req.force_minor.unwrap_or(true);

    if force_major {
        state.gc_force_major.store(true, Ordering::Relaxed);
    }
    if force_minor {
        state.gc_force_minor.store(true, Ordering::Relaxed);
    }
    (StatusCode::OK, Json(serde_json::json!({"triggered": true})))
}

// ---------------------------------------------------------------------------
// /api/v1/bookie/entry_location_compact — trigger RocksDB compaction
// Matches Java's TriggerLocationCompactService
// ---------------------------------------------------------------------------

async fn trigger_entry_location_compact(State(state): State<HttpState>) -> impl IntoResponse {
    if let Some(ref storage) = state.storage {
        let storage = storage.clone();
        tokio::task::spawn_blocking(move || {
            let _ = storage.compact_entry_locations();
        });
        (StatusCode::OK, Json(serde_json::json!({"triggered": true})))
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "no storage"})))
    }
}

// ---------------------------------------------------------------------------
// /api/v1/config/server_config — return server configuration
// Matches Java's ConfigurationService (GET only)
// ---------------------------------------------------------------------------

async fn get_server_config(State(state): State<HttpState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "ledgerDirectories": state.ledger_dirs,
        "journalDirectory": state.journal_dir,
        "readOnly": state.state_manager.state() == BookieState::ReadOnly,
    }))
}

// ---------------------------------------------------------------------------
// /api/v1/bookie/sanity — local sanity check
// Matches Java's BookieSanityService
// ---------------------------------------------------------------------------

async fn bookie_sanity(State(state): State<HttpState>) -> impl IntoResponse {
    let read_only = state.state_manager.state() == BookieState::ReadOnly;
    let passed = state.state_manager.is_readable() && state.storage.is_some();
    (StatusCode::OK, Json(serde_json::json!({
        "passed": passed,
        "readOnly": read_only,
    })))
}

// ---------------------------------------------------------------------------
// /api/v1/bookie/gc_details — GC status
// Matches Java's GCDetailsService
// ---------------------------------------------------------------------------

async fn gc_details(State(state): State<HttpState>) -> Json<serde_json::Value> {
    use std::sync::atomic::Ordering;
    let gs = &state.gc_status;
    Json(serde_json::json!([{
        "forceCompacting": gs.force_compacting.load(Ordering::Relaxed),
        "majorCompacting": gs.major_compacting.load(Ordering::Relaxed),
        "minorCompacting": gs.minor_compacting.load(Ordering::Relaxed),
        "lastMajorCompactionTime": gs.last_major_compaction_time.load(Ordering::Relaxed),
        "lastMinorCompactionTime": gs.last_minor_compaction_time.load(Ordering::Relaxed),
        "majorCompactionCounter": gs.major_compaction_counter.load(Ordering::Relaxed),
        "minorCompactionCounter": gs.minor_compaction_counter.load(Ordering::Relaxed),
    }]))
}

// ---------------------------------------------------------------------------
// /api/v1/bookie/cluster_info — cluster overview
// Matches Java's ClusterInfoService
// ---------------------------------------------------------------------------

async fn cluster_info(State(state): State<HttpState>) -> impl IntoResponse {
    let (client, paths) = match (&state.zk_client, &state.zk_paths) {
        (Some(c), Some(p)) => (c, p),
        _ => return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "error": "ZK not configured"
        }))),
    };

    let writable = match client.get_children(paths.available()).await {
        Ok((children, _)) => children.into_iter().filter(|c| c != "readonly").collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };
    let readonly = match client.get_children(paths.available_readonly()).await {
        Ok((children, _)) => children,
        Err(_) => Vec::new(),
    };

    // Auto-recovery enabled?
    let disable_path = format!("{}/disable", paths.underreplication());
    let auto_recovery = client.check_stat(&disable_path).await.ok().flatten().is_none();

    // Auditor
    let election_path = format!("{}/auditorelection", paths.underreplication());
    let auditor_id = match client.get_children(&election_path).await {
        Ok((mut children, _)) => {
            children.retain(|c| c.starts_with("V_"));
            children.sort();
            if let Some(leader) = children.first() {
                let vote_path = format!("{}/{}", election_path, leader);
                match client.get_data(&vote_path).await {
                    Ok((data, _)) => {
                        let text = String::from_utf8_lossy(&data);
                        text.lines()
                            .find_map(|l| l.trim().strip_prefix("bookieId: \"")
                                .and_then(|r| r.strip_suffix('"')))
                            .map(|s| s.to_string())
                            .unwrap_or_default()
                    }
                    Err(_) => String::new(),
                }
            } else {
                String::new()
            }
        }
        Err(_) => String::new(),
    };

    // Under-replicated ledgers (presence check only)
    let ur_path = format!("{}/ledgers", paths.underreplication());
    let cluster_under_replicated = has_underreplicated_ledgers(client, &ur_path).await;

    (StatusCode::OK, Json(serde_json::json!({
        "auditorElected": !auditor_id.is_empty(),
        "auditorId": auditor_id,
        "clusterUnderReplicated": cluster_under_replicated,
        "ledgerReplicationEnabled": auto_recovery,
        "totalBookiesCount": writable.len() + readonly.len(),
        "writableBookiesCount": writable.len(),
        "readonlyBookiesCount": readonly.len(),
        "unavailableBookiesCount": 0,
    })))
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

// ---------------------------------------------------------------------------
// /api/v1/bookie/list_bookies — list bookies in cluster
// Matches Java's ListBookiesService
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ListBookiesQuery {
    #[serde(rename = "type")]
    bookie_type: Option<String>,
    print_hostnames: Option<String>,
}

async fn list_bookies_endpoint(
    State(state): State<HttpState>,
    Query(query): Query<ListBookiesQuery>,
) -> impl IntoResponse {
    let (client, paths) = match (&state.zk_client, &state.zk_paths) {
        (Some(c), Some(p)) => (c, p),
        _ => return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "error": "ZK not configured"
        }))),
    };

    let show_ro = query.bookie_type.as_deref() == Some("ro");
    let print_hostnames = query.print_hostnames.as_deref() == Some("true");
    let path = if show_ro {
        paths.available_readonly().to_string()
    } else {
        paths.available().to_string()
    };

    let bookies = match client.get_children(&path).await {
        Ok((c, _)) => c.into_iter().filter(|c| c != "readonly").collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };

    // Java format: { bookieId: hostname-or-null }
    let mut map = serde_json::Map::new();
    for b in &bookies {
        let value = if print_hostnames {
            // Resolve hostname from "host:port"
            let host = b.split(':').next().unwrap_or(b);
            serde_json::Value::String(host.to_string())
        } else {
            serde_json::Value::Null
        };
        map.insert(b.clone(), value);
    }
    (StatusCode::OK, Json(serde_json::Value::Object(map)))
}

// ---------------------------------------------------------------------------
// /api/v1/ledger/list — list ledgers (local)
// Matches Java's ListLedgerService (subset — local only)
// ---------------------------------------------------------------------------

async fn list_ledgers_endpoint(State(state): State<HttpState>) -> impl IntoResponse {
    let storage = match &state.storage {
        Some(s) => s,
        None => return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "error": "no storage"
        }))),
    };

    let ledgers = match storage.get_active_ledgers() {
        Ok(l) => l,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
            "error": format!("{e}")
        }))),
    };

    let mut map = serde_json::Map::new();
    for lid in &ledgers {
        map.insert(lid.to_string(), serde_json::Value::String(String::new()));
    }
    (StatusCode::OK, Json(serde_json::Value::Object(map)))
}

// ---------------------------------------------------------------------------
// /api/v1/ledger/delete — delete a ledger (local + ZK)
// Matches Java's DeleteLedgerService
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct DeleteLedgerQuery {
    ledger_id: i64,
}

async fn delete_ledger_endpoint(
    State(state): State<HttpState>,
    Query(query): Query<DeleteLedgerQuery>,
) -> impl IntoResponse {
    let (client, paths) = match (&state.zk_client, &state.zk_paths) {
        (Some(c), Some(p)) => (c, p),
        _ => return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "error": "ZK not configured"
        }))),
    };

    use bookkeeper_metadata::traits::LedgerManager;
    let lm = bookkeeper_metadata::zk::ledger_manager::ZkLedgerManager::new(
        client.clone(), paths.clone(),
    );

    match lm.remove_ledger_metadata(query.ledger_id, -1).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"deleted": true}))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
            "error": format!("{e}")
        }))),
    }
}

// ---------------------------------------------------------------------------
// /api/v1/autorecovery/who_is_auditor
// Matches Java's WhoIsAuditorService
// ---------------------------------------------------------------------------

async fn who_is_auditor_endpoint(State(state): State<HttpState>) -> impl IntoResponse {
    let (client, paths) = match (&state.zk_client, &state.zk_paths) {
        (Some(c), Some(p)) => (c, p),
        _ => return (StatusCode::SERVICE_UNAVAILABLE, "ZK not configured".to_string()),
    };

    let election_path = format!("{}/auditorelection", paths.underreplication());
    let (mut children, _) = match client.get_children(&election_path).await {
        Ok(c) => c,
        Err(_) => return (StatusCode::NOT_FOUND, "No auditor elected".to_string()),
    };
    children.retain(|c| c.starts_with("V_"));
    children.sort();

    let auditor = if let Some(leader) = children.first() {
        let vote_path = format!("{}/{}", election_path, leader);
        match client.get_data(&vote_path).await {
            Ok((data, _)) => {
                let text = String::from_utf8_lossy(&data);
                text.lines()
                    .find_map(|l| l.trim().strip_prefix("bookieId: \"")
                        .and_then(|r| r.strip_suffix('"')))
                    .map(|s| s.to_string())
                    .unwrap_or_default()
            }
            Err(_) => String::new(),
        }
    } else {
        return (StatusCode::NOT_FOUND, "No auditor elected".to_string());
    };

    if auditor.is_empty() {
        (StatusCode::NOT_FOUND, "No auditor elected".to_string())
    } else {
        // Java format: plain string body
        (StatusCode::OK, format!("Auditor: {auditor}"))
    }
}

// ---------------------------------------------------------------------------
// /api/v1/autorecovery/status — get/set auto-recovery enabled
// Matches Java's AutoRecoveryStatusService
// ---------------------------------------------------------------------------

async fn get_autorecovery_status(State(state): State<HttpState>) -> impl IntoResponse {
    let (client, paths) = match (&state.zk_client, &state.zk_paths) {
        (Some(c), Some(p)) => (c, p),
        _ => return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "error": "ZK not configured"
        }))),
    };

    let disable_path = format!("{}/disable", paths.underreplication());
    let enabled = client.check_stat(&disable_path).await.ok().flatten().is_none();
    (StatusCode::OK, Json(serde_json::json!({"enabled": enabled})))
}

#[derive(Deserialize)]
struct AutorecoveryStatusRequest {
    enabled: bool,
}

async fn set_autorecovery_status(
    State(state): State<HttpState>,
    Query(query): Query<AutorecoveryStatusRequest>,
) -> impl IntoResponse {
    let body = query;
    let (client, paths) = match (&state.zk_client, &state.zk_paths) {
        (Some(c), Some(p)) => (c, p),
        _ => return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "error": "ZK not configured"
        }))),
    };

    let disable_path = format!("{}/disable", paths.underreplication());
    if body.enabled {
        // Delete disable node
        match client.delete(&disable_path, None).await {
            Ok(()) | Err(zookeeper_client::Error::NoNode) => {}
            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "error": format!("{e}")
            }))),
        }
    } else {
        let options = zookeeper_client::CreateMode::Persistent
            .with_acls(zookeeper_client::Acls::anyone_all());
        match client.create(&disable_path, &[], &options).await {
            Ok(_) | Err(zookeeper_client::Error::NodeExists) => {}
            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "error": format!("{e}")
            }))),
        }
    }
    (StatusCode::OK, Json(serde_json::json!({"enabled": body.enabled})))
}

// ---------------------------------------------------------------------------
// /api/v1/autorecovery/lost_bookie_recovery_delay — get/set delay
// Matches Java's LostBookieRecoveryDelayService
// ---------------------------------------------------------------------------

async fn get_recovery_delay(State(state): State<HttpState>) -> impl IntoResponse {
    let (client, paths) = match (&state.zk_client, &state.zk_paths) {
        (Some(c), Some(p)) => (c, p),
        _ => return (StatusCode::SERVICE_UNAVAILABLE,
            "ZK not configured".to_string()),
    };

    let delay_path = format!("{}/lostBookieRecoveryDelay", paths.underreplication());
    let delay = match client.get_data(&delay_path).await {
        Ok((data, _)) => String::from_utf8_lossy(&data).trim().parse::<i32>().unwrap_or(0),
        Err(_) => 0,
    };
    // Java format: plain string body
    (StatusCode::OK, format!("lostBookieRecoveryDelay value: {delay}"))
}

#[derive(Deserialize)]
struct RecoveryDelayRequest {
    delay_seconds: i32,
}

async fn set_recovery_delay(
    State(state): State<HttpState>,
    Json(body): Json<RecoveryDelayRequest>,
) -> impl IntoResponse {
    let (client, paths) = match (&state.zk_client, &state.zk_paths) {
        (Some(c), Some(p)) => (c, p),
        _ => return (StatusCode::SERVICE_UNAVAILABLE,
            "ZK not configured".to_string()),
    };

    let delay_path = format!("{}/lostBookieRecoveryDelay", paths.underreplication());
    let data = body.delay_seconds.to_string().into_bytes();
    let result = match client.set_data(&delay_path, &data, None).await {
        Ok(_) => Ok(()),
        Err(zookeeper_client::Error::NoNode) => {
            let options = zookeeper_client::CreateMode::Persistent
                .with_acls(zookeeper_client::Acls::anyone_all());
            client.create(&delay_path, &data, &options).await.map(|_| ())
                .map_err(|e| format!("{e}"))
        }
        Err(e) => Err(format!("{e}")),
    };
    match result {
        Ok(()) => (StatusCode::OK,
            format!("Success set lostBookieRecoveryDelay to {}", body.delay_seconds)),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

// ---------------------------------------------------------------------------
// /api/v1/autorecovery/trigger_audit — trigger audit
// Matches Java's TriggerAuditService
// ---------------------------------------------------------------------------

async fn trigger_audit_endpoint(State(state): State<HttpState>) -> impl IntoResponse {
    let (client, paths) = match (&state.zk_client, &state.zk_paths) {
        (Some(c), Some(p)) => (c, p),
        _ => return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "error": "ZK not configured"
        }))),
    };

    // Pre-check: replication enabled?
    let disable_path = format!("{}/disable", paths.underreplication());
    if client.check_stat(&disable_path).await.ok().flatten().is_some() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "error": "Auto-recovery is disabled"
        })));
    }

    // Pre-check: auditor elected?
    let election_path = format!("{}/auditorelection", paths.underreplication());
    let has_auditor = match client.get_children(&election_path).await {
        Ok((children, _)) => children.iter().any(|c| c.starts_with("V_")),
        Err(_) => false,
    };
    if !has_auditor {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "error": "No auditor elected"
        })));
    }

    // Reset lostBookieRecoveryDelay to current value to trigger audit
    let delay_path = format!("{}/lostBookieRecoveryDelay", paths.underreplication());
    let current = client.get_data(&delay_path).await
        .map(|(d, _)| d)
        .unwrap_or_else(|_| b"0".to_vec());

    match client.set_data(&delay_path, &current, None).await {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({"triggered": true}))),
        Err(zookeeper_client::Error::NoNode) => {
            let options = zookeeper_client::CreateMode::Persistent
                .with_acls(zookeeper_client::Acls::anyone_all());
            let _ = client.create(&delay_path, &current, &options).await;
            (StatusCode::OK, Json(serde_json::json!({"triggered": true})))
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
            "error": format!("{e}")
        }))),
    }
}

// ---------------------------------------------------------------------------
// /api/v1/autorecovery/list_under_replicated_ledger
// Matches Java's ListUnderReplicatedLedgerService
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ListUnderReplicatedQuery {
    missingreplica: Option<String>,
    excludingmissingreplica: Option<String>,
    #[allow(dead_code)]
    printmissingreplica: Option<String>,
}

async fn list_under_replicated_endpoint(
    State(state): State<HttpState>,
    Query(_query): Query<ListUnderReplicatedQuery>,
) -> impl IntoResponse {
    let (client, paths) = match (&state.zk_client, &state.zk_paths) {
        (Some(c), Some(p)) => (c, p),
        _ => return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!([
            "ZK not configured"
        ]))),
    };

    // Note: filters (missingreplica/excludingmissingreplica) require reading
    // znode data; current scan_underreplicated only collects IDs.
    let ur_path = format!("{}/ledgers", paths.underreplication());
    let mut results = Vec::new();
    scan_underreplicated(client, &ur_path, &mut results).await;

    if results.is_empty() {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!([
            "No under replicated ledgers found"
        ])));
    }

    // Java format: JSON array of ledger IDs
    let array: Vec<serde_json::Value> = results.into_iter()
        .map(|lid| serde_json::Value::Number(serde_json::Number::from(lid)))
        .collect();
    (StatusCode::OK, Json(serde_json::Value::Array(array)))
}

async fn scan_underreplicated(
    client: &zookeeper_client::Client,
    path: &str,
    results: &mut Vec<i64>,
) {
    let children = match client.get_children(path).await {
        Ok((c, _)) => c,
        Err(_) => return,
    };
    for child in &children {
        if let Some(suffix) = child.strip_prefix("urL") {
            if let Ok(lid) = suffix.parse::<i64>() {
                results.push(lid);
                continue;
            }
        }
        let child_path = format!("{}/{}", path, child);
        Box::pin(scan_underreplicated(client, &child_path, results)).await;
    }
}

// ---------------------------------------------------------------------------
// /api/v1/bookie/gc/suspend_compaction — suspend GC compaction
// /api/v1/bookie/gc/resume_compaction — resume GC compaction
// Matches Java's SuspendCompactionService / ResumeCompactionService
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct SuspendCompactionRequest {
    #[serde(rename = "suspendMajor")]
    suspend_major: Option<bool>,
    #[serde(rename = "suspendMinor")]
    suspend_minor: Option<bool>,
}

async fn get_suspend_compaction(State(state): State<HttpState>) -> Json<serde_json::Value> {
    use std::sync::atomic::Ordering;
    Json(serde_json::json!({
        "isMajorGcSuspended": state.gc_suspend_major.load(Ordering::Relaxed).to_string(),
        "isMinorGcSuspended": state.gc_suspend_minor.load(Ordering::Relaxed).to_string(),
    }))
}

async fn suspend_compaction(
    State(state): State<HttpState>,
    Json(body): Json<SuspendCompactionRequest>,
) -> impl IntoResponse {
    use std::sync::atomic::Ordering;
    if body.suspend_major.unwrap_or(false) {
        state.gc_suspend_major.store(true, Ordering::Relaxed);
    }
    if body.suspend_minor.unwrap_or(false) {
        state.gc_suspend_minor.store(true, Ordering::Relaxed);
    }
    (StatusCode::OK, Json(serde_json::json!({"suspended": true})))
}

#[derive(Deserialize)]
struct ResumeCompactionRequest {
    #[serde(rename = "resumeMajor")]
    resume_major: Option<bool>,
    #[serde(rename = "resumeMinor")]
    resume_minor: Option<bool>,
}

async fn resume_compaction(
    State(state): State<HttpState>,
    Json(body): Json<ResumeCompactionRequest>,
) -> impl IntoResponse {
    use std::sync::atomic::Ordering;
    if body.resume_major.unwrap_or(false) {
        state.gc_suspend_major.store(false, Ordering::Relaxed);
    }
    if body.resume_minor.unwrap_or(false) {
        state.gc_suspend_minor.store(false, Ordering::Relaxed);
    }
    (StatusCode::OK, Json(serde_json::json!({"resumed": true})))
}

// ---------------------------------------------------------------------------
// /api/v1/bookie/expand_storage — validate cookies after expansion
// Matches Java's ExpandStorageService
// ---------------------------------------------------------------------------

async fn expand_storage_endpoint(State(state): State<HttpState>) -> impl IntoResponse {
    let bookie_id = match &state.bookie_id {
        Some(id) => id,
        None => return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "error": "no bookie_id"
        }))),
    };
    let (client, paths) = match (&state.zk_client, &state.zk_paths) {
        (Some(c), Some(p)) => (c, p),
        _ => return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "error": "ZK not configured"
        }))),
    };

    // Verify cookie exists in ZK (storage expansion is handled at startup
    // via cookie validation; this endpoint just confirms ZK side is healthy).
    let cookie_path = paths.cookie_path(bookie_id);
    match client.get_data(&cookie_path).await {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({
            "expanded": true,
            "note": "cookie validated; ensure new dirs are added to config and bookie restarted"
        }))),
        Err(zookeeper_client::Error::NoNode) => (StatusCode::NOT_FOUND, Json(serde_json::json!({
            "error": "no cookie found for bookie"
        }))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
            "error": format!("{e}")
        }))),
    }
}

// ---------------------------------------------------------------------------
// /api/v1/bookie/list_bookie_info — disk info for all bookies
// Matches Java's ListBookieInfoService
// ---------------------------------------------------------------------------

async fn list_bookie_info_endpoint(State(state): State<HttpState>) -> impl IntoResponse {
    use bookkeeper_client::bookie_connection::BookieConnection;
    use bookkeeper_protocol::v2::response::V2Response;

    let (client, paths) = match (&state.zk_client, &state.zk_paths) {
        (Some(c), Some(p)) => (c, p),
        _ => return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "error": "ZK not configured"
        }))),
    };

    let bookies = match client.get_children(paths.available()).await {
        Ok((c, _)) => c.into_iter().filter(|c| c != "readonly").collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };

    let mut result = serde_json::Map::new();
    let mut total_free: i64 = 0;
    let mut total_total: i64 = 0;

    for bookie in &bookies {
        let addr: std::net::SocketAddr = match bookie.parse() {
            Ok(a) => a,
            Err(_) => continue,
        };
        let conn = match BookieConnection::connect(addr).await {
            Ok(c) => c,
            Err(_) => continue,
        };
        match conn.send_get_bookie_info(0x3 /* both free + total */).await {
            Ok(V2Response::GetBookieInfo { free_disk_space, total_disk_capacity, .. }) => {
                total_free += free_disk_space;
                total_total += total_disk_capacity;
                result.insert(bookie.clone(), serde_json::json!({
                    "freeDiskSpace": free_disk_space,
                    "totalDiskCapacity": total_disk_capacity,
                }));
            }
            _ => continue,
        }
    }

    (StatusCode::OK, Json(serde_json::json!({
        "ClusterInfo": result,
        "TotalFreeDiskSpace": total_free,
        "TotalDiskCapacity": total_total,
    })))
}

// ---------------------------------------------------------------------------
// /api/v1/ledger/metadata — read ledger metadata from ZK
// Matches Java's GetLedgerMetaService
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct LedgerIdQuery {
    ledger_id: i64,
}

async fn ledger_metadata_endpoint(
    State(state): State<HttpState>,
    Query(query): Query<LedgerIdQuery>,
) -> impl IntoResponse {
    use bookkeeper_metadata::traits::LedgerManager;

    let (client, paths) = match (&state.zk_client, &state.zk_paths) {
        (Some(c), Some(p)) => (c, p),
        _ => return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "error": "ZK not configured"
        }))),
    };

    let lm = bookkeeper_metadata::zk::ledger_manager::ZkLedgerManager::new(
        client.clone(), paths.clone(),
    );

    match lm.read_ledger_metadata(query.ledger_id).await {
        Ok(versioned) => {
            let md = versioned.value();
            let segments: Vec<_> = md.all_ensembles().iter()
                .map(|(k, v)| serde_json::json!({"firstEntry": k, "ensemble": v}))
                .collect();
            // Java format: { ledger_id: <metadata> }
            let metadata = serde_json::json!({
                "metadataFormatVersion": versioned.version(),
                "ensembleSize": md.ensemble_size(),
                "writeQuorumSize": md.write_quorum_size(),
                "ackQuorumSize": md.ack_quorum_size(),
                "state": format!("{:?}", md.state()).to_uppercase(),
                "lastEntryId": md.last_entry_id(),
                "length": md.length(),
                "allEnsembles": segments,
            });
            let mut result = serde_json::Map::new();
            result.insert(query.ledger_id.to_string(), metadata);
            (StatusCode::OK, Json(serde_json::Value::Object(result)))
        }
        Err(e) => (StatusCode::NOT_FOUND, Json(serde_json::json!({
            "error": format!("{e}")
        }))),
    }
}

// ---------------------------------------------------------------------------
// /api/v1/ledger/read — read entries (local storage only)
// Matches Java's ReadLedgerEntryService (local subset)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ReadLedgerQuery {
    ledger_id: i64,
    start_entry_id: Option<i64>,
    end_entry_id: Option<i64>,
}

async fn read_ledger_endpoint(
    State(state): State<HttpState>,
    Query(query): Query<ReadLedgerQuery>,
) -> impl IntoResponse {
    let storage = match &state.storage {
        Some(s) => s,
        None => return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "error": "no storage"
        }))),
    };

    let start = query.start_entry_id.unwrap_or(0);
    let end = query.end_entry_id.unwrap_or(start);

    let mut entries = serde_json::Map::new();
    for eid in start..=end {
        match storage.get_entry(query.ledger_id, eid).await {
            Ok(data) => {
                entries.insert(eid.to_string(), serde_json::json!({
                    "size": data.len(),
                    "preview": format!("{:?}", &data[..data.len().min(64)]),
                }));
            }
            Err(_) => break,
        }
    }
    (StatusCode::OK, Json(serde_json::Value::Object(entries)))
}

// ---------------------------------------------------------------------------
// /api/v1/autorecovery/bookie — recover a bookie
// Matches Java's RecoveryBookieService
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct RecoverBookieRequest {
    bookie_src: Vec<String>,
    #[serde(default)]
    delete_cookie: bool,
}

async fn recover_bookie_endpoint(
    State(state): State<HttpState>,
    Json(body): Json<RecoverBookieRequest>,
) -> impl IntoResponse {
    let (client, paths) = match (&state.zk_client, &state.zk_paths) {
        (Some(c), Some(p)) => (c, p),
        _ => return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "error": "ZK not configured"
        }))),
    };

    // Mark all ledgers on the bookies as under-replicated.
    // Spawn as background task — recovery is asynchronous (auditor/worker
    // performs the actual data movement).
    let zk = client.clone();
    let p = paths.clone();
    let bookies = body.bookie_src.clone();
    let delete_cookie = body.delete_cookie;
    tokio::spawn(async move {
        use bookkeeper_metadata::traits::LedgerManager;
        let lm = bookkeeper_metadata::zk::ledger_manager::ZkLedgerManager::new(zk.clone(), p.clone());
        let ur_mgr = bookkeeper_replication::underreplication::ZkUnderreplicationManager::new(
            zk.clone(), p.clone(), "http-api".to_string(),
        ).await;
        let ur_mgr = match ur_mgr {
            Ok(m) => m,
            Err(_) => return,
        };
        if let Ok(ledgers) = lm.get_ledger_ranges().await {
            for lid in &ledgers {
                if let Ok(versioned) = lm.read_ledger_metadata(*lid).await {
                    let md = versioned.value();
                    let has_bookie = md.all_ensembles().values().any(|ensemble| {
                        ensemble.iter().any(|b| bookies.contains(b))
                    });
                    if has_bookie {
                        let _ = ur_mgr.mark_ledger_underreplicated(*lid, &bookies).await;
                    }
                }
            }
        }
        // Delete cookies if requested
        if delete_cookie {
            for bookie in &bookies {
                let cookie_path = p.cookie_path(bookie);
                let _ = zk.delete(&cookie_path, None).await;
                tracing::info!("Recover: deleted cookie for {bookie}");
            }
        }
    });

    (StatusCode::OK, Json(serde_json::json!({
        "triggered": true,
        "note": "recovery scheduled asynchronously"
    })))
}

// ---------------------------------------------------------------------------
// /api/v1/autorecovery/decommission — decommission a bookie
// Matches Java's DecommissionService
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct DecommissionRequest {
    bookie_src: String,
    #[serde(default)]
    delete_cookie: bool,
}

async fn decommission_endpoint(
    State(state): State<HttpState>,
    Json(body): Json<DecommissionRequest>,
) -> impl IntoResponse {
    let (client, paths) = match (&state.zk_client, &state.zk_paths) {
        (Some(c), Some(p)) => (c, p),
        _ => return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "error": "ZK not configured"
        }))),
    };

    // Spawn decommission as background task — long-running
    let zk = client.clone();
    let p = paths.clone();
    let bookie = body.bookie_src.clone();
    let delete_cookie = body.delete_cookie;
    tokio::spawn(async move {
        use bookkeeper_metadata::traits::LedgerManager;
        let lm = bookkeeper_metadata::zk::ledger_manager::ZkLedgerManager::new(zk.clone(), p.clone());
        let ur_mgr = bookkeeper_replication::underreplication::ZkUnderreplicationManager::new(
            zk.clone(), p.clone(), "http-api-decommission".to_string(),
        ).await;
        let ur_mgr = match ur_mgr {
            Ok(m) => m,
            Err(_) => return,
        };
        if let Ok(ledgers) = lm.get_ledger_ranges().await {
            for lid in &ledgers {
                if let Ok(versioned) = lm.read_ledger_metadata(*lid).await {
                    let md = versioned.value();
                    let has_bookie = md.all_ensembles().values().any(|ensemble| {
                        ensemble.iter().any(|b| b == &bookie)
                    });
                    if has_bookie {
                        let _ = ur_mgr.mark_ledger_underreplicated(*lid, &[bookie.clone()]).await;
                    }
                }
            }
        }
        // Delete cookie if requested
        if delete_cookie {
            let cookie_path = p.cookie_path(&bookie);
            let _ = zk.delete(&cookie_path, None).await;
            tracing::info!("Decommission: deleted cookie for {bookie}");
        }
    });

    (StatusCode::OK, Json(serde_json::json!({
        "triggered": true,
        "note": "decommission scheduled asynchronously"
    })))
}

// ---------------------------------------------------------------------------
// /debug/pprof — CPU profiling endpoints (gated on the `profiling` feature)
// ---------------------------------------------------------------------------

/// CPU flamegraph SVG for `seconds` (default 30).
/// Usage: curl http://localhost:8080/debug/pprof/flamegraph?seconds=10 > flamegraph.svg
#[cfg(feature = "profiling")]
async fn pprof_flamegraph(
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let seconds = params.get("seconds")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(30);

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(99)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(seconds)).await;

    match guard.report().build() {
        Ok(report) => {
            let mut body = Vec::new();
            if let Err(e) = report.flamegraph(&mut body) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [("content-type", "text/plain")],
                    format!("Failed to generate flamegraph: {e}").into_bytes(),
                );
            }
            (
                StatusCode::OK,
                [("content-type", "image/svg+xml")],
                body,
            )
        }
        Err(e) => {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [("content-type", "text/plain")],
                format!("Failed to build report: {e}").into_bytes(),
            )
        }
    }
}
