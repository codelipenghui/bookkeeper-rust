# Phase 9: Admin HTTP API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an HTTP admin API to the bookie server for health checks, state monitoring, and basic administration — enabling Kubernetes readiness probes and operational tooling.

**Architecture:** Add HTTP endpoints to the `bookkeeper-server` crate using `axum` (lightweight async HTTP framework). The HTTP server runs alongside the TCP bookie server, sharing the same `BookieStateManager` and `LedgerStorage`. Start with the essential endpoints: heartbeat, readiness, bookie state, and bookie info.

**Tech Stack:** `axum` (HTTP framework), `serde_json` (JSON responses), `tokio`

**Spec reference:** `docs/superpowers/specs/09-admin-http-api.md`

---

## File Structure

```
crates/bookkeeper-server/
└── src/
    ├── server.rs         # MODIFY: optionally start HTTP server alongside TCP
    └── http_api.rs       # NEW: HTTP routes and handlers
```

---

### Task 1: HTTP Admin API

**Files:**
- Modify: `Cargo.toml` (workspace root — add axum, serde, serde_json)
- Modify: `crates/bookkeeper-server/Cargo.toml`
- Create: `crates/bookkeeper-server/src/http_api.rs`
- Modify: `crates/bookkeeper-server/src/server.rs`
- Modify: `crates/bookkeeper-server/src/lib.rs`
- Test: `crates/bookkeeper-server/tests/http_api_test.rs`

- [ ] **Step 1: Add dependencies**

In workspace root Cargo.toml:
```toml
axum = "0.7"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

In crates/bookkeeper-server/Cargo.toml:
```toml
axum = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
```

- [ ] **Step 2: Write failing tests**

`crates/bookkeeper-server/tests/http_api_test.rs`:
```rust
use bookkeeper_server::server::BookieServer;
use bookkeeper_server::config::BookieConfig;
use tempfile::TempDir;

fn test_config(dir: &TempDir) -> BookieConfig {
    BookieConfig {
        port: 0,
        ledger_dirs: vec![dir.path().to_string_lossy().to_string()],
        journal_dir: dir.path().to_string_lossy().to_string(),
        max_frame_size: 5 * 1024 * 1024,
        zk_connect: None,
        zk_ledger_root: "/ledgers".to_string(),
        bookie_id: None,
        http_port: Some(0), // OS-assigned HTTP port
    }
}

#[tokio::test]
async fn heartbeat() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/heartbeat", http_addr)).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "OK");

    server.shutdown().await;
}

#[tokio::test]
async fn readiness() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/api/v1/bookie/is_ready", http_addr)).await.unwrap();
    assert_eq!(resp.status(), 200);

    server.shutdown().await;
}

#[tokio::test]
async fn bookie_state() {
    let dir = TempDir::new().unwrap();
    let server = BookieServer::start(test_config(&dir)).await.unwrap();
    let http_addr = server.http_addr().unwrap();

    let resp = reqwest::get(format!("http://{}/api/v1/bookie/state", http_addr)).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["running"], true);
    assert_eq!(body["readOnly"], false);
    assert_eq!(body["shuttingDown"], false);

    server.shutdown().await;
}
```

- [ ] **Step 3: Implement http_api.rs**

```rust
use std::sync::Arc;
use axum::{Router, Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use crate::state::{BookieState, BookieStateManager};

#[derive(Clone)]
pub struct HttpState {
    pub state_manager: Arc<BookieStateManager>,
}

pub fn create_router(http_state: HttpState) -> Router {
    Router::new()
        .route("/heartbeat", axum::routing::get(heartbeat))
        .route("/api/v1/bookie/is_ready", axum::routing::get(is_ready))
        .route("/api/v1/bookie/state", axum::routing::get(bookie_state))
        .with_state(http_state)
}

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
```

- [ ] **Step 4: Add http_port to BookieConfig and wire into server.rs**

In config.rs, add `pub http_port: Option<u16>` (default `None`).

In server.rs `BookieServer`, add `http_addr: Option<std::net::SocketAddr>`.

In `start()`, if `config.http_port` is `Some`:
```rust
if let Some(http_port) = config.http_port {
    let http_state = http_api::HttpState { state_manager: state.clone() };
    let router = http_api::create_router(http_state);
    let http_listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{http_port}")).await?;
    let http_local_addr = http_listener.local_addr()?;
    tokio::spawn(async move {
        axum::serve(http_listener, router).await.ok();
    });
    // Store http_local_addr
}
```

Add `pub fn http_addr(&self) -> Option<SocketAddr>` getter.

- [ ] **Step 5: Add reqwest as dev dependency for tests**

In crates/bookkeeper-server/Cargo.toml dev-dependencies:
```toml
reqwest = { version = "0.12", default-features = false, features = ["json"] }
```

- [ ] **Step 6: Update lib.rs**

Add `pub mod http_api;` to lib.rs.

- [ ] **Step 7: Run tests, commit**

```bash
git commit -m "feat: add HTTP admin API with heartbeat, readiness probe, and bookie state

Endpoints: GET /heartbeat, GET /api/v1/bookie/is_ready, GET /api/v1/bookie/state
Uses axum for the HTTP server, runs alongside TCP bookie server.
Enabled via http_port config option.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Spec Coverage

| Spec Section | Covered | Notes |
|---|---|---|
| §2 GET /heartbeat | Yes | Returns "OK" |
| §2 GET /api/v1/bookie/is_ready | Yes | 200 if readable, 503 if not |
| §3 GET /api/v1/bookie/state | Yes | JSON with running/readOnly/shuttingDown |
| §3 GET /api/v1/bookie/info | Deferred | Needs disk space query |
| §4 GET /api/v1/config/server_config | Deferred | Needs ServerConfig wiring |
| §5 Ledger operations | Deferred | Needs LedgerManager |
| §6 Auto-recovery | Deferred | Phase 7b |
| §7 GC endpoints | Deferred | Phase 3b |
| §8 GET /metrics | Deferred | Needs metrics framework |
