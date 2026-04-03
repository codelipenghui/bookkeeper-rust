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
