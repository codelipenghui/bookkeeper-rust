use liboxia::errors::OxiaError;

use crate::ledger_metadata::MetadataError;

/// Map a liboxia error into the backend-neutral `MetadataError`.
/// `ledger_id_hint`: `Some(id)` when the operation was ledger-scoped;
/// `None` otherwise (use 0 in the resulting `NotFound`).
pub fn map_oxia_error(err: OxiaError, ledger_id_hint: Option<i64>) -> MetadataError {
    match err {
        OxiaError::KeyNotFound() => MetadataError::NotFound(ledger_id_hint.unwrap_or(0)),
        OxiaError::UnexpectedVersionId() => MetadataError::VersionConflict {
            expected: -1,
            actual: -1,
        },
        OxiaError::SessionDoesNotExist() => {
            MetadataError::ConnectionError("oxia session does not exist".into())
        }
        OxiaError::KeyLeaderNotFound(key) => {
            MetadataError::ConnectionError(format!("oxia leader not found for key '{key}' (retriable)"))
        }
        OxiaError::ShardLeaderNotFound(shard) => {
            MetadataError::ConnectionError(format!("oxia shard leader not found (shard {shard}) (retriable)"))
        }
        OxiaError::InternalRetryable() => {
            MetadataError::ConnectionError("oxia internal retriable error".into())
        }
        OxiaError::Cancelled() => {
            MetadataError::ConnectionError("oxia operation cancelled".into())
        }
        OxiaError::IllegalArgument(msg) => {
            MetadataError::InvalidConfig(format!("oxia illegal argument: {msg}"))
        }
        OxiaError::Transport(msg) => {
            MetadataError::ConnectionError(format!("oxia transport error: {msg}"))
        }
        other => MetadataError::ConnectionError(format!("oxia: {other:?}")),
    }
}
