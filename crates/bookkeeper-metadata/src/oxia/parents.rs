use futures::future::try_join_all;
use liboxia::client::{OxiaClient, PutOption};
use liboxia::errors::OxiaError;

use super::errors::map_oxia_error;
use super::paths::ancestors_of;
use crate::ledger_metadata::MetadataError;

/// Ensure every ancestor directory of `key` exists as an empty-valued
/// record. Matches `OxiaMetadataStore.createParents` in Pulsar.
///
/// Uses `PutOption::ExpectVersionId(-1)` (oxia's "record must not exist"
/// sentinel) so the put is a no-op when a concurrent writer already
/// created the ancestor. `UnexpectedVersionId` is swallowed for the
/// same reason.
///
/// Ancestor creates are independent — oxia has no inherent parent→child
/// ordering constraint — so they fan out in parallel with
/// `try_join_all`. For a 3-level ledger path this turns 3 sequential
/// RTTs into 1 on every `create_ledger_metadata`.
pub async fn create_parents(client: &OxiaClient, key: &str) -> Result<(), MetadataError> {
    let ancestors = ancestors_of(key);
    if ancestors.is_empty() {
        return Ok(());
    }
    try_join_all(ancestors.into_iter().map(|ancestor| async move {
        match client
            .put_with_options(
                ancestor,
                Vec::new(),
                vec![PutOption::ExpectVersionId(-1)],
            )
            .await
        {
            Ok(_) | Err(OxiaError::UnexpectedVersionId()) => Ok(()),
            Err(e) => Err(map_oxia_error(e, None)),
        }
    }))
    .await?;
    Ok(())
}
