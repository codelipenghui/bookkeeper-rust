use liboxia::client::{OxiaClient, PutOption};
use liboxia::errors::OxiaError;

use super::errors::map_oxia_error;
use super::paths::ancestors_of;
use crate::ledger_metadata::MetadataError;

/// Ensure every ancestor directory of `key` exists as an empty-valued
/// record. Matches `OxiaMetadataStore.createParents` in Pulsar.
///
/// Uses `PutOption::ExpectVersionId(-1)` (the "record must not exist"
/// sentinel in the oxia protocol) so that the put is a no-op when a
/// concurrent writer already created the ancestor.
///
/// Swallows `UnexpectedVersionId` (another client raced) — that
/// means the record already exists, which is fine.
pub async fn create_parents(client: &OxiaClient, key: &str) -> Result<(), MetadataError> {
    for ancestor in ancestors_of(key) {
        match client
            .put_with_options(
                ancestor.clone(),
                Vec::new(),
                vec![PutOption::ExpectVersionId(-1)],
            )
            .await
        {
            Ok(_) => {}
            Err(OxiaError::UnexpectedVersionId()) => {}
            Err(e) => return Err(map_oxia_error(e, None)),
        }
    }
    Ok(())
}
