use bookkeeper_metadata::ledger_metadata::{LedgerMetadata, LedgerMetadataBuilder};

/// Build a minimal valid `LedgerMetadata` suitable for contract tests.
/// Uses ensemble/write/ack quorum of 1 with a single bookie to keep
/// the test data as simple as possible.
pub fn fake_ledger_metadata() -> LedgerMetadata {
    LedgerMetadataBuilder::new()
        .ledger_id(0) // caller may override the path; the stored key is the ledger_id arg
        .ensemble_size(1)
        .write_quorum_size(1)
        .ack_quorum_size(1)
        .new_ensemble_entry(0, vec!["b:3181".to_string()])
        .build()
        .unwrap()
}
