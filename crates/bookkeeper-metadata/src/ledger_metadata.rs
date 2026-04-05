use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LedgerState {
    Open,
    InRecovery,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DigestType {
    Crc32,
    Crc32c,
    Hmac,
    Dummy,
}

/// Immutable ledger metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct LedgerMetadata {
    ledger_id: i64,
    metadata_format_version: i32,
    ensemble_size: i32,
    write_quorum_size: i32,
    ack_quorum_size: i32,
    state: LedgerState,
    last_entry_id: Option<i64>,
    length: Option<i64>,
    ensembles: BTreeMap<i64, Vec<String>>,
    digest_type: Option<DigestType>,
    password: Option<Vec<u8>>,
    ctime: i64,
    store_ctime: bool,
    custom_metadata: BTreeMap<String, Vec<u8>>,
    c_token: i64,
}

impl LedgerMetadata {
    pub fn ledger_id(&self) -> i64 { self.ledger_id }
    pub fn metadata_format_version(&self) -> i32 { self.metadata_format_version }
    pub fn ensemble_size(&self) -> i32 { self.ensemble_size }
    pub fn write_quorum_size(&self) -> i32 { self.write_quorum_size }
    pub fn ack_quorum_size(&self) -> i32 { self.ack_quorum_size }
    pub fn state(&self) -> LedgerState { self.state }
    pub fn last_entry_id(&self) -> Option<i64> { self.last_entry_id }
    pub fn length(&self) -> Option<i64> { self.length }
    pub fn digest_type(&self) -> Option<DigestType> { self.digest_type }
    pub fn password(&self) -> Option<&[u8]> { self.password.as_deref() }
    pub fn ctime(&self) -> i64 { self.ctime }
    pub fn store_ctime(&self) -> bool { self.store_ctime }
    pub fn custom_metadata(&self) -> &BTreeMap<String, Vec<u8>> { &self.custom_metadata }
    pub fn c_token(&self) -> i64 { self.c_token }
    pub fn all_ensembles(&self) -> &BTreeMap<i64, Vec<String>> { &self.ensembles }

    /// Get the ensemble responsible for the given entry ID.
    /// Returns the ensemble from the highest segment whose firstEntryId <= entryId.
    pub fn ensemble_at(&self, entry_id: i64) -> Option<&[String]> {
        self.ensembles.range(..=entry_id).next_back().map(|(_, v)| v.as_slice())
    }
}

pub struct LedgerMetadataBuilder {
    ledger_id: i64,
    metadata_format_version: i32,
    ensemble_size: i32,
    write_quorum_size: i32,
    ack_quorum_size: i32,
    state: LedgerState,
    last_entry_id: Option<i64>,
    length: Option<i64>,
    ensembles: BTreeMap<i64, Vec<String>>,
    digest_type: Option<DigestType>,
    password: Option<Vec<u8>>,
    ctime: i64,
    store_ctime: bool,
    custom_metadata: BTreeMap<String, Vec<u8>>,
    c_token: i64,
}

impl LedgerMetadataBuilder {
    pub fn new() -> Self {
        Self {
            ledger_id: -1,
            metadata_format_version: 3,
            ensemble_size: 3,
            write_quorum_size: 3,
            ack_quorum_size: 2,
            state: LedgerState::Open,
            last_entry_id: None,
            length: None,
            ensembles: BTreeMap::new(),
            digest_type: None,
            password: None,
            ctime: -1,
            store_ctime: false,
            custom_metadata: BTreeMap::new(),
            c_token: 0,
        }
    }

    /// Create a builder pre-populated from existing metadata.
    /// Copies all fields (ensemble_size, quorum, digest, password, ctime, ensembles, etc.).
    /// Useful for CAS metadata updates where most fields are preserved.
    pub fn from_existing(md: &LedgerMetadata) -> Self {
        let mut builder = Self::new()
            .ledger_id(md.ledger_id)
            .metadata_format_version(md.metadata_format_version)
            .ensemble_size(md.ensemble_size)
            .write_quorum_size(md.write_quorum_size)
            .ack_quorum_size(md.ack_quorum_size);
        if let Some(dt) = md.digest_type {
            builder = builder.with_digest_type(dt);
        }
        if let Some(ref pw) = md.password {
            builder = builder.with_password(pw.clone());
        }
        if md.store_ctime {
            builder = builder.with_ctime(md.ctime);
        }
        for (first_entry, ens) in &md.ensembles {
            builder = builder.new_ensemble_entry(*first_entry, ens.clone());
        }
        if !md.custom_metadata.is_empty() {
            builder = builder.with_custom_metadata(md.custom_metadata.clone());
        }
        if md.c_token != 0 {
            builder = builder.with_c_token(md.c_token);
        }
        builder
    }

    pub fn ledger_id(mut self, v: i64) -> Self { self.ledger_id = v; self }
    pub fn metadata_format_version(mut self, v: i32) -> Self { self.metadata_format_version = v; self }
    pub fn ensemble_size(mut self, v: i32) -> Self { self.ensemble_size = v; self }
    pub fn write_quorum_size(mut self, v: i32) -> Self { self.write_quorum_size = v; self }
    pub fn ack_quorum_size(mut self, v: i32) -> Self { self.ack_quorum_size = v; self }
    pub fn with_digest_type(mut self, v: DigestType) -> Self { self.digest_type = Some(v); self }
    pub fn with_password(mut self, v: Vec<u8>) -> Self { self.password = Some(v); self }
    pub fn with_ctime(mut self, v: i64) -> Self { self.ctime = v; self.store_ctime = true; self }
    pub fn with_custom_metadata(mut self, v: BTreeMap<String, Vec<u8>>) -> Self { self.custom_metadata = v; self }
    pub fn with_c_token(mut self, v: i64) -> Self { self.c_token = v; self }

    pub fn new_ensemble_entry(mut self, first_entry_id: i64, ensemble: Vec<String>) -> Self {
        self.ensembles.insert(first_entry_id, ensemble);
        self
    }

    /// Clear all ensemble entries (used before replacing with overrides).
    pub fn clear_ensembles(&mut self) {
        self.ensembles.clear();
    }

    pub fn with_in_recovery_state(mut self) -> Self {
        self.state = LedgerState::InRecovery;
        self
    }

    pub fn with_closed_state(mut self, last_entry_id: i64, length: i64) -> Self {
        self.state = LedgerState::Closed;
        self.last_entry_id = Some(last_entry_id);
        self.length = Some(length);
        self
    }

    pub fn build(self) -> Result<LedgerMetadata, MetadataError> {
        if self.ledger_id < 0 {
            return Err(MetadataError::InvalidConfig("ledger_id must be >= 0".into()));
        }
        if self.ensemble_size < self.write_quorum_size {
            return Err(MetadataError::InvalidConfig(
                format!("ensemble_size ({}) must be >= write_quorum_size ({})",
                        self.ensemble_size, self.write_quorum_size)));
        }
        if self.write_quorum_size < self.ack_quorum_size {
            return Err(MetadataError::InvalidConfig(
                format!("write_quorum_size ({}) must be >= ack_quorum_size ({})",
                        self.write_quorum_size, self.ack_quorum_size)));
        }
        if self.ensembles.is_empty() {
            return Err(MetadataError::InvalidConfig("at least one ensemble segment required".into()));
        }
        for (_, ensemble) in &self.ensembles {
            if ensemble.len() != self.ensemble_size as usize {
                return Err(MetadataError::InvalidConfig(
                    format!("ensemble has {} members but ensemble_size is {}",
                            ensemble.len(), self.ensemble_size)));
            }
        }
        if self.state == LedgerState::Closed && (self.last_entry_id.is_none() || self.length.is_none()) {
            return Err(MetadataError::InvalidConfig(
                "CLOSED state requires last_entry_id and length".into()));
        }
        Ok(LedgerMetadata {
            ledger_id: self.ledger_id,
            metadata_format_version: self.metadata_format_version,
            ensemble_size: self.ensemble_size,
            write_quorum_size: self.write_quorum_size,
            ack_quorum_size: self.ack_quorum_size,
            state: self.state,
            last_entry_id: self.last_entry_id,
            length: self.length,
            ensembles: self.ensembles,
            digest_type: self.digest_type,
            password: self.password,
            ctime: self.ctime,
            store_ctime: self.store_ctime,
            custom_metadata: self.custom_metadata,
            c_token: self.c_token,
        })
    }
}

impl Default for LedgerMetadataBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("serialization error: {0}")]
    SerializationError(String),
    #[error("unknown metadata version: {0}")]
    UnknownVersion(i32),
    #[error("ledger not found: {0}")]
    NotFound(i64),
    #[error("ledger already exists: {0}")]
    LedgerExists(i64),
    #[error("version conflict: expected {expected}, got {actual}")]
    VersionConflict { expected: i32, actual: i32 },
    #[error("metadata store connection error: {0}")]
    ConnectionError(String),
    #[error("ZooKeeper error: {0}")]
    ZkError(String),
    #[error("ledger id overflow")]
    LedgerIdOverflow,
}
