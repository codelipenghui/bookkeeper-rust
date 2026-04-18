//! Path helpers shared by every metadata backend. The paths produced
//! here are the same Apache Pulsar uses in its `pulsar-metadata`
//! driver, so they are wire-compatible with a Pulsar-managed oxia
//! metadata store.

/// Compute the path for a ledger ID using the hybrid hierarchical scheme.
/// For `ledger_id < i32::MAX` (2^31 - 1): 3-level (short) hierarchy with 2-4-4 split.
/// For `ledger_id >= i32::MAX`: 5-level (long) hierarchy with 3-4-4-4-4 split.
pub fn ledger_path(root: &str, ledger_id: i64) -> String {
    if ledger_id < i32::MAX as i64 {
        short_hierarchical_path(root, ledger_id)
    } else {
        long_hierarchical_path(root, ledger_id)
    }
}

fn short_hierarchical_path(root: &str, ledger_id: i64) -> String {
    let s = format!("{:010}", ledger_id);
    format!("{}/{}/{}/L{}", root, &s[0..2], &s[2..6], &s[6..10])
}

fn long_hierarchical_path(root: &str, ledger_id: i64) -> String {
    let s = format!("{:019}", ledger_id);
    format!("{}/{}/{}/{}/{}/L{}", root, &s[0..3], &s[3..7], &s[7..11], &s[11..15], &s[15..19])
}

/// Compute the path for an underreplicated ledger marker.
/// Uses 4-level hex hierarchy from 64-bit ledger ID.
/// Ledger ID in the leaf name is zero-padded to 10 digits (%010d), matching Java.
pub fn underreplication_ledger_path(base: &str, ledger_id: i64) -> String {
    let id = ledger_id as u64;
    format!(
        "{}/{:04x}/{:04x}/{:04x}/{:04x}/urL{:010}",
        base,
        (id >> 48) & 0xFFFF,
        (id >> 32) & 0xFFFF,
        (id >> 16) & 0xFFFF,
        id & 0xFFFF,
        ledger_id,
    )
}

/// Compute the lock path for an underreplicated ledger.
/// Lock path uses flat structure: `{base}/urL{%010d}`.
pub fn underreplication_lock_path(base: &str, ledger_id: i64) -> String {
    format!("{}/urL{:010}", base, ledger_id)
}

/// Well-known sub-paths below the ledger root. Pure strings — no
/// backend-specific semantics. Both the ZK and oxia backends use the
/// same names to stay wire-compatible with Apache Pulsar.
#[derive(Clone, Debug)]
pub struct LedgerPaths {
    root: String,
    available: String,
    available_readonly: String,
    cookies: String,
    layout: String,
    instance_id: String,
    idgen: String,
    idgen_long: String,
    underreplication: String,
}

impl LedgerPaths {
    pub fn new(root: &str) -> Self {
        Self {
            available: format!("{}/available", root),
            available_readonly: format!("{}/available/readonly", root),
            cookies: format!("{}/cookies", root),
            layout: format!("{}/LAYOUT", root),
            instance_id: format!("{}/INSTANCEID", root),
            idgen: format!("{}/idgen", root),
            idgen_long: format!("{}/idgen-long", root),
            underreplication: format!("{}/underreplication", root),
            root: root.to_string(),
        }
    }

    pub fn root(&self) -> &str { &self.root }
    pub fn available(&self) -> &str { &self.available }
    pub fn available_readonly(&self) -> &str { &self.available_readonly }
    pub fn cookies(&self) -> &str { &self.cookies }
    pub fn layout(&self) -> &str { &self.layout }
    pub fn instance_id(&self) -> &str { &self.instance_id }
    pub fn idgen(&self) -> &str { &self.idgen }
    pub fn idgen_long(&self) -> &str { &self.idgen_long }
    pub fn underreplication(&self) -> &str { &self.underreplication }

    pub fn cookie_path(&self, bookie_id: &str) -> String {
        format!("{}/{}", self.cookies, bookie_id)
    }
    pub fn bookie_registration(&self, bookie_id: &str) -> String {
        format!("{}/{}", self.available, bookie_id)
    }
    pub fn bookie_readonly_registration(&self, bookie_id: &str) -> String {
        format!("{}/{}", self.available_readonly, bookie_id)
    }
}

/// The parsed form of a `metadataServiceUri` — a sum type over the
/// supported backends. Produced by the parser in the config crate;
/// consumed by `crate::backend::open_metadata_backend`.
#[derive(Debug, Clone, PartialEq)]
pub enum ParsedMetadataUri {
    Zk { servers: String, ledgers_root: String },
    Oxia { service_addr: String, namespace: String, ledgers_root: String },
}

impl ParsedMetadataUri {
    pub fn ledgers_root(&self) -> &str {
        match self {
            Self::Zk { ledgers_root, .. } | Self::Oxia { ledgers_root, .. } => ledgers_root,
        }
    }
}
