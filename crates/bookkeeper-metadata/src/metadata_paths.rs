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

/// Reconstruct the full ledger ID from the hierarchical path components.
///
/// The hierarchical scheme encodes the full ledger ID across multiple
/// path segments: every directory segment above the leaf contributes
/// some digits of the ID, and the leaf `L{NNNN}` contributes the last
/// four digits. Parsing only the leaf produces the wrong ID for any
/// ledger with `id >= 10000`.
///
/// `parent_path` is the full path up to (but not including) the `L{NNNN}`
/// leaf, e.g. `/ledgers/00/0001`. `root` is the root of the ledger tree,
/// e.g. `/ledgers`. `leaf_id` is the numeric value of the leaf (the
/// `NNNN` in `L{NNNN}`).
///
/// Both the short (3-level) and long (5-level) hierarchies are handled:
/// - Short: `{root}/XX/XXXX/L{XXXX}` → concatenate `XX`+`XXXX`+`XXXX` = 10-digit ID.
/// - Long:  `{root}/XXX/XXXX/XXXX/XXXX/L{XXXX}` → concatenate all five groups = 19-digit ID.
pub fn reconstruct_ledger_id_from_path(parent_path: &str, root: &str, leaf_id: i64) -> i64 {
    // Strip root prefix to get the relative path segments.
    let relative = if parent_path.len() > root.len() {
        &parent_path[root.len() + 1..]
    } else {
        return leaf_id;
    };

    // Collect all digit characters from the intermediate path segments.
    let mut prefix_digits = String::new();
    for part in relative.split('/') {
        for ch in part.chars() {
            if ch.is_ascii_digit() {
                prefix_digits.push(ch);
            }
        }
    }

    // The prefix digits form the high-order part; the leaf (zero-padded to 4
    // digits) forms the low-order part. Concatenate and parse.
    let leaf_str = format!("{:04}", leaf_id);
    let full_str = format!("{}{}", prefix_digits, leaf_str);
    full_str.parse::<i64>().unwrap_or(leaf_id)
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

// ---------------------------------------------------------------------------
// Ledger-ID generator constants
//
// Shared between the ZK and oxia idgen implementations. Both ports match
// Java BookKeeper's `LongZkLedgerIdGenerator` / Apache Pulsar's
// `PulsarLedgerIdGenerator` — the constants below are what both Java
// implementations write to the metadata store.
// ---------------------------------------------------------------------------

/// Prefix for sequential child records under `idgen` and
/// `idgen-long/HOB-*`.
pub const SHORT_ID_PREFIX: &str = "ID-";

/// Prefix for high-order-bits directories under `idgen-long`.
pub const HOB_PREFIX: &str = "HOB-";

/// Number of most-recent HOB directories to keep during GC.
pub const HOB_GC_KEEP: usize = 3;

// ---------------------------------------------------------------------------
// URI resolution
// ---------------------------------------------------------------------------

/// Resolve a metadata-service URI from either an explicit
/// `metadataServiceUri` or the legacy `--zk` + ledger-root pair.
///
/// Used by both the bookie server and the admin CLI so the precedence
/// rules stay in one place:
///
/// - `metadata_service_uri` wins when set — parsed via
///   `bookkeeper-config::parse_metadata_service_uri`.
/// - Otherwise, `zk_servers` + `ledgers_root` synthesise a
///   `ParsedMetadataUri::Zk`.
/// - If neither is set, returns an error.
///
/// The returned tuple's second element is a human-readable URI for
/// logs (`oxia://...` or `zk+hierarchical://...`).
pub fn resolve_metadata_uri(
    metadata_service_uri: Option<&str>,
    zk_servers: Option<&str>,
    ledgers_root: &str,
    parse: impl FnOnce(&str) -> Option<ParsedMetadataUri>,
) -> Result<(ParsedMetadataUri, String), String> {
    if let Some(uri) = metadata_service_uri {
        let parsed = parse(uri).ok_or_else(|| format!("unrecognised metadataServiceUri: {uri}"))?;
        return Ok((parsed, uri.to_string()));
    }
    let Some(servers) = zk_servers else {
        return Err("metadataServiceUri or zkServers required".into());
    };
    Ok((
        ParsedMetadataUri::Zk {
            servers: servers.to_string(),
            ledgers_root: ledgers_root.to_string(),
        },
        format!("zk+hierarchical://{servers}{ledgers_root}"),
    ))
}
