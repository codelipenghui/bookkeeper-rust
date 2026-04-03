/// Compute the ZK path for a ledger ID using the hybrid hierarchical scheme.
/// For ledgerId < i32::MAX (2^31 - 1): 3-level (short) hierarchy with 2-4-4 split.
/// For ledgerId >= i32::MAX: 5-level (long) hierarchy with 3-4-4-4-4 split.
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

/// Compute the ZK path for an underreplicated ledger marker.
/// Uses 4-level hex hierarchy from 64-bit ledger ID.
pub fn underreplication_ledger_path(base: &str, ledger_id: i64) -> String {
    let id = ledger_id as u64;
    format!(
        "{}/{:04x}/{:04x}/{:04x}/{:04x}/urL{}",
        base,
        (id >> 48) & 0xFFFF,
        (id >> 32) & 0xFFFF,
        (id >> 16) & 0xFFFF,
        id & 0xFFFF,
        ledger_id,
    )
}

/// Well-known ZK paths relative to the ledger root.
pub struct ZkPaths {
    root: String,
}

impl ZkPaths {
    pub fn new(root: &str) -> Self {
        Self { root: root.to_string() }
    }

    pub fn root(&self) -> &str { &self.root }
    pub fn available(&self) -> String { format!("{}/available", self.root) }
    pub fn available_readonly(&self) -> String { format!("{}/available/readonly", self.root) }
    pub fn cookies(&self) -> String { format!("{}/cookies", self.root) }
    pub fn cookie_path(&self, bookie_id: &str) -> String { format!("{}/cookies/{}", self.root, bookie_id) }
    pub fn layout(&self) -> String { format!("{}/LAYOUT", self.root) }
    pub fn instance_id(&self) -> String { format!("{}/instanceid", self.root) }
    pub fn idgen(&self) -> String { format!("{}/idgen", self.root) }
    pub fn idgen_long(&self) -> String { format!("{}/idgen-long", self.root) }
    pub fn underreplication(&self) -> String { format!("{}/underreplication", self.root) }
    pub fn bookie_registration(&self, bookie_id: &str) -> String {
        format!("{}/available/{}", self.root, bookie_id)
    }
    pub fn bookie_readonly_registration(&self, bookie_id: &str) -> String {
        format!("{}/available/readonly/{}", self.root, bookie_id)
    }
}
