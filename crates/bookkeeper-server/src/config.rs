/// Minimal bookie configuration for Phase 4.
#[derive(Debug, Clone)]
pub struct BookieConfig {
    pub port: u16,
    pub ledger_dirs: Vec<String>,
    pub journal_dir: String,
    pub max_frame_size: usize,
    pub zk_connect: Option<String>,   // ZK connection string, None = no ZK
    pub zk_ledger_root: String,       // ZK ledger root path
    pub bookie_id: Option<String>,    // Optional explicit bookie ID
}

impl Default for BookieConfig {
    fn default() -> Self {
        Self {
            port: 3181,
            ledger_dirs: vec!["/tmp/bk-data".to_string()],
            journal_dir: "/tmp/bk-txn".to_string(),
            max_frame_size: 5 * 1024 * 1024,
            zk_connect: None,
            zk_ledger_root: "/ledgers".to_string(),
            bookie_id: None,
        }
    }
}
