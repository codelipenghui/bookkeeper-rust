use std::sync::Mutex;
use async_trait::async_trait;
use zookeeper_client as zk;

use crate::ledger_metadata::MetadataError;
use crate::traits::LedgerIdGenerator;
use crate::zk_path::ZkPaths;

use crate::metadata_paths::{HOB_GC_KEEP, HOB_PREFIX, SHORT_ID_PREFIX as LEDGER_ID_GEN_PREFIX};

/// Status of the long ledger ID generation path in ZK.
/// Matches Java's HighOrderLedgerIdGenPathStatus.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LongIdPathStatus {
    Unknown,
    Present,
    NotPresent,
}

/// ZooKeeper-backed ledger ID generator with automatic transition from
/// 31-bit to 63-bit IDs.
///
/// Matches Java's `LongZkLedgerIdGenerator`:
/// 1. **Phase 1** (31-bit): Uses sequential znodes at `{root}/idgen/ID-`
///    - IDs range from 0 to `i32::MAX - 1` (2,147,483,646)
/// 2. **Phase 2** (63-bit): Auto-transitions when 31-bit space is exhausted
///    - Uses two-tier scheme at `{root}/idgen-long/HOB-{%010d}/ID-`
///    - High-order bits (HOB) start at 1, low bits from sequential counter
///    - Combined: `(high_bits << 32) | low_bits`
///    - Old HOB directories are garbage collected (keep 3 most recent)
pub struct ZkLedgerIdGenerator {
    client: zk::Client,
    /// Path prefix for short IDs: `{root}/idgen/ID-`
    short_prefix: String,
    /// Base path for long IDs: `{root}/idgen-long`
    long_base: String,
    /// Cached status of long ID path existence.
    long_path_status: Mutex<LongIdPathStatus>,
    /// Cached list of HOB directories (sorted).
    hob_cache: Mutex<Option<Vec<i32>>>,
}

impl ZkLedgerIdGenerator {
    pub async fn new(client: zk::Client, paths: &ZkPaths) -> Result<Self, MetadataError> {
        let idgen_path = paths.idgen().to_string();
        let short_prefix = format!("{}/{}", idgen_path, LEDGER_ID_GEN_PREFIX);
        let long_base = paths.idgen_long().to_string();

        // Ensure the short idgen directory exists
        crate::zk_path::ensure_zk_path(&client, &idgen_path).await?;

        Ok(Self {
            client,
            short_prefix,
            long_base,
            long_path_status: Mutex::new(LongIdPathStatus::Unknown),
            hob_cache: Mutex::new(None),
        })
    }

    /// Check if the long ID generation path exists in ZK.
    /// Caches the result after first check.
    async fn is_long_path_present(&self) -> Result<bool, MetadataError> {
        {
            let status = self.long_path_status.lock().unwrap();
            match *status {
                LongIdPathStatus::Present => return Ok(true),
                LongIdPathStatus::NotPresent => return Ok(false),
                LongIdPathStatus::Unknown => {} // need to check
            }
        }

        let exists = self.client.check_stat(&self.long_base).await
            .map_err(|e| MetadataError::ZkError(format!("check idgen-long: {e}")))?
            .is_some();

        let mut status = self.long_path_status.lock().unwrap();
        *status = if exists {
            LongIdPathStatus::Present
        } else {
            LongIdPathStatus::NotPresent
        };

        Ok(exists)
    }

    /// Generate a 31-bit ledger ID using the short sequential scheme.
    async fn generate_short_id(&self) -> Result<i64, MetadataError> {
        let options = zk::CreateMode::EphemeralSequential.with_acls(zk::Acls::anyone_all());
        let (_stat, seq) = self
            .client
            .create(&self.short_prefix, &[], &options)
            .await
            .map_err(|e| MetadataError::ZkError(format!("create short idgen: {e}")))?;

        let ledger_id = seq.into_i64();

        // Cleanup ephemeral znode
        let created_path = format!("{}{}", self.short_prefix, seq);
        let _ = self.client.delete(&created_path, None).await;

        if ledger_id >= i32::MAX as i64 {
            return Err(MetadataError::LedgerIdOverflow);
        }

        Ok(ledger_id)
    }

    /// Transition to long ID generation by creating the idgen-long path.
    async fn create_long_id_path(&self) -> Result<(), MetadataError> {
        crate::zk_path::ensure_zk_path(&self.client, &self.long_base).await?;
        let mut status = self.long_path_status.lock().unwrap();
        *status = LongIdPathStatus::Present;
        Ok(())
    }

    /// Generate a 63-bit ledger ID using the two-tier HOB scheme.
    /// Retries with a new HOB directory if the current one's low bits overflow.
    async fn generate_long_id(&self) -> Result<i64, MetadataError> {
        // Allow up to a few retries for HOB overflow (each HOB holds 2^31 IDs)
        for _ in 0..5 {
            let high_bits = self.get_current_hob().await?;

            match self.generate_long_id_low_bits(high_bits).await {
                Ok(id) => {
                    self.gc_old_hobs().await;
                    return Ok(id);
                }
                Err(MetadataError::LedgerIdOverflow) => {
                    // Low bits exhausted — create next HOB and loop
                    self.create_hob_directory(high_bits + 1).await?;
                    self.invalidate_hob_cache();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(MetadataError::ZkError("long ID generation failed after retries".into()))
    }

    /// Find the current (largest) HOB value from ZK.
    /// Matches Java's getCurrentRange() logic.
    async fn get_current_hob(&self) -> Result<i32, MetadataError> {
        // Try cached value first
        {
            let cache = self.hob_cache.lock().unwrap();
            if let Some(ref dirs) = *cache {
                if let Some(&max) = dirs.last() {
                    return Ok(max);
                }
            }
        }

        // Fetch from ZK
        let hob_dirs = self.fetch_hob_directories().await?;

        if let Some(&max) = hob_dirs.last() {
            let mut cache = self.hob_cache.lock().unwrap();
            *cache = Some(hob_dirs);
            Ok(max)
        } else {
            // No HOB directories exist yet — create the first one (HOB-0000000001)
            self.create_hob_directory(1).await?;
            self.invalidate_hob_cache();
            Ok(1)
        }
    }

    /// List all HOB-{n} directories under idgen-long, sorted ascending.
    async fn fetch_hob_directories(&self) -> Result<Vec<i32>, MetadataError> {
        let (children, _) = self.client.get_children(&self.long_base).await
            .map_err(|e| MetadataError::ZkError(format!("list HOB dirs: {e}")))?;

        let mut hobs: Vec<i32> = children
            .iter()
            .filter_map(|c| {
                c.strip_prefix(HOB_PREFIX)
                    .and_then(|s| s.parse::<i32>().ok())
            })
            .collect();
        hobs.sort_unstable();
        Ok(hobs)
    }

    fn hob_path(&self, hob: i32) -> String {
        format!("{}/{}{:010}", self.long_base, HOB_PREFIX, hob)
    }

    /// Create a new HOB directory. Ignores NodeExists (race-safe).
    async fn create_hob_directory(&self, hob: i32) -> Result<(), MetadataError> {
        let path = self.hob_path(hob);
        let options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
        match self.client.create(&path, &[], &options).await {
            Ok(_) => Ok(()),
            Err(zk::Error::NodeExists) => Ok(()), // another generator created it
            Err(e) => Err(MetadataError::ZkError(format!("create HOB-{hob}: {e}"))),
        }
    }

    /// Generate low bits under a specific HOB directory.
    /// Returns the combined 63-bit ID: `(high_bits << 32) | low_bits`.
    async fn generate_long_id_low_bits(&self, high_bits: i32) -> Result<i64, MetadataError> {
        let id_prefix = format!("{}/{}", self.hob_path(high_bits), LEDGER_ID_GEN_PREFIX);

        // Ensure HOB directory exists
        let _ = self.create_hob_directory(high_bits).await;

        let options = zk::CreateMode::EphemeralSequential.with_acls(zk::Acls::anyone_all());
        let (_stat, seq) = self
            .client
            .create(&id_prefix, &[], &options)
            .await
            .map_err(|e| MetadataError::ZkError(format!("create long idgen: {e}")))?;

        let low_bits = seq.into_i64();

        // Cleanup
        let created_path = format!("{}{}", id_prefix, seq);
        let _ = self.client.delete(&created_path, None).await;

        if low_bits >= i32::MAX as i64 {
            return Err(MetadataError::LedgerIdOverflow);
        }

        // Combine: (highBits << 32) | lowBits
        let combined = ((high_bits as i64) << 32) | low_bits;
        Ok(combined)
    }

    /// Garbage collect old HOB directories, keeping the 3 most recent.
    /// Matches Java's cleanup logic. Errors are silently ignored.
    async fn gc_old_hobs(&self) {
        let hobs = match self.fetch_hob_directories().await {
            Ok(h) => h,
            Err(_) => return,
        };

        if hobs.len() <= HOB_GC_KEEP {
            return;
        }

        // Delete all but the last 3
        let to_delete = &hobs[..hobs.len() - HOB_GC_KEEP];
        for &hob in to_delete {
            // Ignore errors — directory might not be empty or already deleted
            let _ = self.client.delete(&self.hob_path(hob), None).await;
        }
    }

    fn invalidate_hob_cache(&self) {
        let mut cache = self.hob_cache.lock().unwrap();
        *cache = None;
    }
}

#[async_trait]
impl LedgerIdGenerator for ZkLedgerIdGenerator {
    /// Generate a unique ledger ID.
    ///
    /// Matches Java's LongZkLedgerIdGenerator.generateLedgerId():
    /// - If long ID path does NOT exist: use short 31-bit generator
    /// - If short generator overflows: create long path and transition
    /// - If long ID path exists: use two-tier HOB generator
    async fn generate_ledger_id(&self) -> Result<i64, MetadataError> {
        if !self.is_long_path_present().await? {
            // Phase 1: Try short 31-bit generation
            match self.generate_short_id().await {
                Ok(id) => return Ok(id),
                Err(MetadataError::LedgerIdOverflow) => {
                    // Transition to long IDs
                    self.create_long_id_path().await?;
                    // Fall through to long generation
                }
                Err(e) => return Err(e),
            }
        }

        // Phase 2: Long 63-bit generation
        self.generate_long_id().await
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_id_composition() {
        // Matches Java: (highBits << 32) | lowBits
        let high: i64 = 1;
        let low: i64 = 42;
        let combined = (high << 32) | low;
        assert_eq!(combined, 4294967338); // 1 * 2^32 + 42

        // Verify decomposition
        assert_eq!(combined >> 32, 1);
        assert_eq!(combined & 0xFFFF_FFFF, 42);
    }

    #[test]
    fn test_id_composition_high_bits() {
        let high: i64 = 100;
        let low: i64 = 999;
        let combined = (high << 32) | low;
        assert_eq!(combined >> 32, 100);
        assert_eq!(combined & 0xFFFF_FFFF, 999);
    }

    #[test]
    fn test_hob_format() {
        // HOB directories use %010d padding
        assert_eq!(format!("HOB-{:010}", 1), "HOB-0000000001");
        assert_eq!(format!("HOB-{:010}", 999), "HOB-0000000999");
    }

    #[test]
    fn test_short_id_overflow_threshold() {
        assert_eq!(i32::MAX as i64, 2147483647);
    }
}
