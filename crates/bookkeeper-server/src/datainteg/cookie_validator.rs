//! Cookie validation with optional auto-stamp on mismatch.
//!
//! The server computes the "master" cookie it would stamp now from
//! current config + ZK `instanceId`, then compares it against the ZK cookie
//! and every local directory's cookie. If anything disagrees AND the
//! `data_integrity_stamp_missing_cookies` knob is on, the validator
//! triggers a data-integrity pre-boot check (limbo-and-fence the open
//! ledgers we own) and stamps fresh cookies everywhere. If the knob is
//! off, the mismatch is a hard error, matching Java's behavior.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bookkeeper_metadata::cookie::Cookie;
use bookkeeper_metadata::traits::RegistrationManager;
use thiserror::Error;

use super::check::{DataIntegError, DataIntegrityCheck};

const COOKIE_FILENAME: &str = "VERSION";
const CURRENT_DIR: &str = "current";

#[derive(Debug, Error)]
pub enum CookieValidationError {
    #[error("cluster instance id unavailable")]
    MissingInstanceId,
    #[error("ZK cookie missing but local cookies exist — refusing to start")]
    ZkMissingButLocalsPresent,
    #[error("cookie mismatch and data-integrity stamping is disabled")]
    MismatchStampingDisabled,
    #[error("metadata error: {0}")]
    Metadata(#[from] bookkeeper_metadata::ledger_metadata::MetadataError),
    #[error("data-integrity error: {0}")]
    DataInteg(#[from] DataIntegError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Layout information the validator needs to compute the master cookie
/// and find existing local cookies.
#[derive(Debug, Clone)]
pub struct CookieLayout {
    pub bookie_id: String,
    pub journal_dir: PathBuf,
    pub ledger_dirs: Vec<PathBuf>,
    pub index_dirs: Vec<PathBuf>,
}

impl CookieLayout {
    /// All directories that should carry a `current/VERSION` cookie file.
    /// Matches Java's `Cookie.verify()` which stamps journal + every
    /// ledger dir + every index dir.
    pub fn cookie_dirs(&self) -> Vec<PathBuf> {
        let mut out = vec![self.journal_dir.clone()];
        out.extend(self.ledger_dirs.iter().cloned());
        out.extend(self.index_dirs.iter().cloned());
        out
    }
}

/// Validator that combines cookie verification with data-integrity stamping.
pub struct DataIntegrityCookieValidator {
    layout: CookieLayout,
    registration: Arc<dyn RegistrationManager>,
    check: Option<Arc<DataIntegrityCheck>>,
    stamp_missing_enabled: bool,
}

impl DataIntegrityCookieValidator {
    pub fn new(
        layout: CookieLayout,
        registration: Arc<dyn RegistrationManager>,
        check: Option<Arc<DataIntegrityCheck>>,
        stamp_missing_enabled: bool,
    ) -> Self {
        Self { layout, registration, check, stamp_missing_enabled }
    }

    /// Validate cookies and optionally stamp on mismatch.
    ///
    /// Error taxonomy matches Java's `DataIntegrityCookieValidation`:
    /// - All cookies absent → new install; stamp fresh.
    /// - ZK cookie absent but any directory cookie present → error.
    /// - Mismatch with stamping disabled → error.
    /// - Mismatch with stamping enabled → runs pre-boot check, then
    ///   stamps the master cookie to ZK and every directory.
    /// - Everything matches → no-op.
    pub async fn check_and_stamp(&self) -> Result<(), CookieValidationError> {
        let instance_id = self
            .registration
            .get_cluster_instance_id()
            .await?
            .ok_or(CookieValidationError::MissingInstanceId)?;

        let master = self.build_master_cookie(&instance_id);
        let reg_cookie = self.read_zk_cookie().await?;
        let dir_cookies = self.read_dir_cookies()?;

        match &reg_cookie {
            None => {
                if dir_cookies.iter().any(|c| c.is_some()) {
                    return Err(CookieValidationError::ZkMissingButLocalsPresent);
                }
                tracing::info!("data-integ cookie: new install, stamping cookies");
                self.stamp_all(&master, None).await?;
                return Ok(());
            }
            Some((zk, _version)) => {
                let all_match = zk == &master
                    && dir_cookies.iter().all(|c| c.as_ref() == Some(&master));
                if all_match {
                    return Ok(());
                }
            }
        }

        // Mismatch. Either stamp or error.
        if !self.stamp_missing_enabled {
            return Err(CookieValidationError::MismatchStampingDisabled);
        }

        tracing::warn!(
            "data-integ cookie: cookies disagree with master; running pre-boot check \
             before stamping"
        );
        if let Some(check) = &self.check {
            check.run_preboot_check("INVALID_COOKIE").await?;
        }
        let zk_version = reg_cookie.as_ref().map(|(_, v)| *v);
        self.stamp_all(&master, zk_version).await?;
        Ok(())
    }

    fn build_master_cookie(&self, instance_id: &str) -> Cookie {
        let ledger_dirs: Vec<&str> =
            self.layout.ledger_dirs.iter().map(|p| p.to_str().unwrap_or("")).collect();
        let index_dirs: Option<Vec<&str>> = if self.layout.index_dirs.is_empty() {
            None
        } else {
            Some(self.layout.index_dirs.iter().map(|p| p.to_str().unwrap_or("")).collect())
        };
        Cookie::new(
            &self.layout.bookie_id,
            self.layout.journal_dir.to_str().unwrap_or(""),
            &ledger_dirs,
            Some(instance_id),
            index_dirs.as_deref(),
        )
    }

    async fn read_zk_cookie(&self) -> Result<Option<(Cookie, i64)>, CookieValidationError> {
        match self.registration.read_cookie(&self.layout.bookie_id).await? {
            None => Ok(None),
            Some((data, version)) => {
                let s = String::from_utf8(data)
                    .map_err(|e| bookkeeper_metadata::ledger_metadata::MetadataError::SerializationError(
                        format!("ZK cookie not UTF-8: {e}"),
                    ))?;
                let cookie = Cookie::deserialize(&s)?;
                Ok(Some((cookie, version)))
            }
        }
    }

    fn read_dir_cookies(&self) -> Result<Vec<Option<Cookie>>, CookieValidationError> {
        let mut out = Vec::with_capacity(self.layout.cookie_dirs().len());
        for dir in self.layout.cookie_dirs() {
            out.push(read_cookie_from_dir(&dir)?);
        }
        Ok(out)
    }

    async fn stamp_all(
        &self,
        master: &Cookie,
        zk_version: Option<i64>,
    ) -> Result<(), CookieValidationError> {
        // ZK first: it's the authoritative copy. If disk writes fail after
        // this, the next boot will see a fresh ZK cookie and re-run the
        // data-integrity stamp path, which is idempotent.
        let serialized = master.serialize();
        match zk_version {
            Some(v) => {
                self.registration
                    .update_cookie(&self.layout.bookie_id, serialized.as_bytes(), v)
                    .await?;
            }
            None => {
                self.registration
                    .create_cookie(&self.layout.bookie_id, serialized.as_bytes())
                    .await?;
            }
        }
        for dir in self.layout.cookie_dirs() {
            write_cookie_to_dir(&dir, &serialized)?;
        }
        Ok(())
    }
}

fn cookie_path(dir: &Path) -> PathBuf {
    dir.join(CURRENT_DIR).join(COOKIE_FILENAME)
}

fn read_cookie_from_dir(dir: &Path) -> Result<Option<Cookie>, CookieValidationError> {
    let path = cookie_path(dir);
    match std::fs::read_to_string(&path) {
        Ok(s) => Ok(Some(Cookie::deserialize(&s)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(CookieValidationError::Io(e)),
    }
}

fn write_cookie_to_dir(dir: &Path, serialized: &str) -> Result<(), CookieValidationError> {
    let current = dir.join(CURRENT_DIR);
    std::fs::create_dir_all(&current)?;
    std::fs::write(current.join(COOKIE_FILENAME), serialized)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bookkeeper_metadata::ledger_metadata::MetadataError;
    use bookkeeper_metadata::service_info::BookieServiceInfo;
    use bookkeeper_metadata::traits::{LedgerLayout, RegistrationManager};
    use std::sync::Mutex;
    use tempfile::TempDir;

    struct MockReg {
        instance_id: String,
        cookie: Mutex<Option<(Vec<u8>, i64)>>,
    }
    impl MockReg {
        fn new(instance_id: &str) -> Arc<Self> {
            Arc::new(Self {
                instance_id: instance_id.to_string(),
                cookie: Mutex::new(None),
            })
        }
        fn with_cookie(instance_id: &str, data: &[u8], version: i64) -> Arc<Self> {
            Arc::new(Self {
                instance_id: instance_id.to_string(),
                cookie: Mutex::new(Some((data.to_vec(), version))),
            })
        }
    }

    #[async_trait]
    impl RegistrationManager for MockReg {
        async fn register_bookie(
            &self,
            _id: &str,
            _ro: bool,
            _info: &BookieServiceInfo,
        ) -> Result<(), MetadataError> {
            Ok(())
        }
        async fn unregister_bookie(&self, _id: &str, _ro: bool) -> Result<(), MetadataError> {
            Ok(())
        }
        async fn get_writable_bookies(&self) -> Result<Vec<String>, MetadataError> {
            Ok(vec![])
        }
        async fn get_readonly_bookies(&self) -> Result<Vec<String>, MetadataError> {
            Ok(vec![])
        }
        async fn read_cookie(&self, _id: &str) -> Result<Option<(Vec<u8>, i64)>, MetadataError> {
            Ok(self.cookie.lock().unwrap().clone())
        }
        async fn create_cookie(&self, _id: &str, data: &[u8]) -> Result<(), MetadataError> {
            *self.cookie.lock().unwrap() = Some((data.to_vec(), 0));
            Ok(())
        }
        async fn update_cookie(
            &self,
            _id: &str,
            data: &[u8],
            version: i64,
        ) -> Result<(), MetadataError> {
            *self.cookie.lock().unwrap() = Some((data.to_vec(), version + 1));
            Ok(())
        }
        async fn delete_cookie(&self, _id: &str, _v: i64) -> Result<(), MetadataError> {
            *self.cookie.lock().unwrap() = None;
            Ok(())
        }
        async fn get_cluster_instance_id(&self) -> Result<Option<String>, MetadataError> {
            Ok(Some(self.instance_id.clone()))
        }
        async fn init_new_cluster(
            &self,
            _layout: &LedgerLayout,
            _id: &str,
        ) -> Result<(), MetadataError> {
            Ok(())
        }
    }

    fn layout(journal: &Path, ledger: &Path) -> CookieLayout {
        CookieLayout {
            bookie_id: "bk1:3181".to_string(),
            journal_dir: journal.to_path_buf(),
            ledger_dirs: vec![ledger.to_path_buf()],
            index_dirs: vec![],
        }
    }

    #[tokio::test]
    async fn new_install_stamps_cookies_everywhere() {
        let tmp = TempDir::new().unwrap();
        let journal = tmp.path().join("journal");
        let ledger = tmp.path().join("ledger");
        let reg = MockReg::new("inst-1");
        let v = DataIntegrityCookieValidator::new(
            layout(&journal, &ledger),
            reg.clone(),
            None,
            false,
        );
        v.check_and_stamp().await.unwrap();
        assert!(reg.cookie.lock().unwrap().is_some(), "ZK cookie written");
        assert!(journal.join("current/VERSION").exists());
        assert!(ledger.join("current/VERSION").exists());
    }

    #[tokio::test]
    async fn matching_cookies_are_noop() {
        let tmp = TempDir::new().unwrap();
        let journal = tmp.path().join("journal");
        let ledger = tmp.path().join("ledger");
        // Bootstrap: first call stamps; second call should be a no-op.
        let reg = MockReg::new("inst-1");
        let v = DataIntegrityCookieValidator::new(
            layout(&journal, &ledger),
            reg.clone(),
            None,
            false,
        );
        v.check_and_stamp().await.unwrap();
        let version_before = reg.cookie.lock().unwrap().as_ref().unwrap().1;
        v.check_and_stamp().await.unwrap();
        let version_after = reg.cookie.lock().unwrap().as_ref().unwrap().1;
        assert_eq!(
            version_before, version_after,
            "no-op pass must not bump the ZK version"
        );
    }

    #[tokio::test]
    async fn zk_missing_but_locals_present_errors() {
        let tmp = TempDir::new().unwrap();
        let journal = tmp.path().join("journal");
        let ledger = tmp.path().join("ledger");
        // Drop a stale local cookie.
        std::fs::create_dir_all(journal.join("current")).unwrap();
        let stale = Cookie::new("bk1:3181", journal.to_str().unwrap(), &[ledger.to_str().unwrap()], Some("old"), None);
        std::fs::write(journal.join("current/VERSION"), stale.serialize()).unwrap();

        let reg = MockReg::new("inst-1");
        let v = DataIntegrityCookieValidator::new(
            layout(&journal, &ledger),
            reg,
            None,
            true,
        );
        let err = v.check_and_stamp().await.unwrap_err();
        assert!(matches!(err, CookieValidationError::ZkMissingButLocalsPresent));
    }

    #[tokio::test]
    async fn mismatch_with_stamping_disabled_errors() {
        let tmp = TempDir::new().unwrap();
        let journal = tmp.path().join("journal");
        let ledger = tmp.path().join("ledger");
        // Pre-seed a ZK cookie with a *different* instance id than what
        // the validator will compute.
        let stale = Cookie::new(
            "bk1:3181",
            journal.to_str().unwrap(),
            &[ledger.to_str().unwrap()],
            Some("OLD-INSTANCE"),
            None,
        );
        let reg = MockReg::with_cookie("inst-1", stale.serialize().as_bytes(), 7);

        let v = DataIntegrityCookieValidator::new(
            layout(&journal, &ledger),
            reg,
            None,
            false,
        );
        let err = v.check_and_stamp().await.unwrap_err();
        assert!(matches!(err, CookieValidationError::MismatchStampingDisabled));
    }

    #[tokio::test]
    async fn mismatch_with_stamping_updates_zk_and_disks() {
        let tmp = TempDir::new().unwrap();
        let journal = tmp.path().join("journal");
        let ledger = tmp.path().join("ledger");
        let stale = Cookie::new(
            "bk1:3181",
            journal.to_str().unwrap(),
            &[ledger.to_str().unwrap()],
            Some("OLD-INSTANCE"),
            None,
        );
        let reg = MockReg::with_cookie("inst-1", stale.serialize().as_bytes(), 3);
        let v = DataIntegrityCookieValidator::new(
            layout(&journal, &ledger),
            reg.clone(),
            None,  // no DataIntegrityCheck injected — stamping still proceeds
            true,
        );

        v.check_and_stamp().await.unwrap();

        let zk_cookie = reg.cookie.lock().unwrap().clone().unwrap();
        assert_eq!(zk_cookie.1, 4, "ZK version should have been bumped via CAS");
        let stamped = Cookie::deserialize(std::str::from_utf8(&zk_cookie.0).unwrap()).unwrap();
        assert_eq!(stamped.instance_id(), Some("inst-1"));
        // Local dirs now carry the fresh cookie too.
        let journal_cookie = std::fs::read_to_string(journal.join("current/VERSION")).unwrap();
        assert_eq!(Cookie::deserialize(&journal_cookie).unwrap(), stamped);
    }

    #[tokio::test]
    async fn mismatch_with_stamping_runs_preboot_check() {
        use crate::datainteg::check::{DataIntegrityCheck, DataIntegrityConfig};
        use crate::datainteg::entry_copier::EntryCopier;
        use crate::datainteg::test_helpers::{EmptyLedgerManager, NoopRecovery, StubStorage};
        use bookkeeper_metadata::traits::LedgerManager;
        use bookkeeper_storage::ledger_storage::{LedgerStorage, StorageStateFlag};

        let tmp = TempDir::new().unwrap();
        let journal = tmp.path().join("journal");
        let ledger = tmp.path().join("ledger");
        let stale = Cookie::new(
            "bk1:3181",
            journal.to_str().unwrap(),
            &[ledger.to_str().unwrap()],
            Some("OLD-INSTANCE"),
            None,
        );
        let reg = MockReg::with_cookie("inst-1", stale.serialize().as_bytes(), 1);

        // Build a real DataIntegrityCheck over a stub storage — run_preboot_check
        // sets `NEEDS_INTEGRITY_CHECK` on the storage, which we can observe.
        let storage = StubStorage::new(0);
        let lm: Arc<dyn LedgerManager> = Arc::new(EmptyLedgerManager);
        let copier = Arc::new(EntryCopier::new(
            "bk1:3181".to_string(),
            storage.clone() as Arc<dyn LedgerStorage>,
        ));
        let check = Arc::new(DataIntegrityCheck::with_config(
            "bk1:3181".to_string(),
            lm,
            storage.clone() as Arc<dyn LedgerStorage>,
            copier,
            Arc::new(NoopRecovery),
            DataIntegrityConfig { max_inflight: 4, max_entries_inflight: 4 },
        ));

        let v = DataIntegrityCookieValidator::new(
            layout(&journal, &ledger),
            reg,
            Some(check),
            true,
        );
        v.check_and_stamp().await.unwrap();

        let flags = *storage.flags.lock().unwrap();
        assert_eq!(
            flags & StorageStateFlag::NeedsIntegrityCheck.bits(),
            StorageStateFlag::NeedsIntegrityCheck.bits(),
            "pre-boot hook must have been invoked (NEEDS_INTEGRITY_CHECK set)"
        );
    }
}
