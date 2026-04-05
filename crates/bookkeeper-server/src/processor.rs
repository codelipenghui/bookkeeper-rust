use std::sync::Arc;
use bytes::Bytes;

use bookkeeper_common::error::BKError;
use bookkeeper_common::types::LAST_ADD_CONFIRMED;
use bookkeeper_protocol::constants::*;
use bookkeeper_protocol::v2::request::V2Request;
use bookkeeper_protocol::v2::response::V2Response;
use bookkeeper_storage::ledger_storage::LedgerStorage;
use crate::state::BookieStateManager;

pub struct RequestProcessor {
    storage: Arc<dyn LedgerStorage>,
    state: Arc<BookieStateManager>,
}

impl RequestProcessor {
    pub fn new(storage: Arc<dyn LedgerStorage>, state: Arc<BookieStateManager>) -> Self {
        Self { storage, state }
    }

    pub fn process_request(&self, req: V2Request) -> V2Response {
        match req {
            V2Request::AddEntry { flags, master_key, entry_data } => {
                let entry_size = entry_data.len();
                let is_recovery = flags & FLAG_RECOVERY_ADD != 0;
                crate::metrics::ADD_ENTRY_IN_PROGRESS.inc();
                let timer = std::time::Instant::now();
                let resp = self.handle_add_entry(flags, &master_key, entry_data);
                let elapsed = timer.elapsed().as_secs_f64();
                crate::metrics::ADD_ENTRY_IN_PROGRESS.dec();
                let success = matches!(resp, V2Response::Add { error_code, .. } if error_code == V2_EOK);
                let label = if success { "true" } else { "false" };
                crate::metrics::ADD_ENTRY_REQUEST.with_label_values(&[label]).observe(elapsed);
                crate::metrics::BOOKIE_ADD_ENTRY.with_label_values(&[label]).observe(elapsed);
                if is_recovery {
                    crate::metrics::BOOKIE_RECOVERY_ADD_ENTRY.with_label_values(&[label]).observe(elapsed);
                }
                if success {
                    crate::metrics::WRITE_BYTES.inc_by(entry_size as u64);
                    crate::metrics::BOOKIE_ADD_ENTRY_BYTES.with_label_values(&["true"]).observe(entry_size as f64);
                } else {
                    crate::metrics::ADD_ENTRY_REJECTED.inc();
                }
                resp
            }
            V2Request::ReadEntry { flags, ledger_id, entry_id, master_key } => {
                let is_fence = flags & FLAG_DO_FENCING != 0;
                crate::metrics::READ_ENTRY_IN_PROGRESS.inc();
                let timer = std::time::Instant::now();
                let resp = self.handle_read_entry(flags, ledger_id, entry_id, master_key);
                let elapsed = timer.elapsed().as_secs_f64();
                crate::metrics::READ_ENTRY_IN_PROGRESS.dec();
                let success = matches!(resp, V2Response::Read { error_code, .. } if error_code == V2_EOK);
                let label = if success { "true" } else { "false" };
                crate::metrics::READ_ENTRY_REQUEST.with_label_values(&[label]).observe(elapsed);
                crate::metrics::BOOKIE_READ_ENTRY.with_label_values(&[label]).observe(elapsed);
                if is_fence {
                    crate::metrics::READ_ENTRY_FENCE_REQUEST.with_label_values(&[label]).observe(elapsed);
                }
                if success {
                    if let V2Response::Read { data: Some(ref d), .. } = resp {
                        let sz = d.len() as u64;
                        crate::metrics::READ_BYTES.inc_by(sz);
                        crate::metrics::BOOKIE_READ_ENTRY_BYTES.with_label_values(&["true"]).observe(sz as f64);
                    }
                }
                resp
            }
            V2Request::WriteLac { ledger_id, lac, master_key, body: _ } => {
                // Validate master key before writing LAC
                if let Err(e) = self.storage.check_master_key(ledger_id, &master_key) {
                    return V2Response::WriteLac {
                        error_code: self.storage_error_to_bk(&e).to_v2_code(),
                        ledger_id,
                        entry_id: 0,
                    };
                }
                self.handle_write_lac(ledger_id, lac)
            }
            V2Request::ReadLac { ledger_id } => {
                self.handle_read_lac(ledger_id)
            }
            V2Request::GetBookieInfo { flags: _, requested } => {
                self.handle_get_bookie_info(requested)
            }
            V2Request::Auth { payload } => {
                V2Response::Auth { error_code: V2_EOK, payload }
            }
            V2Request::BatchReadEntry { flags, ledger_id, start_entry_id, request_id, max_count, max_size } => {
                self.handle_batch_read(flags, ledger_id, start_entry_id, request_id, max_count, max_size)
            }
        }
    }

    fn handle_add_entry(&self, flags: u16, master_key: &[u8; MASTER_KEY_LENGTH], entry_data: Bytes) -> V2Response {
        if entry_data.len() < 16 {
            return self.add_error_response(BKError::BadRequest, 0, 0);
        }
        let ledger_id = i64::from_be_bytes(entry_data[0..8].try_into().unwrap());
        let entry_id = i64::from_be_bytes(entry_data[8..16].try_into().unwrap());
        let is_recovery = flags & FLAG_RECOVERY_ADD != 0;

        if !self.state.is_writable() {
            return self.add_error_response(BKError::ReadOnly, ledger_id, entry_id);
        }

        // Single RocksDB read: validate existence + master key + get fenced status
        match self.storage.validate_for_write(ledger_id, master_key) {
            Ok(is_fenced) => {
                if is_fenced && !is_recovery {
                    return self.add_error_response(BKError::Fenced, ledger_id, entry_id);
                }
                if is_recovery {
                    if let Err(e) = self.storage.set_fenced(ledger_id) {
                        // Best-effort fencing for recovery adds — only fail on serious errors
                        if !matches!(e, bookkeeper_storage::StorageError::LedgerNotFound(_)) {
                            return self.add_error_response(self.storage_error_to_bk(&e), ledger_id, entry_id);
                        }
                    }
                }
            }
            Err(e) => return self.add_error_response(self.storage_error_to_bk(&e), ledger_id, entry_id),
        }

        // Matches Java's BookieImpl.addEntryInternal: storage first, then journal.
        match self.storage.add_entry(&entry_data) {
            Ok(()) => {
                if let Some(journal) = self.storage.journal() {
                    // Journal master key on first write to this ledger (matching Java)
                    let _ = journal.maybe_journal_master_key(ledger_id, master_key);
                    // Journal the entry for durability
                    if let Err(_e) = journal.log_add_entry(&entry_data) {
                        return self.add_error_response(BKError::Io, ledger_id, entry_id);
                    }
                }
                V2Response::Add { error_code: V2_EOK, ledger_id, entry_id }
            }
            Err(e) => self.add_error_response(self.storage_error_to_bk(&e), ledger_id, entry_id),
        }
    }

    fn handle_read_entry(&self, flags: u16, ledger_id: i64, entry_id: i64, master_key: Option<[u8; MASTER_KEY_LENGTH]>) -> V2Response {
        // Check readable state
        if !self.state.is_readable() {
            return self.read_error_response(BKError::ReadOnly, ledger_id, entry_id);
        }

        // Handle fencing
        if flags & FLAG_DO_FENCING != 0 {
            if let Some(key) = master_key {
                // Validate master key before fencing
                if let Err(e) = self.storage.check_master_key(ledger_id, &key) {
                    return self.read_error_response(self.storage_error_to_bk(&e), ledger_id, entry_id);
                }
                if let Err(e) = self.storage.set_fenced(ledger_id) {
                    return self.read_error_response(self.storage_error_to_bk(&e), ledger_id, entry_id);
                }
            }
        }

        // Special case: LAST_ADD_CONFIRMED (-1)
        // Java client expects the last entry body so it can verify digest and extract LAC+length
        if entry_id == LAST_ADD_CONFIRMED {
            match self.storage.get_last_add_confirmed(ledger_id) {
                Ok(lac) => {
                    // Try to return the actual last entry body for digest verification
                    let data = if lac >= 0 {
                        self.storage.get_entry(ledger_id, lac).ok()
                    } else {
                        None
                    };
                    return V2Response::Read {
                        error_code: V2_EOK,
                        ledger_id,
                        entry_id: lac,
                        data,
                    };
                }
                Err(_) => {
                    return self.read_error_response(BKError::NoLedger, ledger_id, entry_id);
                }
            }
        }

        // Normal read
        match self.storage.get_entry(ledger_id, entry_id) {
            Ok(data) => V2Response::Read {
                error_code: V2_EOK,
                ledger_id,
                entry_id,
                data: Some(data),
            },
            Err(e) => self.read_error_response(self.storage_error_to_bk(&e), ledger_id, entry_id),
        }
    }

    fn handle_write_lac(&self, ledger_id: i64, lac: i64) -> V2Response {
        match self.storage.set_explicit_lac(ledger_id, lac) {
            Ok(()) => V2Response::WriteLac {
                error_code: V2_EOK,
                ledger_id,
                entry_id: 0,
            },
            Err(e) => V2Response::WriteLac {
                error_code: self.storage_error_to_bk(&e).to_v2_code(),
                ledger_id,
                entry_id: 0,
            },
        }
    }

    fn handle_read_lac(&self, ledger_id: i64) -> V2Response {
        match self.storage.get_last_add_confirmed(ledger_id) {
            Ok(lac) => V2Response::ReadLac {
                error_code: V2_EOK,
                ledger_id,
                entry_id: lac,
                lac_body: None,
                last_entry_body: None,
            },
            Err(e) => V2Response::ReadLac {
                error_code: self.storage_error_to_bk(&e).to_v2_code(),
                ledger_id,
                entry_id: 0,
                lac_body: None,
                last_entry_body: None,
            },
        }
    }

    fn handle_get_bookie_info(&self, _requested: i64) -> V2Response {
        let (free, total) = get_disk_space();
        V2Response::GetBookieInfo {
            error_code: V2_EOK,
            ledger_id: 0,
            entry_id: 0,
            free_disk_space: free,
            total_disk_capacity: total,
        }
    }

    fn handle_batch_read(&self, flags: u16, ledger_id: i64, start_entry_id: i64, request_id: i64, max_count: i32, max_size: i64) -> V2Response {
        // Check readable state
        if !self.state.is_readable() {
            return V2Response::BatchRead {
                error_code: BKError::ReadOnly.to_v2_code(),
                ledger_id,
                entry_id: start_entry_id,
                request_id,
                entries: vec![],
            };
        }

        // Note: batch_read does not fence — fencing requires master key auth
        // which our v2 BatchReadEntry codec does not yet include.
        let _ = flags; // suppress unused warning

        let mut entries = Vec::new();
        let mut total_size: i64 = 0;
        for i in 0..max_count as i64 {
            let eid = start_entry_id + i;
            match self.storage.get_entry(ledger_id, eid) {
                Ok(data) => {
                    total_size += data.len() as i64;
                    if total_size > max_size && !entries.is_empty() {
                        break;
                    }
                    entries.push(data);
                }
                Err(_) => break,
            }
        }

        V2Response::BatchRead {
            error_code: if entries.is_empty() {
                BKError::NoEntry.to_v2_code()
            } else {
                V2_EOK
            },
            ledger_id,
            entry_id: start_entry_id,
            request_id,
            entries,
        }
    }

    pub fn force_flush(&self) -> Result<(), bookkeeper_storage::StorageError> {
        self.storage.flush()
    }

    fn add_error_response(&self, err: BKError, ledger_id: i64, entry_id: i64) -> V2Response {
        V2Response::Add {
            error_code: err.to_v2_code(),
            ledger_id,
            entry_id,
        }
    }

    fn read_error_response(&self, err: BKError, ledger_id: i64, entry_id: i64) -> V2Response {
        V2Response::Read { error_code: err.to_v2_code(), ledger_id, entry_id, data: None }
    }

    fn storage_error_to_bk(&self, err: &bookkeeper_storage::StorageError) -> BKError {
        match err {
            bookkeeper_storage::StorageError::EntryNotFound { .. } => BKError::NoEntry,
            bookkeeper_storage::StorageError::LedgerNotFound(_) => BKError::NoLedger,
            bookkeeper_storage::StorageError::Fenced(_) => BKError::Fenced,
            bookkeeper_storage::StorageError::StorageFull => BKError::ReadOnly,
            bookkeeper_storage::StorageError::Unauthorized(_) => BKError::Unauthorized,
            _ => BKError::Io,
        }
    }
}

fn get_disk_space() -> (i64, i64) {
    #[cfg(unix)]
    {
        match nix_disk_space("/") {
            Some((free, total)) => (free, total),
            None => (0, 0),
        }
    }
    #[cfg(not(unix))]
    {
        (0, 0)
    }
}

#[cfg(unix)]
fn nix_disk_space(path: &str) -> Option<(i64, i64)> {
    use std::ffi::CString;
    let c_path = CString::new(path).ok()?;
    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
            let total = (stat.f_blocks as i64) * (stat.f_frsize as i64);
            let free = (stat.f_bavail as i64) * (stat.f_frsize as i64);
            Some((free, total))
        } else {
            None
        }
    }
}
