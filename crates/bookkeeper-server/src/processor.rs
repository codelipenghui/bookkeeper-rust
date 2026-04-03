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
                self.handle_add_entry(flags, &master_key, entry_data)
            }
            V2Request::ReadEntry { flags, ledger_id, entry_id, master_key } => {
                self.handle_read_entry(flags, ledger_id, entry_id, master_key)
            }
            V2Request::WriteLac { ledger_id, lac, master_key: _, body: _ } => {
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
        // Extract ledger_id and entry_id from entry data (first 16 bytes)
        if entry_data.len() < 16 {
            return self.add_error_response(BKError::BadRequest, 0, 0);
        }
        let ledger_id = i64::from_be_bytes(entry_data[0..8].try_into().unwrap());
        let entry_id = i64::from_be_bytes(entry_data[8..16].try_into().unwrap());
        let is_recovery = flags & FLAG_RECOVERY_ADD != 0;

        // Check bookie is writable
        if !self.state.is_writable() {
            return self.add_error_response(BKError::ReadOnly, ledger_id, entry_id);
        }

        // Check fencing (skip for recovery adds)
        if !is_recovery {
            if let Ok(fenced) = self.storage.is_fenced(ledger_id) {
                if fenced {
                    return self.add_error_response(BKError::Fenced, ledger_id, entry_id);
                }
            }
        } else {
            // Recovery add: set fenced flag on the ledger
            let _ = self.storage.set_fenced(ledger_id);
        }

        // Set master key on first write
        let _ = self.storage.set_master_key(ledger_id, master_key);

        // Write to storage
        match self.storage.add_entry(&entry_data) {
            Ok(()) => V2Response::Add {
                error_code: V2_EOK,
                ledger_id,
                entry_id,
            },
            Err(e) => self.add_error_response(self.storage_error_to_bk(&e), ledger_id, entry_id),
        }
    }

    fn handle_read_entry(&self, flags: u16, ledger_id: i64, entry_id: i64, master_key: Option<[u8; MASTER_KEY_LENGTH]>) -> V2Response {
        // Handle fencing
        if flags & FLAG_DO_FENCING != 0 {
            if let Some(_key) = master_key {
                let _ = self.storage.set_fenced(ledger_id);
            }
        }

        // Special case: LAST_ADD_CONFIRMED (-1)
        if entry_id == LAST_ADD_CONFIRMED {
            match self.storage.get_last_add_confirmed(ledger_id) {
                Ok(lac) => {
                    return V2Response::Read {
                        error_code: V2_EOK,
                        ledger_id,
                        entry_id: lac,
                        data: None,
                    };
                }
                Err(_) => {
                    return V2Response::Read {
                        error_code: BKError::NoLedger.to_v2_code(),
                        ledger_id,
                        entry_id,
                        data: None,
                    };
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
            Err(e) => V2Response::Read {
                error_code: self.storage_error_to_bk(&e).to_v2_code(),
                ledger_id,
                entry_id,
                data: None,
            },
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
        // Handle fencing
        if flags & FLAG_DO_FENCING != 0 {
            let _ = self.storage.set_fenced(ledger_id);
        }

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

    fn add_error_response(&self, err: BKError, ledger_id: i64, entry_id: i64) -> V2Response {
        V2Response::Add {
            error_code: err.to_v2_code(),
            ledger_id,
            entry_id,
        }
    }

    fn storage_error_to_bk(&self, err: &bookkeeper_storage::StorageError) -> BKError {
        match err {
            bookkeeper_storage::StorageError::EntryNotFound { .. } => BKError::NoEntry,
            bookkeeper_storage::StorageError::LedgerNotFound(_) => BKError::NoLedger,
            bookkeeper_storage::StorageError::Fenced(_) => BKError::Fenced,
            bookkeeper_storage::StorageError::StorageFull => BKError::ReadOnly,
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
