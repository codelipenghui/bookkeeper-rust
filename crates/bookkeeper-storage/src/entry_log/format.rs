use crate::StorageError;

pub const ENTRY_LOG_MAGIC: &[u8; 4] = b"BKLO";
pub const LOGFILE_HEADER_SIZE: usize = 1024;
pub const HEADER_V0: i32 = 0;
pub const HEADER_V1: i32 = 1;
pub const HEADER_CURRENT_VERSION: i32 = HEADER_V1;
pub const INVALID_LID: i64 = -1;
pub const LEDGERS_MAP_ENTRY_ID: i64 = -2;
pub const LEDGERS_MAP_HEADER_SIZE: usize = 4 + 8 + 8 + 4; // 24 bytes
pub const LEDGERS_MAP_ENTRY_SIZE: usize = 16; // 8 + 8
pub const LEDGERS_MAP_MAX_BATCH_SIZE: usize = 10_000;
pub const MIN_SANE_ENTRY_SIZE: usize = 16; // ledgerId(8) + entryId(8)

/// Entry log file header (1024 bytes).
#[derive(Debug, Clone)]
pub struct EntryLogHeader {
    version: i32,
    ledgers_map_offset: i64,
    ledgers_count: i32,
}

impl EntryLogHeader {
    pub fn new(version: i32) -> Self {
        Self {
            version,
            ledgers_map_offset: 0,
            ledgers_count: 0,
        }
    }

    pub fn version(&self) -> i32 {
        self.version
    }
    pub fn ledgers_map_offset(&self) -> i64 {
        self.ledgers_map_offset
    }
    pub fn ledgers_count(&self) -> i32 {
        self.ledgers_count
    }
    pub fn set_ledgers_map_offset(&mut self, v: i64) {
        self.ledgers_map_offset = v;
    }
    pub fn set_ledgers_count(&mut self, v: i32) {
        self.ledgers_count = v;
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; LOGFILE_HEADER_SIZE];
        buf[0..4].copy_from_slice(ENTRY_LOG_MAGIC);
        buf[4..8].copy_from_slice(&self.version.to_be_bytes());
        buf[8..16].copy_from_slice(&self.ledgers_map_offset.to_be_bytes());
        buf[16..20].copy_from_slice(&self.ledgers_count.to_be_bytes());
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self, StorageError> {
        if data.len() < 20 {
            return Err(StorageError::CorruptData(
                "entry log header too short".into(),
            ));
        }
        if &data[0..4] != ENTRY_LOG_MAGIC {
            return Err(StorageError::CorruptData(
                "invalid entry log magic".into(),
            ));
        }
        let version = i32::from_be_bytes(data[4..8].try_into().unwrap());
        let ledgers_map_offset = i64::from_be_bytes(data[8..16].try_into().unwrap());
        let ledgers_count = i32::from_be_bytes(data[16..20].try_into().unwrap());
        Ok(Self {
            version,
            ledgers_map_offset,
            ledgers_count,
        })
    }
}

/// Encode an entry for the entry log: [size(4)][payload].
pub fn encode_entry_log_entry(payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// Entry location: (logId, position) packed as u64.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryLocation {
    log_id: u32,
    position: u32,
}

impl EntryLocation {
    pub fn new(log_id: u32, position: u32) -> Self {
        Self { log_id, position }
    }

    pub fn log_id(&self) -> u32 {
        self.log_id
    }
    pub fn position(&self) -> u32 {
        self.position
    }

    pub fn to_u64(&self) -> u64 {
        ((self.log_id as u64) << 32) | (self.position as u64)
    }

    pub fn from_u64(v: u64) -> Self {
        Self {
            log_id: (v >> 32) as u32,
            position: (v & 0xFFFFFFFF) as u32,
        }
    }
}

/// A ledger's presence in an entry log file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerMapEntry {
    pub ledger_id: i64,
    pub size: i64,
}

/// Encode a ledger map batch: [contentSize(4)][INVALID_LID(8)][MAP_ENTRY_ID(8)][count(4)][entries...].
pub fn encode_ledger_map_batch(entries: &[LedgerMapEntry]) -> Vec<u8> {
    let content_size = 8 + 8 + 4 + entries.len() * LEDGERS_MAP_ENTRY_SIZE;
    let mut buf = Vec::with_capacity(4 + content_size);
    buf.extend_from_slice(&(content_size as i32).to_be_bytes());
    buf.extend_from_slice(&INVALID_LID.to_be_bytes());
    buf.extend_from_slice(&LEDGERS_MAP_ENTRY_ID.to_be_bytes());
    buf.extend_from_slice(&(entries.len() as i32).to_be_bytes());
    for e in entries {
        buf.extend_from_slice(&e.ledger_id.to_be_bytes());
        buf.extend_from_slice(&e.size.to_be_bytes());
    }
    buf
}

/// Decode a ledger map batch from bytes.
pub fn decode_ledger_map_batch(data: &[u8]) -> Result<Vec<LedgerMapEntry>, StorageError> {
    if data.len() < LEDGERS_MAP_HEADER_SIZE {
        return Err(StorageError::CorruptData(
            "ledger map batch too short".into(),
        ));
    }
    let _content_size = i32::from_be_bytes(data[0..4].try_into().unwrap());
    let lid = i64::from_be_bytes(data[4..12].try_into().unwrap());
    let map_id = i64::from_be_bytes(data[12..20].try_into().unwrap());
    if lid != INVALID_LID || map_id != LEDGERS_MAP_ENTRY_ID {
        return Err(StorageError::CorruptData(
            "invalid ledger map markers".into(),
        ));
    }
    let count = i32::from_be_bytes(data[20..24].try_into().unwrap()) as usize;
    let mut entries = Vec::with_capacity(count);
    let mut pos = 24;
    for _ in 0..count {
        if pos + 16 > data.len() {
            break;
        }
        let ledger_id = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        let size = i64::from_be_bytes(data[pos + 8..pos + 16].try_into().unwrap());
        entries.push(LedgerMapEntry { ledger_id, size });
        pos += 16;
    }
    Ok(entries)
}

/// Scan entry log payload (after 1024-byte header) for entries.
/// Returns entry payloads (without size prefix). Stops at INVALID_LID marker (ledger map).
pub fn scan_entry_log_entries(data: &[u8]) -> Vec<&[u8]> {
    let mut entries = Vec::new();
    let mut pos = 0;
    while pos + 4 <= data.len() {
        let size = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        if size < MIN_SANE_ENTRY_SIZE || pos + 4 + size > data.len() {
            break;
        }
        let entry = &data[pos + 4..pos + 4 + size];
        let ledger_id = i64::from_be_bytes(entry[0..8].try_into().unwrap());
        if ledger_id == INVALID_LID {
            break; // reached ledger map
        }
        entries.push(entry);
        pos += 4 + size;
    }
    entries
}
