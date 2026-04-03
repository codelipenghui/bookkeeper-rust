pub use crate::StorageError;

pub const JOURNAL_MAGIC: &[u8; 4] = b"BKLG";
pub const SECTOR_SIZE: usize = 512;
pub const CURRENT_JOURNAL_FORMAT_VERSION: i32 = 6;
pub const MIN_COMPAT_JOURNAL_FORMAT_VERSION: i32 = 1;
pub const PADDING_MASK: i32 = -0x100; // 0xFFFFFF00 as signed i32

/// Journal file header (512 bytes for V5+).
#[derive(Debug, Clone)]
pub struct JournalHeader {
    version: i32,
}

impl JournalHeader {
    pub fn new(version: i32) -> Self {
        Self { version }
    }

    pub fn version(&self) -> i32 {
        self.version
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; SECTOR_SIZE];
        buf[0..4].copy_from_slice(JOURNAL_MAGIC);
        buf[4..8].copy_from_slice(&self.version.to_be_bytes());
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self, StorageError> {
        if data.len() < 8 {
            return Err(StorageError::CorruptData("journal header too short".into()));
        }
        if &data[0..4] != JOURNAL_MAGIC {
            return Err(StorageError::CorruptData("invalid journal magic".into()));
        }
        let version = i32::from_be_bytes(data[4..8].try_into().unwrap());
        Ok(Self { version })
    }
}

/// Encode an entry for writing to the journal: [size(4)][payload].
pub fn encode_journal_entry(payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// Calculate padding bytes to align current position to alignment boundary.
/// Returns the complete padding block: [PADDING_MASK(4)][length(4)][zeros].
pub fn calculate_padding(current_pos: u64, alignment: u64) -> Vec<u8> {
    let next_aligned = ((current_pos + alignment - 1) / alignment) * alignment;
    let mut pad_len = (next_aligned - current_pos) as usize;
    if pad_len < 8 {
        pad_len += alignment as usize;
    }
    let mut buf = vec![0u8; pad_len];
    buf[0..4].copy_from_slice(&PADDING_MASK.to_be_bytes());
    buf[4..8].copy_from_slice(&(pad_len as i32).to_be_bytes());
    buf
}

/// Scan journal entry payloads from a byte buffer (after the header).
/// Handles padding blocks (V5+) by skipping them.
pub fn scan_journal_entries(data: &[u8], version: i32) -> Vec<&[u8]> {
    let mut entries = Vec::new();
    let mut pos = 0;
    while pos + 4 <= data.len() {
        let size = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
        if size == 0 {
            break; // end of data
        }
        if size == PADDING_MASK && version >= 5 {
            if pos + 8 > data.len() {
                break;
            }
            let pad_len = i32::from_be_bytes(data[pos + 4..pos + 8].try_into().unwrap()) as usize;
            pos += pad_len;
            continue;
        }
        let entry_size = size as usize;
        let entry_start = pos + 4;
        let entry_end = entry_start + entry_size;
        if entry_end > data.len() {
            break; // truncated entry
        }
        entries.push(&data[entry_start..entry_end]);
        pos = entry_end;
    }
    entries
}

/// Last mark: checkpoint position in the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LastMark {
    log_id: i64,
    offset: i64,
}

impl LastMark {
    pub fn new(log_id: i64, offset: i64) -> Self {
        Self { log_id, offset }
    }

    pub fn log_id(&self) -> i64 {
        self.log_id
    }
    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn to_bytes(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&self.log_id.to_be_bytes());
        buf[8..16].copy_from_slice(&self.offset.to_be_bytes());
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self, StorageError> {
        if data.len() < 16 {
            return Err(StorageError::CorruptData("last mark too short".into()));
        }
        let log_id = i64::from_be_bytes(data[0..8].try_into().unwrap());
        let offset = i64::from_be_bytes(data[8..16].try_into().unwrap());
        Ok(Self { log_id, offset })
    }
}

/// Encoded journal position: (fileId << 32) | offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalPosition {
    file_id: u32,
    offset: u32,
}

impl JournalPosition {
    pub fn new(file_id: u32, offset: u32) -> Self {
        Self { file_id, offset }
    }

    pub fn file_id(&self) -> u32 {
        self.file_id
    }
    pub fn offset(&self) -> u32 {
        self.offset
    }

    pub fn to_u64(&self) -> u64 {
        ((self.file_id as u64) << 32) | (self.offset as u64)
    }

    pub fn from_u64(v: u64) -> Self {
        Self {
            file_id: (v >> 32) as u32,
            offset: (v & 0xFFFFFFFF) as u32,
        }
    }
}

