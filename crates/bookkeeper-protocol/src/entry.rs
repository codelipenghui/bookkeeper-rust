use bytes::{BufMut, Bytes, BytesMut};
use crate::constants::METADATA_LENGTH;
use crate::digest::DigestManager;

#[derive(Debug, Clone)]
pub struct Entry {
    pub ledger_id: i64,
    pub entry_id: i64,
    pub lac: i64,
    pub length: i64,
    pub payload: Bytes,
}

pub struct EntryBuilder<'a> {
    dm: &'a DigestManager,
    ledger_id: i64,
    entry_id: i64,
    lac: i64,
    length: i64,
    payload: Bytes,
}

impl<'a> EntryBuilder<'a> {
    pub fn new(dm: &'a DigestManager) -> Self {
        Self { dm, ledger_id: 0, entry_id: 0, lac: -1, length: 0, payload: Bytes::new() }
    }
    pub fn ledger_id(mut self, v: i64) -> Self { self.ledger_id = v; self }
    pub fn entry_id(mut self, v: i64) -> Self { self.entry_id = v; self }
    pub fn lac(mut self, v: i64) -> Self { self.lac = v; self }
    pub fn length(mut self, v: i64) -> Self { self.length = v; self }
    pub fn payload(mut self, v: Bytes) -> Self { self.payload = v; self }

    pub fn build(self) -> BytesMut {
        let digest_len = self.dm.digest_type().digest_length();
        let total = METADATA_LENGTH + digest_len + self.payload.len();
        let mut buf = BytesMut::with_capacity(total);
        buf.put_i64(self.ledger_id);
        buf.put_i64(self.entry_id);
        buf.put_i64(self.lac);
        buf.put_i64(self.length);
        let metadata: [u8; 32] = buf[..32].try_into().unwrap();
        let digest = self.dm.compute(&metadata, &self.payload);
        buf.put_slice(digest.as_slice());
        buf.put_slice(&self.payload);
        buf
    }
}

pub fn parse_entry(dm: &DigestManager, data: &[u8]) -> Result<Entry, EntryError> {
    let digest_len = dm.digest_type().digest_length();
    let min_len = METADATA_LENGTH + digest_len;
    if data.len() < min_len {
        return Err(EntryError::TooShort { actual: data.len(), minimum: min_len });
    }
    let metadata: &[u8; 32] = data[..32].try_into().unwrap();
    let ledger_id = i64::from_be_bytes(data[0..8].try_into().unwrap());
    let entry_id = i64::from_be_bytes(data[8..16].try_into().unwrap());
    let lac = i64::from_be_bytes(data[16..24].try_into().unwrap());
    let length = i64::from_be_bytes(data[24..32].try_into().unwrap());
    let digest = &data[METADATA_LENGTH..METADATA_LENGTH + digest_len];
    let payload = &data[METADATA_LENGTH + digest_len..];
    if !dm.verify(metadata, payload, digest) {
        return Err(EntryError::DigestMismatch);
    }
    Ok(Entry { ledger_id, entry_id, lac, length, payload: Bytes::copy_from_slice(payload) })
}

#[derive(Debug, thiserror::Error)]
pub enum EntryError {
    #[error("entry too short: {actual} bytes, need at least {minimum}")]
    TooShort { actual: usize, minimum: usize },
    #[error("digest mismatch")]
    DigestMismatch,
}
