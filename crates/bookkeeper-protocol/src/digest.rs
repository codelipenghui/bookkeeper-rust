use hmac::{Hmac, Mac};
use sha1::Sha1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DigestType {
    Crc32,
    Crc32c,
    HmacSha1,
    Dummy,
}

impl DigestType {
    pub fn digest_length(&self) -> usize {
        match self {
            Self::Crc32 => 8,      // stored as i64 via writeLong
            Self::Crc32c => 4,     // stored as i32 via writeInt
            Self::HmacSha1 => 20,
            Self::Dummy => 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DigestManager {
    digest_type: DigestType,
    password: Vec<u8>,
}

impl DigestManager {
    pub fn new(digest_type: DigestType) -> Self {
        Self { digest_type, password: Vec::new() }
    }

    pub fn with_password(mut self, password: &[u8]) -> Self {
        self.password = password.to_vec();
        self
    }

    pub fn digest_type(&self) -> DigestType {
        self.digest_type
    }

    pub fn compute(&self, metadata: &[u8; 32], payload: &[u8]) -> Vec<u8> {
        match self.digest_type {
            DigestType::Crc32 => {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(metadata);
                hasher.update(payload);
                let crc = hasher.finalize();
                (crc as i64).to_be_bytes().to_vec()
            }
            DigestType::Crc32c => {
                let mut crc = crc32c::crc32c(metadata);
                crc = crc32c::crc32c_append(crc, payload);
                (crc as i32).to_be_bytes().to_vec()  // 4 bytes, not 8
            }
            DigestType::HmacSha1 => {
                use sha1::Digest;  // Need the Digest trait for SHA1 hashing
                type HmacSha1 = Hmac<Sha1>;

                // Java derives HMAC key as: SHA1("mac" + password)
                let mut key_hasher = Sha1::new();
                key_hasher.update(b"mac");
                key_hasher.update(&self.password);
                let derived_key = key_hasher.finalize();

                let mut mac = HmacSha1::new_from_slice(&derived_key)
                    .expect("HMAC accepts any key length");
                mac.update(metadata);
                mac.update(payload);
                mac.finalize().into_bytes().to_vec()
            }
            DigestType::Dummy => Vec::new(),
        }
    }

    pub fn verify(&self, metadata: &[u8; 32], payload: &[u8], expected: &[u8]) -> bool {
        if self.digest_type == DigestType::Dummy {
            return true;
        }
        let computed = self.compute(metadata, payload);
        computed == expected
    }
}
