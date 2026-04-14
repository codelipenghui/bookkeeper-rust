use hmac::{Hmac, KeyInit, Mac};
use sha1::Sha1;

/// Compute SHA1(pad + password). Used for master key and HMAC key derivation.
/// Matches Java's MacDigestManager.genDigest().
pub fn gen_digest(pad: &[u8], password: &[u8]) -> [u8; 20] {
    use sha1::Digest;
    let mut hasher = sha1::Sha1::new();
    hasher.update(pad);
    hasher.update(password);
    let result = hasher.finalize();
    let mut key = [0u8; 20];
    key.copy_from_slice(&result);
    key
}

/// Derive the master key from a password. Matches Java's DigestManager.generateMasterKey().
pub fn generate_master_key(password: &[u8]) -> [u8; 20] {
    gen_digest(b"ledger", password)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DigestType {
    Crc32,
    Crc32c,
    HmacSha1,
    Dummy,
}

impl std::str::FromStr for DigestType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "crc32" => Ok(DigestType::Crc32),
            "crc32c" => Ok(DigestType::Crc32c),
            "hmac" | "hmac_sha1" | "hmacsha1" | "mac" => Ok(DigestType::HmacSha1),
            "dummy" => Ok(DigestType::Dummy),
            other => Err(format!("unknown digest type: {other}")),
        }
    }
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

/// Stack-allocated digest output (max 20 bytes for HMAC-SHA1).
pub struct DigestOutput {
    buf: [u8; 20],
    len: u8,
}

impl DigestOutput {
    pub fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl std::ops::Deref for DigestOutput {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

#[derive(Debug, Clone)]
pub struct DigestManager {
    digest_type: DigestType,
    password: Vec<u8>,
    hmac_key: [u8; 20],  // cached SHA1("mac" + password), only used for HmacSha1
}

impl DigestManager {
    pub fn new(digest_type: DigestType) -> Self {
        Self { digest_type, password: Vec::new(), hmac_key: [0u8; 20] }
    }

    pub fn with_password(mut self, password: &[u8]) -> Self {
        self.password = password.to_vec();
        self.hmac_key = gen_digest(b"mac", password);
        self
    }

    pub fn digest_type(&self) -> DigestType {
        self.digest_type
    }

    fn derived_key(&self) -> &[u8; 20] {
        &self.hmac_key
    }

    pub fn compute(&self, metadata: &[u8; 32], payload: &[u8]) -> DigestOutput {
        let mut output = DigestOutput { buf: [0u8; 20], len: 0 };
        match self.digest_type {
            DigestType::Crc32 => {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(metadata);
                hasher.update(payload);
                let bytes = (hasher.finalize() as i64).to_be_bytes();
                output.buf[..8].copy_from_slice(&bytes);
                output.len = 8;
            }
            DigestType::Crc32c => {
                let mut crc = crc32c::crc32c(metadata);
                crc = crc32c::crc32c_append(crc, payload);
                let bytes = (crc as i32).to_be_bytes();
                output.buf[..4].copy_from_slice(&bytes);
                output.len = 4;
            }
            DigestType::HmacSha1 => {
                type HmacSha1 = Hmac<Sha1>;
                let mut mac = HmacSha1::new_from_slice(self.derived_key())
                    .expect("HMAC accepts any key length");
                mac.update(metadata);
                mac.update(payload);
                let result = mac.finalize().into_bytes();
                output.buf.copy_from_slice(&result);
                output.len = 20;
            }
            DigestType::Dummy => {}
        }
        output
    }

    /// Compute digest for WriteLac body: 16-byte metadata `[ledgerId:8][lac:8]`.
    /// Matches Java's `DigestManager.computeDigestAndPackageForSendingLac()`
    /// which uses `LAC_METADATA_LENGTH = 16` (not the full 32-byte entry header).
    pub fn compute_lac(&self, lac_metadata: &[u8; 16]) -> DigestOutput {
        let mut output = DigestOutput { buf: [0u8; 20], len: 0 };
        match self.digest_type {
            DigestType::Crc32 => {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(lac_metadata);
                let bytes = (hasher.finalize() as i64).to_be_bytes();
                output.buf[..8].copy_from_slice(&bytes);
                output.len = 8;
            }
            DigestType::Crc32c => {
                let crc = crc32c::crc32c(lac_metadata);
                let bytes = (crc as i32).to_be_bytes();
                output.buf[..4].copy_from_slice(&bytes);
                output.len = 4;
            }
            DigestType::HmacSha1 => {
                type HmacSha1 = Hmac<Sha1>;
                let mut mac = HmacSha1::new_from_slice(self.derived_key())
                    .expect("HMAC accepts any key length");
                mac.update(lac_metadata);
                let result = mac.finalize().into_bytes();
                output.buf.copy_from_slice(&result);
                output.len = 20;
            }
            DigestType::Dummy => {}
        }
        output
    }

    pub fn verify(&self, metadata: &[u8; 32], payload: &[u8], expected: &[u8]) -> bool {
        if self.digest_type == DigestType::Dummy {
            return true;
        }
        let computed = self.compute(metadata, payload);
        computed.as_slice() == expected
    }
}
