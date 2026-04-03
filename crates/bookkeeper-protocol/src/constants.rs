// Protocol versions
pub const CURRENT_PROTOCOL_VERSION: u8 = 2;

// Pre-v3 operation codes
pub const OP_ADD_ENTRY: u8 = 1;
pub const OP_READ_ENTRY: u8 = 2;
pub const OP_AUTH: u8 = 3;
pub const OP_READ_LAC: u8 = 4;
pub const OP_WRITE_LAC: u8 = 5;
pub const OP_GET_BOOKIE_INFO: u8 = 6;
pub const OP_BATCH_READ_ENTRY: u8 = 7;

// Flags (16-bit)
pub const FLAG_NONE: u16 = 0x0000;
pub const FLAG_DO_FENCING: u16 = 0x0001;
pub const FLAG_RECOVERY_ADD: u16 = 0x0002;
pub const FLAG_HIGH_PRIORITY: u16 = 0x0004;

// Write flags (v3)
pub const WRITE_FLAG_DEFERRED_SYNC: i32 = 0x1;

// Pre-v3 error codes
pub const V2_EOK: i32 = 0;

// Master key
pub const MASTER_KEY_LENGTH: usize = 20;

// Entry format
pub const METADATA_LENGTH: usize = 32;

// Pre-v3 response header size
pub const V2_RESPONSE_HEADER_SIZE: usize = 24;
