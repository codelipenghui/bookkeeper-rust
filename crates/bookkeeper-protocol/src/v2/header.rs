#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacketHeader {
    version: u8,
    opcode: u8,
    flags: u16,
}

impl PacketHeader {
    pub fn new(version: u8, opcode: u8, flags: u16) -> Self {
        Self { version, opcode, flags }
    }
    pub fn version(&self) -> u8 { self.version }
    pub fn opcode(&self) -> u8 { self.opcode }
    pub fn flags(&self) -> u16 { self.flags }

    pub fn to_u32(&self) -> u32 {
        if self.version == 0 {
            self.opcode as u32
        } else {
            ((self.version as u32) << 24) | ((self.opcode as u32) << 16) | (self.flags as u32)
        }
    }

    pub fn from_u32(v: u32) -> Self {
        let version = (v >> 24) as u8;
        if version == 0 {
            Self { version: 0, opcode: v as u8, flags: 0 }
        } else {
            Self { version, opcode: ((v >> 16) & 0xFF) as u8, flags: (v & 0xFFFF) as u16 }
        }
    }

    pub fn has_flag(&self, flag: u16) -> bool {
        self.flags & flag != 0
    }
}
