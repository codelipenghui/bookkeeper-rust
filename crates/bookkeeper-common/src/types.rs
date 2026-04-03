use std::fmt;
use std::str::FromStr;

pub type LedgerId = i64;
pub type EntryId = i64;

pub const INVALID_ENTRY_ID: EntryId = -1;
pub const LAST_ADD_CONFIRMED: EntryId = -1;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BookieId {
    host: String,
    port: u16,
}

impl BookieId {
    pub fn new(host: &str, port: u16) -> Self {
        Self { host: host.to_string(), port }
    }
    pub fn host(&self) -> &str { &self.host }
    pub fn port(&self) -> u16 { self.port }
}

impl fmt::Display for BookieId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl FromStr for BookieId {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (host, port_str) = s.rsplit_once(':')
            .ok_or_else(|| format!("invalid bookie id: missing ':' in '{s}'"))?;
        let port: u16 = port_str.parse()
            .map_err(|e| format!("invalid port in '{s}': {e}"))?;
        Ok(Self::new(host, port))
    }
}
