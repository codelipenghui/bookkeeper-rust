use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BKError {
    #[error("OK")]
    Ok,
    #[error("no such ledger")]
    NoLedger,
    #[error("no such entry")]
    NoEntry,
    #[error("bad request")]
    BadRequest,
    #[error("I/O error")]
    Io,
    #[error("unauthorized")]
    Unauthorized,
    #[error("bad version")]
    BadVersion,
    #[error("ledger fenced")]
    Fenced,
    #[error("bookie read-only")]
    ReadOnly,
    #[error("too many requests")]
    TooManyRequests,
    #[error("unknown ledger state")]
    UnknownLedgerState,
}

impl BKError {
    pub fn from_v2_code(code: i32) -> Self {
        match code {
            0 => Self::Ok, 1 => Self::NoLedger, 2 => Self::NoEntry,
            100 => Self::BadRequest, 101 => Self::Io, 102 => Self::Unauthorized,
            103 => Self::BadVersion, 104 => Self::Fenced, 105 => Self::ReadOnly,
            106 => Self::TooManyRequests, 107 => Self::UnknownLedgerState,
            _ => Self::BadRequest,
        }
    }
    pub fn to_v2_code(self) -> i32 {
        match self {
            Self::Ok => 0, Self::NoLedger => 1, Self::NoEntry => 2,
            Self::BadRequest => 100, Self::Io => 101, Self::Unauthorized => 102,
            Self::BadVersion => 103, Self::Fenced => 104, Self::ReadOnly => 105,
            Self::TooManyRequests => 106, Self::UnknownLedgerState => 107,
        }
    }
    pub fn from_v3_code(code: i32) -> Self {
        match code {
            0 => Self::Ok, 402 => Self::NoLedger, 403 => Self::NoEntry,
            404 => Self::BadRequest, 501 => Self::Io, 502 => Self::Unauthorized,
            503 => Self::BadVersion, 504 => Self::Fenced, 505 => Self::ReadOnly,
            506 => Self::TooManyRequests, 507 => Self::UnknownLedgerState,
            _ => Self::BadRequest,
        }
    }
    pub fn to_v3_code(self) -> i32 {
        match self {
            Self::Ok => 0, Self::NoLedger => 402, Self::NoEntry => 403,
            Self::BadRequest => 404, Self::Io => 501, Self::Unauthorized => 502,
            Self::BadVersion => 503, Self::Fenced => 504, Self::ReadOnly => 505,
            Self::TooManyRequests => 506, Self::UnknownLedgerState => 507,
        }
    }
}
