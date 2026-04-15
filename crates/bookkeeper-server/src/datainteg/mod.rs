//! Bookie-side data-integrity check and recovery.
//!
//! Port of Java's `bookkeeper/bookie/datainteg/*` package. The feature
//! lets a bookie that lost local data (disk failure, journal disabled,
//! etc.) rebuild the entries it owns by copying from the peer bookies
//! that still have them.

pub mod check;
pub mod cookie_validator;
pub mod entry_copier;
pub mod service;
