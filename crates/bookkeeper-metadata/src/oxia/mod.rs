//! Oxia metadata backend. Wire-compatible with Apache Pulsar's
//! pulsar-metadata/bookkeeper driver: same keys, same CAS contract.

pub mod client;
pub mod errors;
pub mod layout;
pub mod ledger_manager;
pub mod parents;
pub mod paths;
pub mod registration;
