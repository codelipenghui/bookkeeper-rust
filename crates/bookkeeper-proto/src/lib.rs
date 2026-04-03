/// BookKeeper wire protocol messages (BookkeeperProtocol.proto).
///
/// This is the default package since BookkeeperProtocol.proto has no `package` directive.
#[allow(clippy::all)]
mod bk_protocol {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

pub use bk_protocol::*;
