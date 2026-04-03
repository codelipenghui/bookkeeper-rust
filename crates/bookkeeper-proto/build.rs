fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(
        &[
            "proto/BookkeeperProtocol.proto",
            "proto/DataFormats.proto",
            "proto/DbLedgerStorageDataFormats.proto",
        ],
        &["proto/"],
    )?;
    Ok(())
}
