fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/keep_alive_sync.proto")?;
    Ok(())
}
