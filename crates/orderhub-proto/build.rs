fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_descriptors = protox::compile(["proto/orderhub.proto"], ["proto"])?;
    tonic_prost_build::configure()
        .compile_fds(file_descriptors)
        .map_err(|e| format!("{e}"))?;
    Ok(())
}
