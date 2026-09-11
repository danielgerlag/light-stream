use std::{error::Error, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let proto = root.join("proto/lightstream/v1/public.proto");
    let include = root.join("proto");
    let mut prost = prost_build::Config::new();
    prost.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure().compile_with_config(prost, &[proto], &[include])?;
    println!("cargo:rerun-if-changed=../../proto/lightstream/v1/public.proto");
    Ok(())
}
