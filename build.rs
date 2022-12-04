use std::error::Error;

use prost_build::Config;

fn main() -> Result<(), Box<dyn Error>> {
    let mut config = Config::new();
    config.bytes(["."]);

    tonic_build::configure().compile_with_config(config, &["proto/protocol.proto"], &["proto"])?;

    Ok(())
}
