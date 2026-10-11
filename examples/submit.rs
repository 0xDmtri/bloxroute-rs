use std::{env, error::Error, io};

use alloy_primitives::keccak256;
use bloxroute_rs::{BloxrouteClient, BloxrouteNetwork};

const USAGE: &str =
    "usage: cargo run --example submit -- <ethereum|bnb|base|robinhood> <signed-tx.hex>";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let network = match args.next().as_deref() {
        Some("ethereum") => BloxrouteNetwork::Ethereum,
        Some("bnb") => BloxrouteNetwork::Bnb,
        Some("base") => BloxrouteNetwork::Base,
        Some("robinhood") => BloxrouteNetwork::Robinhood,
        _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, USAGE).into()),
    };
    let path = args
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, USAGE))?;
    if args.next().is_some() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, USAGE).into());
    }

    let encoded = std::fs::read_to_string(path)?;
    let hex = encoded.trim();
    let hex = hex.strip_prefix("0x").unwrap_or(hex);
    if hex.is_empty() || !hex.len().is_multiple_of(2) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "signed transaction must contain a nonempty, even-length hex string",
        )
        .into());
    }
    let mut signed_tx = vec![0; hex.len() / 2];
    faster_hex::hex_decode(hex.as_bytes(), &mut signed_tx)?;

    let client = BloxrouteClient::cloud(network, env::var("BLXR_AUTH")?);
    match client.send_raw_tx(&signed_tx, None).await {
        Ok(hash) => println!("acknowledged: {hash}"),
        Err(error) => {
            if error.may_have_been_submitted() {
                eprintln!(
                    "delivery uncertain; reconcile {} before retrying",
                    keccak256(&signed_tx)
                );
            }
            return Err(error.into());
        }
    }
    Ok(())
}
