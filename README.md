# bloxroute-rs

Async Rust library for submitting signed EVM transactions through bloXroute's
WebSocket API. Supports Ethereum, BNB Smart Chain, Base, and Robinhood Chain.

One client maintains a persistent connection for one network. Concurrent
submissions share that connection, with bounded in-flight requests, deadlines,
ping/pong keepalives, and transaction hash verification.

## Usage

Add the library to your application's `Cargo.toml`:

```toml
[dependencies]
bloxroute-rs = { git = "https://github.com/0xDmtri/bloxroute-rs.git" }
tokio = { version = "1.53", features = ["macros", "rt-multi-thread"] }
```

The package name is `bloxroute-rs`; Rust imports use `bloxroute_rs`.
Pass the complete bloXroute `Authorization` header value to the constructor.
The library uses it as supplied, without adding a prefix or reading environment
variables. This example reads it from `BLXR_AUTH`.

```rust,no_run
use bloxroute_rs::{BloxrouteClient, BloxrouteNetwork};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let auth = std::env::var("BLXR_AUTH")?;
    let client = BloxrouteClient::cloud(BloxrouteNetwork::Base, auth);

    // Optional: establish the connection before the first submission.
    client.connect().await?;

    // Binary transaction bytes, already signed for the selected chain.
    let signed_tx = std::fs::read("signed-tx.bin")?;
    let hash = client.send_raw_tx(&signed_tx, None).await?;
    println!("acknowledged: {hash}");
    Ok(())
}
```

`cloud` uses `wss://api.blxrbdn.com/ws`. Construction performs no network I/O;
`send_raw_tx` connects automatically when needed. Keep the client alive and
reuse it for subsequent submissions. Share it with `Arc<BloxrouteClient>`
when submitting from multiple tasks.

The caller supplies the complete signed transaction encoding as bytes. The
client hex-encodes those bytes for the API; it does not sign transactions or
check their chain ID. A successful result is the acknowledged
`alloy_primitives::B256` hash, verified against the submitted bytes. Track
inclusion and execution separately using your chain RPC provider.

## Supported networks

| Network | `BloxrouteNetwork` | RPC method | `blockchain_network` | Reward address |
| --- | --- | --- | --- | --- |
| [Ethereum](https://docs.bloxroute.com/eth/sending-transactions/public-transactions) | `Ethereum` | `blxr_tx` | Omitted | Unsupported |
| [BNB Smart Chain](https://docs.bloxroute.com/bsc/submit-transactions/public-transactions) | `Bnb` | `blxr_tx` | `BSC-Mainnet` | Optional |
| [Base](https://docs.bloxroute.com/base/submit-transactions/base-transactions) | `Base` | `blxr_tx` | `Base-Mainnet` | Optional |
| [Robinhood Chain](https://docs.bloxroute.com/robinhood/submit-transactions/robinhood-transactions) | `Robinhood` | `robinhood_tx` | Omitted | Optional |

BNB and Base select their mainnet routes. Ethereum uses the API's default
network; Robinhood uses its dedicated method.

The second argument to `send_raw_tx` is an optional
`alloy_primitives::Address`, sent as `backrunme_reward_address`. Use `None` for
Ethereum's public route. Supplying a reward address on Ethereum returns
`UnsupportedRewardAddress` before connecting.

## Configuration

Use `with_options` to select a WebSocket endpoint and tune transport limits:

```rust,no_run
use std::time::Duration;
use bloxroute_rs::{BloxrouteClient, BloxrouteClientError, BloxrouteNetwork, BloxrouteOptions};

fn configured_client(auth: String) -> Result<BloxrouteClient, BloxrouteClientError> {
    let options = BloxrouteOptions {
        request_timeout: Duration::from_secs(2),
        max_in_flight: 128,
        ..Default::default()
    };

    BloxrouteClient::with_options(
        BloxrouteNetwork::Base,
        "wss://api.blxrbdn.com/ws",
        auth,
        options,
    )
}
```

| Option | Default | Purpose |
| --- | --- | --- |
| `connect_timeout` | 5 s | Connection setup and explicit connection warmup |
| `request_timeout` | 5 s | Whole submission, including queueing, connection setup, write, and acknowledgement |
| `write_timeout` | 1 s | Each WebSocket write |
| `ping_interval` | 15 s | Keepalive ping interval |
| `pong_timeout` | 5 s | Time allowed for a keepalive pong |
| `max_in_flight` | 64 | Maximum admitted submissions awaiting completion |
| `max_request_bytes` | 2 MiB | Encoded request budget, including JSON and hex expansion |
| `max_response_bytes` | 64 KiB | Maximum incoming WebSocket message or frame size |

All deadlines and limits must be nonzero. The request budget must be at least
256 bytes, and `max_in_flight` must fit Tokio's semaphore limit. The constructor
returns `InvalidOptions` for invalid limits; endpoint and authorization header
validation happens when connecting.

## Delivery and errors

After a disconnect, a later submission establishes a new connection. If a
write has been attempted and its acknowledgement is lost, the client returns
`BloxrouteClientError::AcknowledgementUnknown` with the locally computed
transaction hash. It never automatically replays that transaction.

Use `error.may_have_been_submitted()` to identify uncertain delivery and
reconcile the transaction hash before retrying or replacing it. An API
acknowledgement confirms submission, without confirming on-chain inclusion.

- `Timeout` means the deadline elapsed before a write attempt.
- `AcknowledgementUnknown` means delivery is uncertain after a write attempt,
  including a mismatched response hash.
- `Rpc { code, message }` preserves a bloXroute API rejection.
- `InvalidOptions`, `InvalidAuthHeader`, `RequestTooLarge`, and
  `UnsupportedRewardAddress` describe local configuration or input failures.

Cancelling a submission before its write prevents a queued request from being
sent. Cancelling after a write attempt leaves delivery uncertain.

## Runnable example

`examples/submit.rs` reads a signed transaction from a
UTF-8 hex file, with an optional `0x` prefix, and submits it to the chosen network.
The example reads `BLXR_AUTH` from the environment and prints the acknowledged
hash. Running it submits the supplied transaction.

```sh
cargo run --example submit -- base signed-tx.hex
```

The network argument accepts `ethereum`, `bnb`, `base`, or `robinhood`.

## Development

```sh
cargo fmt --check
cargo check --locked --lib
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo doc --locked --no-deps
cargo package --locked
```

Normal tests use local mock WebSocket servers. Live checks are ignored by
default and require `BLXR_AUTH`. The README also serves as the crate's Rustdoc
overview, so its Rust examples are compiled by `cargo test`.

## License

Licensed under MIT. See `LICENSE` in the repository for the full license text.
