//! Persistent WebSocket transaction submission and transport configuration.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};

use alloy_primitives::{Address, B256, keccak256};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::AbortHandle,
    time::{Instant, MissedTickBehavior, interval, timeout, timeout_at},
};
use tokio_tungstenite::{
    WebSocketStream, connect_async_with_config,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{HeaderValue, header::AUTHORIZATION},
        protocol::WebSocketConfig,
    },
};

use crate::network::BloxrouteNetwork;

type FastMap<K, V> = hashbrown::HashMap<K, V, std::hash::BuildHasherDefault<ahash::AHasher>>;

/// Submission, connection, and configuration failures.
///
/// Use [`Self::may_have_been_submitted`] when deciding whether a transaction's
/// delivery needs reconciliation before retrying.
#[derive(Debug, Error)]
pub enum BloxrouteClientError {
    /// WebSocket setup failed before a submission write was attempted.
    #[error("ws connection failed: {0}")]
    Connect(#[from] tokio_tungstenite::tungstenite::Error),
    /// The connection closed before a submission write was attempted.
    #[error("connection closed before submission")]
    Closed,
    /// An API response could not be decoded.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// The API rejected a submission.
    #[error("rpc error {code}: {message}")]
    Rpc {
        /// Error code returned by the API.
        code: i64,
        /// Error message returned by the API.
        message: String,
    },
    /// An API response did not contain a valid 32-byte transaction hash.
    #[error("rpc result missing or invalid tx_hash")]
    MissingHash,
    /// The deadline elapsed before a submission write was attempted.
    #[error("submission deadline elapsed before sending")]
    Timeout,
    /// A write was attempted, but a matching acknowledgement was not received.
    #[error("acknowledgement unknown for {tx_hash}: {reason}")]
    AcknowledgementUnknown {
        /// Hash computed from the submitted transaction bytes.
        tx_hash: B256,
        /// Description of the acknowledgement failure.
        reason: &'static str,
    },
    /// The authorization value could not be represented as an HTTP header.
    #[error("invalid Authorization header")]
    InvalidAuthHeader,
    /// A transport deadline or limit was outside its supported range.
    #[error("invalid bloXroute options: limits and deadlines must be nonzero")]
    InvalidOptions,
    /// The signed transaction exceeded the encoded request budget.
    #[error("transaction request exceeds {limit} bytes")]
    RequestTooLarge {
        /// Configured request budget in bytes, including JSON and hex encoding.
        limit: usize,
    },
    /// The selected route does not support a BackRunMe reward address.
    #[error("BackRunMe reward addresses are not supported by the {network:?} submission route")]
    UnsupportedRewardAddress {
        /// Network selected when constructing the client.
        network: BloxrouteNetwork,
    },
}

impl BloxrouteClientError {
    /// Whether delivery remains uncertain after a submission failure.
    ///
    /// [`BloxrouteClient::send_raw_tx`] reports transport failures after a write
    /// attempt as [`Self::AcknowledgementUnknown`]. Invalid responses also leave
    /// delivery uncertain.
    #[must_use]
    pub fn may_have_been_submitted(&self) -> bool {
        match self {
            Self::AcknowledgementUnknown { .. } | Self::Json(_) | Self::MissingHash => true,
            Self::Connect(_)
            | Self::Closed
            | Self::Rpc { .. }
            | Self::Timeout
            | Self::InvalidAuthHeader
            | Self::InvalidOptions
            | Self::RequestTooLarge { .. }
            | Self::UnsupportedRewardAddress { .. } => false,
        }
    }
}

/// Deadlines, connection keepalives, and per-client resource limits.
///
/// All submission work, including queueing and connection setup, shares the
/// [`Self::request_timeout`] deadline. Limits are validated by
/// [`BloxrouteClient::with_options`].
#[derive(Debug, Clone, Copy)]
pub struct BloxrouteOptions {
    /// Connection setup deadline, including queueing for [`BloxrouteClient::connect`].
    /// Defaults to 5 seconds.
    pub connect_timeout: Duration,
    /// Whole submission deadline, including admission, connection, write, and acknowledgement.
    /// Defaults to 5 seconds.
    pub request_timeout: Duration,
    /// Deadline for each WebSocket write, also bounded by the submission deadline.
    /// Defaults to 1 second.
    pub write_timeout: Duration,
    /// Interval between keepalive pings. Defaults to 15 seconds.
    pub ping_interval: Duration,
    /// Deadline for a keepalive pong after a ping. Defaults to 5 seconds.
    pub pong_timeout: Duration,
    /// Maximum admitted submissions awaiting completion. Defaults to 64.
    pub max_in_flight: usize,
    /// Encoded request budget, including JSON and hexadecimal expansion.
    /// Defaults to 2 MiB; must be at least 256 bytes.
    pub max_request_bytes: usize,
    /// Maximum incoming WebSocket message or frame size. Defaults to 64 KiB.
    pub max_response_bytes: usize,
}

impl Default for BloxrouteOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(1),
            ping_interval: Duration::from_secs(15),
            pong_timeout: Duration::from_secs(5),
            max_in_flight: 64,
            max_request_bytes: 2 * 1024 * 1024,
            max_response_bytes: 64 * 1024,
        }
    }
}

/// Persistent WebSocket submission client for one explicitly selected network.
///
/// Reuse a client across submissions and share it with [`Arc`] across tasks.
/// Methods that connect or submit must run within a Tokio runtime with I/O and
/// time enabled. A later submission reconnects after a closed connection.
pub struct BloxrouteClient {
    network: BloxrouteNetwork,
    endpoint: String,
    auth_header: String,
    connection: Mutex<Option<Arc<Connection>>>,
    slots: Arc<Semaphore>,
    id: AtomicU64,
    options: BloxrouteOptions,
}

impl fmt::Debug for BloxrouteClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BloxrouteClient")
            .field("network", &self.network)
            .field("endpoint", &self.endpoint)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

struct Connection {
    requests: mpsc::Sender<Submission>,
    task: AbortHandle,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

const QUEUED: u8 = 0;
const WRITING: u8 = 1;
const CANCELLED: u8 = 2;

struct CancelQueued(Arc<AtomicU8>);

impl Drop for CancelQueued {
    fn drop(&mut self) {
        let _ = self
            .0
            .compare_exchange(QUEUED, CANCELLED, Ordering::AcqRel, Ordering::Acquire);
    }
}

#[cfg_attr(test, derive(Debug))]
struct Submission {
    id: u64,
    text: String,
    pending: Pending,
    attempted: Arc<AtomicU8>,
}

#[cfg_attr(test, derive(Debug))]
struct Pending {
    tx_hash: B256,
    deadline: Instant,
    reply: oneshot::Sender<Result<B256, BloxrouteClientError>>,
    _permit: OwnedSemaphorePermit,
}

impl BloxrouteClient {
    /// Create a client using the shared bloXroute Cloud API endpoint.
    ///
    /// Uses `wss://api.blxrbdn.com/ws` and [`BloxrouteOptions::default`].
    /// `auth_header` is the complete `Authorization` value, used as supplied.
    /// Construction performs no network I/O; the first submission connects lazily.
    pub fn cloud(network: BloxrouteNetwork, auth_header: impl Into<String>) -> Self {
        Self::with_options(
            network,
            "wss://api.blxrbdn.com/ws",
            auth_header,
            BloxrouteOptions::default(),
        )
        .expect("default bloXroute options are valid")
    }

    /// Create a client with an explicit network, endpoint, and transport options.
    ///
    /// `endpoint` is a `ws://` or `wss://` URL. `auth_header` is the complete
    /// `Authorization` value, used as supplied. Construction performs no network
    /// I/O and validates only the transport options.
    ///
    /// # Errors
    ///
    /// Returns [`BloxrouteClientError::InvalidOptions`] for zero deadlines or
    /// limits, a request budget below 256 bytes, or an in-flight limit exceeding
    /// Tokio's semaphore capacity. Endpoint and header errors are reported when
    /// connecting.
    pub fn with_options(
        network: BloxrouteNetwork,
        endpoint: impl Into<String>,
        auth_header: impl Into<String>,
        options: BloxrouteOptions,
    ) -> Result<Self, BloxrouteClientError> {
        if options.connect_timeout.is_zero()
            || options.request_timeout.is_zero()
            || options.write_timeout.is_zero()
            || options.ping_interval.is_zero()
            || options.pong_timeout.is_zero()
            || options.max_in_flight == 0
            || options.max_in_flight > Semaphore::MAX_PERMITS
            || options.max_request_bytes < 256
            || options.max_response_bytes == 0
        {
            return Err(BloxrouteClientError::InvalidOptions);
        }
        Ok(Self {
            network,
            endpoint: endpoint.into(),
            auth_header: auth_header.into(),
            connection: Mutex::new(None),
            slots: Arc::new(Semaphore::new(options.max_in_flight)),
            id: AtomicU64::new(1),
            options,
        })
    }

    /// The network used for every transaction submitted by this client.
    pub const fn network(&self) -> BloxrouteNetwork {
        self.network
    }

    /// Warm the connection; queueing behind another connection attempt is bounded too.
    ///
    /// This is optional: [`Self::send_raw_tx`] establishes a connection as needed.
    /// Uses [`BloxrouteOptions::connect_timeout`] and sends no transaction.
    pub async fn connect(&self) -> Result<(), BloxrouteClientError> {
        timeout(self.options.connect_timeout, self.get_connection())
            .await
            .map_err(|_| BloxrouteClientError::Timeout)?
            .map(|_| ())
    }

    /// Submit signed transaction bytes and return the acknowledged transaction hash.
    ///
    /// `tx_bytes` must contain the complete signed transaction encoding for this
    /// client's network. The client does not sign transactions or validate their
    /// chain ID. Success verifies the acknowledged hash against the input bytes;
    /// on-chain inclusion must be tracked separately.
    ///
    /// `reward_address` is supported on BNB Smart Chain, Base, and Robinhood.
    /// Ethereum's public submission route requires `None`.
    ///
    /// The request deadline includes queueing, connection setup, writing, and
    /// acknowledgement. Cancelling before a write prevents a queued submission
    /// from being sent; cancellation after a write leaves delivery uncertain.
    ///
    /// # Errors
    ///
    /// A connection failure or deadline after a write attempt returns
    /// [`BloxrouteClientError::AcknowledgementUnknown`]; the client never silently
    /// resubmits that transaction. Use
    /// [`BloxrouteClientError::may_have_been_submitted`] to decide whether delivery
    /// needs reconciliation before retrying. Unsupported reward addresses and
    /// oversized transactions are rejected before connecting.
    pub async fn send_raw_tx(
        &self,
        tx_bytes: &[u8],
        reward_address: Option<Address>,
    ) -> Result<B256, BloxrouteClientError> {
        if reward_address.is_some() && !self.network.supports_reward_address() {
            return Err(BloxrouteClientError::UnsupportedRewardAddress {
                network: self.network,
            });
        }
        let deadline = Instant::now() + self.options.request_timeout;
        let attempted = Arc::new(AtomicU8::new(QUEUED));
        let _cancel_queued = CancelQueued(Arc::clone(&attempted));
        if tx_bytes.len() > (self.options.max_request_bytes - 256) / 2 {
            return Err(BloxrouteClientError::RequestTooLarge {
                limit: self.options.max_request_bytes,
            });
        }
        let tx_hash = keccak256(tx_bytes);
        let result = timeout_at(deadline, async {
            let permit = Arc::clone(&self.slots)
                .acquire_owned()
                .await
                .map_err(|_| BloxrouteClientError::Closed)?;
            let id = self.id.fetch_add(1, Ordering::Relaxed);
            let text = build_request(self.network, tx_bytes, reward_address, id);
            let (reply, response) = oneshot::channel();
            let mut request = Submission {
                id,
                text,
                attempted: Arc::clone(&attempted),
                pending: Pending {
                    tx_hash,
                    deadline,
                    reply,
                    _permit: permit,
                },
            };
            // Retry only if the old connection closed before accepting the command.
            for attempt in 0..2 {
                let connection = self.get_connection().await?;
                match connection.requests.send(request).await {
                    Ok(()) => {
                        return response.await.unwrap_or_else(|_| {
                            Err(if attempted.load(Ordering::Acquire) == WRITING {
                                BloxrouteClientError::AcknowledgementUnknown {
                                    tx_hash,
                                    reason: "connection closed before acknowledgement",
                                }
                            } else {
                                BloxrouteClientError::Closed
                            })
                        });
                    }
                    Err(error) if attempt == 0 => request = error.0,
                    Err(_) => return Err(BloxrouteClientError::Closed),
                }
            }
            unreachable!()
        })
        .await;
        result.unwrap_or_else(|_| {
            let previous =
                attempted.compare_exchange(QUEUED, CANCELLED, Ordering::AcqRel, Ordering::Acquire);
            Err(if matches!(previous, Err(WRITING)) {
                BloxrouteClientError::AcknowledgementUnknown {
                    tx_hash,
                    reason: "submission deadline elapsed",
                }
            } else {
                BloxrouteClientError::Timeout
            })
        })
    }

    async fn get_connection(&self) -> Result<Arc<Connection>, BloxrouteClientError> {
        let mut slot = self.connection.lock().await;
        if let Some(connection) = slot
            .as_ref()
            .filter(|connection| !connection.requests.is_closed())
        {
            return Ok(Arc::clone(connection));
        }
        let mut request = self.endpoint.as_str().into_client_request()?;
        let mut authorization = HeaderValue::from_str(&self.auth_header)
            .map_err(|_| BloxrouteClientError::InvalidAuthHeader)?;
        authorization.set_sensitive(true);
        request.headers_mut().insert(AUTHORIZATION, authorization);
        let mut config = WebSocketConfig::default();
        config.read_buffer_size = 16 * 1024;
        config.write_buffer_size = 16 * 1024;
        config.max_message_size = Some(self.options.max_response_bytes);
        config.max_frame_size = Some(self.options.max_response_bytes);
        let (socket, _) = timeout(
            self.options.connect_timeout,
            connect_async_with_config(request, Some(config), true),
        )
        .await
        .map_err(|_| BloxrouteClientError::Timeout)??;
        let (requests, receiver) = mpsc::channel(self.options.max_in_flight);
        let options = self.options;
        let task = tokio::spawn(async move {
            if let Err(error) = run_connection(socket, receiver, options).await {
                tracing::debug!(%error, "bloXroute submission connection ended");
            }
        });
        let connection = Arc::new(Connection {
            requests,
            task: task.abort_handle(),
        });
        *slot = Some(Arc::clone(&connection));
        Ok(connection)
    }
}

async fn run_connection<S>(
    mut socket: WebSocketStream<S>,
    mut requests: mpsc::Receiver<Submission>,
    options: BloxrouteOptions,
) -> Result<(), BloxrouteClientError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut pending: FastMap<u64, Pending> =
        FastMap::with_capacity_and_hasher(options.max_in_flight, Default::default());
    let mut ping = interval(options.ping_interval);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ping.tick().await;
    let mut pong_deadline: Option<Instant> = None;
    loop {
        let now = Instant::now();
        if pong_deadline.is_some_and(|deadline| deadline <= now) {
            return Err(BloxrouteClientError::Closed);
        }
        for (_, request) in
            pending.extract_if(|_, request| request.deadline <= now || request.reply.is_closed())
        {
            let _ = request
                .reply
                .send(Err(BloxrouteClientError::AcknowledgementUnknown {
                    tx_hash: request.tx_hash,
                    reason: "submission deadline elapsed",
                }));
        }
        let next_deadline = pending
            .values()
            .map(|request| request.deadline)
            .chain(pong_deadline)
            .min()
            .unwrap_or(now + Duration::from_secs(3600));
        tokio::select! {
            _ = tokio::time::sleep_until(next_deadline) => {}
            request = requests.recv() => {
                let Some(request) = request else { return Ok(()); };
                if request.pending.reply.is_closed() || request.pending.deadline <= Instant::now() {
                    let _ = request.pending.reply.send(Err(BloxrouteClientError::Timeout));
                    continue;
                }
                let deadline = request.pending.deadline.min(Instant::now() + options.write_timeout);
                if request.attempted.compare_exchange(QUEUED, WRITING, Ordering::AcqRel, Ordering::Acquire).is_err() {
                    let _ = request.pending.reply.send(Err(BloxrouteClientError::Timeout));
                    continue;
                }
                pending.insert(request.id, request.pending);
                timeout_at(deadline, socket.send(Message::Text(request.text.into()))).await
                    .map_err(|_| BloxrouteClientError::Closed)??;
            }
            response = socket.next() => match response {
                Some(Ok(Message::Text(text))) => {
                    let (id, result) = parse_response(&text)?;
                    if let Some(request) = pending.remove(&id) {
                        let result = match result {
                            Ok(hash) if hash != request.tx_hash => Err(BloxrouteClientError::AcknowledgementUnknown {
                                tx_hash: request.tx_hash, reason: "response transaction hash mismatch",
                            }),
                            other => other,
                        };
                        let _ = request.reply.send(result);
                    }
                }
                Some(Ok(Message::Ping(payload))) => {
                    timeout(options.write_timeout, socket.send(Message::Pong(payload))).await
                        .map_err(|_| BloxrouteClientError::Closed)??;
                }
                Some(Ok(Message::Pong(_))) => pong_deadline = None,
                Some(Ok(Message::Close(_))) | None => return Err(BloxrouteClientError::Closed),
                Some(Err(error)) => return Err(error.into()),
                _ => {}
            },
            _ = ping.tick(), if pong_deadline.is_none() => {
                timeout(options.write_timeout, socket.send(Message::Ping(Default::default()))).await
                    .map_err(|_| BloxrouteClientError::Closed)??;
                pong_deadline = Some(Instant::now() + options.pong_timeout);
            }
        }
    }
}

fn build_request(
    network: BloxrouteNetwork,
    tx_bytes: &[u8],
    reward_address: Option<Address>,
    id: u64,
) -> String {
    let mut body = Vec::with_capacity(256 + tx_bytes.len() * 2);
    body.extend_from_slice(b"{\"jsonrpc\":\"2.0\",\"id\":");
    body.extend_from_slice(itoa::Buffer::new().format(id).as_bytes());
    body.extend_from_slice(b",\"method\":\"");
    body.extend_from_slice(network.rpc_method().as_bytes());
    body.extend_from_slice(b"\",\"params\":{\"transaction\":\"");
    let start = body.len();
    body.resize(start + tx_bytes.len() * 2, 0);
    faster_hex::hex_encode(tx_bytes, &mut body[start..]).expect("hex buffer has exact length");
    body.push(b'"');
    if let Some(blockchain_network) = network.blockchain_network() {
        body.extend_from_slice(b",\"blockchain_network\":\"");
        body.extend_from_slice(blockchain_network.as_bytes());
        body.push(b'"');
    }
    if let Some(address) = reward_address {
        body.extend_from_slice(b",\"backrunme_reward_address\":\"0x");
        let start = body.len();
        body.resize(start + 40, 0);
        faster_hex::hex_encode(address.as_slice(), &mut body[start..])
            .expect("address hex buffer has exact length");
        body.push(b'"');
    }
    body.extend_from_slice(b"}}");
    String::from_utf8(body).expect("JSON literals and hex are valid UTF-8")
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
struct RpcOk<'a> {
    #[serde(borrow, rename = "tx_hash", alias = "txHash")]
    tx_hash: &'a str,
}

#[derive(Deserialize)]
struct RpcResponse<'a> {
    #[serde(deserialize_with = "deserialize_response_id")]
    id: u64,
    #[serde(borrow)]
    result: Option<RpcOk<'a>>,
    error: Option<RpcError>,
}

fn deserialize_response_id<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct ResponseIdVisitor;

    impl<'de> serde::de::Visitor<'de> for ResponseIdVisitor {
        type Value = u64;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("an unsigned integer or a decimal string")
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<u64, E> {
            Ok(value)
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<u64, E> {
            if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(E::invalid_value(serde::de::Unexpected::Str(value), &self));
            }
            value.parse().map_err(E::custom)
        }
    }

    deserializer.deserialize_any(ResponseIdVisitor)
}

fn parse_response(
    s: &str,
) -> Result<(u64, Result<B256, BloxrouteClientError>), BloxrouteClientError> {
    let response: RpcResponse<'_> = serde_json::from_str(s)?;
    let result = match (response.result, response.error) {
        (None, Some(error)) => Err(BloxrouteClientError::Rpc {
            code: error.code,
            message: error.message,
        }),
        (Some(ok), None) => {
            let hex = ok.tx_hash.strip_prefix("0x").unwrap_or(ok.tx_hash);
            let bytes = faster_hex::hex_decode_array::<32>(hex.as_bytes())
                .map_err(|_| BloxrouteClientError::MissingHash)?;
            Ok(B256::from(bytes))
        }
        _ => return Err(BloxrouteClientError::MissingHash),
    };
    Ok((response.id, result))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_options() -> BloxrouteOptions {
        BloxrouteOptions {
            request_timeout: Duration::from_secs(1),
            connect_timeout: Duration::from_secs(1),
            write_timeout: Duration::from_millis(100),
            ..Default::default()
        }
    }

    async fn listener_client(
        network: BloxrouteNetwork,
        options: BloxrouteOptions,
    ) -> (tokio::net::TcpListener, BloxrouteClient) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let client = BloxrouteClient::with_options(network, endpoint, "test", options).unwrap();
        (listener, client)
    }

    fn response_for(request: &Message) -> Message {
        let request: serde_json::Value = serde_json::from_str(request.to_text().unwrap()).unwrap();
        let hex = request["params"]["transaction"].as_str().unwrap();
        let mut tx = vec![0; hex.len() / 2];
        faster_hex::hex_decode(hex.as_bytes(), &mut tx).unwrap();
        let hash = keccak256(tx);
        Message::Text(
            format!(
                r#"{{"jsonrpc":"2.0","id":{},"result":{{"txHash":"{hash}"}}}}"#,
                request["id"]
            )
            .into(),
        )
    }

    #[test]
    fn every_address_byte_encodes_as_valid_json_hex() {
        for byte in 0..=255 {
            let address = Address::repeat_byte(byte);
            let text = build_request(BloxrouteNetwork::Base, &[0, 255], Some(address), 7);
            let request: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(
                request["params"]["backrunme_reward_address"],
                address.to_string().to_ascii_lowercase()
            );
            assert_eq!(request["params"]["transaction"], "00ff");
        }
        assert_eq!(
            build_request(BloxrouteNetwork::Base, &[1, 2], None, 7),
            r#"{"jsonrpc":"2.0","id":7,"method":"blxr_tx","params":{"transaction":"0102","blockchain_network":"Base-Mainnet"}}"#
        );
    }

    #[tokio::test]
    async fn selected_network_routes_submissions_with_string_acknowledgements() {
        let reward = Some(Address::repeat_byte(0xab));
        let cases = [
            (
                BloxrouteNetwork::Ethereum,
                None,
                r#"{"jsonrpc":"2.0","id":1,"method":"blxr_tx","params":{"transaction":"0102"}}"#,
            ),
            (
                BloxrouteNetwork::Bnb,
                reward,
                r#"{"jsonrpc":"2.0","id":1,"method":"blxr_tx","params":{"transaction":"0102","blockchain_network":"BSC-Mainnet","backrunme_reward_address":"0xabababababababababababababababababababab"}}"#,
            ),
            (
                BloxrouteNetwork::Base,
                reward,
                r#"{"jsonrpc":"2.0","id":1,"method":"blxr_tx","params":{"transaction":"0102","blockchain_network":"Base-Mainnet","backrunme_reward_address":"0xabababababababababababababababababababab"}}"#,
            ),
            (
                BloxrouteNetwork::Robinhood,
                reward,
                r#"{"jsonrpc":"2.0","id":1,"method":"robinhood_tx","params":{"transaction":"0102","backrunme_reward_address":"0xabababababababababababababababababababab"}}"#,
            ),
        ];
        for (network, reward, expected) in cases {
            let (listener, client) = listener_client(network, test_options()).await;
            assert_eq!(client.network(), network);
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                let mut expected: serde_json::Value = serde_json::from_str(expected).unwrap();
                for id in 1..=2 {
                    let request = socket.next().await.unwrap().unwrap();
                    let actual: serde_json::Value =
                        serde_json::from_str(request.to_text().unwrap()).unwrap();
                    if id == 2 {
                        expected["id"] = serde_json::json!(2);
                        expected["params"]["transaction"] = serde_json::json!("0304");
                        expected["params"]
                            .as_object_mut()
                            .unwrap()
                            .remove("backrunme_reward_address");
                    }
                    assert_eq!(actual, expected, "{network:?}");

                    let response = response_for(&request);
                    let mut response: serde_json::Value =
                        serde_json::from_str(response.to_text().unwrap()).unwrap();
                    response["id"] = serde_json::json!(id.to_string());
                    socket
                        .send(Message::Text(response.to_string().into()))
                        .await
                        .unwrap();
                }
            });
            assert_eq!(
                client.send_raw_tx(&[1, 2], reward).await.unwrap(),
                keccak256([1, 2]),
                "{network:?}"
            );
            assert_eq!(
                client.send_raw_tx(&[3, 4], None).await.unwrap(),
                keccak256([3, 4]),
                "{network:?}"
            );
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn unsupported_reward_address_is_rejected_before_connecting() {
        let client = BloxrouteClient::with_options(
            BloxrouteNetwork::Ethereum,
            "invalid endpoint",
            "test",
            test_options(),
        )
        .unwrap();
        let error = client
            .send_raw_tx(&[1, 2], Some(Address::ZERO))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            BloxrouteClientError::UnsupportedRewardAddress {
                network: BloxrouteNetwork::Ethereum
            }
        ));
        assert!(!error.may_have_been_submitted());
        assert!(client.connection.lock().await.is_none());
    }

    #[tokio::test]
    async fn concurrent_requests_are_written_before_responses_and_match_out_of_order() {
        let (listener, client) = listener_client(BloxrouteNetwork::Base, test_options()).await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let first = socket.next().await.unwrap().unwrap();
            let second = socket.next().await.unwrap().unwrap();
            socket.send(response_for(&second)).await.unwrap();
            socket.send(response_for(&first)).await.unwrap();
        });
        let (first, second) = tokio::join!(
            client.send_raw_tx(&[1], None),
            client.send_raw_tx(&[2], None)
        );
        assert_eq!(first.unwrap(), keccak256([1]));
        assert_eq!(second.unwrap(), keccak256([2]));
        server.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn submission_deadline_includes_admission_and_connection_lock() {
        let options = BloxrouteOptions {
            max_in_flight: 1,
            request_timeout: Duration::from_millis(20),
            ..test_options()
        };
        let client = BloxrouteClient::with_options(
            BloxrouteNetwork::Base,
            "ws://127.0.0.1:1",
            "test",
            options,
        )
        .unwrap();
        let occupied = Arc::clone(&client.slots).acquire_owned().await.unwrap();
        let started = Instant::now();
        let error = client.send_raw_tx(&[1], None).await.unwrap_err();
        assert!(matches!(error, BloxrouteClientError::Timeout));
        assert!(!error.may_have_been_submitted());
        assert_eq!(started.elapsed(), options.request_timeout);
        drop(occupied);
        let _connecting = client.connection.lock().await;
        let started = Instant::now();
        let error = client.send_raw_tx(&[1], None).await.unwrap_err();
        assert!(matches!(error, BloxrouteClientError::Timeout));
        assert!(!error.may_have_been_submitted());
        assert_eq!(started.elapsed(), options.request_timeout);
        assert_eq!(client.slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn handshake_is_bounded_and_never_reports_a_submission_attempt() {
        let options = BloxrouteOptions {
            connect_timeout: Duration::from_millis(100),
            ..test_options()
        };
        let (listener, client) = listener_client(BloxrouteNetwork::Base, options).await;
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let error = client.send_raw_tx(&[1], None).await.unwrap_err();
        server.abort();
        assert!(matches!(error, BloxrouteClientError::Timeout));
        assert!(!error.may_have_been_submitted());
        assert_eq!(client.slots.available_permits(), options.max_in_flight);
    }

    #[tokio::test]
    async fn connection_loss_after_write_is_unknown_and_next_call_reconnects() {
        let (listener, client) = listener_client(BloxrouteNetwork::Base, test_options()).await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _first = socket.next().await.unwrap().unwrap();
            socket.close(None).await.unwrap();
            drop(socket);
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let second = socket.next().await.unwrap().unwrap();
            socket.send(response_for(&second)).await.unwrap();
        });
        let error = client.send_raw_tx(&[1], None).await.unwrap_err();
        assert!(matches!(
            error,
            BloxrouteClientError::AcknowledgementUnknown { .. }
        ));
        assert!(error.may_have_been_submitted());
        assert_eq!(
            client.send_raw_tx(&[2], None).await.unwrap(),
            keccak256([2])
        );
        server.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_write_is_bounded_and_acknowledgement_remains_unknown() {
        let options = BloxrouteOptions {
            write_timeout: Duration::from_millis(10),
            ..test_options()
        };
        let client = BloxrouteClient::with_options(
            BloxrouteNetwork::Base,
            "ws://127.0.0.1:1",
            "test",
            options,
        )
        .unwrap();
        let (transport, _peer) = tokio::io::duplex(1);
        let socket = WebSocketStream::from_raw_socket(
            transport,
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        let (requests, receiver) = mpsc::channel(1);
        let task = tokio::spawn(run_connection(socket, receiver, options));
        *client.connection.lock().await = Some(Arc::new(Connection {
            requests,
            task: task.abort_handle(),
        }));
        let started = Instant::now();
        let error = client.send_raw_tx(&[1], None).await.unwrap_err();
        assert!(matches!(
            error,
            BloxrouteClientError::AcknowledgementUnknown { .. }
        ));
        assert!(error.may_have_been_submitted());
        assert_eq!(started.elapsed(), options.write_timeout);
        assert_eq!(client.slots.available_permits(), options.max_in_flight);
        assert!(task.await.unwrap().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn acknowledgement_deadline_after_write_requires_reconciliation() {
        let options = test_options();
        let client = BloxrouteClient::with_options(
            BloxrouteNetwork::Base,
            "ws://127.0.0.1:1",
            "test",
            options,
        )
        .unwrap();
        let (transport, peer) = tokio::io::duplex(1024);
        let socket = WebSocketStream::from_raw_socket(
            transport,
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        let mut peer = WebSocketStream::from_raw_socket(
            peer,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            None,
        )
        .await;
        let (requests, receiver) = mpsc::channel(1);
        let task = tokio::spawn(run_connection(socket, receiver, options));
        *client.connection.lock().await = Some(Arc::new(Connection {
            requests,
            task: task.abort_handle(),
        }));

        let started = Instant::now();
        let (result, ()) = tokio::join!(client.send_raw_tx(&[1], None), async {
            // The peer receives the transaction but never acknowledges it.
            let request = peer.next().await.unwrap().unwrap();
            assert_eq!(
                request.to_text().unwrap(),
                build_request(BloxrouteNetwork::Base, &[1], None, 1)
            );
        });
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            BloxrouteClientError::AcknowledgementUnknown {
                tx_hash,
                reason: "submission deadline elapsed",
            } if tx_hash == keccak256([1])
        ));
        assert!(error.may_have_been_submitted());
        assert_eq!(started.elapsed(), options.request_timeout);
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn expired_queued_transactions_are_not_written() {
        let options = test_options();
        let (transport, peer) = tokio::io::duplex(1024);
        let socket = WebSocketStream::from_raw_socket(
            transport,
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        let mut peer = WebSocketStream::from_raw_socket(
            peer,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            None,
        )
        .await;
        let (requests, receiver) = mpsc::channel(1);
        let (reply, response) = oneshot::channel();
        let attempted = Arc::new(AtomicU8::new(QUEUED));
        let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        requests
            .send(Submission {
                id: 1,
                text: build_request(BloxrouteNetwork::Base, &[1], None, 1),
                attempted: Arc::clone(&attempted),
                pending: Pending {
                    tx_hash: keccak256([1]),
                    deadline: Instant::now() - Duration::from_millis(1),
                    reply,
                    _permit: permit,
                },
            })
            .await
            .unwrap();
        let task = tokio::spawn(run_connection(socket, receiver, options));
        assert!(matches!(
            response.await.unwrap(),
            Err(BloxrouteClientError::Timeout)
        ));
        assert_eq!(attempted.load(Ordering::Acquire), QUEUED);
        assert!(
            timeout(Duration::from_millis(10), peer.next())
                .await
                .is_err()
        );
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_wins_atomically_before_a_queued_write() {
        let options = test_options();
        let (transport, peer) = tokio::io::duplex(1024);
        let socket = WebSocketStream::from_raw_socket(
            transport,
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        let mut peer = WebSocketStream::from_raw_socket(
            peer,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            None,
        )
        .await;
        let (requests, receiver) = mpsc::channel(1);
        let (reply, response) = oneshot::channel();
        let attempted = Arc::new(AtomicU8::new(QUEUED));
        let cancellation = CancelQueued(Arc::clone(&attempted));
        requests
            .send(Submission {
                id: 1,
                text: build_request(BloxrouteNetwork::Base, &[1], None, 1),
                attempted: Arc::clone(&attempted),
                pending: Pending {
                    tx_hash: keccak256([1]),
                    deadline: Instant::now() + options.request_timeout,
                    reply,
                    _permit: Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap(),
                },
            })
            .await
            .unwrap();
        drop(cancellation);
        let task = tokio::spawn(run_connection(socket, receiver, options));
        assert!(matches!(
            response.await.unwrap(),
            Err(BloxrouteClientError::Timeout)
        ));
        assert_eq!(attempted.load(Ordering::Acquire), CANCELLED);
        assert!(
            timeout(Duration::from_millis(10), peer.next())
                .await
                .is_err()
        );
        task.abort();
    }

    #[test]
    fn malformed_responses_leave_delivery_uncertain() {
        for body in [
            "not JSON",
            r#"{"id":1}"#,
            r#"{"id":1,"result":{"tx_hash":"invalid"}}"#,
        ] {
            let error = parse_response(body).unwrap_err();
            assert!(error.may_have_been_submitted(), "{body}");
        }
    }

    #[test]
    fn numeric_and_string_response_ids_accept_supported_hash_formats() {
        for (wire_id, expected_id) in [
            ("7", 7),
            (r#""7""#, 7),
            ("18446744073709551615", u64::MAX),
            (r#""18446744073709551615""#, u64::MAX),
        ] {
            for field in ["txHash", "tx_hash"] {
                for prefix in ["", "0x"] {
                    let hex = "ab".repeat(32);
                    let body =
                        format!(r#"{{"id":{wire_id},"result":{{"{field}":"{prefix}{hex}"}}}}"#);
                    let (id, result) = parse_response(&body).unwrap();
                    assert_eq!(id, expected_id, "{body}");
                    assert_eq!(result.unwrap(), B256::repeat_byte(0xab), "{body}");
                }
            }
        }
        let (id, result) =
            parse_response(r#"{"id":"7","error":{"code":-32000,"message":"rejected"}}"#).unwrap();
        assert_eq!(id, 7);
        assert!(matches!(
            result,
            Err(BloxrouteClientError::Rpc { code: -32000, .. })
        ));
    }

    #[test]
    fn invalid_response_ids_are_rejected() {
        for wire_id in [
            "-1",
            "1.0",
            "true",
            "null",
            "[]",
            "{}",
            r#""""#,
            r#""-1""#,
            r#""+1""#,
            r#"" 1""#,
            r#""1 ""#,
            r#""1.0""#,
            r#""1e0""#,
            "18446744073709551616",
            r#""18446744073709551616""#,
        ] {
            let hex = "ab".repeat(32);
            let body = format!(r#"{{"id":{wire_id},"result":{{"txHash":"{hex}"}}}}"#);
            let error = parse_response(&body).unwrap_err();
            assert!(matches!(error, BloxrouteClientError::Json(_)), "{body}");
            assert!(error.may_have_been_submitted(), "{body}");
        }
    }

    #[test]
    fn response_hash_requires_exactly_32_bytes() {
        let digits = "ab".repeat(33);
        for prefix in ["", "0x"] {
            for len in 0..=digits.len() {
                let hex = &digits[..len];
                let body = format!(r#"{{"id":7,"result":{{"txHash":"{prefix}{hex}"}}}}"#);
                if len == 64 {
                    let (id, result) = parse_response(&body).unwrap();
                    assert_eq!(id, 7);
                    assert_eq!(result.unwrap(), B256::repeat_byte(0xab));
                } else {
                    assert!(
                        matches!(
                            parse_response(&body),
                            Err(BloxrouteClientError::MissingHash)
                        ),
                        "accepted a hash with {len} hex digits: {body}"
                    );
                }
            }
        }
    }

    #[test]
    fn rpc_rejections_remain_distinct_from_transport_failures() {
        let (_, result) =
            parse_response(r#"{"id":1,"error":{"code":-32000,"message":"rejected"}}"#).unwrap();
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            BloxrouteClientError::Rpc { code: -32000, .. }
        ));
        assert!(!error.may_have_been_submitted());
    }

    #[tokio::test]
    #[ignore = "manual live connection check; requires BLXR_AUTH"]
    async fn can_connect_blxr_client() {
        let auth = std::env::var("BLXR_AUTH").expect("BLXR_AUTH must be set");
        let blxr_client = BloxrouteClient::cloud(BloxrouteNetwork::Base, auth);
        blxr_client.connect().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "manual live sequencer latency benchmark; requires BLXR_AUTH"]
    async fn test_rtt() {
        use tokio::time::{Duration, Instant};

        let auth = std::env::var("BLXR_AUTH").expect("BLXR_AUTH must be set");
        let client = BloxrouteClient::cloud(BloxrouteNetwork::Base, auth);
        client.connect().await.unwrap();

        let dummy_tx: &[u8] = &[6, 9, 4, 2, 0];
        let sample_size = 20u32;

        let mut latencies = Vec::with_capacity(sample_size as usize);
        for i in 0..sample_size {
            let now = Instant::now();
            let resp = client.send_raw_tx(dummy_tx, None).await;
            let elapsed = now.elapsed();

            match &resp {
                Ok(hash) => println!("[blxr-ws] req {i}: {elapsed:?} hash={hash}"),
                Err(err) => println!("[blxr-ws] req {i}: {elapsed:?} err={err}"),
            }

            latencies.push(elapsed);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        latencies.sort();
        let total: Duration = latencies.iter().sum();
        let avg = total / sample_size;
        let p50 = latencies[latencies.len() / 2];
        let p99 = latencies[latencies.len() * 99 / 100];
        let min = latencies[0];
        let max = *latencies.last().unwrap();

        println!(
            "[blxr-ws] n={sample_size}  avg={avg:?}  p50={p50:?}  p99={p99:?}  min={min:?}  max={max:?}"
        );
    }
}
