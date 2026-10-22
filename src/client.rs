//! Persistent WebSocket transaction submission and transport configuration.

use std::{
    fmt,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant as Timestamp},
};

use alloy_primitives::{Address, B256, keccak256};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::AbortHandle,
    time::{Instant, MissedTickBehavior, interval, sleep, timeout, timeout_at},
};
use tokio_tungstenite::{
    WebSocketStream, connect_async_with_config,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        handshake::client::Request as Handshake,
        http::{HeaderValue, header::AUTHORIZATION},
        protocol::WebSocketConfig,
    },
};

use crate::network::BloxrouteNetwork;

type FastMap<K, V> = hashbrown::HashMap<K, V, std::hash::BuildHasherDefault<ahash::AHasher>>;

const RECONNECT_MIN: Duration = Duration::from_millis(100);
const RECONNECT_MAX: Duration = Duration::from_secs(1);

/// Submission, connection, and configuration failures.
///
/// Use [`Self::may_have_been_submitted`] to decide whether delivery needs
/// reconciliation before retrying.
#[derive(Debug, Error)]
pub enum BloxrouteClientError {
    /// WebSocket setup failed before a submission write was attempted.
    #[error("ws connection failed: {0}")]
    Connect(#[from] tokio_tungstenite::tungstenite::Error),
    /// No established connection is available for immediate submission.
    #[error("submission connection is not ready")]
    NotConnected,
    /// All submission slots are occupied; nothing was enqueued.
    #[error("submission capacity is full")]
    Full,
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
        /// Description supplied by the API.
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
        /// The submitted transaction's local hash.
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
        /// Configured request budget, including JSON and hex encoding.
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
    #[must_use]
    pub fn may_have_been_submitted(&self) -> bool {
        match self {
            Self::AcknowledgementUnknown { .. } | Self::Json(_) | Self::MissingHash => true,
            Self::Connect(_)
            | Self::NotConnected
            | Self::Full
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

/// Deadlines, keepalives, and per-client resource limits.
#[derive(Debug, Clone, Copy)]
pub struct BloxrouteOptions {
    /// Deadline for connection warmup and each background handshake. Defaults to 5 s.
    pub connect_timeout: Duration,
    /// Submission deadline, including admission, write, and acknowledgement. Defaults to 5 s.
    /// [`BloxrouteClient::send_raw_tx`] also includes waiting for connection readiness.
    pub request_timeout: Duration,
    /// Deadline for each WebSocket write, bounded by the submission deadline. Defaults to 1 s.
    pub write_timeout: Duration,
    /// Interval between keepalive pings. Defaults to 15 s.
    pub ping_interval: Duration,
    /// Deadline for a keepalive pong. Defaults to 5 s.
    pub pong_timeout: Duration,
    /// Maximum admitted submissions awaiting completion. Defaults to 64.
    pub max_in_flight: usize,
    /// Encoded request budget, including JSON and hex expansion. Defaults to 2 MiB.
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

/// Monotonic timestamps recorded at admission and inside the socket driver.
///
/// Write completion means the WebSocket sink was flushed to the underlying
/// transport. It does not prove delivery to the remote server or chain inclusion.
#[derive(Debug, Clone, Copy)]
pub struct SubmissionTiming {
    /// Immediately before the request is handed to the driver, after encoding.
    pub enqueued_at: Timestamp,
    /// Immediately before attempting the WebSocket write.
    pub write_started_at: Option<Timestamp>,
    /// After the WebSocket write and flush completed successfully.
    pub write_completed_at: Option<Timestamp>,
    /// When the driver received the matching API response, including rejection.
    pub response_received_at: Option<Timestamp>,
}

/// A submission's acknowledgement or failure, including transport timing.
#[derive(Debug)]
pub struct SubmissionOutcome {
    /// The locally supplied or computed transaction hash.
    pub tx_hash: B256,
    /// Success means the server acknowledged this hash; inclusion is separate.
    pub result: Result<(), BloxrouteClientError>,
    /// Driver timestamps, retained on failures as well as success.
    pub timing: SubmissionTiming,
}

/// Observe a request already enqueued by [`BloxrouteClient::try_submit`].
///
/// Sending and driver deadlines progress without calling [`Self::wait`]. Keep
/// this handle alive: dropping it cancels a request only if writing has not
/// started. Cancellation after a write cannot establish non-delivery.
#[must_use = "keep the submission alive until its outcome is known"]
#[derive(Debug)]
pub struct Submission {
    deadline: Instant,
    response: oneshot::Receiver<SubmissionOutcome>,
    cancellation: CancelQueued,
}

impl Submission {
    /// The locally supplied or computed transaction hash.
    pub fn tx_hash(&self) -> B256 {
        self.cancellation.state.tx_hash
    }

    /// Wait for the outcome within the original submission deadline.
    ///
    /// The local deadline also covers a driver currently blocked on another
    /// socket write. Timing records whatever the driver had observed at return.
    pub async fn wait(self) -> SubmissionOutcome {
        let state = &self.cancellation.state;

        match timeout_at(self.deadline, self.response).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => state.outcome(Err(state.delivery_error()), None),
            Err(_) => {
                let previous = state.phase.compare_exchange(
                    QUEUED,
                    CANCELLED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                let error = if matches!(previous, Err(WRITING)) {
                    BloxrouteClientError::AcknowledgementUnknown {
                        tx_hash: state.tx_hash,
                        reason: "submission deadline elapsed",
                    }
                } else {
                    BloxrouteClientError::Timeout
                };

                state.outcome(Err(error), None)
            }
        }
    }
}

/// One persistent WebSocket driver for one explicitly selected network.
///
/// Construction performs no I/O. [`Self::connect`] starts the driver on the
/// calling Tokio runtime and waits for readiness. The driver then reconnects
/// independently until the client is dropped. Use a multi-thread runtime when
/// the submitting thread performs synchronous computation.
pub struct BloxrouteClient {
    network: BloxrouteNetwork,
    endpoint: String,
    auth_header: String,
    driver: OnceLock<Driver>,
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

struct Driver {
    requests: mpsc::Sender<QueuedSubmission>,
    generation: Arc<AtomicU64>,
    readiness: watch::Receiver<()>,
    task: AbortHandle,
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.generation.store(0, Ordering::Release);
        self.task.abort();
    }
}

const QUEUED: u8 = 0;
const WRITING: u8 = 1;
const CANCELLED: u8 = 2;

#[derive(Debug)]
struct SubmissionState {
    tx_hash: B256,
    enqueued_at: Timestamp,
    phase: AtomicU8,
    write_started_at: OnceLock<Timestamp>,
    write_completed_at: OnceLock<Timestamp>,
}

impl SubmissionState {
    fn new(tx_hash: B256) -> Self {
        Self {
            tx_hash,
            enqueued_at: Instant::now().into_std(),
            phase: AtomicU8::new(QUEUED),
            write_started_at: OnceLock::new(),
            write_completed_at: OnceLock::new(),
        }
    }

    fn delivery_error(&self) -> BloxrouteClientError {
        if self.phase.load(Ordering::Acquire) == WRITING {
            BloxrouteClientError::AcknowledgementUnknown {
                tx_hash: self.tx_hash,
                reason: "connection closed before acknowledgement",
            }
        } else {
            BloxrouteClientError::Closed
        }
    }

    fn outcome(
        &self,
        result: Result<(), BloxrouteClientError>,
        response_received_at: Option<Timestamp>,
    ) -> SubmissionOutcome {
        // Read completion first: observing it must also observe the preceding start.
        let write_completed_at = self.write_completed_at.get().copied();
        let write_started_at = self.write_started_at.get().copied();

        SubmissionOutcome {
            tx_hash: self.tx_hash,
            result,
            timing: SubmissionTiming {
                enqueued_at: self.enqueued_at,
                write_started_at,
                write_completed_at,
                response_received_at,
            },
        }
    }
}

#[derive(Debug)]
struct CancelQueued {
    state: Arc<SubmissionState>,
}

impl Drop for CancelQueued {
    fn drop(&mut self) {
        let _ = self.state.phase.compare_exchange(
            QUEUED,
            CANCELLED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

#[derive(Debug)]
struct QueuedSubmission {
    id: u64,
    generation: u64,
    text: String,
    pending: Pending,
}

#[derive(Debug)]
struct Pending {
    state: Arc<SubmissionState>,
    deadline: Instant,
    reply: Option<oneshot::Sender<SubmissionOutcome>>,
    permit: Option<OwnedSemaphorePermit>,
}

impl Pending {
    fn finish(&mut self, result: Result<(), BloxrouteClientError>, received_at: Option<Timestamp>) {
        // Capacity must be available before another thread observes completion.
        drop(self.permit.take());

        if let Some(reply) = self.reply.take() {
            let _ = reply.send(self.state.outcome(result, received_at));
        }
    }

    fn observer_closed(&self) -> bool {
        self.reply.as_ref().is_none_or(oneshot::Sender::is_closed)
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        // Resolve even if the driver is aborted or a socket operation fails.
        self.finish(Err(self.state.delivery_error()), None);
    }
}

impl BloxrouteClient {
    /// Create a client for `wss://api.blxrbdn.com/ws` using default options.
    /// The complete `Authorization` value is used as supplied. No I/O starts yet.
    pub fn cloud(network: BloxrouteNetwork, auth_header: impl Into<String>) -> Self {
        Self::with_options(
            network,
            "wss://api.blxrbdn.com/ws",
            auth_header,
            BloxrouteOptions::default(),
        )
        .expect("default bloXroute options are valid")
    }

    /// Create a client with an explicit `ws://` or `wss://` endpoint.
    ///
    /// # Errors
    /// Returns [`BloxrouteClientError::InvalidOptions`] for zero deadlines or
    /// limits, a request budget below 256 bytes, or a limit exceeding Tokio's
    /// semaphore capacity. Endpoint and header validation happens at connection.
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
            driver: OnceLock::new(),
            slots: Arc::new(Semaphore::new(options.max_in_flight)),
            id: AtomicU64::new(1),
            options,
        })
    }

    /// The network used for every transaction submitted by this client.
    pub const fn network(&self) -> BloxrouteNetwork {
        self.network
    }

    /// Start the persistent driver and wait for a connected socket.
    ///
    /// Requires a Tokio runtime with I/O and time enabled. Network failures are
    /// retried in the background with 100 ms–1 s backoff. Warmup is bounded by
    /// `connect_timeout`; a timeout leaves the driver reconnecting.
    pub async fn connect(&self) -> Result<(), BloxrouteClientError> {
        timeout(
            self.options.connect_timeout,
            wait_connected(self.start_driver()?),
        )
        .await
        .map_err(|_| BloxrouteClientError::Timeout)?
    }

    /// Immediately enqueue signed bytes using their already computed hash.
    ///
    /// Call [`Self::connect`] before submitting. This method never waits for a
    /// connection, capacity, write, or acknowledgement. The driver may begin
    /// sending before this returns, without calling [`Submission::wait`]. Record
    /// the transaction locally before calling this method.
    ///
    /// `tx_hash` must be the hash of exactly `tx_bytes`. It is deliberately not
    /// recomputed; the server's response is checked against it. The client does
    /// not sign transactions or validate their chain ID. Ethereum requires
    /// `reward_address` to be `None`.
    ///
    /// # Errors
    /// [`BloxrouteClientError::NotConnected`] and [`BloxrouteClientError::Full`]
    /// reject admission immediately. Every error returned directly by this
    /// method means nothing was enqueued. After admission, call
    /// [`Submission::wait`] and use [`BloxrouteClientError::may_have_been_submitted`]
    /// on its outcome.
    pub fn try_submit(
        &self,
        tx_bytes: &[u8],
        tx_hash: B256,
        reward_address: Option<Address>,
    ) -> Result<Submission, BloxrouteClientError> {
        let deadline = Instant::now() + self.options.request_timeout;
        self.validate_submission(tx_bytes, reward_address)?;

        let driver = self
            .driver
            .get()
            .ok_or(BloxrouteClientError::NotConnected)?;
        let permit = Arc::clone(&self.slots)
            .try_acquire_owned()
            .map_err(|_| BloxrouteClientError::Full)?;

        self.enqueue(driver, tx_bytes, tx_hash, reward_address, permit, deadline)
    }

    /// Convenience API: compute the hash, wait for capacity and connection, then
    /// await acknowledgement. Use [`Self::try_submit`] for immediate admission.
    ///
    /// The whole operation is bounded by `request_timeout`. Dropping the future
    /// before a write starts prevents sending; later cancellation leaves delivery
    /// uncertain. A disconnected request is never replayed on another connection.
    pub async fn send_raw_tx(
        &self,
        tx_bytes: &[u8],
        reward_address: Option<Address>,
    ) -> Result<B256, BloxrouteClientError> {
        let deadline = Instant::now() + self.options.request_timeout;
        self.validate_submission(tx_bytes, reward_address)?;
        let hash = keccak256(tx_bytes);

        let submission = timeout_at(deadline, async {
            let permit = Arc::clone(&self.slots)
                .acquire_owned()
                .await
                .map_err(|_| BloxrouteClientError::Closed)?;
            let driver = self.start_driver()?;
            wait_connected(driver).await?;

            self.enqueue(driver, tx_bytes, hash, reward_address, permit, deadline)
        })
        .await
        .map_err(|_| BloxrouteClientError::Timeout)??;

        let outcome = submission.wait().await;
        outcome.result.map(|()| outcome.tx_hash)
    }

    fn validate_submission(
        &self,
        tx_bytes: &[u8],
        reward_address: Option<Address>,
    ) -> Result<(), BloxrouteClientError> {
        if reward_address.is_some() && !self.network.supports_reward_address() {
            return Err(BloxrouteClientError::UnsupportedRewardAddress {
                network: self.network,
            });
        }

        if tx_bytes.len() > (self.options.max_request_bytes - 256) / 2 {
            return Err(BloxrouteClientError::RequestTooLarge {
                limit: self.options.max_request_bytes,
            });
        }

        Ok(())
    }

    fn enqueue(
        &self,
        driver: &Driver,
        tx_bytes: &[u8],
        tx_hash: B256,
        reward_address: Option<Address>,
        permit: OwnedSemaphorePermit,
        deadline: Instant,
    ) -> Result<Submission, BloxrouteClientError> {
        let generation = driver.generation.load(Ordering::Acquire);
        if generation == 0 {
            return Err(BloxrouteClientError::NotConnected);
        }
        if Instant::now() >= deadline {
            return Err(BloxrouteClientError::Timeout);
        }

        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let text = build_request(self.network, tx_bytes, reward_address, id);

        let (reply, response) = oneshot::channel();
        let state = Arc::new(SubmissionState::new(tx_hash));
        let request = QueuedSubmission {
            id,
            generation,
            text,
            pending: Pending {
                state: Arc::clone(&state),
                deadline,
                reply: Some(reply),
                permit: Some(permit),
            },
        };

        driver
            .requests
            .try_send(request)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => BloxrouteClientError::Full,
                mpsc::error::TrySendError::Closed(_) => BloxrouteClientError::Closed,
            })?;

        Ok(Submission {
            deadline,
            response,
            cancellation: CancelQueued { state },
        })
    }

    fn start_driver(&self) -> Result<&Driver, BloxrouteClientError> {
        if let Some(driver) = self.driver.get() {
            return Ok(driver);
        }

        let mut request = self.endpoint.as_str().into_client_request()?;
        let mut authorization = HeaderValue::from_str(&self.auth_header)
            .map_err(|_| BloxrouteClientError::InvalidAuthHeader)?;
        authorization.set_sensitive(true);
        request.headers_mut().insert(AUTHORIZATION, authorization);

        Ok(self.driver.get_or_init(|| {
            let (requests, receiver) = mpsc::channel(self.options.max_in_flight);
            let generation = Arc::new(AtomicU64::new(0));
            let (ready, readiness) = watch::channel(());
            let task = tokio::spawn(run_driver(
                request,
                receiver,
                self.options,
                Arc::clone(&generation),
                ready,
            ));

            Driver {
                requests,
                generation,
                readiness,
                task: task.abort_handle(),
            }
        }))
    }
}

async fn wait_connected(driver: &Driver) -> Result<(), BloxrouteClientError> {
    let mut readiness = driver.readiness.clone();

    loop {
        if driver.requests.is_closed() {
            return Err(BloxrouteClientError::Closed);
        }
        if driver.generation.load(Ordering::Acquire) != 0 {
            return Ok(());
        }

        readiness
            .changed()
            .await
            .map_err(|_| BloxrouteClientError::Closed)?;
    }
}

async fn run_driver(
    request: Handshake,
    mut requests: mpsc::Receiver<QueuedSubmission>,
    options: BloxrouteOptions,
    generation: Arc<AtomicU64>,
    readiness: watch::Sender<()>,
) {
    let mut config = WebSocketConfig::default();
    config.read_buffer_size = 16 * 1024;
    config.write_buffer_size = 16 * 1024;
    config.max_message_size = Some(options.max_response_bytes);
    config.max_frame_size = Some(options.max_response_bytes);

    let mut next_generation = 1_u64;
    let mut delay = Duration::ZERO;
    let mut backoff = RECONNECT_MIN;

    loop {
        // Late admissions from the previous connection are rejected even while
        // reconnecting. No queued transaction crosses connection generations.
        let connection = {
            let connecting = async {
                if !delay.is_zero() {
                    sleep(delay).await;
                }

                timeout(
                    options.connect_timeout,
                    connect_async_with_config(request.clone(), Some(config), true),
                )
                .await
            };
            tokio::pin!(connecting);

            loop {
                tokio::select! {
                    result = &mut connecting => break result,
                    queued = requests.recv() => match queued {
                        Some(queued) => drop(queued),
                        None => return,
                    },
                }
            }
        };

        match connection {
            Ok(Ok((socket, _))) => {
                let current_generation = next_generation;
                let Some(next) = next_generation.checked_add(1) else {
                    return;
                };
                next_generation = next;

                generation.store(current_generation, Ordering::Release);
                readiness.send_replace(());
                backoff = RECONNECT_MIN;

                let result =
                    run_connection(socket, &mut requests, options, current_generation).await;
                generation.store(0, Ordering::Release);
                readiness.send_replace(());

                match result {
                    Ok(()) => return,
                    Err(error) => {
                        tracing::debug!(%error, "bloXroute connection ended; reconnecting")
                    }
                }
            }
            Ok(Err(error)) => tracing::debug!(%error, "bloXroute connection failed; retrying"),
            Err(_) => tracing::debug!("bloXroute connection timed out; retrying"),
        }

        delay = backoff;
        backoff = (backoff * 2).min(RECONNECT_MAX);
    }
}

async fn run_connection<S>(
    mut socket: WebSocketStream<S>,
    requests: &mut mpsc::Receiver<QueuedSubmission>,
    options: BloxrouteOptions,
    generation: u64,
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

        for (_, mut request) in
            pending.extract_if(|_, request| request.deadline <= now || request.observer_closed())
        {
            request.finish(
                Err(BloxrouteClientError::AcknowledgementUnknown {
                    tx_hash: request.state.tx_hash,
                    reason: "submission deadline elapsed",
                }),
                None,
            );
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
                let Some(mut request) = request else {
                    return Ok(());
                };

                if request.generation != generation {
                    request.pending.finish(Err(BloxrouteClientError::Closed), None);
                    continue;
                }

                if request.pending.observer_closed()
                    || request.pending.deadline <= Instant::now()
                {
                    request.pending.finish(Err(BloxrouteClientError::Timeout), None);
                    continue;
                }

                let state = &request.pending.state;
                if state
                    .phase
                    .compare_exchange(QUEUED, WRITING, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    request.pending.finish(Err(BloxrouteClientError::Timeout), None);
                    continue;
                }

                let deadline = request.pending.deadline
                    .min(Instant::now() + options.write_timeout);
                let _ = state.write_started_at.set(Instant::now().into_std());
                timeout_at(deadline, socket.send(Message::Text(request.text.into())))
                    .await
                    .map_err(|_| BloxrouteClientError::Closed)??;
                let _ = state.write_completed_at.set(Instant::now().into_std());

                pending.insert(request.id, request.pending);
            }
            response = socket.next() => match response {
                Some(Ok(Message::Text(text))) => {
                    let received_at = Instant::now().into_std();
                    let (id, result) = parse_response(&text)?;
                    if let Some(mut request) = pending.remove(&id) {
                        let result = match result {
                            Ok(hash) if hash != request.state.tx_hash => {
                                Err(BloxrouteClientError::AcknowledgementUnknown {
                                    tx_hash: request.state.tx_hash,
                                    reason: "response transaction hash mismatch",
                                })
                            }
                            result => result.map(|_| ()),
                        };
                        request.finish(result, Some(received_at));
                    }
                }
                Some(Ok(Message::Ping(payload))) => {
                    timeout(options.write_timeout, socket.send(Message::Pong(payload)))
                        .await
                        .map_err(|_| BloxrouteClientError::Closed)??;
                }
                Some(Ok(Message::Pong(_))) => pong_deadline = None,
                Some(Ok(Message::Close(_))) | None => return Err(BloxrouteClientError::Closed),
                Some(Err(error)) => return Err(error.into()),
                _ => {}
            },
            _ = ping.tick(), if pong_deadline.is_none() => {
                timeout(options.write_timeout, socket.send(Message::Ping(Default::default())))
                    .await
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

    fn test_client(options: BloxrouteOptions) -> BloxrouteClient {
        BloxrouteClient::with_options(BloxrouteNetwork::Base, "ws://127.0.0.1:1", "test", options)
            .unwrap()
    }

    type TestSocket = WebSocketStream<tokio::io::DuplexStream>;

    async fn socket_pair(capacity: usize) -> (TestSocket, TestSocket) {
        use tokio_tungstenite::tungstenite::protocol::Role;
        let (client, server) = tokio::io::duplex(capacity);
        (
            WebSocketStream::from_raw_socket(client, Role::Client, None).await,
            WebSocketStream::from_raw_socket(server, Role::Server, None).await,
        )
    }

    fn attach_socket<S>(
        client: &BloxrouteClient,
        socket: WebSocketStream<S>,
        generation: u64,
    ) -> tokio::task::JoinHandle<Result<(), BloxrouteClientError>>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (requests, mut receiver) = mpsc::channel(client.options.max_in_flight);
        let (ready, readiness) = watch::channel(());
        let options = client.options;
        let task = tokio::spawn(async move {
            let _ready = ready;
            run_connection(socket, &mut receiver, options, generation).await
        });
        assert!(
            client
                .driver
                .set(Driver {
                    requests,
                    generation: Arc::new(AtomicU64::new(generation)),
                    readiness,
                    task: task.abort_handle(),
                })
                .is_ok()
        );
        task
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
        assert!(client.driver.get().is_none());
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

    #[test]
    fn immediate_admission_requires_warmup_and_valid_input() {
        let client = test_client(test_options());
        let error = client.try_submit(&[1], keccak256([1]), None).unwrap_err();
        assert!(matches!(error, BloxrouteClientError::NotConnected));
        assert!(!error.may_have_been_submitted());
        assert!(client.driver.get().is_none());

        let client = test_client(BloxrouteOptions {
            max_request_bytes: 258,
            ..test_options()
        });
        let error = client.try_submit(&[1, 2], B256::ZERO, None).unwrap_err();
        assert!(matches!(
            error,
            BloxrouteClientError::RequestTooLarge { .. }
        ));
        assert!(!error.may_have_been_submitted());
        assert_eq!(
            client.slots.available_permits(),
            client.options.max_in_flight
        );
    }

    #[tokio::test]
    async fn immediate_submissions_send_without_waiters_and_match_reversed_responses() {
        let client = test_client(test_options());
        let (socket, mut peer) = socket_pair(1024).await;
        let task = attach_socket(&client, socket, 1);
        let first = client.try_submit(&[1], keccak256([1]), None).unwrap();
        let second = client.try_submit(&[2], keccak256([2]), None).unwrap();

        // Neither acknowledgement handle is polled while both requests are sent.
        let first_request = peer.next().await.unwrap().unwrap();
        let second_request = peer.next().await.unwrap().unwrap();
        peer.send(response_for(&second_request)).await.unwrap();
        peer.send(response_for(&first_request)).await.unwrap();

        for (submission, hash) in [(first, keccak256([1])), (second, keccak256([2]))] {
            assert_eq!(submission.tx_hash(), hash);
            let outcome = submission.wait().await;
            assert!(outcome.result.is_ok());
            assert_eq!(outcome.tx_hash, hash);
            let timing = outcome.timing;
            assert!(timing.enqueued_at <= timing.write_started_at.unwrap());
            assert!(timing.write_started_at.unwrap() <= timing.write_completed_at.unwrap());
            assert!(timing.write_completed_at.unwrap() <= timing.response_received_at.unwrap());
        }
        task.abort();
    }

    #[tokio::test]
    async fn full_capacity_rejects_immediately_and_reopens_after_acknowledgement() {
        let client = test_client(BloxrouteOptions {
            max_in_flight: 1,
            ..test_options()
        });
        let (socket, mut peer) = socket_pair(1024).await;
        let task = attach_socket(&client, socket, 1);
        let first = client.try_submit(&[1], keccak256([1]), None).unwrap();
        let request = peer.next().await.unwrap().unwrap();
        let error = client.try_submit(&[2], keccak256([2]), None).unwrap_err();
        assert!(matches!(error, BloxrouteClientError::Full));
        assert!(!error.may_have_been_submitted());

        peer.send(response_for(&request)).await.unwrap();
        // Capacity belongs to the driver, not to the unpolled result handle.
        timeout(Duration::from_secs(1), async {
            while client.slots.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let second = client.try_submit(&[2], keccak256([2]), None).unwrap();
        let request = peer.next().await.unwrap().unwrap();
        peer.send(response_for(&request)).await.unwrap();
        assert!(first.wait().await.result.is_ok());
        assert!(second.wait().await.result.is_ok());
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn driver_expires_written_requests_without_polling_the_handle() {
        let options = test_options();
        let client = test_client(options);
        let (socket, mut peer) = socket_pair(1024).await;
        let task = attach_socket(&client, socket, 1);
        let submission = client.try_submit(&[1], keccak256([1]), None).unwrap();
        let _request = peer.next().await.unwrap().unwrap();
        tokio::time::advance(options.request_timeout).await;
        tokio::task::yield_now().await;

        assert_eq!(client.slots.available_permits(), options.max_in_flight);
        let outcome = submission.wait().await;
        assert!(outcome.result.unwrap_err().may_have_been_submitted());
        assert!(outcome.timing.write_started_at.is_some());
        assert!(outcome.timing.write_completed_at.is_some());
        assert!(outcome.timing.response_received_at.is_none());
        task.abort();
    }

    #[tokio::test]
    async fn driver_abort_preserves_delivery_evidence_and_releases_capacity() {
        for written in [false, true] {
            let client = test_client(test_options());
            let (socket, mut peer) = socket_pair(1024).await;
            let task = attach_socket(&client, socket, 1);
            let submission = client.try_submit(&[1], keccak256([1]), None).unwrap();
            if written {
                let _request = peer.next().await.unwrap().unwrap();
            }
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());

            let outcome = submission.wait().await;
            assert_eq!(
                outcome.result.unwrap_err().may_have_been_submitted(),
                written
            );
            assert_eq!(outcome.timing.write_started_at.is_some(), written);
            assert_eq!(outcome.timing.write_completed_at.is_some(), written);
            assert_eq!(
                client.slots.available_permits(),
                client.options.max_in_flight
            );
        }
    }

    #[tokio::test]
    async fn wrong_server_hash_keeps_the_supplied_hash_and_write_timing() {
        let client = test_client(test_options());
        let (socket, mut peer) = socket_pair(1024).await;
        let task = attach_socket(&client, socket, 1);
        let hash = keccak256([1]);
        let submission = client.try_submit(&[1], hash, None).unwrap();
        let _request = peer.next().await.unwrap().unwrap();
        peer.send(Message::Text(
            format!(r#"{{"id":1,"result":{{"txHash":"{}"}}}}"#, B256::ZERO,).into(),
        ))
        .await
        .unwrap();

        let outcome = submission.wait().await;
        assert_eq!(outcome.tx_hash, hash);
        assert!(
            matches!(outcome.result, Err(BloxrouteClientError::AcknowledgementUnknown { tx_hash, .. }) if tx_hash == hash)
        );
        assert!(outcome.timing.write_completed_at.is_some());
        assert!(outcome.timing.response_received_at.is_some());
        task.abort();
    }

    #[tokio::test]
    async fn idle_disconnect_reconnects_without_another_submission() {
        let (listener, client) = listener_client(BloxrouteNetwork::Base, test_options()).await;
        let (disconnect, disconnect_now) = oneshot::channel();
        let (reconnected, observe_reconnect) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            disconnect_now.await.unwrap();
            socket.close(None).await.unwrap();
            drop(socket);

            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            reconnected.send(()).unwrap();
            let request = socket.next().await.unwrap().unwrap();
            socket.send(response_for(&request)).await.unwrap();
        });
        client.connect().await.unwrap();
        disconnect.send(()).unwrap();

        // No client method is invoked to trigger reconnection.
        timeout(Duration::from_secs(2), observe_reconnect)
            .await
            .unwrap()
            .unwrap();
        client.connect().await.unwrap();
        let outcome = client
            .try_submit(&[1], keccak256([1]), None)
            .unwrap()
            .wait()
            .await;
        assert!(outcome.result.is_ok());
        server.await.unwrap();
    }

    #[test]
    fn socket_driver_progresses_while_the_submitting_thread_is_blocked() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (listener, client) = listener_client(BloxrouteNetwork::Base, test_options()).await;
            let (written, observed) = std::sync::mpsc::sync_channel(1);
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                let request = socket.next().await.unwrap().unwrap();
                written.send(()).unwrap();
                socket.send(response_for(&request)).await.unwrap();
            });
            client.connect().await.unwrap();
            let submission = client.try_submit(&[1], keccak256([1]), None).unwrap();

            // Deliberately block the submitting thread. The single runtime
            // worker must send the request without any per-submission task.
            observed.recv_timeout(Duration::from_secs(2)).unwrap();
            assert!(submission.wait().await.result.is_ok());
            server.await.unwrap();
        });
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_deadline_cancels_queued_work_behind_a_blocked_write() {
        let options = BloxrouteOptions {
            request_timeout: Duration::from_secs(2),
            write_timeout: Duration::from_secs(1),
            ..test_options()
        };
        let client = test_client(options);
        let (socket, _peer) = socket_pair(1).await;
        let task = attach_socket(&client, socket, 1);
        let first = client.try_submit(&[1], keccak256([1]), None).unwrap();
        tokio::task::yield_now().await;
        assert_eq!(
            first.cancellation.state.phase.load(Ordering::Acquire),
            WRITING
        );

        // The convenience API can arrive with an earlier deadline after waiting
        // for admission. Its waiter must expire even while the driver is writing.
        let second = client
            .enqueue(
                client.driver.get().unwrap(),
                &[2],
                keccak256([2]),
                None,
                Arc::clone(&client.slots).try_acquire_owned().unwrap(),
                Instant::now() + Duration::from_millis(20),
            )
            .unwrap();
        let state = Arc::clone(&second.cancellation.state);
        let started = Instant::now();
        let outcome = second.wait().await;
        assert!(matches!(outcome.result, Err(BloxrouteClientError::Timeout)));
        assert!(outcome.timing.write_started_at.is_none());
        assert_eq!(state.phase.load(Ordering::Acquire), CANCELLED);
        assert_eq!(started.elapsed(), Duration::from_millis(20));

        task.abort();
        assert!(
            first
                .wait()
                .await
                .result
                .unwrap_err()
                .may_have_been_submitted()
        );
    }

    #[tokio::test]
    async fn dropping_the_client_stops_its_driver_and_resolves_submissions() {
        let (listener, client) = listener_client(BloxrouteNetwork::Base, test_options()).await;
        let (written, observed) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _request = socket.next().await.unwrap().unwrap();
            written.send(()).unwrap();
            assert!(matches!(
                socket.next().await,
                None | Some(Err(_)) | Some(Ok(Message::Close(_)))
            ));
            assert!(
                timeout(Duration::from_millis(200), listener.accept())
                    .await
                    .is_err()
            );
        });
        client.connect().await.unwrap();
        let submission = client.try_submit(&[1], keccak256([1]), None).unwrap();
        observed.await.unwrap();
        drop(client);

        let outcome = submission.wait().await;
        assert!(outcome.result.unwrap_err().may_have_been_submitted());
        assert!(outcome.timing.write_completed_at.is_some());
        timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    #[ignore = "manual loopback latency benchmark; no live transactions"]
    fn local_submission_latency_under_cpu_load() {
        const SAMPLES: usize = 1_000;
        const WARMUP: usize = 100;

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async {
            let (listener, client) = listener_client(BloxrouteNetwork::Base, test_options()).await;
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                stream.set_nodelay(true).unwrap();
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                for _ in 0..(SAMPLES + WARMUP) * 2 {
                    let request = socket.next().await.unwrap().unwrap();
                    socket.send(response_for(&request)).await.unwrap();
                }
            });
            client.connect().await.unwrap();
            let bytes = [0xab; 512];
            let hash = keccak256(bytes);

            for load in [Duration::ZERO, Duration::from_micros(200)] {
                let mut admission = Vec::with_capacity(SAMPLES);
                let mut write_start = Vec::with_capacity(SAMPLES);
                let mut write_complete = Vec::with_capacity(SAMPLES);
                for index in 0..SAMPLES + WARMUP {
                    let started = Timestamp::now();
                    let submission = client.try_submit(&bytes, hash, None).unwrap();
                    let admitted = Timestamp::now();
                    while admitted.elapsed() < load {
                        std::hint::spin_loop();
                    }
                    let outcome = submission.wait().await;
                    outcome.result.unwrap();
                    if index >= WARMUP {
                        admission.push(admitted.duration_since(started));
                        write_start.push(
                            outcome
                                .timing
                                .write_started_at
                                .unwrap()
                                .duration_since(started),
                        );
                        write_complete.push(
                            outcome
                                .timing
                                .write_completed_at
                                .unwrap()
                                .duration_since(started),
                        );
                    }
                }
                for (name, mut samples) in [
                    ("admission", admission),
                    ("write_start", write_start),
                    ("write_complete", write_complete),
                ] {
                    samples.sort_unstable();
                    println!(
                        "load={load:?} {name}: p50={:?} p99={:?} max={:?}",
                        samples[SAMPLES / 2],
                        samples[SAMPLES * 99 / 100],
                        samples[SAMPLES - 1]
                    );
                }
            }
            server.await.unwrap();
        });
    }

    #[tokio::test(start_paused = true)]
    async fn convenience_deadline_includes_capacity_and_reconnection() {
        let options = BloxrouteOptions {
            max_in_flight: 1,
            request_timeout: Duration::from_millis(20),
            ..test_options()
        };
        let client = test_client(options);
        let occupied = Arc::clone(&client.slots).acquire_owned().await.unwrap();
        let started = Instant::now();
        let error = client.send_raw_tx(&[1], None).await.unwrap_err();
        assert!(matches!(error, BloxrouteClientError::Timeout));
        assert!(!error.may_have_been_submitted());
        assert_eq!(started.elapsed(), options.request_timeout);

        drop(occupied);

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
    async fn connection_loss_is_unknown_and_later_submissions_succeed() {
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
        let client = test_client(options);
        let (socket, _peer) = socket_pair(1).await;
        let task = attach_socket(&client, socket, 1);
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
        let client = test_client(options);
        let (socket, mut peer) = socket_pair(1024).await;
        let task = attach_socket(&client, socket, 1);

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
        let client = test_client(options);
        let (socket, mut peer) = socket_pair(1024).await;
        let task = attach_socket(&client, socket, 1);
        let submission = client.try_submit(&[1], keccak256([1]), None).unwrap();

        // The driver has not been polled yet. Expiry must prevent a late write.
        tokio::time::advance(options.request_timeout).await;
        let outcome = submission.wait().await;
        assert!(matches!(outcome.result, Err(BloxrouteClientError::Timeout)));
        assert!(outcome.timing.write_started_at.is_none());
        assert!(
            timeout(Duration::from_millis(10), peer.next())
                .await
                .is_err()
        );
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_an_unwritten_submission_prevents_sending() {
        let client = test_client(test_options());
        let (socket, mut peer) = socket_pair(1024).await;
        let task = attach_socket(&client, socket, 1);
        let submission = client.try_submit(&[1], keccak256([1]), None).unwrap();
        let state = Arc::clone(&submission.cancellation.state);
        drop(submission);

        assert_eq!(state.phase.load(Ordering::Acquire), CANCELLED);
        assert!(
            timeout(Duration::from_millis(10), peer.next())
                .await
                .is_err()
        );
        assert_eq!(
            client.slots.available_permits(),
            client.options.max_in_flight
        );
        task.abort();
    }

    #[tokio::test]
    async fn admission_from_a_previous_connection_is_never_replayed() {
        let client = test_client(test_options());
        let (socket, mut peer) = socket_pair(1024).await;
        let task = attach_socket(&client, socket, 2);
        let driver = client.driver.get().unwrap();
        // Model a producer that read generation 1 before reconnect, then enqueued late.
        driver.generation.store(1, Ordering::Release);
        let stale = client.try_submit(&[1], keccak256([1]), None).unwrap();
        driver.generation.store(2, Ordering::Release);
        let fresh = client.try_submit(&[2], keccak256([2]), None).unwrap();

        let stale = stale.wait().await;
        assert!(matches!(stale.result, Err(BloxrouteClientError::Closed)));
        assert!(stale.timing.write_started_at.is_none());
        let request = peer.next().await.unwrap().unwrap();
        assert!(request.to_text().unwrap().contains(r#""transaction":"02""#));
        peer.send(response_for(&request)).await.unwrap();
        assert!(fresh.wait().await.result.is_ok());
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
