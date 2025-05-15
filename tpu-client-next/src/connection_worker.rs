//! This module defines [`ConnectionWorker`] which encapsulates the functionality
//! needed to handle one connection within the scope of task.

use {
    super::SendTransactionStats,
    crate::{
        quic_networking::send_data_over_stream, send_transaction_stats::record_error,
        transaction_batch::TransactionBatch,
    },
    log::*,
    quinn::{ConnectError, Connection, Endpoint},
    solana_clock::{DEFAULT_MS_PER_SLOT, MAX_PROCESSING_AGE, NUM_CONSECUTIVE_LEADER_SLOTS},
    solana_measure::measure::Measure,
    solana_streamer::tpu_feedback::{
        TpuFeedback, TYPE_PRIORITY_FEES, TYPE_TIMESTAMP, TYPE_TRANSACTION_STATE, TYPE_VERSION,
    },
    solana_time_utils::timestamp,
    std::{
        net::SocketAddr,
        sync::{atomic::Ordering, Arc},
    },
    tokio::{
        sync::{broadcast, mpsc},
        task::JoinHandle,
        time::{sleep, Duration},
    },
    tokio_util::sync::CancellationToken,
};

/// Interval between retry attempts for creating a new connection. This value is
/// a best-effort estimate, based on current network conditions.
const RETRY_SLEEP_INTERVAL: Duration =
    Duration::from_millis(NUM_CONSECUTIVE_LEADER_SLOTS * DEFAULT_MS_PER_SLOT);

/// Maximum age (in milliseconds) of a blockhash, beyond which transaction
/// batches are dropped.
const MAX_PROCESSING_AGE_MS: u64 = MAX_PROCESSING_AGE as u64 * DEFAULT_MS_PER_SLOT;

/// [`ConnectionState`] represents the current state of a quic connection.
///
/// It tracks the lifecycle of connection from initial setup to closing phase.
/// The transition function between states is defined in `ConnectionWorker`
/// implementation.
enum ConnectionState {
    NotSetup,
    Active(Connection),
    Retry(usize),
    Closing,
}

fn parse_feedback(data: &[u8]) -> Option<TpuFeedback> {
    let mut offset = 0;
    let mut version: Option<u8> = None;
    let mut timestamp: Option<u64> = None;
    let mut transactions: Vec<(u64, u32)> = vec![];
    let mut priority_fees: Option<(u64, u64, u64)> = None;

    while offset + 3 <= data.len() {
        let t = data[offset];
        let l = u16::from_le_bytes([data[offset + 1], data[offset + 2]]) as usize;
        offset += 3;

        if offset + l > data.len() {
            break;
        }

        match t {
            TYPE_VERSION if l == 1 => {
                version = Some(data[offset]);
            }
            TYPE_TIMESTAMP if l == 8 => {
                timestamp = Some(u64::from_le_bytes(
                    data[offset..offset + 8].try_into().ok()?,
                ));
            }
            TYPE_TRANSACTION_STATE => {
                let mut sub_offset = 0;
                while sub_offset + 12 <= l {
                    let tx_sig = u64::from_le_bytes(
                        data[offset + sub_offset..offset + sub_offset + 8]
                            .try_into()
                            .ok()?,
                    );
                    let state = u32::from_le_bytes(
                        data[offset + sub_offset + 8..offset + sub_offset + 12]
                            .try_into()
                            .ok()?,
                    );
                    transactions.push((tx_sig, state));
                    sub_offset += 12;
                }
            }
            TYPE_PRIORITY_FEES if l == 24 => {
                let min = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
                let median = u64::from_le_bytes(data[offset + 8..offset + 16].try_into().ok()?);
                let max = u64::from_le_bytes(data[offset + 16..offset + 24].try_into().ok()?);
                priority_fees = Some((min, median, max));
            }
            _ => {
                debug!("Unknown or malformed TLV type: {} with length {}", t, l);
            }
        }

        offset += l;
    }

    Some(TpuFeedback {
        version: version?,
        timestamp: timestamp?,
        transactions,
        priority_fees: priority_fees?,
    })
}

impl Drop for ConnectionState {
    /// When [`ConnectionState`] is dropped, underlying connection is closed
    /// which means that there is no guarantee that the open streams will
    /// finish.
    fn drop(&mut self) {
        if let Self::Active(connection) = self {
            debug!(
                "Close connection with {:?}, stats: {:?}. All pending streams will be dropped.",
                connection.remote_address(),
                connection.stats()
            );
            connection.close(0u32.into(), b"done");
        }
    }
}

/// [`ConnectionWorker`] holds connection to the validator with address `peer`.
///
/// If connection has been closed, [`ConnectionWorker`] tries to reconnect
/// `max_reconnect_attempts` times. If connection is in `Active` state, it sends
/// transactions received from `transactions_receiver`. Additionally, it
/// accumulates statistics about connections and streams failures.
pub(crate) struct ConnectionWorker {
    endpoint: Endpoint,
    peer: SocketAddr,
    transactions_receiver: mpsc::Receiver<TransactionBatch>,
    connection: ConnectionState,
    skip_check_transaction_age: bool,
    max_reconnect_attempts: usize,
    send_txs_stats: Arc<SendTransactionStats>,
    cancel: CancellationToken,
    datagram_task: Option<JoinHandle<()>>,
    feedback_sender: Option<broadcast::Sender<TpuFeedback>>,
}

impl ConnectionWorker {
    /// Constructs a [`ConnectionWorker`].
    ///
    /// [`ConnectionWorker`] maintains a connection to a `peer` and processes
    /// transactions from `transactions_receiver`. If
    /// `skip_check_transaction_age` is set to `true`, the worker skips checking
    /// for transaction blockhash expiration. The `max_reconnect_attempts`
    /// parameter controls how many times the worker will attempt to reconnect
    /// in case of connection failure. Returns the created `ConnectionWorker`
    /// along with a cancellation token that can be used by the caller to stop
    /// the worker.
    pub fn new(
        endpoint: Endpoint,
        peer: SocketAddr,
        transactions_receiver: mpsc::Receiver<TransactionBatch>,
        skip_check_transaction_age: bool,
        max_reconnect_attempts: usize,
        send_txs_stats: Arc<SendTransactionStats>,
    ) -> (Self, CancellationToken) {
        let cancel = CancellationToken::new();
        let this = Self {
            endpoint,
            peer,
            transactions_receiver,
            connection: ConnectionState::NotSetup,
            skip_check_transaction_age,
            max_reconnect_attempts,
            send_txs_stats,
            cancel: cancel.clone(),
            datagram_task: None,
            feedback_sender: None,
        };

        (this, cancel)
    }

    /// Starts the main loop of the [`ConnectionWorker`].
    ///
    /// This method manages the connection to the peer and handles state
    /// transitions. It runs indefinitely until the connection is closed or an
    /// unrecoverable error occurs.
    pub async fn run(&mut self) {
        let cancel = self.cancel.clone();

        let main_loop = async move {
            loop {
                match &self.connection {
                    ConnectionState::Closing => {
                        break;
                    }
                    ConnectionState::NotSetup => {
                        self.create_connection(0).await;
                    }
                    ConnectionState::Active(connection) => {
                        let Some(transactions) = self.transactions_receiver.recv().await else {
                            debug!("Transactions sender has been dropped.");
                            self.connection = ConnectionState::Closing;
                            continue;
                        };
                        self.send_transactions(connection.clone(), transactions)
                            .await;
                    }
                    ConnectionState::Retry(num_reconnects) => {
                        if *num_reconnects > self.max_reconnect_attempts {
                            error!("Failed to establish connection: reach max reconnect attempts.");
                            self.connection = ConnectionState::Closing;
                            continue;
                        }
                        sleep(RETRY_SLEEP_INTERVAL).await;
                        self.reconnect(*num_reconnects).await;
                    }
                }
            }
        };

        tokio::select! {
            () = main_loop => (),
            () = cancel.cancelled() => (),
        }
    }

    /// Sends a batch of transactions using the provided `connection`.
    ///
    /// Each transaction in the batch is sent over the QUIC streams one at the
    /// time, which prevents traffic fragmentation and shows better TPS in
    /// comparison with multistream send. If the batch is determined to be
    /// outdated and flag `skip_check_transaction_age` is unset, it will be
    /// dropped without being sent.
    ///
    /// In case of error, it doesn't retry to send the same transactions again.
    async fn send_transactions(&mut self, connection: Connection, transactions: TransactionBatch) {
        let now = timestamp();
        if !self.skip_check_transaction_age
            && now.saturating_sub(transactions.timestamp()) > MAX_PROCESSING_AGE_MS
        {
            debug!("Drop outdated transaction batch.");
            return;
        }
        let mut measure_send = Measure::start("send transaction batch");
        for data in transactions.into_iter() {
            let result = send_data_over_stream(&connection, &data).await;

            if let Err(error) = result {
                trace!("Failed to send transaction over stream with error: {error}.");
                record_error(error, &self.send_txs_stats);
                self.connection = ConnectionState::Retry(0);
            } else {
                self.send_txs_stats
                    .successfully_sent
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        measure_send.stop();
        debug!(
            "Time to send transactions batch: {} us",
            measure_send.as_us()
        );
    }

    async fn recv_datagrams(
        connection: Connection,
        cancel: CancellationToken,
        sender: broadcast::Sender<TpuFeedback>,
    ) {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    debug!("Recv datagrams task cancelled.");
                    break;
                }
                result = connection.read_datagram() => {
                    match result {
                        Ok(data) => {
                            debug!("Received datagram of size: {}", data.len());
                            let feedback = parse_feedback(&data);
                            if let Some(feedback) = feedback {
                                trace!("Received feedback: {:?}", feedback);
                                match sender.send(feedback) {
                                        Ok(_) => {
                                            debug!("Feedback sent to sender.");
                                        }
                                        Err(e) => {
                                            debug!("Failed to send feedback: {:?}", e);
                                        }
                                    }
                            } else {
                                debug!("Failed to parse feedback.");
                            }
                        }
                        Err(e) => {
                            debug!("Error receiving datagram: {e}");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Attempts to create a new connection to the specified `peer` address.
    ///
    /// If the connection is successful, the state is updated to `Active`.
    ///
    /// If an error occurs, the state may transition to `Retry` or `Closing`,
    /// depending on the nature of the error.
    async fn create_connection(&mut self, retries_attempt: usize) {
        let connecting = self.endpoint.connect(self.peer, "connect");
        match connecting {
            Ok(connecting) => {
                let mut measure_connection = Measure::start("establish connection");
                let res = connecting.await;
                measure_connection.stop();
                debug!(
                    "Establishing connection with {} took: {} us",
                    self.peer,
                    measure_connection.as_us()
                );
                match res {
                    Ok(connection) => {
                        let cancel = self.cancel.clone();
                        if let Some(feedback_sender) = &self.feedback_sender {
                            let recv_task = tokio::spawn(Self::recv_datagrams(
                                connection.clone(),
                                cancel,
                                feedback_sender.clone(),
                            ));
                            self.datagram_task = Some(recv_task);
                        }

                        self.connection = ConnectionState::Active(connection);
                    }
                    Err(err) => {
                        warn!("Connection error {}: {}", self.peer, err);
                        record_error(err.into(), &self.send_txs_stats);
                        self.connection = ConnectionState::Retry(retries_attempt.saturating_add(1));
                    }
                }
            }
            Err(connecting_error) => {
                record_error(connecting_error.clone().into(), &self.send_txs_stats);
                match connecting_error {
                    ConnectError::EndpointStopping => {
                        debug!("Endpoint stopping, exit connection worker.");
                        self.connection = ConnectionState::Closing;
                    }
                    ConnectError::InvalidRemoteAddress(_) => {
                        warn!("Invalid remote address.");
                        self.connection = ConnectionState::Closing;
                    }
                    e => {
                        error!("Unexpected error has happen while trying to create connection {e}");
                        self.connection = ConnectionState::Closing;
                    }
                }
            }
        }
    }

    /// Attempts to reconnect to the peer after a connection failure.
    async fn reconnect(&mut self, num_reconnects: usize) {
        debug!("Trying to reconnect. Reopen connection, 0rtt is not implemented yet.");
        // We can reconnect using 0rtt, but not a priority for now. Check if we
        // need to call config.enable_0rtt() on the client side and where
        // session tickets are stored.
        self.create_connection(num_reconnects).await;
    }
}
