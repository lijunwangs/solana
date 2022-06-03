//! Simple client that connects to a given UDP port with the QUIC protocol and provides
//! an interface for sending transactions which is restricted by the server's flow control.

use {
    crate::{
        client_error::ClientErrorKind,
        connection_cache::ConnectionCacheStats,
        tpu_connection::{ClientStats, TpuConnection},
    },
    async_mutex::Mutex,
    futures::future::join_all,
    itertools::Itertools,
    lazy_static::lazy_static,
    log::*,
    quinn::{
        ClientConfig, Endpoint, EndpointConfig, IdleTimeout, NewConnection, VarInt, WriteError,
    },
    solana_measure::measure::Measure,
    solana_net_utils::VALIDATOR_PORT_RANGE,
    solana_sdk::{
        quic::{
            QUIC_KEEP_ALIVE_MS, QUIC_MAX_CONCURRENT_STREAMS, QUIC_MAX_TIMEOUT_MS, QUIC_PORT_OFFSET,
        },
        transport::Result as TransportResult,
    },
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
        sync::{atomic::Ordering, Arc},
        thread,
        time::Duration,
    },
    tokio::{runtime::Runtime, sync::RwLock},
};

struct SkipServerVerification;

impl SkipServerVerification {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}
lazy_static! {
    static ref RUNTIME: Runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    static ref CONTROL_RUNTIME: Runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
}

/// A wrapper over NewConnection with additional capability to create the endpoint as part
/// of creating a new connection.
#[derive(Clone)]
struct QuicNewConnection {
    endpoint: Arc<Endpoint>,
    connection: Arc<NewConnection>,
}

/// A lazy initialized Quic Endpoint
pub struct QuicLazyEndpoint {
    endpoint: RwLock<Option<Arc<Endpoint>>>,
}

impl QuicLazyEndpoint {
    pub fn new() -> Self {
        Self {
            endpoint: RwLock::new(None),
        }
    }

    fn create_endpoint() -> Endpoint {
        let (_, client_socket) = solana_net_utils::bind_in_range(
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            VALIDATOR_PORT_RANGE,
        )
        .unwrap();

        let mut crypto = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_no_client_auth();
        crypto.enable_early_data = true;

        let mut endpoint =
            QuicNewConnection::create_endpoint(EndpointConfig::default(), client_socket);

        let mut config = ClientConfig::new(Arc::new(crypto));
        let transport_config = Arc::get_mut(&mut config.transport).unwrap();
        let timeout = IdleTimeout::from(VarInt::from_u32(QUIC_MAX_TIMEOUT_MS));
        transport_config.max_idle_timeout(Some(timeout));
        transport_config.keep_alive_interval(Some(Duration::from_millis(QUIC_KEEP_ALIVE_MS)));

        endpoint.set_default_client_config(config);
        endpoint
    }

    async fn get_endpoint(&self) -> Arc<Endpoint> {
        let lock = self.endpoint.read().await;
        let endpoint = lock.as_ref();

        match endpoint {
            Some(endpoint) => endpoint.clone(),
            None => {
                drop(lock);
                let mut lock = self.endpoint.write().await;
                let endpoint = lock.as_ref();

                match endpoint {
                    Some(endpoint) => endpoint.clone(),
                    None => {
                        let connection = Arc::new(Self::create_endpoint());
                        *lock = Some(connection.clone());
                        connection
                    }
                }
            }
        }
    }
}

impl QuicNewConnection {
    /// Create a QuicNewConnection given the remote address 'addr'.
    async fn make_connection(
        endpoint: Arc<QuicLazyEndpoint>,
        addr: SocketAddr,
        stats: &ClientStats,
    ) -> Result<Self, WriteError> {
        let mut make_connection_measure = Measure::start("make_connection_measure");
        let endpoint = endpoint.get_endpoint().await;

        let connecting = endpoint.connect(addr, "connect").unwrap();
        stats.total_connections.fetch_add(1, Ordering::Relaxed);
        let connecting_result = connecting.await;
        if connecting_result.is_err() {
            stats.connection_errors.fetch_add(1, Ordering::Relaxed);
        }
        make_connection_measure.stop();
        stats
            .make_connection_ms
            .fetch_add(make_connection_measure.as_ms(), Ordering::Relaxed);

        let connection = connecting_result?;

        // info!(
        //     "zzzzz Made connection to {} id {}",
        //     addr,
        //     connection.connection.stable_id()
        // );
        Ok(Self {
            endpoint,
            connection: Arc::new(connection),
        })
    }

    fn create_endpoint(config: EndpointConfig, client_socket: UdpSocket) -> Endpoint {
        quinn::Endpoint::new(config, None, client_socket).unwrap().0
    }

    // Attempts to make a faster connection by taking advantage of pre-existing key material.
    // Only works if connection to this endpoint was previously established.
    async fn make_connection_0rtt(
        &mut self,
        addr: SocketAddr,
        stats: &ClientStats,
    ) -> Result<Arc<NewConnection>, WriteError> {
        let connecting = self.endpoint.connect(addr, "connect").unwrap();
        stats.total_connections.fetch_add(1, Ordering::Relaxed);
        let connection = match connecting.into_0rtt() {
            Ok((connection, zero_rtt)) => {
                if zero_rtt.await {
                    stats.zero_rtt_accepts.fetch_add(1, Ordering::Relaxed);
                } else {
                    stats.zero_rtt_rejects.fetch_add(1, Ordering::Relaxed);
                }
                connection
            }
            Err(connecting) => {
                stats.connection_errors.fetch_add(1, Ordering::Relaxed);
                let connecting = connecting.await;
                connecting?
            }
        };
        self.connection = Arc::new(connection);
        Ok(self.connection.clone())
    }
}

struct QuicClient {
    endpoint: Arc<QuicLazyEndpoint>,
    connection: Arc<Mutex<Option<QuicNewConnection>>>,
    addr: SocketAddr,
    stats: Arc<ClientStats>,
}

pub struct QuicTpuConnection {
    client: Arc<QuicClient>,
    connection_stats: Arc<ConnectionCacheStats>,
}

impl QuicTpuConnection {
    pub fn base_stats(&self) -> Arc<ClientStats> {
        self.client.stats.clone()
    }
}

impl TpuConnection for QuicTpuConnection {
    fn tpu_addr(&self) -> &SocketAddr {
        &self.client.addr
    }

    fn send_wire_transaction<T>(
        &self,
        wire_transaction: T,
        stats: &ClientStats,
    ) -> TransportResult<()>
    where
        T: AsRef<[u8]>,
    {
        let _guard = RUNTIME.enter();
        let send_buffer =
            self.client
                .send_buffer(wire_transaction, stats, self.connection_stats.clone());
        RUNTIME.block_on(send_buffer)?;
        Ok(())
    }

    fn send_wire_transaction_batch<T>(
        &self,
        buffers: &[T],
        stats: &ClientStats,
    ) -> TransportResult<()>
    where
        T: AsRef<[u8]>,
    {
        let _guard = RUNTIME.enter();
        let send_batch = self
            .client
            .send_batch(buffers, stats, self.connection_stats.clone());
        RUNTIME.block_on(send_batch)?;
        Ok(())
    }

    fn send_wire_transaction_async(
        &self,
        wire_transaction: Vec<u8>,
        stats: Arc<ClientStats>,
    ) -> TransportResult<()> {
        let _guard = RUNTIME.enter();
        let client = self.client.clone();
        let connection_stats = self.connection_stats.clone();
        //drop and detach the task
        let _ = RUNTIME.spawn(async move {
            let send_buffer = client.send_buffer(wire_transaction, &stats, connection_stats);
            if let Err(e) = send_buffer.await {
                warn!(
                    "Failed to send transaction async to {}, error: {:?} ",
                    client.addr, e
                );
                datapoint_warn!("send-wire-async", ("failure", 1, i64),);
            }
        });
        Ok(())
    }

    fn send_wire_transaction_batch_async(
        &self,
        buffers: Vec<Vec<u8>>,
        stats: Arc<ClientStats>,
    ) -> TransportResult<()> {
        let _guard = RUNTIME.enter();
        let client = self.client.clone();
        let connection_stats = self.connection_stats.clone();
        //drop and detach the task
        let _ = RUNTIME.spawn(async move {
            let send_batch = client.send_batch(&buffers, &stats, connection_stats);
            if let Err(e) = send_batch.await {
                warn!("Failed to send transaction batch async to {:?}", e);
                datapoint_warn!("send-wire-batch-async", ("failure", 1, i64),);
            }
        });
        Ok(())
    }
}

impl QuicTpuConnection {
    pub fn new(
        endpoint: Arc<QuicLazyEndpoint>,
        tpu_addr: SocketAddr,
        connection_stats: Arc<ConnectionCacheStats>,
    ) -> Self {
        let tpu_addr = SocketAddr::new(tpu_addr.ip(), tpu_addr.port() + QUIC_PORT_OFFSET);
        let client = Arc::new(QuicClient::new(endpoint, tpu_addr));

        Self {
            client,
            connection_stats,
        }
    }
}

impl QuicClient {
    pub fn new(endpoint: Arc<QuicLazyEndpoint>, addr: SocketAddr) -> Self {
        Self {
            endpoint,
            connection: Arc::new(Mutex::new(None)),
            addr,
            stats: Arc::new(ClientStats::default()),
        }
    }

    async fn _send_buffer_using_conn(
        data: &[u8],
        connection: &NewConnection,
    ) -> Result<(), WriteError> {
        let mut send_stream = connection.connection.open_uni().await?;

        send_stream.write_all(data).await?;
        send_stream.finish().await?;
        Ok(())
    }

    // Attempts to send data, connecting/reconnecting as necessary
    // On success, returns the connection used to successfully send the data
    async fn _send_buffer(
        &self,
        data: &[u8],
        stats: &ClientStats,
        connection_stats: Arc<ConnectionCacheStats>,
    ) -> Result<Arc<NewConnection>, WriteError> {
        let mut try_count = 0;
        let mut last_connection_id = 0;
        let mut last_error = None;

        while try_count < 3 {
            let connection = {
                let mut conn_guard = self.connection.lock().await;

                let maybe_conn = conn_guard.clone();
                match maybe_conn {
                    Some(conn) => {
                        if conn.connection.connection.stable_id() == last_connection_id {
                            // this is the problematic connection we had used before, create a new one
                            let conn = QuicNewConnection::make_connection(
                                self.endpoint.clone(),
                                self.addr,
                                stats,
                            )
                            .await;
                            match conn {
                                Ok(conn) => {
                                    *conn_guard = Some(conn.clone());
                                    info!(
                                        "Made retry connection to {} id {} try_count {} old_connection_id: {}, last error: {:?}",
                                        self.addr,
                                        conn.connection.connection.stable_id(),
                                        try_count,
                                        last_connection_id,
                                        last_error
                                    );
                                    try_count += 1;

                                    conn.connection.clone()
                                }
                                Err(err) => {
                                    info!(
                                        "Cannot make connection to {}, error {:}",
                                        self.addr, err
                                    );
                                    return Err(err);
                                }
                            }
                        } else {
                            stats.connection_reuse.fetch_add(1, Ordering::Relaxed);
                            conn.connection.clone()
                        }
                    }
                    None => {
                        let conn = QuicNewConnection::make_connection(
                            self.endpoint.clone(),
                            self.addr,
                            stats,
                        )
                        .await;
                        match conn {
                            Ok(conn) => {
                                *conn_guard = Some(conn.clone());
                                info!(
                                    "Made connection to {} id {} try_count {}",
                                    self.addr,
                                    conn.connection.connection.stable_id(),
                                    try_count
                                );
                                try_count += 1;
                                conn.connection.clone()
                            }
                            Err(err) => {
                                info!("Cannot make connection to {}, error {:}", self.addr, err);
                                return Err(err);
                            }
                        }
                    }
                }
            };

            let new_stats = connection.connection.stats();

            connection_stats
                .total_client_stats
                .congestion_events
                .update_stat(
                    &self.stats.congestion_events,
                    new_stats.path.congestion_events,
                );

            connection_stats
                .total_client_stats
                .tx_streams_blocked_uni
                .update_stat(
                    &self.stats.tx_streams_blocked_uni,
                    new_stats.frame_tx.streams_blocked_uni,
                );

            connection_stats
                .total_client_stats
                .tx_data_blocked
                .update_stat(&self.stats.tx_data_blocked, new_stats.frame_tx.data_blocked);

            connection_stats
                .total_client_stats
                .tx_acks
                .update_stat(&self.stats.tx_acks, new_stats.frame_tx.acks);

            last_connection_id = connection.connection.stable_id();
            match Self::_send_buffer_using_conn(data, &connection).await {
                Ok(()) => {
                    // info!(
                    //     "Successfully sent data to  {} id {}",
                    //     self.addr,
                    //     connection.connection.stable_id()
                    // );
                    return Ok(connection);
                }
                Err(err) => {
                    // info!(
                    //     "zzzzz error sending to {} id {} {} thread {:?}",
                    //     self.addr,
                    //     connection.connection.stable_id(),
                    //     err,
                    //     thread::current().id(),
                    // );
                    match err {
                        WriteError::ConnectionLost(_) => {
                            last_error = Some(err);
                        }
                        _ => {
                            info!(
                                "zzzzz error sending to {} id {} {:?} thread {:?}",
                                self.addr,
                                connection.connection.stable_id(),
                                err,
                                thread::current().id(),
                            );
                            return Err(err);
                        }
                    }
                }
            }
        }

        // if we come here, that means we have exhausted maximum retries, return the error
        info!(
            "Ran into error sending transactions {:?}, exhausted retries to {}",
            last_error, self.addr
        );
        Err(last_error.unwrap())
    }

    pub async fn send_buffer<T>(
        &self,
        data: T,
        stats: &ClientStats,
        connection_stats: Arc<ConnectionCacheStats>,
    ) -> Result<(), ClientErrorKind>
    where
        T: AsRef<[u8]>,
    {
        self._send_buffer(data.as_ref(), stats, connection_stats)
            .await?;
        Ok(())
    }

    pub async fn send_batch<T>(
        &self,
        buffers: &[T],
        stats: &ClientStats,
        connection_stats: Arc<ConnectionCacheStats>,
    ) -> Result<(), ClientErrorKind>
    where
        T: AsRef<[u8]>,
    {
        // Start off by "testing" the connection by sending the first transaction
        // This will also connect to the server if not already connected
        // and reconnect and retry if the first send attempt failed
        // (for example due to a timed out connection), returning an error
        // or the connection that was used to successfully send the transaction.
        // We will use the returned connection to send the rest of the transactions in the batch
        // to avoid touching the mutex in self, and not bother reconnecting if we fail along the way
        // since testing even in the ideal GCE environment has found no cases
        // where reconnecting and retrying in the middle of a batch send
        // (i.e. we encounter a connection error in the middle of a batch send, which presumably cannot
        // be due to a timed out connection) has succeeded
        if buffers.is_empty() {
            return Ok(());
        }
        let connection = self
            ._send_buffer(buffers[0].as_ref(), stats, connection_stats)
            .await?;

        // Used to avoid dereferencing the Arc multiple times below
        // by just getting a reference to the NewConnection once
        let connection_ref: &NewConnection = &connection;

        let chunks = buffers[1..buffers.len()]
            .iter()
            .chunks(QUIC_MAX_CONCURRENT_STREAMS);

        let futures: Vec<_> = chunks
            .into_iter()
            .map(|buffs| {
                join_all(
                    buffs
                        .into_iter()
                        .map(|buf| Self::_send_buffer_using_conn(buf.as_ref(), connection_ref)),
                )
            })
            .collect();

        for f in futures {
            f.await.into_iter().try_for_each(|res| res)?;
        }
        Ok(())
    }
}
