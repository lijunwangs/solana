use {
    crate::{
        nonblocking::{
            quic::{
                ClientConnectionTracker, ConnectionHandlerError, ConnectionPeerType,
                ConnectionQosParams, ConnectionStakeInfo, ConnectionTable, ConnectionTableKey, Qos,
            },
            stream_throttle::{ConnectionStreamCounter, STREAM_THROTTLING_INTERVAL},
        },
        quic::StreamerStats,
        streamer::StakedNodes,
    },
    quinn::Connection,
    solana_time_utils as timing,
    std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    tokio::sync::{Mutex, MutexGuard},
};

pub struct SimpleQos {
    max_streams_per_second: u64,
    max_staked_connections: usize,
    max_connections_per_peer: usize,
    stats: Arc<StreamerStats>,
    staked_connection_table: Arc<Mutex<ConnectionTable>>,
}

impl SimpleQos {
    pub fn new(
        max_streams_per_second: u64,
        max_connections_per_peer: usize,
        max_staked_connections: usize,
        stats: Arc<StreamerStats>,
    ) -> Self {
        Self {
            max_streams_per_second,
            max_connections_per_peer,
            max_staked_connections,
            stats,
            staked_connection_table: Arc::new(Mutex::new(ConnectionTable::new())),
        }
    }

    fn handle_and_cache_new_connection(
        &self,
        client_connection_tracker: ClientConnectionTracker,
        connection: &Connection,
        mut connection_table_l: MutexGuard<ConnectionTable>,
        params: &SimpleQosParams,
    ) -> Result<
        (
            Arc<AtomicU64>,
            tokio_util::sync::CancellationToken,
            Arc<ConnectionStreamCounter>,
        ),
        ConnectionHandlerError,
    > {
        let remote_addr = connection.remote_address();

        debug!(
            "Peer type {:?}, from peer {}",
            params.peer_type(),
            remote_addr,
        );

        if let Some((last_update, cancel_connection, stream_counter)) = connection_table_l
            .try_add_connection(
                ConnectionTableKey::new(remote_addr.ip(), params.remote_pubkey),
                remote_addr.port(),
                client_connection_tracker,
                Some(connection.clone()),
                params.peer_type(),
                timing::timestamp(),
                params.max_connections_per_peer(),
            )
        {
            drop(connection_table_l);

            Ok((last_update, cancel_connection, stream_counter))
        } else {
            self.stats
                .connection_add_failed
                .fetch_add(1, Ordering::Relaxed);
            Err(ConnectionHandlerError::ConnectionAddError)
        }
    }
}

struct SimpleQosParams {
    peer_type: ConnectionPeerType,
    max_connections_per_peer: usize,
    stats: Arc<StreamerStats>,
    remote_pubkey: Option<solana_pubkey::Pubkey>,
}

impl ConnectionQosParams for SimpleQosParams {
    fn stake_info(&self) -> Option<ConnectionStakeInfo> {
        None // Simple QoS does not consider stake
    }

    fn peer_type(&self) -> ConnectionPeerType {
        self.peer_type.clone()
    }

    fn max_connections_per_peer(&self) -> usize {
        self.max_connections_per_peer
    }

    fn stats(&self) -> Arc<StreamerStats> {
        self.stats.clone()
    }
}

impl Qos<SimpleQosParams> for SimpleQos {
    fn derive_qos_params(
        &self,
        stake_info: Option<ConnectionStakeInfo>,
        _connection: &Connection,
        _staked_nodes: &RwLock<StakedNodes>,
    ) -> SimpleQosParams {
        // Based on QosMode::SimpleStreamsPerSecond branch in original code
        // All connections get the same limit regardless of stake
        let (peer_type, remote_pubkey) =
            stake_info
                .as_ref()
                .map_or((ConnectionPeerType::Unstaked, None), |stake_info| {
                    (
                        ConnectionPeerType::Staked(stake_info.stake),
                        Some(stake_info.pubkey),
                    )
                });

        SimpleQosParams {
            peer_type,
            max_connections_per_peer: self.max_connections_per_peer,
            stats: self.stats.clone(),
            remote_pubkey,
        }
    }

    fn try_add_connection(
        &self,
        client_connection_tracker: ClientConnectionTracker,
        connection: &quinn::Connection,
        params: SimpleQosParams,
    ) -> impl std::future::Future<
        Output = Option<(
            Arc<AtomicU64>,
            tokio_util::sync::CancellationToken,
            Arc<ConnectionStreamCounter>,
        )>,
    > + Send {
        async move {
            const PRUNE_RANDOM_SAMPLE_SIZE: usize = 2;
            match params.peer_type() {
                ConnectionPeerType::Staked(stake) => {
                    let mut connection_table_l = self.staked_connection_table.lock().await;

                    if connection_table_l.total_size >= self.max_staked_connections {
                        let num_pruned =
                            connection_table_l.prune_random(PRUNE_RANDOM_SAMPLE_SIZE, stake);
                        self.stats
                            .num_evictions
                            .fetch_add(num_pruned, Ordering::Relaxed);
                    }

                    if connection_table_l.total_size < self.max_staked_connections {
                        if let Ok((last_update, cancel_connection, stream_counter)) = self
                            .handle_and_cache_new_connection(
                                client_connection_tracker,
                                connection,
                                connection_table_l,
                                &params,
                            )
                        {
                            self.stats
                                .connection_added_from_staked_peer
                                .fetch_add(1, Ordering::Relaxed);
                            return Some((last_update, cancel_connection, stream_counter));
                        }
                    }
                    None
                }
                ConnectionPeerType::Unstaked => None,
            }
        }
    }

    fn on_stream_accepted(&self, _params: &SimpleQosParams) {}

    fn on_stream_error(&self, _params: &SimpleQosParams) {}

    fn on_stream_closed(&self, _params: &SimpleQosParams) {}

    fn report_qos_stats(&self) {
        todo!()
    }

    fn max_streams_per_throttling_interval(&self, _params: &SimpleQosParams) -> u64 {
        let interval_ms = STREAM_THROTTLING_INTERVAL.as_millis() as u64;
        (self.max_streams_per_second * interval_ms / 1000).max(1)
    }
}
