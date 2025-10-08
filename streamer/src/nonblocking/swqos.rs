use {
    crate::{
        nonblocking::{
            quic::{
                compute_max_allowed_uni_streams, compute_recieve_window, get_connection_stake,
                ClientConnectionTracker, ConnectionHandlerError, ConnectionPeerType,
                ConnectionQosParams, ConnectionStakeInfo, ConnectionTable, ConnectionTableKey, Qos,
                CONNECTION_CLOSE_CODE_DISALLOWED, CONNECTION_CLOSE_CODE_EXCEED_MAX_STREAM_COUNT,
                CONNECTION_CLOSE_REASON_DISALLOWED,
                CONNECTION_CLOSE_REASON_EXCEED_MAX_STREAM_COUNT,
            },
            stream_throttle::{
                ConnectionStreamCounter, StakedStreamLoadEMA, STREAM_THROTTLING_INTERVAL_MS,
            },
        },
        quic::StreamerStats,
        streamer::StakedNodes,
    },
    percentage::Percentage,
    quinn::{Connection, VarInt},
    solana_time_utils as timing,
    std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    tokio::sync::{Mutex, MutexGuard},
    tokio_util::sync::CancellationToken,
};

pub struct SwQos {
    max_staked_connections: usize,
    max_unstaked_connections: usize,
    max_connections_per_peer: usize,
    staked_stream_load_ema: Arc<StakedStreamLoadEMA>,
    stats: Arc<StreamerStats>,
    unstaked_connection_table: Arc<Mutex<ConnectionTable>>,
    staked_connection_table: Arc<Mutex<ConnectionTable>>,
}

// QoS Params for Stake weighted QoS
struct SwQosParams {
    stake_info: Option<ConnectionStakeInfo>,
    peer_type: ConnectionPeerType,
    max_connections_per_peer: usize,
    stats: Arc<StreamerStats>,
    max_stake: u64,
    min_stake: u64,
    remote_pubkey: Option<solana_pubkey::Pubkey>,
    total_stake: u64,
}

impl ConnectionQosParams for SwQosParams {
    fn stake_info(&self) -> Option<ConnectionStakeInfo> {
        self.stake_info.clone()
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

impl SwQos {
    pub fn new(
        max_streams_per_ms: u64,
        max_staked_connections: usize,
        max_unstaked_connections: usize,
        max_connections_per_peer: usize,
        stats: Arc<StreamerStats>,
    ) -> Self {
        Self {
            max_staked_connections,
            max_unstaked_connections,
            max_connections_per_peer,
            staked_stream_load_ema: Arc::new(StakedStreamLoadEMA::new(
                stats.clone(),
                max_unstaked_connections,
                max_streams_per_ms,
            )),
            stats,
            unstaked_connection_table: Arc::new(Mutex::new(ConnectionTable::new())),
            staked_connection_table: Arc::new(Mutex::new(ConnectionTable::new())),
        }
    }
}

impl SwQos {
    fn handle_and_cache_new_connection(
        &self,
        client_connection_tracker: ClientConnectionTracker,
        connection: &Connection,
        mut connection_table_l: MutexGuard<ConnectionTable>,
        params: &SwQosParams,
    ) -> Result<
        (
            Arc<AtomicU64>,
            CancellationToken,
            Arc<ConnectionStreamCounter>,
        ),
        ConnectionHandlerError,
    > {
        if let Ok(max_uni_streams) = VarInt::from_u64(compute_max_allowed_uni_streams(
            params.peer_type(),
            params.total_stake,
        ) as u64)
        {
            let remote_addr = connection.remote_address();
            let receive_window =
                compute_recieve_window(params.max_stake, params.min_stake, params.peer_type());

            debug!(
                "Peer type {:?}, total stake {}, max streams {} receive_window {:?} from peer {}",
                params.peer_type(),
                params.total_stake,
                max_uni_streams.into_inner(),
                receive_window,
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

                if let Ok(receive_window) = receive_window {
                    connection.set_receive_window(receive_window);
                }
                connection.set_max_concurrent_uni_streams(max_uni_streams);

                Ok((last_update, cancel_connection, stream_counter))
            } else {
                self.stats
                    .connection_add_failed
                    .fetch_add(1, Ordering::Relaxed);
                Err(ConnectionHandlerError::ConnectionAddError)
            }
        } else {
            connection.close(
                CONNECTION_CLOSE_CODE_EXCEED_MAX_STREAM_COUNT.into(),
                CONNECTION_CLOSE_REASON_EXCEED_MAX_STREAM_COUNT,
            );
            self.stats
                .connection_add_failed_invalid_stream_count
                .fetch_add(1, Ordering::Relaxed);
            Err(ConnectionHandlerError::MaxStreamError)
        }
    }

    fn prune_unstaked_connection_table(
        &self,
        unstaked_connection_table: &mut ConnectionTable,
        max_unstaked_connections: usize,
        stats: Arc<StreamerStats>,
    ) {
        if unstaked_connection_table.total_size >= max_unstaked_connections {
            const PRUNE_TABLE_TO_PERCENTAGE: u8 = 90;
            let max_percentage_full = Percentage::from(PRUNE_TABLE_TO_PERCENTAGE);

            let max_connections = max_percentage_full.apply_to(max_unstaked_connections);
            let num_pruned = unstaked_connection_table.prune_oldest(max_connections);
            stats.num_evictions.fetch_add(num_pruned, Ordering::Relaxed);
        }
    }

    async fn prune_unstaked_connections_and_add_new_connection(
        &self,
        client_connection_tracker: ClientConnectionTracker,
        connection: &Connection,
        connection_table: Arc<Mutex<ConnectionTable>>,
        max_connections: usize,
        params: &SwQosParams,
    ) -> Result<
        (
            Arc<AtomicU64>,
            CancellationToken,
            Arc<ConnectionStreamCounter>,
        ),
        ConnectionHandlerError,
    > {
        let stats = params.stats.clone();
        if max_connections > 0 {
            let mut connection_table = connection_table.lock().await;
            self.prune_unstaked_connection_table(&mut connection_table, max_connections, stats);
            self.handle_and_cache_new_connection(
                client_connection_tracker,
                connection,
                connection_table,
                params,
            )
        } else {
            connection.close(
                CONNECTION_CLOSE_CODE_DISALLOWED.into(),
                CONNECTION_CLOSE_REASON_DISALLOWED,
            );
            Err(ConnectionHandlerError::ConnectionAddError)
        }
    }
}

impl Qos<SwQosParams> for SwQos {
    fn derive_qos_params(
        &self,
        stake_info: Option<ConnectionStakeInfo>,
        connection: &Connection,
        staked_nodes: &RwLock<StakedNodes>,
    ) -> SwQosParams {
        get_connection_stake(connection, staked_nodes).map_or(
            SwQosParams {
                stake_info: None,
                peer_type: ConnectionPeerType::Unstaked,
                max_connections_per_peer: self.max_connections_per_peer,
                stats: self.stats.clone(),
                max_stake: 0,
                min_stake: 0,
                total_stake: 0,
                remote_pubkey: None,
            },
            |(pubkey, stake, total_stake, max_stake, min_stake)| {
                // The heuristic is that the stake should be large engouh to have 1 stream pass throuh within one throttle
                // interval during which we allow max (MAX_STREAMS_PER_MS * STREAM_THROTTLING_INTERVAL_MS) streams.

                let peer_type = {
                    let max_streams_per_ms = self.staked_stream_load_ema.max_streams_per_ms();
                    let min_stake_ratio =
                        1_f64 / (max_streams_per_ms * STREAM_THROTTLING_INTERVAL_MS) as f64;
                    let stake_ratio = stake as f64 / total_stake as f64;
                    if stake_ratio < min_stake_ratio {
                        // If it is a staked connection with ultra low stake ratio, treat it as unstaked.
                        ConnectionPeerType::Unstaked
                    } else {
                        ConnectionPeerType::Staked(stake)
                    }
                };

                SwQosParams {
                    stake_info,
                    peer_type,
                    max_connections_per_peer: self.max_connections_per_peer,
                    stats: self.stats.clone(),
                    max_stake,
                    min_stake,
                    total_stake,
                    remote_pubkey: Some(pubkey),
                }
            },
        )
    }

    fn try_add_connection(
        &self,
        client_connection_tracker: ClientConnectionTracker,
        connection: &quinn::Connection,
        params: SwQosParams,
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
                    } else {
                        // If we couldn't prune a connection in the staked connection table, let's
                        // put this connection in the unstaked connection table. If needed, prune a
                        // connection from the unstaked connection table.
                        if let Ok((last_update, cancel_connection, stream_counter)) = self
                            .prune_unstaked_connections_and_add_new_connection(
                                client_connection_tracker,
                                connection,
                                self.unstaked_connection_table.clone(),
                                self.max_unstaked_connections,
                                &params,
                            )
                            .await
                        {
                            self.stats
                                .connection_added_from_staked_peer
                                .fetch_add(1, Ordering::Relaxed);
                            return Some((last_update, cancel_connection, stream_counter));
                        } else {
                            self.stats
                                .connection_add_failed_on_pruning
                                .fetch_add(1, Ordering::Relaxed);
                            self.stats
                                .connection_add_failed_staked_node
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                ConnectionPeerType::Unstaked => {
                    if let Ok((last_update, cancel_connection, stream_counter)) = self
                        .prune_unstaked_connections_and_add_new_connection(
                            client_connection_tracker,
                            connection,
                            self.unstaked_connection_table.clone(),
                            self.max_unstaked_connections,
                            &params,
                        )
                        .await
                    {
                        self.stats
                            .connection_added_from_unstaked_peer
                            .fetch_add(1, Ordering::Relaxed);
                        return Some((last_update, cancel_connection, stream_counter));
                    } else {
                        self.stats
                            .connection_add_failed_unstaked_node
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            None
        }
    }

    fn on_stream_accepted(&self, params: &SwQosParams) {
        self.staked_stream_load_ema.increment_load(params.peer_type);
    }

    fn on_stream_error(&self, _params: &SwQosParams) {
        self.staked_stream_load_ema.update_ema_if_needed();
    }

    fn on_stream_closed(&self, _params: &SwQosParams) {
        self.staked_stream_load_ema.update_ema_if_needed();
    }

    fn report_qos_stats(&self) {
        todo!()
    }

    fn max_streams_per_throttling_interval(&self, params: &SwQosParams) -> u64 {
        self.staked_stream_load_ema
            .available_load_capacity_in_throttling_duration(params.peer_type, params.total_stake)
    }
}
