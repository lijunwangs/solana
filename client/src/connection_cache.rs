use {
    crate::{
        nonblocking::quic_client::QuicLazyInitializedEndpoint,
        quic_client::QuicTpuConnection,
        tpu_connection::{ClientStats, Connection},
        udp_client::UdpTpuConnection,
    },
    indexmap::map::IndexMap,
    lazy_static::lazy_static,
    log::*,
    rand::{thread_rng, Rng},
    solana_measure::measure::Measure,
    solana_sdk::{quic::QUIC_PORT_OFFSET, timing::AtomicInterval},
    std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, RwLock,
        },
    },
};

// Should be non-zero
static MAX_CONNECTIONS: usize = 1024;


#[derive(Default)]
pub struct ConnectionCacheStats {
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_evictions: AtomicU64,
    eviction_time_ms: AtomicU64,
    sent_packets: AtomicU64,
    total_batches: AtomicU64,
    batch_success: AtomicU64,
    batch_failure: AtomicU64,
    get_connection_ms: AtomicU64,
    get_connection_lock_ms: AtomicU64,
    get_connection_hit_ms: AtomicU64,
    get_connection_miss_ms: AtomicU64,

    // Need to track these separately per-connection
    // because we need to track the base stat value from quinn
    pub total_client_stats: ClientStats,
}

const CONNECTION_STAT_SUBMISSION_INTERVAL: u64 = 2000;

impl ConnectionCacheStats {
    pub fn add_client_stats(
        &self,
        client_stats: &ClientStats,
        num_packets: usize,
        is_success: bool,
    ) {
        self.total_client_stats.total_connections.fetch_add(
            client_stats.total_connections.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_client_stats.connection_reuse.fetch_add(
            client_stats.connection_reuse.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_client_stats.connection_errors.fetch_add(
            client_stats.connection_errors.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_client_stats.zero_rtt_accepts.fetch_add(
            client_stats.zero_rtt_accepts.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_client_stats.zero_rtt_rejects.fetch_add(
            client_stats.zero_rtt_rejects.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_client_stats.make_connection_ms.fetch_add(
            client_stats.make_connection_ms.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.sent_packets
            .fetch_add(num_packets as u64, Ordering::Relaxed);
        self.total_batches.fetch_add(1, Ordering::Relaxed);
        if is_success {
            self.batch_success.fetch_add(1, Ordering::Relaxed);
        } else {
            self.batch_failure.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn report(&self) {
        datapoint_info!(
            "quic-client-connection-stats",
            (
                "cache_hits",
                self.cache_hits.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "cache_misses",
                self.cache_misses.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "cache_evictions",
                self.cache_evictions.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "eviction_time_ms",
                self.eviction_time_ms.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "get_connection_ms",
                self.get_connection_ms.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "get_connection_lock_ms",
                self.get_connection_lock_ms.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "get_connection_hit_ms",
                self.get_connection_hit_ms.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "get_connection_miss_ms",
                self.get_connection_miss_ms.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "make_connection_ms",
                self.total_client_stats
                    .make_connection_ms
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "total_connections",
                self.total_client_stats
                    .total_connections
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "connection_reuse",
                self.total_client_stats
                    .connection_reuse
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "connection_errors",
                self.total_client_stats
                    .connection_errors
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "zero_rtt_accepts",
                self.total_client_stats
                    .zero_rtt_accepts
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "zero_rtt_rejects",
                self.total_client_stats
                    .zero_rtt_rejects
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "congestion_events",
                self.total_client_stats.congestion_events.load_and_reset(),
                i64
            ),
            (
                "tx_streams_blocked_uni",
                self.total_client_stats
                    .tx_streams_blocked_uni
                    .load_and_reset(),
                i64
            ),
            (
                "tx_data_blocked",
                self.total_client_stats.tx_data_blocked.load_and_reset(),
                i64
            ),
            (
                "tx_acks",
                self.total_client_stats.tx_acks.load_and_reset(),
                i64
            ),
            (
                "num_packets",
                self.sent_packets.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "total_batches",
                self.total_batches.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "batch_failure",
                self.batch_failure.swap(0, Ordering::Relaxed),
                i64
            ),
        );
    }
}

pub const DEFAULT_CONNECTION_POOL_SIZE: usize = 4;

/// Models the pool of connections
struct ConnectionPool {
    /// The connections in the pool
    connections: Vec<Arc<Connection>>,

    /// Connections in this pool share the same endpoint
    endpoint: Arc<QuicLazyInitializedEndpoint>,
}

impl ConnectionPool {
    /// Get a connection from the pool. It must have at least one connection in the pool.
    /// This randomly picks a connection in the pool.
    fn borrow_connection(&self) -> Arc<Connection> {
        let mut rng = thread_rng();
        let n = rng.gen_range(0, self.connections.len());
        self.connections[n].clone()
    }

    /// Check if we need to create a new connection. If the count of the connections
    /// is smaller than the pool size.
    fn need_new_connection(&self, required_pool_size: usize) -> bool {
        self.connections.len() < required_pool_size
    }
}

struct ConnectionMap {
    /// From SocketAddr maps to a pool of connection for the address
    map: IndexMap<SocketAddr, ConnectionPool>,
    stats: Arc<ConnectionCacheStats>,
    last_stats: AtomicInterval,
    use_quic: bool,
    connection_pool_size: usize,
}

impl ConnectionMap {
    pub fn new() -> Self {
        Self {
            map: IndexMap::with_capacity(MAX_CONNECTIONS),
            stats: Arc::new(ConnectionCacheStats::default()),
            last_stats: AtomicInterval::default(),
            use_quic: false,
            connection_pool_size: DEFAULT_CONNECTION_POOL_SIZE,
        }
    }

    pub fn set_use_quic(&mut self, use_quic: bool) {
        self.use_quic = use_quic;
    }

    /// Set the connection pool size per remote address. The minimum is 1.
    fn set_connection_pool_size(&mut self, connection_pool_size: usize) {
        info!(
            "Configure the connection pool size to {}",
            connection_pool_size
        );
        self.connection_pool_size = 1.max(connection_pool_size);
    }
}

lazy_static! {
    static ref CONNECTION_MAP: RwLock<ConnectionMap> = RwLock::new(ConnectionMap::new());
}

/// Configure whether to use quic for tpu
pub fn set_use_quic(use_quic: bool) {
    let mut map = (*CONNECTION_MAP).write().unwrap();
    map.set_use_quic(use_quic);
}

/// Configure the connection pool size per address
pub fn set_connection_pool_size(connection_pool_size: usize) {
    let mut map = (*CONNECTION_MAP).write().unwrap();
    map.set_connection_pool_size(connection_pool_size);
}

struct GetConnectionResult {
    connection: Arc<Connection>,
    cache_hit: bool,
    report_stats: bool,
    map_timing_ms: u64,
    lock_timing_ms: u64,
    connection_cache_stats: Arc<ConnectionCacheStats>,
    num_evictions: u64,
    eviction_timing_ms: u64,
}

struct CreateConnectionResult {
    connection: Arc<Connection>,
    cache_hit: bool,
    connection_cache_stats: Arc<ConnectionCacheStats>,
    num_evictions: u64,
    eviction_timing_ms: u64,
}

/// Create a lazy connection object under the exclusive lock of the cache map if there is not
/// enough unsed connections in the connection pool for the specified address.
/// Returns CreateConnectionResult.
fn create_connection(lock_timing_ms: &mut u64, addr: &SocketAddr) -> CreateConnectionResult {
    let mut get_connection_map_lock_measure = Measure::start("get_connection_map_lock_measure");
    let mut map = (*CONNECTION_MAP).write().unwrap();
    get_connection_map_lock_measure.stop();
    *lock_timing_ms = lock_timing_ms.saturating_add(get_connection_map_lock_measure.as_ms());
    // Read again, as it is possible that between read lock dropped and the write lock acquired
    // another thread could have setup the connection.

    let (to_create_connection, endpoint) = map.map.get(addr).map_or(
        (true, Arc::new(QuicLazyInitializedEndpoint::new())),
        |pool| {
            (
                pool.need_new_connection(map.connection_pool_size),
                pool.endpoint.clone(),
            )
        },
    );

    let (cache_hit, connection_cache_stats, num_evictions, eviction_timing_ms) =
        if to_create_connection {
            let connection: Connection = if map.use_quic {
                QuicTpuConnection::new(endpoint, *addr, map.stats.clone()).into()
            } else {
                UdpTpuConnection::new(*addr, map.stats.clone()).into()
            };

            let connection = Arc::new(connection);

            // evict a connection if the cache is reaching upper bounds
            let mut num_evictions = 0;
            let mut get_connection_cache_eviction_measure =
                Measure::start("get_connection_cache_eviction_measure");
            while map.map.len() >= MAX_CONNECTIONS {
                let mut rng = thread_rng();
                let n = rng.gen_range(0, MAX_CONNECTIONS);
                map.map.swap_remove_index(n);
                num_evictions += 1;
            }
            get_connection_cache_eviction_measure.stop();

            match map.map.entry(*addr) {
                Entry::Occupied(mut entry) => {
                    let pool = entry.get_mut();
                    pool.connections.push(connection);
                }
                Entry::Vacant(entry) => {
                    entry.insert(ConnectionPool {
                        connections: vec![connection],
                        endpoint: Arc::new(QuicLazyInitializedEndpoint::new()),
                    });
                }
            }
            (
                false,
                map.stats.clone(),
                num_evictions,
                get_connection_cache_eviction_measure.as_ms(),
            )
        } else {
            (true, map.stats.clone(), 0, 0)
        };

    let pool = map.map.get(addr).unwrap();
    let connection = pool.borrow_connection();

    CreateConnectionResult {
        connection,
        cache_hit,
        connection_cache_stats,
        num_evictions,
        eviction_timing_ms,
    }
}

fn get_or_add_connection(addr: &SocketAddr) -> GetConnectionResult {
    let mut get_connection_map_lock_measure = Measure::start("get_connection_map_lock_measure");
    let map = (*CONNECTION_MAP).read().unwrap();
    get_connection_map_lock_measure.stop();

    let port_offset = if map.use_quic { QUIC_PORT_OFFSET } else { 0 };

    let addr = SocketAddr::new(addr.ip(), addr.port() + port_offset);

    let mut lock_timing_ms = get_connection_map_lock_measure.as_ms();

    let report_stats = map
        .last_stats
        .should_update(CONNECTION_STAT_SUBMISSION_INTERVAL);

    let mut get_connection_map_measure = Measure::start("get_connection_hit_measure");
    let CreateConnectionResult {
        connection,
        cache_hit,
        connection_cache_stats,
        num_evictions,
        eviction_timing_ms,
    } = match map.map.get(&addr) {
        Some(pool) => {
            if pool.need_new_connection(map.connection_pool_size) {
                // create more connection and put it in the pool
                drop(map);
                create_connection(&mut lock_timing_ms, &addr)
            } else {
                let connection = pool.borrow_connection();
                CreateConnectionResult {
                    connection,
                    cache_hit: true,
                    connection_cache_stats: map.stats.clone(),
                    num_evictions: 0,
                    eviction_timing_ms: 0,
                }
            }
        }
        None => {
            // Upgrade to write access by dropping read lock and acquire write lock
            drop(map);
            create_connection(&mut lock_timing_ms, &addr)
        }
    };
    get_connection_map_measure.stop();

    GetConnectionResult {
        connection,
        cache_hit,
        report_stats,
        map_timing_ms: get_connection_map_measure.as_ms(),
        lock_timing_ms,
        connection_cache_stats,
        num_evictions,
        eviction_timing_ms,
    }
}

// TODO: see https://github.com/solana-labs/solana/issues/23661
// remove lazy_static and optimize and refactor this
pub fn get_connection(addr: &SocketAddr) -> Arc<Connection> {
    let mut get_connection_measure = Measure::start("get_connection_measure");
    let GetConnectionResult {
        connection,
        cache_hit,
        report_stats,
        map_timing_ms,
        lock_timing_ms,
        connection_cache_stats,
        num_evictions,
        eviction_timing_ms,
    } = get_or_add_connection(addr);

    if report_stats {
        connection_cache_stats.report();
    }

    if cache_hit {
        connection_cache_stats
            .cache_hits
            .fetch_add(1, Ordering::Relaxed);
        connection_cache_stats
            .get_connection_hit_ms
            .fetch_add(map_timing_ms, Ordering::Relaxed);
    } else {
        connection_cache_stats
            .cache_misses
            .fetch_add(1, Ordering::Relaxed);
        connection_cache_stats
            .get_connection_miss_ms
            .fetch_add(map_timing_ms, Ordering::Relaxed);
        connection_cache_stats
            .cache_evictions
            .fetch_add(num_evictions, Ordering::Relaxed);
        connection_cache_stats
            .eviction_time_ms
            .fetch_add(eviction_timing_ms, Ordering::Relaxed);
    }

    get_connection_measure.stop();
    connection_cache_stats
        .get_connection_lock_ms
        .fetch_add(lock_timing_ms, Ordering::Relaxed);
    connection_cache_stats
        .get_connection_ms
        .fetch_add(get_connection_measure.as_ms(), Ordering::Relaxed);

    connection
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            connection_cache::{get_connection, CONNECTION_MAP, MAX_CONNECTIONS},
            tpu_connection::TpuConnection,
        },
        rand::{Rng, SeedableRng},
        rand_chacha::ChaChaRng,
        std::net::SocketAddr,
    };

    fn get_addr(rng: &mut ChaChaRng) -> SocketAddr {
        let a = rng.gen_range(1, 255);
        let b = rng.gen_range(1, 255);
        let c = rng.gen_range(1, 255);
        let d = rng.gen_range(1, 255);

        let addr_str = format!("{}.{}.{}.{}:80", a, b, c, d);

        addr_str.parse().expect("Invalid address")
    }

    #[test]
    fn test_connection_cache() {
        solana_logger::setup();
        // Allow the test to run deterministically
        // with the same pseudorandom sequence between runs
        // and on different platforms - the cryptographic security
        // property isn't important here but ChaChaRng provides a way
        // to get the same pseudorandom sequence on different platforms
        let mut rng = ChaChaRng::seed_from_u64(42);

        // Generate a bunch of random addresses and create TPUConnections to them
        // Since TPUConnection::new is infallible, it should't matter whether or not
        // we can actually connect to those addresses - TPUConnection implementations should either
        // be lazy and not connect until first use or handle connection errors somehow
        // (without crashing, as would be required in a real practical validator)
        let addrs = (0..MAX_CONNECTIONS)
            .into_iter()
            .map(|_| {
                let addr = get_addr(&mut rng);
                get_connection(&addr);
                addr
            })
            .collect::<Vec<_>>();
        {
            let map = (*CONNECTION_MAP).read().unwrap();
            assert!(map.map.len() == MAX_CONNECTIONS);
            addrs.iter().for_each(|a| {
                let conn = &map.map.get(a).expect("Address not found").connections[0];
                assert!(a.ip() == conn.tpu_addr().ip());
            });
        }

        let addr = get_addr(&mut rng);
        get_connection(&addr);

        let map = (*CONNECTION_MAP).read().unwrap();
        assert!(map.map.len() == MAX_CONNECTIONS);
        let _conn = map.map.get(&addr).expect("Address not found");
    }
}
