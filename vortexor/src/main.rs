use {
    clap::{value_t, value_t_or_exit, values_t},
    crossbeam_channel::unbounded,
    solana_clap_utils::input_parsers::keypair_of,
    solana_core::banking_trace::BankingTracer,
    solana_net_utils::{bind_in_range_with_config, SocketConfig},
    solana_sdk::net::DEFAULT_TPU_COALESCE,
    solana_streamer::streamer::StakedNodes,
    solana_vortexor::{
        cli::{app, DefaultArgs},
        sender::{
            PacketBatchSender, DEFAULT_BATCH_SIZE, DEFAULT_RECV_TIMEOUT,
            DEFAULT_SENDER_THREADS_COUNT,
        },
        vortexor::Vortexor,
    },
    std::{
        collections::HashSet,
        sync::{atomic::AtomicBool, Arc, RwLock},
        time::Duration,
    },
};

pub fn main() {
    let default_args = DefaultArgs::default();
    let solana_version = solana_version::version!();
    let cli_app = app(solana_version, &default_args);
    let matches = cli_app.get_matches();

    let identity_keypair = keypair_of(&matches, "identity").unwrap_or_else(|| {
        clap::Error::with_description(
            "The --identity <KEYPAIR> argument is required",
            clap::ErrorKind::ArgumentNotFound,
        )
        .exit();
    });

    let bind_address = solana_net_utils::parse_host(matches.value_of("bind_address").unwrap())
        .expect("invalid bind_address");
    let max_connections_per_peer = value_t_or_exit!(matches, "max_connections_per_peer", u64);
    let max_tpu_staked_connections = value_t_or_exit!(matches, "max_tpu_staked_connections", u64);
    let max_tpu_unstaked_connections =
        value_t_or_exit!(matches, "max_tpu_unstaked_connections", u64);

    let max_connections_per_ipaddr_per_min =
        value_t_or_exit!(matches, "max_connections_per_ipaddr_per_minute", u64);
    let num_quic_endpoints = value_t_or_exit!(matches, "num_quic_endpoints", u64);
    let tpu_coalesce = value_t!(matches, "tpu_coalesce_ms", u64)
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_TPU_COALESCE);

    let dynamic_port_range =
        solana_net_utils::parse_port_range(matches.value_of("dynamic_port_range").unwrap())
            .expect("invalid dynamic_port_range");

    let max_streams_per_ms = value_t_or_exit!(matches, "max_streams_per_ms", u64);
    let exit = Arc::new(AtomicBool::new(false));
    // To be linked with the Tpu sigverify and forwarder service
    let (tpu_sender, tpu_receiver) = unbounded();

    let tpu_sockets =
        Vortexor::create_tpu_sockets(bind_address, dynamic_port_range, num_quic_endpoints);

    let (banking_tracer, _) = BankingTracer::new(None).unwrap();

    let config = SocketConfig { reuseport: false };

    let sender_socket =
        bind_in_range_with_config(bind_address, dynamic_port_range, config).unwrap();

    // The _non_vote_receiver will forward the verified transactions to its configured validator
    let (non_vote_sender, non_vote_receiver) = banking_tracer.create_channel_non_vote();

    let destinations = values_t!(matches, "destination", String)
        .unwrap_or_default()
        .into_iter()
        .map(|destination| {
            solana_net_utils::parse_host_port(&destination).unwrap_or_else(|e| {
                panic!("Failed to parse destination address: {e}");
            })
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let destinations = Arc::new(RwLock::new(destinations));
    let packet_sender = PacketBatchSender::new(
        sender_socket.1,
        non_vote_receiver,
        DEFAULT_SENDER_THREADS_COUNT,
        DEFAULT_BATCH_SIZE,
        DEFAULT_RECV_TIMEOUT,
        destinations,
    );

    let sigverify_stage = Vortexor::create_sigverify_stage(tpu_receiver, non_vote_sender);

    // To be linked with StakedNodes service.
    let staked_nodes = Arc::new(RwLock::new(StakedNodes::default()));

    let vortexor = Vortexor::create_vortexor(
        tpu_sockets,
        staked_nodes,
        tpu_sender.clone(),
        tpu_sender,
        max_connections_per_peer,
        max_tpu_staked_connections,
        max_tpu_unstaked_connections,
        max_streams_per_ms,
        max_connections_per_ipaddr_per_min,
        tpu_coalesce,
        &identity_keypair,
        exit,
    );
    vortexor.join().unwrap();
    sigverify_stage.join().unwrap();
    packet_sender.join().unwrap();
}
