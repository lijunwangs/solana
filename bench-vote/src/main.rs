#![allow(clippy::arithmetic_side_effects)]

use {
    clap::{crate_description, crate_name, Arg, Command},
    crossbeam_channel::unbounded,
    solana_client::connection_cache::ConnectionCache,
    solana_connection_cache::client_connection::ClientConnection,
    solana_net_utils::bind_to_unspecified,
    solana_sdk::{
        hash::Hash, message::Message, signature::Keypair, signer::Signer, transaction::Transaction,
    },
    solana_streamer::{
        packet::PacketBatchRecycler,
        streamer::{receiver, PacketBatchReceiver, StreamerReceiveStats},
    },
    solana_vote_program::{vote_instruction, vote_state::Vote},
    std::{
        cmp::max,
        net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        thread::{self, spawn, JoinHandle, Result},
        time::{Duration, Instant, SystemTime},
    },
};

fn sink(
    exit: Arc<AtomicBool>,
    rvs: Arc<AtomicUsize>,
    r: PacketBatchReceiver,
    verbose: bool,
) -> JoinHandle<()> {
    spawn(move || {
        let mut last_report = Instant::now();
        let interval: Duration = Duration::from_secs(5);
        loop {
            if exit.load(Ordering::Relaxed) {
                return;
            }
            let timer = Duration::new(1, 0);
            if let Ok(packet_batch) = r.recv_timeout(timer) {
                rvs.fetch_add(packet_batch.len(), Ordering::Relaxed);
            }

            let count = rvs.load(Ordering::Relaxed);

            if verbose &&
            /*count % 1000 == 0 && count != 0 &&*/
            last_report.elapsed() > interval
            {
                println!("Received txns count: {count}");
                last_report = Instant::now();
            }
        }
    })
}

const TRANSACTIONS_PER_THREAD: u64 = 1000000; // Number of transactions per thread

fn main() -> Result<()> {
    let mut num_sockets = 1usize;

    let matches = Command::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .arg(
            Arg::new("num-recv-sockets")
                .long("num-recv-sockets")
                .value_name("NUM")
                .takes_value(true)
                .help("Use NUM receive sockets"),
        )
        .arg(
            Arg::new("num-producers")
                .long("num-producers")
                .value_name("NUM")
                .takes_value(true)
                .help("Use this many producer threads."),
        )
        .arg(
            Arg::new("server-only")
                .long("server-only")
                .takes_value(false)
                .help("Use this many producer threads."),
        )
        .arg(
            Arg::new("client-only")
                .long("client-only")
                .takes_value(false)
                .requires("destination")
                .help("Use this many producer threads."),
        )
        .arg(
            Arg::with_name("server-address")
                .short('n')
                .long("server-address")
                .value_name("HOST:PORT")
                .takes_value(true)
                .validator(|arg| solana_net_utils::is_host_port(arg.to_string()))
                .help("The destination streamer address to which the client will send transactions to"),
        )
        .arg(
            Arg::new("use-connection-cache")
                .long("use-connection-cache")
                .takes_value(false)
                .help("Use this many producer threads."),
        )
        .arg(
            Arg::new("verbose")
                .long("verbose")
                .takes_value(false)
                .help("Show verbose messages."),
        )
        .get_matches();

    if let Some(n) = matches.value_of("num-recv-sockets") {
        num_sockets = max(num_sockets, n.to_string().parse().expect("integer"));
    }

    let num_producers: u64 = matches.value_of_t("num-producers").unwrap_or(4);

    let use_connection_cache = matches.is_present("use-connection-cache");

    let server_only = matches.is_present("server-only");
    let client_only = matches.is_present("client-only");
    let verbose = matches.is_present("verbose");

    let destination = matches.is_present("server-address").then(|| {
        let addr = matches
            .value_of("server-address")
            .expect("Destination must be set when --client-only is used");
        solana_net_utils::parse_host_port(addr).expect("Expecting a valid server address")
    });

    let port = destination.map_or(0, |addr| addr.port());
    let ip_addr = destination.map_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED), |addr| addr.ip());

    let (exit, read_threads, sin_threads, destination) = if !client_only {
        let exit = Arc::new(AtomicBool::new(false));

        let mut read_channels = Vec::new();
        let mut read_threads = Vec::new();
        let recycler = PacketBatchRecycler::default();
        let (port, read_sockets) = solana_net_utils::multi_bind_in_range(
            ip_addr,
            (port, port + num_sockets as u16),
            num_sockets,
        )
        .unwrap();
        let stats = Arc::new(StreamerReceiveStats::new("bench-streamer-test"));
        for read in read_sockets {
            read.set_read_timeout(Some(Duration::new(1, 0))).unwrap();

            let (s_reader, r_reader) = unbounded();
            read_channels.push(r_reader);
            read_threads.push(receiver(
                "solRcvrBenStrmr".to_string(),
                Arc::new(read),
                exit.clone(),
                s_reader,
                recycler.clone(),
                stats.clone(),
                Duration::from_millis(1), // coalesce
                true,
                None,
                false,
            ));
        }

        let rvs = Arc::new(AtomicUsize::new(0));
        let sink_threads: Vec<_> = read_channels
            .into_iter()
            .map(|r_reader| sink(exit.clone(), rvs.clone(), r_reader, verbose))
            .collect();

        let destination = SocketAddr::new(ip_addr, port);
        println!("Running server at {destination:?}");
        (
            Some(exit),
            Some(read_threads),
            Some(sink_threads),
            destination,
        )
    } else {
        (None, None, None, destination.unwrap())
    };

    let start = SystemTime::now();

    let producer_threads = if !server_only {
        let producer_threads: Vec<_> =
            producer(destination, num_producers, use_connection_cache, verbose);
        Some(producer_threads)
    } else {
        None
    };

    // sleep(Duration::new(5, 0));

    if let Some(producer_threads) = producer_threads {
        for t_producer in producer_threads {
            t_producer.join()?;
        }
    }

    if !server_only {
        if let Some(exit) = exit {
            exit.store(true, Ordering::Relaxed);
        }
    } else {
        println!("To stop the server, please press ^C");
    }

    if let Some(read_threads) = read_threads {
        for t_reader in read_threads {
            t_reader.join()?;
        }
    }

    if let Some(sink_threads) = sin_threads {
        for t_sink in sink_threads {
            t_sink.join()?;
        }
    }

    if !(server_only) {
        let elapsed = start.elapsed().unwrap();
        let time = elapsed.as_secs() * 10_000_000_000 + u64::from(elapsed.subsec_nanos());
        let ftime = (time as f64) / 10_000_000_000_f64;
        let fcount = (TRANSACTIONS_PER_THREAD * num_producers) as f64;

        println!(
            "performance: {:?}, count: {fcount}, time: {ftime}",
            fcount / ftime
        );
    }
    Ok(())
}

#[derive(Clone)]
enum Transporter {
    Cache(Arc<ConnectionCache>),
    DirectSocket(Arc<UdpSocket>),
}

fn producer(
    sock: SocketAddr,
    num_producers: u64,
    use_connection_cache: bool,
    verbose: bool,
) -> Vec<JoinHandle<()>> {
    println!("Running clients against {sock:?}");
    let transporter = if use_connection_cache {
        Transporter::Cache(Arc::new(ConnectionCache::with_udp(
            "connection_cache_vote_udp",
            1,
        )))
    } else {
        Transporter::DirectSocket(Arc::new(bind_to_unspecified().unwrap()))
    };

    let mut handles = vec![];

    let current_slot: u64 = 0;

    let identity_keypair = Keypair::new(); // Replace with loaded keypair

    for _i in 0..num_producers {
        let transporter = transporter.clone();
        let identity_keypair = identity_keypair.insecure_clone();
        handles.push(thread::spawn(move || {
            // Generate and send transactions
            for _j in 0..TRANSACTIONS_PER_THREAD {
                // Create a vote instruction
                let vote = Vote {
                    slots: vec![current_slot], // Voting for the current slot
                    hash: Hash::new_unique(),
                    timestamp: None, // Optional timestamp
                };

                let vote_instruction = vote_instruction::vote(
                    &identity_keypair.pubkey(),
                    &identity_keypair.pubkey(),
                    vote,
                );

                // Build the transaction
                let message = Message::new(&[vote_instruction], Some(&identity_keypair.pubkey()));

                let recent_blockhash = Hash::new_unique();
                let transaction = Transaction::new(&[&identity_keypair], message, recent_blockhash);

                let serialized_transaction = bincode::serialize(&transaction).unwrap();

                match &transporter {
                    Transporter::Cache(cache) => {
                        let connection = cache.get_connection(&sock);

                        match connection.send_data(&serialized_transaction) {
                            Ok(_) => {
                                if verbose {
                                    println!("Sent transaction successfully");
                                }
                            }
                            Err(ex) => {
                                println!("Error sending transaction {ex:?}");
                            }
                        }
                    }
                    Transporter::DirectSocket(socket) => {
                        match socket.send_to(&serialized_transaction, sock) {
                            Ok(_) => {
                                if verbose {
                                    println!(
                                        "Sent transaction via direct socket successfully {sock:?}"
                                    );
                                }
                            }
                            Err(ex) => {
                                println!("Error sending transaction {ex:?}");
                            }
                        }
                    }
                }
            }
        }));
    }
    handles
}
