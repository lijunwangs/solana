//! Responsible for managing the switching between vortexors or the native TPU streamers.
//! When switching to a new vortexor, make gossip to advertise the new vortexor's address
//! for tpu and tpu forward address. And when switching from a vortexor to the native TPU streamers,
//! make gossip to advertise the native TPU streamers' address for tpu and tpu forward address.
//! And make sure the TPU streamers are started and so local sig verifier is also started.
//! If it is switching from the native TPU streamer to the vortexor, make sure the local sig verifier
//! is stopped along with the native TPU streamers.

use {
    crossbeam_channel::Sender,
    log::{error, info},
    solana_gossip::cluster_info::ClusterInfo,
    solana_perf::packet::PacketBatch,
    solana_sdk::signature::Keypair,
    solana_streamer::{
        quic::{spawn_server_multi, QuicServerParams, SpawnServerResult},
        streamer::StakedNodes,
    },
    std::{
        net::{SocketAddr, UdpSocket},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread,
    },
};

//// Configuration for the Vortexor receiver.
pub struct VotexorReceiverConfig {
    pub vortexor_tpu_address: SocketAddr,
    pub vortexor_tpu_forward_address: SocketAddr,
    pub vortexor_receivers: Vec<UdpSocket>,
}

/// Configuration for the TpuSwitch.
pub struct TpuSwitchConfig {
    pub vortexor_receiver_config: Option<VotexorReceiverConfig>,
    pub transactions_quic_sockets: Vec<UdpSocket>,
    pub transactions_forwards_quic_sockets: Vec<UdpSocket>,
    pub keypair: Keypair,
    pub packet_sender: Sender<PacketBatch>,
    pub forwarded_packet_sender: Sender<PacketBatch>,
    pub staked_nodes: Arc<RwLock<StakedNodes>>,
    pub tpu_quic_server_config: QuicServerParams,
    pub tpu_fwd_quic_server_config: QuicServerParams,
}

/// Manages the fallback between vortexors and native TPU streamers.
pub struct TpuSwitch {
    config: TpuSwitchConfig,
    cluster_info: Arc<ClusterInfo>,
    tpu_quic_t: Option<thread::JoinHandle<()>>,
    tpu_forwards_quic_t: Option<thread::JoinHandle<()>>,
    exit: Arc<AtomicBool>,
}

fn clone_udp_sockets(sockets: &[UdpSocket]) -> Vec<UdpSocket> {
    sockets.iter().map(|s| s.try_clone().expect("Expected to be able to clone the socket")).collect()
}

impl TpuSwitch {
    /// Creates a new TpuSwitch.
    pub fn new(
        config: TpuSwitchConfig,
        cluster_info: Arc<ClusterInfo>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let (tpu_quic_t, tpu_forwards_quic_t) = start_quic_tpu_streamers(&config, &exit);

        Self {
            config,
            cluster_info,
            tpu_quic_t,
            tpu_forwards_quic_t,
            exit,
        }
    }

    /// Switches to the vortexor by updating gossip and stopping the native TPU streamers.
    pub fn switch_to_vortexor(&self) {
        info!("Switching to vortexor...");
        self.update_gossip(
            self.config.vortexor_tpu_address,
            self.config.vortexor_tpu_forward_address,
        );

        // Stop the native TPU streamers.
        if let Ok(mut tpu) = self.tpu.write() {
            if let Err(err) = tpu.join() {
                error!("Failed to stop native TPU streamers: {:?}", err);
            }
        }
    }

    /// Switches to the native TPU streamers by updating gossip and starting the TPU.
    pub fn switch_to_native_tpu(&self) {
        info!("Switching to native TPU streamers...");
        self.update_gossip(
            self.config.native_tpu_address,
            self.config.native_tpu_forward_address,
        );

        // Start the native TPU streamers.
        if let Ok(mut tpu) = self.tpu.write() {
            // Restart the TPU (implementation depends on your TPU struct).
            info!("Starting native TPU streamers...");
        }
    }

    /// Updates gossip to advertise the given TPU and TPU forward addresses.
    fn update_gossip(&self, tpu_address: SocketAddr, tpu_forward_address: SocketAddr) {
        info!(
            "Updating gossip to advertise TPU address: {} and TPU forward address: {}",
            tpu_address, tpu_forward_address
        );

        // Implementation for updating gossip (depends on your GossipService).
    }

    /// Starts the fallback manager loop to monitor and handle transitions.
    pub fn start(&self) {
        let exit = self.exit.clone();
        let manager = self.clone();

        thread::spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                // Monitor and handle transitions (e.g., based on heartbeat or other conditions).
                thread::sleep(std::time::Duration::from_secs(1));
            }

            info!("Exiting TpuSwitch...");
        });
    }
}

fn start_quic_tpu_streamers(config: &TpuSwitchConfig, exit: &Arc<AtomicBool>) -> (Option<thread::JoinHandle<()>>, Option<thread::JoinHandle<()>>) {
    let TpuSwitchConfig {
        vortexor_receiver_config,
        keypair,
        transactions_quic_sockets,
        transactions_forwards_quic_sockets,
        packet_sender,
        forwarded_packet_sender,
        staked_nodes,
        tpu_quic_server_config,
        tpu_fwd_quic_server_config,
    } = config;

    let (tpu_quic_t, key_updater) = if vortexor_receiver_config.is_none() {
        // Streamer for TPU
        let SpawnServerResult {
            endpoints: _,
            thread: tpu_quic_t,
            key_updater,
        } = spawn_server_multi(
            "solQuicTpu",
            "quic_streamer_tpu",
            clone_udp_sockets(transactions_quic_sockets),
            keypair,
            packet_sender.clone(),
            exit.clone(),
            staked_nodes.clone(),
            tpu_quic_server_config.clone(),
        )
        .unwrap();
        (Some(tpu_quic_t), Some(key_updater))
    } else {
        (None, None)
    };

    let (tpu_forwards_quic_t, forwards_key_updater) = if vortexor_receiver_config.is_none() {
        // Streamer for TPU forward
        let SpawnServerResult {
            endpoints: _,
            thread: tpu_forwards_quic_t,
            key_updater: forwards_key_updater,
        } = spawn_server_multi(
            "solQuicTpuFwd",
            "quic_streamer_tpu_forwards",
            clone_udp_sockets(transactions_forwards_quic_sockets),
            keypair,
            forwarded_packet_sender.clone(),
            exit.clone(),
            staked_nodes.clone(),
            tpu_fwd_quic_server_config.clone(),
        )
        .unwrap();
        (Some(tpu_forwards_quic_t), Some(forwards_key_updater))
    } else {
        (None, None)
    };
    (tpu_quic_t, tpu_forwards_quic_t)
}

impl Clone for TpuSwitch {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            gossip_service: Arc::clone(&self.gossip_service),
            tpu: Arc::clone(&self.tpu),
            exit: Arc::clone(&self.exit),
        }
    }
}
