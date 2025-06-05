//! Responsible for managing the switching between vortexors or the native TPU streamers.
//! When switching to a new vortexor, make gossip to advertise the new vortexor's address
//! for tpu and tpu forward address. And when switching from a vortexor to the native TPU streamers,
//! make gossip to advertise the native TPU streamers' address for tpu and tpu forward address.
//! And make sure the TPU streamers are started and so local sig verifier is also started.
//! If it is switching from the native TPU streamer to the vortexor, make sure the local sig verifier
//! is stopped along with the native TPU streamers.

use {
    crate::{
        admin_rpc_post_init::{KeyUpdaterType, KeyUpdaters},
        banking_trace::TracedSender,
        sigverify::TransactionSigVerifier,
        sigverify_stage::SigVerifyStage,
        vortexor_heartbeat_monitor::{HeartbeatMessage, HeartbeatMonitor},
        vortexor_receiver_adapter::VortexorReceiverAdapter,
    },
    agave_banking_stage_ingress_types::BankingPacketBatch,
    core::panic,
    crossbeam_channel::{Receiver, Sender},
    log::info,
    solana_gossip::{cluster_info::ClusterInfo, contact_info::Error as ContactInforError},
    solana_perf::packet::PacketBatch,
    solana_sdk::{quic::QUIC_PORT_OFFSET, signature::Keypair},
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
        time::Duration,
    },
};

/// The `SigVerifier` enum is used to determine whether to use a local or remote signature verifier.
enum SigVerifier {
    Local(SigVerifyStage),
    Remote(VortexorReceiverAdapter),
    Mixed((SigVerifyStage, VortexorReceiverAdapter)),
}

enum SigVerifierType {
    /// Using local sig verifier
    Local,
    /// Using vortexor
    Remote,
    /// Local sig verifier is still being run even when we are using vortexor
    Mixed,
}

impl SigVerifier {
    fn join(self) -> thread::Result<()> {
        match self {
            SigVerifier::Local(sig_verify_stage) => sig_verify_stage.join(),
            SigVerifier::Remote(vortexor_receiver_adapter) => vortexor_receiver_adapter.join(),
            SigVerifier::Mixed((sig_verify_stage, vortexor_receiver_adapter)) => {
                sig_verify_stage.join()?;
                vortexor_receiver_adapter.join()
            }
        }
    }
}

/// Configuration for the Vortexor receiver.
pub struct VotexorReceiverConfig {
    pub vortexor_tpu_address: SocketAddr,
    pub vortexor_tpu_forward_address: SocketAddr,
    pub vortexor_receivers: Vec<UdpSocket>,
}

/// Configuration for the TpuSwitch.
pub struct TpuSwitchConfig {
    /// Indicating if UDP based TPU is still enabled
    pub tpu_enable_udp: bool,
    pub vortexor_receiver_config: Option<VotexorReceiverConfig>,
    pub transactions_quic_sockets: Vec<UdpSocket>,
    pub transactions_forwards_quic_sockets: Vec<UdpSocket>,
    pub keypair: Arc<Keypair>,
    pub packet_sender: Sender<PacketBatch>,
    pub packet_receiver: Receiver<PacketBatch>,
    pub forwarded_packet_sender: Sender<PacketBatch>,
    pub staked_nodes: Arc<RwLock<StakedNodes>>,
    pub tpu_quic_server_config: QuicServerParams,
    pub tpu_fwd_quic_server_config: QuicServerParams,
    pub tpu_coalesce: Duration,
    pub enable_block_production_forwarding: bool,
    pub non_vote_sender: TracedSender,
    pub forward_stage_sender: Sender<(BankingPacketBatch, bool)>,
    pub key_notifiers: Arc<RwLock<KeyUpdaters>>,
    pub heartbeat_interval: Duration,
}

/// Manages the fallback between vortexors and native TPU streamers.
pub(crate) struct TpuSwitch {
    config: TpuSwitchConfig,
    cluster_info: Arc<ClusterInfo>,
    tpu_quic_t: Option<thread::JoinHandle<()>>,
    tpu_forwards_quic_t: Option<thread::JoinHandle<()>>,
    sig_verifier: Option<SigVerifier>,
    native_tpu_address: SocketAddr,
    native_tpu_forward_address: SocketAddr,
    heartbeat_sender: Option<Sender<HeartbeatMessage>>,
    sub_service_exit: Arc<AtomicBool>,
}

fn clone_udp_sockets(sockets: &[UdpSocket]) -> Vec<UdpSocket> {
    sockets
        .iter()
        .map(|s| {
            s.try_clone()
                .expect("Expected to be able to clone the socket")
        })
        .collect()
}

pub struct TpuSwitchManager {
    tpu_switch: Arc<RwLock<TpuSwitch>>,
    heartbeat_monitor: Option<HeartbeatMonitor>,
}

impl TpuSwitchManager {
    /// Creates a new TpuSwitchManager.
    pub fn new(config: TpuSwitchConfig, cluster_info: Arc<ClusterInfo>) -> Self {
        let (heartbeat_sender, heartbeat_receiver) = if config.vortexor_receiver_config.is_some() {
            let (heartbeat_sender, heartbeat_receiver) = crossbeam_channel::bounded(1024);
            (Some(heartbeat_sender), Some(heartbeat_receiver))
        } else {
            (None, None)
        };
        let heartbeat_interval = config.heartbeat_interval;
        let tpu_switch = Arc::new(RwLock::new(TpuSwitch::new(
            config,
            cluster_info,
            heartbeat_sender,
        )));

        let heartbeat_monitor = heartbeat_receiver.map(|receiver| {
            HeartbeatMonitor::new(heartbeat_interval, receiver, tpu_switch.clone())
        });

        Self {
            tpu_switch,
            heartbeat_monitor,
        }
    }

    pub fn join(mut self) -> thread::Result<()> {
        let mut tpu_switch = self.tpu_switch.write().unwrap();
        tpu_switch.join();
        if let Some(monitor) = self.heartbeat_monitor.take() {
            monitor.join()?;
        }
        Ok(())
    }
}

impl TpuSwitch {
    /// Creates a new TpuSwitch.
    fn new(
        config: TpuSwitchConfig,
        cluster_info: Arc<ClusterInfo>,
        heartbeat_sender: Option<Sender<HeartbeatMessage>>,
    ) -> Self {
        let native_tpu_address = config.transactions_quic_sockets[0]
            .local_addr()
            .expect("Expected to be able to get local address");
        let native_tpu_address = SocketAddr::new(
            native_tpu_address.ip(),
            native_tpu_address.port() - QUIC_PORT_OFFSET,
        );
        let native_tpu_forward_address = config.transactions_forwards_quic_sockets[0]
            .local_addr()
            .expect("Expected to be able to get local address");
        let native_tpu_forward_address = SocketAddr::new(
            native_tpu_forward_address.ip(),
            native_tpu_forward_address.port() - QUIC_PORT_OFFSET,
        );

        let cluster_info = cluster_info.clone();
        info!(
            "The gossip address is for this node is: {:?}",
            cluster_info.my_contact_info().gossip()
        );
        let sub_service_exit = Arc::new(AtomicBool::new(false));
        let (tpu_quic_t, tpu_forwards_quic_t) =
            start_quic_tpu_streamers(&config, &sub_service_exit);

        let verifier_type = if config.vortexor_receiver_config.is_some() {
            if config.tpu_enable_udp {
                SigVerifierType::Mixed
            } else {
                SigVerifierType::Remote
            }
        } else {
            SigVerifierType::Local
        };

        let sig_verifier = Some(start_sig_verifier(
            verifier_type,
            &config,
            heartbeat_sender.clone(),
            &sub_service_exit,
        ));

        Self {
            config,
            cluster_info,
            tpu_quic_t,
            tpu_forwards_quic_t,
            sig_verifier,
            native_tpu_address,
            native_tpu_forward_address,
            sub_service_exit,
            heartbeat_sender,
        }
    }

    /// Switches to the vortexor by updating gossip and stopping the native TPU streamers.
    pub fn switch_to_vortexor(&mut self) {
        match self.sig_verifier {
            Some(SigVerifier::Local(_)) => {
                info!("Switching to vortexor...");

                if let Some(vortexor_config) = &self.config.vortexor_receiver_config {
                    self.update_gossip_addresses(vortexor_config);

                    let sub_service_exit = Arc::new(AtomicBool::new(false));
                    self.switch_sig_verifier_to_vortexor(
                        self.heartbeat_sender.clone(),
                        &sub_service_exit,
                    );

                    self.stop_native_tpu_streamers();

                    self.sub_service_exit = sub_service_exit;
                } else {
                    error!("Vortexor receiver config is not set");
                }
            }
            Some(SigVerifier::Remote(_) | SigVerifier::Mixed(_)) => {
                // already using vortexor, do nothing
            }
            None => {
                panic!("Unexpected condition!");
            }
        }
    }

    /// Updates gossip to advertise vortexor TPU and TPU forward addresses.
    fn update_gossip_addresses(&self, vortexor_config: &VotexorReceiverConfig) {
        self.update_gossip(
            &vortexor_config.vortexor_tpu_address,
            &vortexor_config.vortexor_tpu_forward_address,
        )
        .expect("Failed to update TPU address via gossip");
    }

    /// Switches the signature verifier to use the vortexor.
    fn switch_sig_verifier_to_vortexor(
        &mut self,
        heartbeat_sender: Option<Sender<HeartbeatMessage>>,
        sub_service_exit: &Arc<AtomicBool>,
    ) {
        self.sig_verifier = match self.sig_verifier.take() {
            Some(SigVerifier::Local(sig_verify_stage)) => {
                if !self.config.tpu_enable_udp {
                    sig_verify_stage
                        .join()
                        .unwrap_or_else(|err| panic!("Failed to stop local sig verifier: {err:?}"));
                    Some(SigVerifier::Remote(start_remote_sig_verifier(
                        &self.config,
                        heartbeat_sender,
                        sub_service_exit,
                    )))
                } else {
                    Some(SigVerifier::Mixed((
                        sig_verify_stage,
                        start_remote_sig_verifier(&self.config, heartbeat_sender, sub_service_exit),
                    )))
                }
            }
            Some(SigVerifier::Mixed((_, _))) => {
                panic!("Wrong TPU switch from Mixed to Remote");
            }
            Some(SigVerifier::Remote(_)) => {
                panic!("Wrong TPU switch from Remote to Remote");
            }
            None => {
                panic!("Sig verifier is not set");
            }
        };
    }

    /// Stops the native TPU streamers and removes their key notifiers.
    fn stop_native_tpu_streamers(&mut self) {
        self.sub_service_exit.store(true, Ordering::Relaxed);

        if let Some(tpu_quic_t) = self.tpu_quic_t.take() {
            tpu_quic_t
                .join()
                .unwrap_or_else(|err| panic!("Failed to stop native TPU streamer: {err:?}"));
            self.config
                .key_notifiers
                .write()
                .unwrap()
                .remove(&KeyUpdaterType::Tpu);
        }

        if let Some(tpu_forwards_quic_t) = self.tpu_forwards_quic_t.take() {
            tpu_forwards_quic_t.join().unwrap_or_else(|err| {
                panic!("Failed to stop native TPU forward streamer: {err:?}")
            });
            self.config
                .key_notifiers
                .write()
                .unwrap()
                .remove(&KeyUpdaterType::TpuForwards);
        }
    }

    /// Switches to the native TPU streamers by updating gossip and starting the TPU.
    pub fn switch_to_native_tpu(&mut self) {
        match self.sig_verifier {
            Some(SigVerifier::Local(_)) => {
                // nothing to do;
            }
            Some(SigVerifier::Remote(_) | SigVerifier::Mixed(_)) => {
                info!("Switching to native TPU streamers...");

                // Update gossip to advertise native TPU addresses
                self.update_native_tpu_gossip();

                // Start native TPU streamers
                self.start_native_tpu_streamers();
            }
            None => {
                panic!("Unexpected condition");
            }
        }
    }

    /// Updates gossip to advertise native TPU and TPU forward addresses.
    fn update_native_tpu_gossip(&self) {
        self.update_gossip(&self.native_tpu_address, &self.native_tpu_forward_address)
            .expect("Failed to update TPU address via gossip");
    }

    /// Stops vortexor-related services and resets the signature verifier.
    /// This will be used when explicitly stopping the vortexor services via
    /// ADMIN RPC.
    pub fn stop_vortexor_services(&mut self) {
        self.sub_service_exit.store(true, Ordering::Relaxed);

        // Stop the current signature verifier
        self.sig_verifier = self.stop_and_reset_sig_verifier();
    }

    /// Stops the current signature verifier and resets it to a local verifier.
    fn stop_and_reset_sig_verifier(&mut self) -> Option<SigVerifier> {
        match self.sig_verifier.take() {
            Some(SigVerifier::Remote(vortexor_receiver_adapter)) => {
                self.stop_vortexor_receiver(vortexor_receiver_adapter);
                Some(SigVerifier::Local(start_local_sig_verifier(&self.config)))
            }
            Some(SigVerifier::Mixed((sig_verify_stage, vortexor_receiver_adapter))) => {
                self.stop_vortexor_receiver(vortexor_receiver_adapter);
                Some(SigVerifier::Local(sig_verify_stage))
            }
            Some(SigVerifier::Local(_)) => {
                panic!("Wrong TPU switch from Local to Local");
            }
            None => {
                panic!("Sig verifier is not set");
            }
        }
    }

    /// Stops the vortexor receiver adapter.
    fn stop_vortexor_receiver(&self, vortexor_receiver_adapter: VortexorReceiverAdapter) {
        vortexor_receiver_adapter
            .join()
            .unwrap_or_else(|err| panic!("Failed to stop vortexor receiver: {err:?}"));
    }

    /// Starts the native TPU streamers and updates the sub-service exit flag.
    fn start_native_tpu_streamers(&mut self) {
        let sub_service_exit = Arc::new(AtomicBool::new(false));
        let (tpu_quic_t, tpu_forwards_quic_t) =
            start_quic_tpu_streamers(&self.config, &sub_service_exit);

        self.tpu_quic_t = tpu_quic_t;
        self.tpu_forwards_quic_t = tpu_forwards_quic_t;
        self.sub_service_exit = sub_service_exit;
    }

    /// Updates gossip to advertise the given TPU and TPU forward addresses.
    fn update_gossip(
        &self,
        tpu_address: &SocketAddr,
        tpu_forward_address: &SocketAddr,
    ) -> Result<(), ContactInforError> {
        info!(
            "Updating gossip to advertise TPU address: {tpu_address} and TPU forward address: {tpu_forward_address}",
        );

        let tpu_ip = tpu_address.ip();
        let gossip_addr = self.cluster_info.my_contact_info().gossip().unwrap().ip();
        let tpu_ip = if tpu_ip.is_unspecified() {
            gossip_addr
        } else {
            tpu_ip
        };

        self.cluster_info
            .set_tpu(SocketAddr::new(tpu_ip, tpu_address.port()))?;

        let tpu_forward_ip = tpu_forward_address.ip();
        let addr = if tpu_forward_ip.is_unspecified() {
            gossip_addr
        } else {
            tpu_forward_ip
        };

        self.cluster_info
            .set_tpu_forwards(SocketAddr::new(addr, tpu_forward_address.port()))
    }

    pub fn join(&mut self) {
        self.sub_service_exit.store(true, Ordering::Relaxed);
        if let Some(sig_verifier) = self.sig_verifier.take() {
            sig_verifier.join().unwrap_or_else(|err| {
                panic!("Failed to stop sig verifier: {err:?}");
            });
        }
    }
}

fn start_sig_verifier(
    verifier_type: SigVerifierType,
    config: &TpuSwitchConfig,
    heartbeat_sender: Option<Sender<HeartbeatMessage>>,
    exit: &Arc<AtomicBool>,
) -> SigVerifier {
    match verifier_type {
        SigVerifierType::Remote => {
            SigVerifier::Remote(start_remote_sig_verifier(config, heartbeat_sender, exit))
        }
        SigVerifierType::Local => SigVerifier::Local(start_local_sig_verifier(config)),
        SigVerifierType::Mixed => {
            let local_sig_verifier = start_local_sig_verifier(config);
            let remote_sig_verifier = start_remote_sig_verifier(config, heartbeat_sender, exit);
            SigVerifier::Mixed((local_sig_verifier, remote_sig_verifier))
        }
    }
}

fn start_local_sig_verifier(config: &TpuSwitchConfig) -> SigVerifyStage {
    info!("starting regular sigverify stage");
    let verifier = TransactionSigVerifier::new(
        config.non_vote_sender.clone(),
        config
            .enable_block_production_forwarding
            .then(|| config.forward_stage_sender.clone()),
    );
    SigVerifyStage::new(
        config.packet_receiver.clone(),
        verifier,
        "solSigVerTpu",
        "tpu-verifier",
    )
}

fn start_remote_sig_verifier(
    config: &TpuSwitchConfig,
    heartbeat_sender: Option<Sender<HeartbeatMessage>>,
    exit: &Arc<AtomicBool>,
) -> VortexorReceiverAdapter {
    info!("starting vortexor adapter");
    let vortexor_config = config
        .vortexor_receiver_config
        .as_ref()
        .expect("Vortexor config is required for remote sig verifier");
    let sockets = clone_udp_sockets(&vortexor_config.vortexor_receivers);
    let sockets = sockets.into_iter().map(Arc::new).collect();

    VortexorReceiverAdapter::new(
        sockets,
        Duration::from_millis(5),
        config.tpu_coalesce,
        config.non_vote_sender.clone(),
        config
            .enable_block_production_forwarding
            .then(|| config.forward_stage_sender.clone()),
        config.heartbeat_interval,
        heartbeat_sender,
        exit.clone(),
    )
}

fn start_quic_tpu_streamers(
    config: &TpuSwitchConfig,
    exit: &Arc<AtomicBool>,
) -> (
    Option<thread::JoinHandle<()>>,
    Option<thread::JoinHandle<()>>,
) {
    let TpuSwitchConfig {
        tpu_enable_udp: _,
        vortexor_receiver_config,
        keypair,
        transactions_quic_sockets,
        transactions_forwards_quic_sockets,
        packet_sender,
        forwarded_packet_sender,
        staked_nodes,
        tpu_quic_server_config,
        tpu_fwd_quic_server_config,
        enable_block_production_forwarding: _,
        non_vote_sender: _,
        forward_stage_sender: _,
        tpu_coalesce: _,
        packet_receiver: _,
        key_notifiers,
        heartbeat_interval: _,
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

    if let Some(key_updater) = key_updater {
        key_notifiers
            .write()
            .unwrap()
            .add(KeyUpdaterType::Tpu, key_updater);
    }
    if let Some(forwards_key_updater) = forwards_key_updater {
        key_notifiers
            .write()
            .unwrap()
            .add(KeyUpdaterType::TpuForwards, forwards_key_updater);
    }
    (tpu_quic_t, tpu_forwards_quic_t)
}
