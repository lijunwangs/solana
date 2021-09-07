use {
    crate::accountsdb_repl_service::AccountsDbReplService,
    crossbeam_channel::unbounded,
    log::*,
    solana_core::{
        accounts_hash_verifier::AccountsHashVerifier,
        snapshot_packager_service::SnapshotPackagerService,
    },
    solana_download_utils::download_snapshot,
    solana_genesis_utils::download_then_check_genesis_hash,
    solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
    solana_ledger::{
        blockstore::Blockstore, blockstore_db::AccessType, blockstore_processor,
        leader_schedule_cache::LeaderScheduleCache,
    },
    solana_measure::measure::Measure,
    solana_metrics::datapoint_info,
    solana_replica_lib::accountsdb_repl_client::AccountsDbReplClientServiceConfig,
    solana_rpc::{
        max_slots::MaxSlots,
        optimistically_confirmed_bank_tracker::{
            BankNotificationReceiver, BankNotificationSender, OptimisticallyConfirmedBank,
            OptimisticallyConfirmedBankTracker,
        },
        rpc::JsonRpcConfig,
        rpc_pubsub_service::{PubSubConfig, PubSubService},
        rpc_service::JsonRpcService,
        rpc_subscriptions::RpcSubscriptions,
    },
    solana_runtime::{
        accounts_background_service::{
            AbsRequestHandler, AbsRequestSender, AccountsBackgroundService, SnapshotRequestHandler,
        },
        accounts_index::AccountSecondaryIndexes,
        bank_forks::BankForks,
        commitment::BlockCommitmentCache,
        hardened_unpack::MAX_GENESIS_ARCHIVE_UNPACKED_SIZE,
        snapshot_config::SnapshotConfig,
        snapshot_package::PendingSnapshotPackage,
        snapshot_utils::{self, ArchiveFormat, BankFromArchiveTimings},
    },
    solana_sdk::{clock::Slot, exit::Exit, genesis_config::GenesisConfig, hash::Hash},
    solana_streamer::socket::SocketAddrSpace,
    std::{
        fs,
        net::SocketAddr,
        path::PathBuf,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            mpsc::channel,
            Arc, RwLock,
        },
    },
};

pub struct ReplicaNodeConfig {
    pub rpc_peer_addr: SocketAddr,
    pub accountsdb_repl_peer_addr: Option<SocketAddr>,
    pub rpc_addr: SocketAddr,
    pub rpc_pubsub_addr: SocketAddr,
    pub ledger_path: PathBuf,
    pub snapshot_archives_dir: PathBuf,
    pub bank_snapshots_dir: PathBuf,
    pub account_paths: Vec<PathBuf>,
    pub snapshot_info: (Slot, Hash),
    pub cluster_info: Arc<ClusterInfo>,
    pub rpc_config: JsonRpcConfig,
    pub snapshot_config: Option<SnapshotConfig>,
    pub pubsub_config: PubSubConfig,
    pub account_indexes: AccountSecondaryIndexes,
    pub accounts_db_caching_enabled: bool,
    pub replica_exit: Arc<RwLock<Exit>>,
    pub socket_addr_space: SocketAddrSpace,
    pub genesis_config: Option<GenesisConfig>,
    pub accounts_db_test_hash_calculation: bool,
    pub accounts_db_use_index_hash_calculation: bool,
    pub abs_request_sender: Option<AbsRequestSender>,
}

pub struct ReplicaNode {
    json_rpc_service: Option<JsonRpcService>,
    pubsub_service: Option<PubSubService>,
    optimistically_confirmed_bank_tracker: Option<OptimisticallyConfirmedBankTracker>,
    accountsdb_repl_service: Option<AccountsDbReplService>,
    snapshot_packager_service: Option<SnapshotPackagerService>,
    accounts_hash_verifier: Option<AccountsHashVerifier>,
    accounts_background_service: Option<AccountsBackgroundService>,
    replica_exit: Arc<RwLock<Exit>>,
}

// Struct maintaining information about banks
pub struct ReplicaNodeBankInfo {
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub optimistically_confirmed_bank: Arc<RwLock<OptimisticallyConfirmedBank>>,
    pub leader_schedule_cache: Arc<LeaderScheduleCache>,
    pub block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
    pub bank_notification_sender: BankNotificationSender,
}

// Initialize the replica by downloading snapshot from the peer, initialize
// the BankForks, OptimisticallyConfirmedBank, LeaderScheduleCache and
// BlockCommitmentCache and return the info wrapped as ReplicaNodeBankInfo.
fn initialize_from_snapshot(
    replica_config: &ReplicaNodeConfig,
    snapshot_config: &SnapshotConfig,
    genesis_config: &GenesisConfig,
    bank_notification_sender: BankNotificationSender,
) -> (ReplicaNodeBankInfo, BankFromArchiveTimings) {
    info!(
        "Downloading snapshot from the peer into {:?}",
        replica_config.snapshot_archives_dir
    );

    download_snapshot(
        &replica_config.rpc_peer_addr,
        &replica_config.snapshot_archives_dir,
        replica_config.snapshot_info,
        false,
        snapshot_config.maximum_full_snapshot_archives_to_retain,
        &mut None,
    )
    .unwrap();

    fs::create_dir_all(&snapshot_config.bank_snapshots_dir)
        .expect("Couldn't create bank snapshot directory");

    let archive_info = snapshot_utils::get_highest_full_snapshot_archive_info(
        &replica_config.snapshot_archives_dir,
    )
    .unwrap();

    let process_options = blockstore_processor::ProcessOptions {
        account_indexes: replica_config.account_indexes.clone(),
        accounts_db_caching_enabled: replica_config.accounts_db_caching_enabled,
        ..blockstore_processor::ProcessOptions::default()
    };

    info!(
        "Build bank from snapshot archive: {:?}",
        &snapshot_config.bank_snapshots_dir
    );
    let (bank0, timings) = snapshot_utils::bank_from_snapshot_archives(
        &replica_config.account_paths,
        &[],
        &snapshot_config.bank_snapshots_dir,
        &archive_info,
        None,
        genesis_config,
        process_options.debug_keys.clone(),
        None,
        process_options.account_indexes.clone(),
        process_options.accounts_db_caching_enabled,
        process_options.limit_load_slot_count_from_snapshot,
        process_options.shrink_ratio,
        process_options.accounts_db_test_hash_calculation,
        true,
        process_options.verify_index,
        process_options.accounts_index_config,
    )
    .unwrap();

    let bank0_slot = bank0.slot();
    let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank0));

    let bank_forks = Arc::new(RwLock::new(BankForks::new(bank0)));

    let optimistically_confirmed_bank =
        OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks);

    let mut block_commitment_cache = BlockCommitmentCache::default();
    block_commitment_cache.initialize_slots(bank0_slot);
    let block_commitment_cache = Arc::new(RwLock::new(block_commitment_cache));

    (
        ReplicaNodeBankInfo {
            bank_forks,
            optimistically_confirmed_bank,
            leader_schedule_cache,
            block_commitment_cache,
            bank_notification_sender,
        },
        timings,
    )
}

fn start_client_rpc_services(
    replica_config: &ReplicaNodeConfig,
    genesis_config: &GenesisConfig,
    cluster_info: Arc<ClusterInfo>,
    bank_info: &ReplicaNodeBankInfo,
    socket_addr_space: &SocketAddrSpace,
    bank_notification_receiver: BankNotificationReceiver,
    exit: Arc<AtomicBool>,
) -> (
    Option<JsonRpcService>,
    Option<PubSubService>,
    Option<OptimisticallyConfirmedBankTracker>,
) {
    let ReplicaNodeBankInfo {
        bank_forks,
        optimistically_confirmed_bank,
        leader_schedule_cache,
        block_commitment_cache,
        bank_notification_sender: _bank_notification_sender,
    } = bank_info;
    let blockstore = Arc::new(
        Blockstore::open_with_access_type(
            &replica_config.ledger_path,
            AccessType::PrimaryOnly,
            None,
            false,
        )
        .unwrap(),
    );

    let max_complete_transaction_status_slot = Arc::new(AtomicU64::new(0));

    let max_slots = Arc::new(MaxSlots::default());

    let subscriptions = Arc::new(RpcSubscriptions::new(
        &exit,
        bank_forks.clone(),
        block_commitment_cache.clone(),
        optimistically_confirmed_bank.clone(),
    ));

    let rpc_override_health_check = Arc::new(AtomicBool::new(false));
    if ContactInfo::is_valid_address(&replica_config.rpc_addr, socket_addr_space) {
        assert!(ContactInfo::is_valid_address(
            &replica_config.rpc_pubsub_addr,
            socket_addr_space
        ));
    } else {
        assert!(!ContactInfo::is_valid_address(
            &replica_config.rpc_pubsub_addr,
            socket_addr_space
        ));
    }

    (
        Some(JsonRpcService::new(
            replica_config.rpc_addr,
            replica_config.rpc_config.clone(),
            replica_config.snapshot_config.clone(),
            bank_forks.clone(),
            block_commitment_cache.clone(),
            blockstore,
            cluster_info,
            None,
            genesis_config.hash(),
            &replica_config.ledger_path,
            replica_config.replica_exit.clone(),
            None,
            rpc_override_health_check,
            optimistically_confirmed_bank.clone(),
            0,
            0,
            max_slots,
            leader_schedule_cache.clone(),
            max_complete_transaction_status_slot,
        )),
        Some(PubSubService::new(
            replica_config.pubsub_config.clone(),
            &subscriptions,
            replica_config.rpc_pubsub_addr,
            &exit,
        )),
        Some(OptimisticallyConfirmedBankTracker::new(
            bank_notification_receiver,
            &exit,
            bank_forks.clone(),
            optimistically_confirmed_bank.clone(),
            subscriptions.clone(),
            None,
        )),
    )
}

impl ReplicaNode {
    pub fn new(mut replica_config: ReplicaNodeConfig) -> Self {
        let mut measure_total = Measure::start("measure_total");
        let mut measure_download_snapshot = Measure::start("download_snapshot");
        let genesis_config = download_then_check_genesis_hash(
            &replica_config.rpc_peer_addr,
            &replica_config.ledger_path,
            None,
            MAX_GENESIS_ARCHIVE_UNPACKED_SIZE,
            false,
            true,
        )
        .unwrap();

        measure_download_snapshot.stop();
        let snapshot_config = SnapshotConfig {
            full_snapshot_archive_interval_slots: std::u64::MAX,
            incremental_snapshot_archive_interval_slots: std::u64::MAX,
            snapshot_archives_dir: replica_config.snapshot_archives_dir.clone(),
            bank_snapshots_dir: replica_config.bank_snapshots_dir.clone(),
            archive_format: ArchiveFormat::TarBzip2,
            snapshot_version: snapshot_utils::SnapshotVersion::default(),
            maximum_full_snapshot_archives_to_retain:
                snapshot_utils::DEFAULT_MAX_FULL_SNAPSHOT_ARCHIVES_TO_RETAIN,
            maximum_incremental_snapshot_archives_to_retain:
                snapshot_utils::DEFAULT_MAX_INCREMENTAL_SNAPSHOT_ARCHIVES_TO_RETAIN,
        };

        let exit = Arc::new(AtomicBool::new(false));
        {
            let exit = exit.clone();
            replica_config
                .replica_exit
                .write()
                .unwrap()
                .register_exit(Box::new(move || exit.store(true, Ordering::Relaxed)));
        }

        let (bank_notification_sender, bank_notification_receiver) = unbounded();

        let mut measure_initialize_from_snapshot = Measure::start("initialize_from_snapshot");

        let (bank_info, timings) = initialize_from_snapshot(
            &replica_config,
            &snapshot_config,
            &genesis_config,
            bank_notification_sender,
        );

        measure_initialize_from_snapshot.stop();

        let (json_rpc_service, pubsub_service, optimistically_confirmed_bank_tracker) =
            start_client_rpc_services(
                &replica_config,
                &genesis_config,
                replica_config.cluster_info.clone(),
                &bank_info,
                &replica_config.socket_addr_space,
                bank_notification_receiver,
                exit.clone(),
            );

        let (snapshot_packager_service, pending_snapshot_package) = {
            // Start a snapshot packaging service
            let pending_snapshot_package = PendingSnapshotPackage::default();

            let snapshot_packager_service = SnapshotPackagerService::new(
                pending_snapshot_package.clone(),
                Some(replica_config.snapshot_info),
                &exit,
                &replica_config.cluster_info,
                snapshot_config.clone(),
            );
            (
                Some(snapshot_packager_service),
                Some(pending_snapshot_package),
            )
        };

        let (accounts_package_sender, accounts_package_receiver) = channel();
        let accounts_hash_verifier = AccountsHashVerifier::new(
            accounts_package_receiver,
            pending_snapshot_package,
            &exit,
            &replica_config.cluster_info,
            None,
            false,
            0,
            Some(snapshot_config.clone()),
        );

        let (snapshot_request_sender, snapshot_request_handler) = {
            let (snapshot_request_sender, snapshot_request_receiver) = unbounded();
            (
                Some(snapshot_request_sender),
                Some(SnapshotRequestHandler {
                    snapshot_config: snapshot_config.clone(),
                    snapshot_request_receiver,
                    accounts_package_sender,
                }),
            )
        };

        let (pruned_banks_sender, pruned_banks_receiver) = unbounded();
        let callback = bank_info
            .bank_forks
            .read()
            .unwrap()
            .root_bank()
            .rc
            .accounts
            .accounts_db
            .create_drop_bank_callback(pruned_banks_sender);
        for bank in bank_info.bank_forks.read().unwrap().banks().values() {
            bank.set_callback(Some(Box::new(callback.clone())));
        }

        let accounts_background_request_sender = AbsRequestSender::new(snapshot_request_sender);

        let accounts_background_request_handler = AbsRequestHandler {
            snapshot_request_handler,
            pruned_banks_receiver,
        };

        let accounts_background_service = AccountsBackgroundService::new(
            bank_info.bank_forks.clone(),
            &exit,
            accounts_background_request_handler,
            replica_config.accounts_db_caching_enabled,
            replica_config.accounts_db_test_hash_calculation,
            replica_config.accounts_db_use_index_hash_calculation,
            None,
        );

        let accountsdb_repl_client_config = AccountsDbReplClientServiceConfig {
            worker_threads: 1,
            replica_server_addr: replica_config.accountsdb_repl_peer_addr.unwrap(),
        };

        let last_replicated_slot = bank_info.bank_forks.read().unwrap().root_bank().slot();
        info!(
            "Starting AccountsDbReplService from slot {:?}",
            last_replicated_slot
        );

        replica_config.abs_request_sender = Some(accounts_background_request_sender);
        replica_config.genesis_config = Some(genesis_config);
        replica_config.snapshot_config = Some(snapshot_config);

        let replica_exit = replica_config.replica_exit.clone();
        let replica_config = Arc::new(replica_config);

        let accountsdb_repl_service = Some(
            AccountsDbReplService::new(
                last_replicated_slot,
                accountsdb_repl_client_config,
                replica_config,
                bank_info,
            )
            .expect("Failed to start AccountsDb replication service"),
        );

        info!(
            "Started AccountsDbReplService from slot {:?}",
            last_replicated_slot
        );

        measure_total.stop();
        datapoint_info!(
            "replica_bootstrap",
            ("total_ms", measure_total.as_ms(), i64),
            (
                "initialize_from_snapshot_ms",
                measure_initialize_from_snapshot.as_ms(),
                i64
            ),
            (
                "download_snapshot_ms",
                measure_download_snapshot.as_ms(),
                i64
            ),
            (
                "full_snapshot_untar_us",
                timings.full_snapshot_untar_us,
                i64
            ),
            (
                "incremental_snapshot_untar_us",
                timings.incremental_snapshot_untar_us,
                i64
            ),
            (
                "rebuild_bank_from_snapshots_us",
                timings.rebuild_bank_from_snapshots_us,
                i64
            ),
            (
                "verify_snapshot_bank_us",
                timings.verify_snapshot_bank_us,
                i64
            ),
        );

        ReplicaNode {
            json_rpc_service,
            pubsub_service,
            optimistically_confirmed_bank_tracker,
            accountsdb_repl_service,
            snapshot_packager_service,
            accounts_hash_verifier: Some(accounts_hash_verifier),
            accounts_background_service: Some(accounts_background_service),
            replica_exit,
        }
    }

    pub fn exit(&mut self) {
        self.replica_exit.write().unwrap().exit();
    }

    pub fn close(mut self) {
        self.exit();
        self.join();
    }

    pub fn join(self) {
        if let Some(json_rpc_service) = self.json_rpc_service {
            json_rpc_service.join().expect("rpc_service");
        }

        if let Some(pubsub_service) = self.pubsub_service {
            pubsub_service.join().expect("pubsub_service");
        }

        if let Some(optimistically_confirmed_bank_tracker) =
            self.optimistically_confirmed_bank_tracker
        {
            optimistically_confirmed_bank_tracker
                .join()
                .expect("optimistically_confirmed_bank_tracker");
        }
        if let Some(accountsdb_repl_service) = self.accountsdb_repl_service {
            accountsdb_repl_service
                .join()
                .expect("accountsdb_repl_service");
        }
        if let Some(snapshot_packager_service) = self.snapshot_packager_service {
            snapshot_packager_service
                .join()
                .expect("snapshot_packager_service")
        }
        if let Some(accounts_hash_verifier) = self.accounts_hash_verifier {
            accounts_hash_verifier
                .join()
                .expect("accounts_hash_verifier")
        }
        if let Some(accounts_background_service) = self.accounts_background_service {
            accounts_background_service
                .join()
                .expect("accounts_background_service")
        }
    }
}
