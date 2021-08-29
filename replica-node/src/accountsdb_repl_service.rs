/// Module responsible for replicating AccountsDb data from its peer to its local AccountsDb in the replica-node
use {
    crate::replica_node::{ReplicaNodeBankInfo, ReplicaNodeConfig},
    bincode,
    log::*,
    solana_replica_lib::accountsdb_repl_client::{
        AccountsDbReplClientService, AccountsDbReplClientServiceConfig, ReplicaRpcError,
    },
    solana_runtime::{
        accounts::Accounts,
        bank::{Bank, BankFieldsToDeserialize, BankRc},
        serde_snapshot::future::DeserializableVersionedBank,
    },
    solana_sdk::{clock::Slot, pubkey::Pubkey},
    std::{
        sync::{Arc, RwLock},
        thread::{self, sleep, Builder, JoinHandle},
        time::Duration,
    },
};

pub struct AccountsDbReplService {
    thread: JoinHandle<()>,
}

// Service implementation
struct AccountsDbReplServiceImpl {
    accountsdb_repl_client: AccountsDbReplClientService,
    last_replicated_slot: Slot,
    replica_config: Arc<ReplicaNodeConfig>,
    bank_info: ReplicaNodeBankInfo,
}

impl AccountsDbReplServiceImpl {
    pub fn new(
        accountsdb_repl_client: AccountsDbReplClientService,
        last_replicated_slot: Slot,
        replica_config: Arc<ReplicaNodeConfig>,
        bank_info: ReplicaNodeBankInfo,
    ) -> Self {
        AccountsDbReplServiceImpl {
            accountsdb_repl_client,
            last_replicated_slot,
            replica_config,
            bank_info,
        }
    }

    fn replicate_accounts_for_slot(&mut self, slot: Slot) -> Result<(), ReplicaRpcError> {
        match self.accountsdb_repl_client.get_slot_accounts(slot) {
            Err(err) => {
                error!(
                    "Ran into error getting accounts for slot {:?}, error: {:?}",
                    slot, err
                );
                Err(err)
            }
            Ok(accounts) => {
                for account in accounts.iter() {
                    debug!(
                        "Received account: {:?}",
                        Pubkey::new(&account.account_meta.as_ref().unwrap().pubkey)
                    );
                }
                Ok(())
            }
        }
    }

    fn replicate_bank(&mut self, slot: Slot) -> Result<Bank, ReplicaRpcError> {
        match self.accountsdb_repl_client.get_bank_info(slot) {
            Err(err) => {
                error!(
                    "Ran into error getting bank for slot {:?}, error: {:?}",
                    slot, err
                );
                Err(err)
            }
            Ok(bank_info) => {
                let deserializable_bank: DeserializableVersionedBank =
                    bincode::deserialize(&bank_info.bank_data).unwrap();
                let bank_fields = BankFieldsToDeserialize::from(deserializable_bank);
                let parent = self.bank_info.bank_forks.read().unwrap().root_bank();

                let bank_rc = BankRc {
                    accounts: Arc::new(Accounts::new_from_parent(
                        &parent.rc.accounts,
                        slot,
                        parent.slot(),
                    )),
                    parent: RwLock::new(Some(parent.clone())),
                    slot,
                    bank_id_generator: parent.rc.bank_id_generator.clone(),
                };

                let bank = Bank::new_from_fields(
                    bank_rc,
                    self.replica_config.genesis_config.as_ref().unwrap(),
                    bank_fields,
                    None,
                    None,
                    false,
                );
                Ok(bank)
            }
        }
    }

    pub fn run_service(&mut self) {
        loop {
            match self
                .accountsdb_repl_client
                .get_confirmed_slots(self.last_replicated_slot)
            {
                Ok(slots) => {
                    info!("Received updated slots: {:?}", slots);
                    if !slots.is_empty() {
                        for slot in slots.iter() {
                            if self.replicate_bank(*slot).is_err() {
                                error!(
                                    "Ran into problem replicating bank for slot {:}. Quit.",
                                    slot
                                );
                                break;
                            }

                            if self.replicate_accounts_for_slot(*slot).is_err() {
                                error!(
                                    "Ran into problem replicating accounts for slot {:}. Quit.",
                                    slot
                                );
                                break;
                            }
                        }
                        self.last_replicated_slot = slots[slots.len() - 1];
                    }
                }
                Err(err) => {
                    error!("Ran into error getting updated slots: {:?}", err);
                }
            }
            sleep(Duration::from_millis(200));
        }
    }
}

impl AccountsDbReplService {
    pub fn new(
        last_replicated_slot: Slot,
        config: AccountsDbReplClientServiceConfig,
        replica_config: Arc<ReplicaNodeConfig>,
        bank_info: ReplicaNodeBankInfo,
    ) -> Result<Self, ReplicaRpcError> {
        let accountsdb_repl_client = AccountsDbReplClientService::new(config)?;
        let mut svc_impl = AccountsDbReplServiceImpl::new(
            accountsdb_repl_client,
            last_replicated_slot,
            replica_config,
            bank_info,
        );
        let thread = Builder::new()
            .name("sol-accountsdb-repl-svc".to_string())
            .spawn(move || {
                svc_impl.run_service();
            })
            .unwrap();
        Ok(Self { thread })
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread.join()
    }
}
