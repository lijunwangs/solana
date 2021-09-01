/// Module responsible for replicating AccountsDb data from its peer to its local AccountsDb in the replica-node
use {
    crate::replica_node::{ReplicaNodeBankInfo, ReplicaNodeConfig},
    bincode,
    log::*,
    solana_replica_lib::accountsdb_repl_client::{
        AccountsDbReplClientService, AccountsDbReplClientServiceConfig, ReplicaAccountInfo,
        ReplicaBankInfo, ReplicaRpcError,
    },
    solana_rpc::optimistically_confirmed_bank_tracker::BankNotification,
    solana_runtime::{
        accounts::Accounts,
        bank::{Bank, BankFieldsToDeserialize, BankRc},
        serde_snapshot::future::DeserializableVersionedBank,
    },
    solana_sdk::{
        account::{Account, AccountSharedData},
        clock::Slot,
        pubkey::Pubkey,
    },
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

    fn persist_accounts(bank: &Bank, accounts: &[ReplicaAccountInfo]) {
        for account in accounts.iter() {
            debug!(
                "Received account: {:?}",
                Pubkey::new(&account.account_meta.as_ref().unwrap().pubkey)
            );

            let meta_data = &account.account_meta.as_ref().unwrap();
            let account = Account {
                lamports: meta_data.lamports,
                owner: Pubkey::new(&meta_data.owner),
                executable: meta_data.executable,
                rent_epoch: meta_data.rent_epoch,
                data: account.data.as_ref().unwrap().data.clone(),
            };
            let account_data = AccountSharedData::from(account);
            let pubkey = Pubkey::new(&meta_data.pubkey);
            bank.rc
                .accounts
                .accounts_db
                .store_cached(meta_data.slot, &[(&pubkey, &account_data)]);
        }
    }

    fn replicate_accounts_for_slot(
        &mut self,
        bank: &Bank,
        slot: Slot,
    ) -> Result<(), ReplicaRpcError> {
        match self.accountsdb_repl_client.get_slot_accounts(slot) {
            Err(err) => {
                error!(
                    "Ran into an error getting accounts for slot {:?}, error: {:?}",
                    slot, err
                );
                Err(err)
            }
            Ok(accounts) => {
                Self::persist_accounts(bank, &accounts);
                Ok(())
            }
        }
    }

    fn create_bank(&self, bank_info: &ReplicaBankInfo) -> Bank {
        let deserializable_bank: DeserializableVersionedBank =
            bincode::deserialize(&bank_info.bank_data).unwrap();
        let bank_fields = BankFieldsToDeserialize::from(deserializable_bank);
        let parent = self.bank_info.bank_forks.read().unwrap().root_bank();

        let bank_rc = BankRc {
            accounts: Arc::new(Accounts::new_from_parent(
                &parent.rc.accounts,
                bank_info.slot,
                parent.slot(),
            )),
            parent: RwLock::new(Some(parent.clone())),
            slot: bank_info.slot,
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

        bank.set_drop_callback_via_parent(&parent);
        bank
    }

    fn replicate_bank(&mut self, slot: Slot) -> Result<Bank, ReplicaRpcError> {
        match self.accountsdb_repl_client.get_bank_info(slot) {
            Err(err) => {
                error!(
                    "Ran into an error getting bank for slot {:?}, error: {:?}",
                    slot, err
                );
                Err(err)
            }
            Ok(bank_info) => Ok(self.create_bank(&bank_info)),
        }
    }

    fn replicate_slot(&mut self, slot: Slot) -> Result<(), ReplicaRpcError> {
        let bank = self.replicate_bank(slot)?;
        self.replicate_accounts_for_slot(&bank, slot)?;

        bank.freeze();

        let mut bank_forks = self.bank_info.bank_forks.write().unwrap();
        bank_forks.insert(bank);
        bank_forks.set_root(
            slot,
            self.replica_config.abs_request_sender.as_ref().unwrap(),
            Some(slot),
        );
        drop(bank_forks);

        self.bank_info
            .bank_notification_sender
            .send(BankNotification::OptimisticallyConfirmed(slot))
            .unwrap_or_else(|err| warn!("bank_notification_sender failed: {:?}", err));

        Ok(())
    }

    pub fn run_service(&mut self) {
        match self
            .accountsdb_repl_client
            .get_diff_between_slot(self.last_replicated_slot, None)
        {
            Ok((latest_slot, bank_info, accounts)) => {
                let bank = self.create_bank(&bank_info);
                Self::persist_accounts(&bank, &accounts);
                self.last_replicated_slot = latest_slot;
            }
            Err(err) => {
                error!(
                    "Ran into an error getting updated slots: {:?} from base_slot: {:?}",
                    err, self.last_replicated_slot
                );
            }
        }

        loop {
            match self
                .accountsdb_repl_client
                .get_confirmed_slots(self.last_replicated_slot)
            {
                Ok(slots) => {
                    info!("Received updated slots: {:?}", slots);
                    if !slots.is_empty() {
                        for slot in slots.iter() {
                            match self.replicate_slot(*slot) {
                                Err(err) => {
                                    error!(
                                        "Ran into an error while replicaing slot {:?}, error: {:?}",
                                        slot, err
                                    );
                                    break;
                                }
                                Ok(_) => {
                                    trace!("Successfully replicated slot {:?}", slot);
                                }
                            }
                        }
                        self.last_replicated_slot = slots[slots.len() - 1];
                    }
                }
                Err(err) => {
                    error!("Ran into an error getting updated slots: {:?}", err);
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
