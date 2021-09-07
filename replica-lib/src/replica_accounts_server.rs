use {
    crate::{
        accountsdb_repl_server::{
            self, ReplicaAccountData, ReplicaAccountInfo, ReplicaAccountMeta,
            ReplicaAccountsServer, ReplicaBankInfo, ReplicaBankInfoRequest,
            ReplicaBankInfoResponse, ReplicaDiffBetweenSlotsRequest,
            ReplicaDiffBetweenSlotsResponse,
        },
        replica_confirmed_slots_server::ReplicaSlotConfirmationServerImpl,
    },
    bincode,
    log::*,
    solana_runtime::{
        accounts_cache::CachedAccount, accounts_db::LoadedAccount, append_vec::StoredAccountMeta,
        bank_forks::BankForks, serde_snapshot::future::SerializableVersionedBank,
    },
    solana_sdk::{account::Account, clock::Slot},
    std::{
        cmp::Eq,
        collections::HashMap,
        sync::{Arc, RwLock},
        thread,
    },
};

pub(crate) struct ReplicaAccountsServerImpl {
    confirmed_slots_server: Arc<RwLock<ReplicaSlotConfirmationServerImpl>>,
    bank_forks: Arc<RwLock<BankForks>>,
}

impl Eq for ReplicaAccountInfo {}

impl ReplicaAccountInfo {
    fn from_stored_account_meta(slot: Slot, stored_account_meta: &StoredAccountMeta) -> Self {
        let account_meta = Some(ReplicaAccountMeta {
            pubkey: stored_account_meta.meta.pubkey.to_bytes().to_vec(),
            lamports: stored_account_meta.account_meta.lamports,
            owner: stored_account_meta.account_meta.owner.to_bytes().to_vec(),
            executable: stored_account_meta.account_meta.executable,
            rent_epoch: stored_account_meta.account_meta.rent_epoch,
            slot,
        });
        let data = Some(ReplicaAccountData {
            data: stored_account_meta.data.to_vec(),
        });
        ReplicaAccountInfo {
            account_meta,
            hash: stored_account_meta.hash.0.to_vec(),
            data,
        }
    }

    fn from_cached_account(slot: Slot, cached_account: &CachedAccount) -> Self {
        let account = Account::from(cached_account.account.clone());
        let account_meta = Some(ReplicaAccountMeta {
            pubkey: cached_account.pubkey().to_bytes().to_vec(),
            lamports: account.lamports,
            owner: account.owner.to_bytes().to_vec(),
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            slot,
        });
        let data = Some(ReplicaAccountData {
            data: account.data.to_vec(),
        });
        ReplicaAccountInfo {
            account_meta,
            hash: cached_account.hash().0.to_vec(),
            data,
        }
    }
}

impl ReplicaAccountsServer for ReplicaAccountsServerImpl {
    fn get_slot_accounts(
        &self,
        request: &accountsdb_repl_server::ReplicaAccountsRequest,
    ) -> Result<accountsdb_repl_server::ReplicaAccountsResponse, tonic::Status> {
        let slot = request.slot;

        match self.bank_forks.read().unwrap().get(slot) {
            None => Err(tonic::Status::not_found("The slot is not found")),
            Some(bank) => {
                let accounts = bank.rc.accounts.scan_slot(slot, |account| match account {
                    LoadedAccount::Stored(stored_account_meta) => Some(
                        ReplicaAccountInfo::from_stored_account_meta(slot, &stored_account_meta),
                    ),
                    LoadedAccount::Cached((_pubkey, cached_account)) => Some(
                        ReplicaAccountInfo::from_cached_account(slot, &cached_account),
                    ),
                });

                Ok(accountsdb_repl_server::ReplicaAccountsResponse { accounts })
            }
        }
    }

    fn get_bank_info(
        &self,
        request: &ReplicaBankInfoRequest,
    ) -> Result<ReplicaBankInfoResponse, tonic::Status> {
        let slot = request.slot;
        match self.bank_forks.read().unwrap().get(slot) {
            None => Err(tonic::Status::not_found("The slot is not found")),
            Some(bank) => {
                let bank_info = Self::get_replica_bank_info(bank);
                Ok(accountsdb_repl_server::ReplicaBankInfoResponse { bank_info })
            }
        }
    }

    fn get_diff_between_slots(
        &self,
        request: &ReplicaDiffBetweenSlotsRequest,
    ) -> Result<ReplicaDiffBetweenSlotsResponse, tonic::Status> {
        let mut latest_slot = request.latest_slot;
        if latest_slot == 0 {
            let confirmed_slots_server = self.confirmed_slots_server.read().unwrap();
            if let Some(slot) = confirmed_slots_server.get_latest_confirmed_slot() {
                latest_slot = slot;
            } else {
                return Err(tonic::Status::not_found(
                    "Could not determine the latest slot",
                ));
            }
        }

        match self.bank_forks.read().unwrap().get(latest_slot) {
            None => {
                return Err(tonic::Status::not_found(format!(
                    "The slot {:?} is not found",
                    latest_slot
                )));
            }
            Some(bank) => {
                info!("zzzzz is frozen: {:?} for slot {:?}", bank.is_frozen(), bank.slot());
                let bank_info = Self::get_replica_bank_info(bank);
                //bank.squash();
                //bank.force_flush_accounts_cache();
                //bank.clean_accounts(true, false, Some(request.base_slot));
                //bank.update_accounts_hash();
                //bank.rehash();
                let storages = bank.get_snapshot_storages(Some(request.base_slot));

                let mut accounts = Vec::new();
                info!("zzzzz storages between {:?} {:?} {:?}", bank.slot(), request.base_slot, storages.len());
                for storage in storages {
                    for store in storage {
                        for account in store.all_accounts() {
                            accounts.push(ReplicaAccountInfo::from_stored_account_meta(
                                store.slot(),
                                &account,
                            ));
                        }
                    }
                }

                let response = accountsdb_repl_server::ReplicaDiffBetweenSlotsResponse {
                    latest_slot,
                    bank_info,
                    accounts,
                };
                return Ok(response);
            }
        }
    }

    fn join(&mut self) -> thread::Result<()> {
        Ok(())
    }
}

impl ReplicaAccountsServerImpl {
    pub fn new(
        confirmed_slots_server: Arc<RwLock<ReplicaSlotConfirmationServerImpl>>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> Self {
        Self {
            confirmed_slots_server,
            bank_forks,
        }
    }

    fn get_replica_bank_info(bank: &Arc<solana_runtime::bank::Bank>) -> Option<ReplicaBankInfo> {
        let ancestors = HashMap::default();
        let bank_fields = bank.get_fields_to_serialize(&ancestors);
        let serializable_bank = SerializableVersionedBank::from(bank_fields);
        let bank_info = Some(ReplicaBankInfo {
            slot: bank.slot(),
            bank_data: bincode::serialize(&serializable_bank).unwrap(),
        });
        bank_info
    }
}
