use {
    crate::{
        accountsdb_repl_server::{AccountsDbReplService, AccountsDbReplServiceConfig},
        replica_accounts_server::ReplicaAccountsServerImpl,
        replica_updated_slots_server::ReplicaUpdatedSlotsServerImpl,
    },
    crossbeam_channel::Receiver,
    solana_sdk::clock::Slot,
    std::sync::{Arc, RwLock},
};

pub struct AccountsDbReplServerFactory {}

impl AccountsDbReplServerFactory {
    pub fn build_accountsdb_repl_server(
        config: AccountsDbReplServiceConfig,
        confirmed_bank_receiver: Receiver<Slot>,
    ) -> AccountsDbReplService {
        AccountsDbReplService::new(
            config,
            Arc::new(RwLock::new(ReplicaUpdatedSlotsServerImpl::new(
                confirmed_bank_receiver,
            ))),
            Arc::new(RwLock::new(ReplicaAccountsServerImpl::new())),
        )
    }
}
