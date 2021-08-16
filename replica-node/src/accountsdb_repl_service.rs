/// Module responsible for replicating AccountsDb data from its peer to its local AccountsDb
use {
    log::*,
    solana_replica_lib::accountsdb_repl_client::{
        AccountsDbReplClientService, AccountsDbReplClientServiceConfig, ReplicaRpcError,
    },
    solana_sdk::clock::Slot,
    std::{
        thread::{self, sleep, Builder, JoinHandle},
        time::Duration,
    },
};

pub struct AccountsDbReplService {
    thread: JoinHandle<()>,
}

impl AccountsDbReplService {
    pub fn new(
        last_replicated_slot: Slot,
        config: AccountsDbReplClientServiceConfig,
    ) -> Result<Self, ReplicaRpcError> {
        let accountsdb_repl_client = AccountsDbReplClientService::new(config)?;
        let thread = Builder::new()
            .name("sol-accountsdb-repl-svc".to_string())
            .spawn(move || {
                Self::run_service(last_replicated_slot, accountsdb_repl_client);
            })
            .unwrap();
        Ok(Self { thread })
    }

    fn run_service(
        mut last_replicated_slot: Slot,
        mut accountsdb_repl_client: AccountsDbReplClientService,
    ) {
        loop {
            match accountsdb_repl_client.get_updated_slots(last_replicated_slot) {
                Ok(slots) => {
                    info!("Received updated slots: {:?}", slots);
                    if !slots.is_empty() {
                        last_replicated_slot = slots[slots.len() - 1];
                    }
                }
                Err(err) => {
                    error!("Ran into error getting updated slots: {:?}", err);
                }
            }
            sleep(Duration::from_millis(200));
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread.join()
    }
}
