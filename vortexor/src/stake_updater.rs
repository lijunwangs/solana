//! Module responsible for updating the staked key map.
//! Adapted from jito-replayer code.

use {
    crate::rpc_load_balancer::RpcLoadBalancer,
    log::warn,
    solana_client::client_error,
    solana_sdk::pubkey::Pubkey,
    solana_streamer::streamer::StakedNodes,
    std::{
        collections::HashMap,
        str::FromStr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::{self, sleep, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

// The interval to refresh the stake information.
const STAKE_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// This service is responsible for periodically refresh the stake information
/// from the network with the assistance of the RpcLoaderBalancer.
pub struct StakeUpdater {
    thread_hdl: JoinHandle<()>,
}

impl StakeUpdater {
    pub fn new(
        exit: Arc<AtomicBool>,
        rpc_load_balancer: Arc<RpcLoadBalancer>,
        shared_staked_nodes: Arc<RwLock<StakedNodes>>,
        staked_nodes_overrides: Arc<HashMap<Pubkey, u64>>,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("stkUpdtr".to_string())
            .spawn(move || {
                let mut last_stakes = Instant::now();
                while !exit.load(Ordering::Relaxed) {
                    let mut stake_map = Arc::new(HashMap::new());
                    match Self::try_refresh_stake_info(
                        &mut last_stakes,
                        &mut stake_map,
                        &rpc_load_balancer,
                    ) {
                        Ok(true) => {
                            let shared =
                                StakedNodes::new(stake_map, staked_nodes_overrides.clone());
                            *shared_staked_nodes.write().unwrap() = shared;
                        }
                        Ok(false) => {}
                        Err(err) => {
                            warn!("Failed to refresh pubkey to stake map! Error: {:?}", err);
                            sleep(STAKE_REFRESH_INTERVAL);
                        }
                    }
                }
            })
            .unwrap();

        Self { thread_hdl }
    }

    /// Update the stake info when it has elapsed more than the
    /// STAKE_REFRESH_INTERVAL since the last time it was refreshed.
    fn try_refresh_stake_info(
        last_stakes: &mut Instant,
        pubkey_stake_map: &mut Arc<HashMap<Pubkey, u64>>,
        rpc_load_balancer: &Arc<RpcLoadBalancer>,
    ) -> client_error::Result<bool> {
        if last_stakes.elapsed() > STAKE_REFRESH_INTERVAL {
            let client = rpc_load_balancer.rpc_client();
            let vote_accounts = client.get_vote_accounts()?;

            *pubkey_stake_map = Arc::new(
                vote_accounts
                    .current
                    .iter()
                    .chain(vote_accounts.delinquent.iter())
                    .filter_map(|vote_account| {
                        Some((
                            Pubkey::from_str(&vote_account.node_pubkey).ok()?,
                            vote_account.activated_stake,
                        ))
                    })
                    .collect(),
            );

            *last_stakes = Instant::now();
            Ok(true)
        } else {
            sleep(Duration::from_secs(1));
            Ok(false)
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
