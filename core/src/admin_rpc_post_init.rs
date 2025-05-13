use {
    crate::{
        cluster_slots_service::cluster_slots::ClusterSlots,
        repair::{outstanding_requests::OutstandingRequests, serve_repair::ShredRepairType},
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_pubkey::Pubkey,
    solana_quic_definitions::NotifyKeyUpdate,
    solana_runtime::bank_forks::BankForks,
    std::{
        collections::{HashMap, HashSet},
        net::UdpSocket,
        sync::{Arc, RwLock},
    },
};

#[derive(Default)]
pub struct KeyNotifiers {
    pub notifies: HashMap<String, Arc<dyn NotifyKeyUpdate + Sync + Send>>,
}

impl KeyNotifiers {
    pub fn add(&mut self, key: String, notify: Arc<dyn NotifyKeyUpdate + Sync + Send>) {
        self.notifies.insert(key, notify);
    }

    pub fn get(&self, key: &str) -> Option<Arc<dyn NotifyKeyUpdate + Sync + Send>> {
        self.notifies.get(key).cloned()
    }

    pub fn remove(&mut self, key: &str) {
        self.notifies.remove(key);
    }

    // pub fn iter(&self) -> impl Iterator<Item = (&String, &Arc<dyn NotifyKeyUpdate + Sync + Send>)> {
    //     self.notifies.iter()
    // }
}

/// Implement the Iterator trait for KeyNotifiers
impl<'a> IntoIterator for &'a KeyNotifiers {
    type Item = (&'a String, &'a Arc<dyn NotifyKeyUpdate + Sync + Send>);
    type IntoIter =
        std::collections::hash_map::Iter<'a, String, Arc<dyn NotifyKeyUpdate + Sync + Send>>;

    fn into_iter(self) -> Self::IntoIter {
        self.notifies.iter()
    }
}

#[derive(Clone)]
pub struct AdminRpcRequestMetadataPostInit {
    pub cluster_info: Arc<ClusterInfo>,
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub vote_account: Pubkey,
    pub repair_whitelist: Arc<RwLock<HashSet<Pubkey>>>,
    pub notifies: Arc<RwLock<KeyNotifiers>>,
    pub repair_socket: Arc<UdpSocket>,
    pub outstanding_repair_requests: Arc<RwLock<OutstandingRequests<ShredRepairType>>>,
    pub cluster_slots: Arc<ClusterSlots>,
}
