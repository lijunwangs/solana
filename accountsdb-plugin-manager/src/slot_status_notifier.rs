use {
    crate::accountsdb_plugin_manager::AccountsDbPluginManager,
    log::*,
    solana_accountsdb_plugin_interface::accountsdb_plugin_interface::SlotStatus,
    solana_ledger::blockstore::Blockstore,
    solana_measure::measure::Measure,
    solana_metrics::*,
    solana_sdk::clock::Slot,
    std::sync::{Arc, RwLock},
};

pub trait SlotStatusNotifierInterface {
    /// Callback to set the blockstore
    fn set_blockstore(&mut self, blockstore: Arc<Blockstore>);

    /// Notified when a slot is optimistically confirmed
    fn notify_slot_confirmed(&self, slot: Slot, parent: Option<Slot>);

    /// Notified when a slot is marked frozen.
    fn notify_slot_processed(&self, slot: Slot, parent: Option<Slot>);

    /// Notified when a slot is rooted.
    fn notify_slot_rooted(&self, slot: Slot, parent: Option<Slot>);
}

pub type SlotStatusNotifier = Arc<RwLock<dyn SlotStatusNotifierInterface + Sync + Send>>;

pub struct SlotStatusNotifierImpl {
    plugin_manager: Arc<RwLock<AccountsDbPluginManager>>,
    blockstore: Option<Arc<Blockstore>>,
}

impl SlotStatusNotifierInterface for SlotStatusNotifierImpl {
    fn set_blockstore(&mut self, blockstore: Arc<Blockstore>) {
        self.blockstore = Some(blockstore);
    }

    fn notify_slot_confirmed(&self, slot: Slot, parent: Option<Slot>) {
        self.notify_slot_status(slot, parent, SlotStatus::Confirmed);
    }

    fn notify_slot_processed(&self, slot: Slot, parent: Option<Slot>) {
        self.notify_slot_status(slot, parent, SlotStatus::Processed);
    }

    fn notify_slot_rooted(&self, slot: Slot, parent: Option<Slot>) {
        self.notify_slot_status(slot, parent, SlotStatus::Rooted);
    }
}

impl SlotStatusNotifierImpl {
    pub fn new(plugin_manager: Arc<RwLock<AccountsDbPluginManager>>) -> Self {
        Self {
            plugin_manager,
            blockstore: None,
        }
    }

    pub fn notify_slot_status(&self, slot: Slot, parent: Option<Slot>, slot_status: SlotStatus) {
        let mut plugin_manager = self.plugin_manager.write().unwrap();
        if plugin_manager.plugins.is_empty() {
            return;
        }

        for plugin in plugin_manager.plugins.iter_mut() {
            let mut measure = Measure::start("accountsdb-plugin-update-slot");
            if slot_status == SlotStatus::Confirmed {
                if let Some(blockstore) = &self.blockstore {
                    let confirmed_block = blockstore.get_complete_block(slot, true);
                    info!(
                        "Received confirmed block {:?} for slot {}",
                        confirmed_block.map(|block| block.blockhash), slot
                    );
                }
            }
            match plugin.update_slot_status(slot, parent, slot_status.clone()) {
                Err(err) => {
                    error!(
                        "Failed to update slot status at slot {}, error: {} to plugin {}",
                        slot,
                        err,
                        plugin.name()
                    )
                }
                Ok(_) => {
                    trace!(
                        "Successfully updated slot status at slot {} to plugin {}",
                        slot,
                        plugin.name()
                    );
                }
            }
            measure.stop();
            inc_new_counter_debug!(
                "accountsdb-plugin-update-slot-us",
                measure.as_us() as usize,
                1000,
                1000
            );
        }
    }
}
