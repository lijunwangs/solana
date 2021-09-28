/// The interface for AccountsDb plugins. A plugin must implement
/// the AccountsDbPlugin trait to work with the Solana Validator.
/// In addition, the dynamic libraray must export a "C" function _create_plugin which
/// creates the implementation of the plugin.
use {std::any::Any, std::io, thiserror::Error};

#[derive(Clone, PartialEq, Default, Debug)]
pub struct ReplicaAccountMeta<'a> {
    pub pubkey: &'a [u8],
    pub lamports: u64,
    pub owner: &'a [u8],
    pub executable: bool,
    pub rent_epoch: u64,
}

impl Eq for ReplicaAccountInfo<'_> {}

#[derive(Clone, PartialEq, Debug)]
pub struct ReplicaAccountInfo<'a> {
    pub account_meta: ReplicaAccountMeta<'a>,
    pub data: &'a [u8],
}

#[derive(Error, Debug)]
pub enum AccountsDbPluginError {
    #[error("Error opening config file.")]
    ConfigFileOpenError(#[from] io::Error),

    #[error("Error reading config file.")]
    ConfigFileReadError { msg: String },

    #[error("Error connecting to the backend data store.")]
    DataStoreConnectionError { msg: String },

    #[error("Error preparing data store schema.")]
    DataSchemaError { msg: String },

    #[error("Error updating account.")]
    AccountsUpdateError { msg: String },
}

#[derive(Debug, Clone)]
pub enum SlotStatus {
    Processed,
    Rooted,
    Confirmed,
}

pub type Result<T> = std::result::Result<T, AccountsDbPluginError>;

pub trait AccountsDbPlugin: Any + Send + Sync + std::fmt::Debug {
    fn name(&self) -> &'static str;

    /// The callback called when a plugin is loaded by the system
    /// Used for doing whatever initialization by the plugin
    /// The _config_file points to the file name contains the name of the
    /// of the config file. The config shall be in JSON format and
    /// it must has a field named "libpath" pointing to the full path
    /// name of the shared library implementing this interface.
    fn on_load(&mut self, _config_file: &str) -> Result<()> {
        Ok(())
    }

    /// The callback called right before a plugin is unloaded by the system
    /// Used for doing cleanup before unload.
    fn on_unload(&mut self) {}

    /// Called when an account is updated at a slot.
    fn update_account(&mut self, account: &ReplicaAccountInfo, slot: u64) -> Result<()>;

    /// Called when a slot status is updated
    fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> Result<()>;
}
