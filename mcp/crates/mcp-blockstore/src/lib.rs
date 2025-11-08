pub mod mem;
#[cfg(feature = "solana-ledger")]
pub mod solana;

use mcp_types::{BatchKey, CommitmentRoot};

pub trait McpStore: Send + Sync + 'static {
    fn put_commit_meta(&self, key: BatchKey, meta: CommitMeta);
    fn get_commit_meta(&self, key: &BatchKey) -> Option<CommitMeta>;
    fn reveal_insert(
        &self,
        key: &BatchKey,
        index: u32,
        coded_symbol: Vec<u8>,
        leaf_randomizer: Vec<u8>,
        merkle_path: Vec<[u8; 32]>,
    ) -> usize;
    fn iter_reveals(
        &self,
        key: &BatchKey,
    ) -> Box<dyn Iterator<Item = (u32, (Vec<u8>, Vec<u8>, Vec<[u8; 32]>))> + Send>;
    fn reveal_count(&self, key: &BatchKey) -> usize;
}

#[derive(Clone)]
pub struct CommitMeta {
    pub commitment_root: CommitmentRoot,
    pub finalized: bool,
    pub phi_ok: bool,
    pub min_symbols_to_decode: usize,
    pub adversary_tolerance: usize,
}
