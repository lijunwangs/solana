use std::collections::{BTreeMap, HashMap};
use mcp_types::{BatchKey, CommitmentRoot};

#[derive(Default)]
pub struct Blockstore {
    commit_meta: HashMap<BatchKey, CommitMeta>,
    reveals: HashMap<BatchKey, RevealIndex>,
}

#[derive(Clone)]
pub struct CommitMeta {
    pub C: CommitmentRoot,
    pub finalized: bool,
    pub phi_ok: bool,
    pub K: usize,
    pub T: usize,
}

#[derive(Default, Clone)]
pub struct RevealIndex {
    pub present: BTreeMap<u32, (Vec<u8>, Vec<u8>, Vec<[u8;32]>)>,
}

impl Blockstore {
    pub fn put_commit_meta(&mut self, key: BatchKey, meta: CommitMeta) { self.commit_meta.insert(key, meta); }
    pub fn get_commit_meta(&self, key: &BatchKey) -> Option<&CommitMeta> { self.commit_meta.get(key) }
    pub fn reveal_insert(&mut self, key: &BatchKey, index: u32, c: Vec<u8>, r: Vec<u8>, w: Vec<[u8;32]>) -> usize {
        let idx = self.reveals.entry(key.clone()).or_default();
        idx.present.insert(index, (c,r,w));
        idx.present.len()
    }
    pub fn get_reveals(&self, key: &BatchKey) -> Option<&RevealIndex> { self.reveals.get(key) }
}
