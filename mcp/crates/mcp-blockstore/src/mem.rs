use std::collections::{BTreeMap, HashMap};
use parking_lot::RwLock;
use mcp_types::BatchKey;
use super::{McpStore, CommitMeta};

#[derive(Default)]
pub struct MemStore {
    commit_meta: RwLock<HashMap<BatchKey, CommitMeta>>,
    reveals: RwLock<HashMap<BatchKey, BTreeMap<u32, (Vec<u8>, Vec<u8>, Vec<[u8;32]>)>>>,
}
impl MemStore { pub fn new() -> Self { Self::default() } }

impl McpStore for MemStore {
    fn put_commit_meta(&self, key: BatchKey, meta: CommitMeta) {
        self.commit_meta.write().insert(key, meta);
    }
    fn get_commit_meta(&self, key: &BatchKey) -> Option<CommitMeta> {
        self.commit_meta.read().get(key).cloned()
    }
    fn reveal_insert(&self, key: &BatchKey, index: u32, c: Vec<u8>, r: Vec<u8>, w: Vec<[u8;32]>) -> usize {
        let mut g = self.reveals.write();
        let entry = g.entry(key.clone()).or_default();
        entry.entry(index).or_insert((c, r, w));
        entry.len()
    }
    fn iter_reveals(&self, key: &BatchKey) -> Box<dyn Iterator<Item=(u32,(Vec<u8>,Vec<u8>,Vec<[u8;32]>))> + Send> {
        let snap = self.reveals.read().get(key).map(|m| m.iter().map(|(i,v)| (*i, v.clone())).collect::<Vec<_>>()).unwrap_or_default();
        Box::new(snap.into_iter())
    }
    fn reveal_count(&self, key: &BatchKey) -> usize {
        self.reveals.read().get(key).map(|m| m.len()).unwrap_or(0)
    }
}
