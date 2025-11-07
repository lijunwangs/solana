#![cfg(feature = "solana-ledger")]
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use rocksdb::{Direction, IteratorMode};
use mcp_types::BatchKey;
use super::{McpStore, CommitMeta};

pub struct BlockstoreLike { pub db: rocksdb::DB }

#[derive(Serialize, Deserialize, Clone, Debug)]
struct MetaSer { C: [u8;32], finalized: bool, phi_ok: bool, K: u16, T: u16 }
#[derive(Serialize, Deserialize, Clone, Debug)]
struct LeafSer { index: u32, c_i: Vec<u8>, r_i: Vec<u8>, w_i: Vec<[u8;32]> }

const CF_META: &str = "McpCommitMeta";
const CF_LEAF: &str = "McpRevealIndex";

pub struct SolanaStore { pub db: Arc<BlockstoreLike> }
impl SolanaStore {
    pub fn new(db: Arc<BlockstoreLike>) -> Self { Self { db } }
    fn key_prefix(key: &BatchKey) -> Vec<u8> {
        let mut out = Vec::with_capacity(8+32+4);
        out.extend_from_slice(&key.slot.to_le_bytes());
        out.extend_from_slice(&key.proposer);
        out.extend_from_slice(&key.batch_id.to_le_bytes());
        out
    }
    fn key_meta(key: &BatchKey) -> Vec<u8> { Self::key_prefix(key) }
    fn key_leaf(key: &BatchKey, index: u32) -> Vec<u8> { let mut k = Self::key_prefix(key); k.extend_from_slice(&index.to_le_bytes()); k }
    fn key_count(key: &BatchKey) -> Vec<u8> { let mut k = Self::key_prefix(key); k.extend_from_slice(&[0xFF,0xFF,0xFF,0xFF]); k }
}
impl McpStore for SolanaStore {
    fn put_commit_meta(&self, key: BatchKey, meta: CommitMeta) {
        let cf = self.db.db.cf_handle(CF_META).unwrap();
        let ser = MetaSer { C: meta.C.0, finalized: meta.finalized, phi_ok: meta.phi_ok, K: meta.K as u16, T: meta.T as u16 };
        self.db.db.put_cf(cf, Self::key_meta(&key), bincode::serialize(&ser).unwrap()).unwrap();
    }
    fn get_commit_meta(&self, key: &BatchKey) -> Option<CommitMeta> {
        let cf = self.db.db.cf_handle(CF_META).unwrap();
        let v = self.db.db.get_cf(cf, Self::key_meta(key)).unwrap()?;
        let ser: MetaSer = bincode::deserialize(&v).ok()?;
        Some(CommitMeta { C: mcp_types::CommitmentRoot(ser.C), finalized: ser.finalized, phi_ok: ser.phi_ok, K: ser.K as usize, T: ser.T as usize })
    }
    fn reveal_insert(&self, key: &BatchKey, index: u32, c: Vec<u8>, r: Vec<u8>, w: Vec<[u8;32]>) -> usize {
        let cf = self.db.db.cf_handle(CF_LEAF).unwrap();
        let k_leaf = Self::key_leaf(key, index);
        if self.db.db.get_cf(cf, &k_leaf).unwrap().is_none() {
            let val = bincode::serialize(&LeafSer { index, c_i: c, r_i: r, w_i: w }).unwrap();
            self.db.db.put_cf(cf, &k_leaf, val).unwrap();
            let k_cnt = Self::key_count(key);
            let old = self.db.db.get_cf(cf, &k_cnt).unwrap().map(|v| bincode::deserialize::<u32>(&v).unwrap_or(0)).unwrap_or(0);
            let new = old.saturating_add(1);
            self.db.db.put_cf(cf, &k_cnt, bincode::serialize(&new).unwrap()).unwrap();
        }
        self.reveal_count(key)
    }
    fn iter_reveals(&self, key: &BatchKey) -> Box<dyn Iterator<Item=(u32,(Vec<u8>,Vec<u8>,Vec<[u8;32]>))> + Send> {
        let cf = self.db.db.cf_handle(CF_LEAF).unwrap();
        let prefix = Self::key_prefix(key);
        let iter = self.db.db.iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));
        let mut out = Vec::new();
        for kv in iter {
            let (k, v) = kv.unwrap();
            if !k.starts_with(&prefix) { break; }
            if &k[k.len()-4..] == [0xFF,0xFF,0xFF,0xFF] { continue; }
            let leaf: LeafSer = bincode::deserialize(&v).unwrap();
            out.push((leaf.index, (leaf.c_i, leaf.r_i, leaf.w_i)));
        }
        out.sort_by_key(|(i, _)| *i);
        Box::new(out.into_iter())
    }
    fn reveal_count(&self, key: &BatchKey) -> usize {
        let cf = self.db.db.cf_handle(CF_LEAF).unwrap();
        let k_cnt = Self::key_count(key);
        self.db.db.get_cf(cf, &k_cnt).unwrap().map(|v| bincode::deserialize::<u32>(&v).unwrap_or(0) as usize).unwrap_or(0)
    }
}
