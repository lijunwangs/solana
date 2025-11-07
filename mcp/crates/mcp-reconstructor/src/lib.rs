use std::collections::{HashMap, HashSet};
use mcp_types::{BatchKey, CommitmentRoot, Params};
use mcp_wire::{LeaderBlockPayload, RevealShred, RelayAttestation};
use mcp_blockstore::{McpStore, CommitMeta as StoreCommitMeta};
use mcp_crypto_vc::VectorCommitment;

pub struct Reconstructor<V: VectorCommitment, S: McpStore> {
    pub params: Params,
    pub store: S,
    pub _vc: std::marker::PhantomData<V>,
}

impl<V: VectorCommitment, S: McpStore> Reconstructor<V, S> {
    pub fn new(params: Params, store: S) -> Self { Self { params, store, _vc: Default::default() } }

    pub fn on_block_finalized(&mut self, payload: &LeaderBlockPayload) -> HashSet<BatchKey> {
        let need_mu = (self.params.mu * self.params.n_relays as f32).ceil() as usize;
        assert!(payload.relay_attestations.len() >= need_mu, "µ-threshold failed");

        let mut per_key_relays: HashMap<BatchKey, HashSet<[u8;32]>> = HashMap::new();
        let mut per_key_C: HashMap<BatchKey, CommitmentRoot> = HashMap::new();

        for RelayAttestation { relay, entries, .. } in &payload.relay_attestations {
            for (key, C, _sigma_q) in entries {
                per_key_relays.entry(key.clone()).or_default().insert(*relay);
                per_key_C.entry(key.clone()).and_modify(|c| assert_eq!(c.0, C.0, "conflicting C"))
                          .or_insert_with(|| C.clone());
            }
        }

        let mut available = HashSet::new();
        let need_phi = (self.params.phi * self.params.n_relays as f32).ceil() as usize;
        let (K, T) = self.params.k_t();

        for (key, relays) in per_key_relays {
            let phi_ok = relays.len() >= need_phi;
            if phi_ok {
                let meta = StoreCommitMeta { C: per_key_C[&key].clone(), finalized: true, phi_ok, K, T };
                self.store.put_commit_meta(key.clone(), meta);
                available.insert(key);
            }
        }
        available
    }

    pub fn on_reveal(&mut self, rs: RevealShred) -> bool {
        let Some(meta) = self.store.get_commit_meta(&rs.key) else { return false; };
        if !meta.finalized || !meta.phi_ok { return false; }
        let C = rs.opt_commitment.unwrap_or_else(|| meta.C.clone());

        if !V::verify(&C, rs.index as usize, &rs.c_i, &rs.r_i, &rs.w_i) {
            return false;
        }

        let count = self.store.reveal_insert(&rs.key, rs.index, rs.c_i, rs.r_i, rs.w_i);
        count >= (meta.K + meta.T)
    }

    pub fn reconstruct_bytes(&self, key: &BatchKey) -> Option<Vec<u8>> {
        let meta = self.store.get_commit_meta(key)?;
        if self.store.reveal_count(key) < (meta.K + meta.T) { return None; }
        let mut pts: Vec<(u8, Vec<u8>)> = Vec::new();
        for (ix, (c_i, _r_i, _w_i)) in self.store.iter_reveals(key) {
            let x = (ix as u8) + 1;
            pts.push((x, c_i));
            if pts.len() == 2 { break; }
        }
        if pts.len() < 2 { return None; }
        let (a0, a1) = mcp_hecc_mock::decode_degree1(&pts);
        let mut out = a0; out.extend_from_slice(&a1);
        Some(out)
    }
}
