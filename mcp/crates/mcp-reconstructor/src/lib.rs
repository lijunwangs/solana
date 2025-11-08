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
        let min_relay_attestations = (self.params.mu * self.params.n_relays as f32).ceil() as usize;
        assert!(payload.relay_attestations.len() >= min_relay_attestations, "µ-threshold failed");

        let mut per_key_relays: HashMap<BatchKey, HashSet<[u8;32]>> = HashMap::new();
        let mut per_key_commitment: HashMap<BatchKey, CommitmentRoot> = HashMap::new();

        for RelayAttestation { relay, entries, .. } in &payload.relay_attestations {
            for (key, commitment_root, _sigma_q) in entries {
                per_key_relays.entry(key.clone()).or_default().insert(*relay);
                per_key_commitment.entry(key.clone()).and_modify(|existing| assert_eq!(existing.0, commitment_root.0, "conflicting commitment root"))
                                  .or_insert_with(|| commitment_root.clone());
            }
        }

        let mut available = HashSet::new();
        let per_batch_quorum = (self.params.phi * self.params.n_relays as f32).ceil() as usize;
        let (min_symbols_to_decode, adversary_tolerance) = self.params.k_t();

        for (key, relays) in per_key_relays {
            let phi_ok = relays.len() >= per_batch_quorum;
            if phi_ok {
                let meta = StoreCommitMeta {
                    commitment_root: per_key_commitment[&key].clone(),
                    finalized: true, phi_ok,
                    min_symbols_to_decode,
                    adversary_tolerance,
                };
                self.store.put_commit_meta(key.clone(), meta);
                available.insert(key);
            }
        }
        available
    }

    pub fn on_reveal(&mut self, rs: RevealShred) -> bool {
        let Some(meta) = self.store.get_commit_meta(&rs.key) else { return false; };
        if !meta.finalized || !meta.phi_ok { return false; }
        let commitment_root = rs.opt_commitment_root.unwrap_or_else(|| meta.commitment_root.clone());

        if !V::verify(&commitment_root, rs.index as usize, &rs.coded_symbol, &rs.leaf_randomizer, &rs.merkle_path) {
            return false;
        }

        let count = self.store.reveal_insert(&rs.key, rs.index, rs.coded_symbol, rs.leaf_randomizer, rs.merkle_path);
        count >= (meta.min_symbols_to_decode + meta.adversary_tolerance)
    }

    pub fn reconstruct_bytes(&self, key: &BatchKey) -> Option<Vec<u8>> {
        let meta = self.store.get_commit_meta(key)?;
        if self.store.reveal_count(key) < (meta.min_symbols_to_decode + meta.adversary_tolerance) { return None; }
        let mut points: Vec<(u8, Vec<u8>)> = Vec::new();
        for (ix, (coded_symbol, _leaf_randomizer, _merkle_path)) in self.store.iter_reveals(key) {
            let x = (ix as u8) + 1;
            points.push((x, coded_symbol));
            if points.len() == 2 { break; } // degree-1 demo
        }
        if points.len() < 2 { return None; }
        let (coeff0, coeff1) = mcp_hecc_mock::decode_degree1(&points);
        let mut out = coeff0; out.extend_from_slice(&coeff1);
        Some(out)
    }
}
