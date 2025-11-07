use std::collections::{HashMap, HashSet};
use mcp_types::{BatchKey, CommitmentRoot, Params};
use mcp_wire::{LeaderBlockPayload, RevealShred, RelayAttestation};
use mcp_blockstore::{Blockstore, CommitMeta};
use mcp_crypto_vc::VectorCommitment;
use mcp_hecc_mock::decode_degree1;

pub struct Reconstructor<V: VectorCommitment> {
    pub params: Params,
    pub bs: Blockstore,
    pub _vc: std::marker::PhantomData<V>,
}

impl<V: VectorCommitment> Reconstructor<V> {
    pub fn new(params: Params) -> Self { Self { params, bs: Blockstore::default(), _vc: Default::default() } }

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
                let meta = CommitMeta { C: per_key_C[&key].clone(), finalized: true, phi_ok, K, T };
                self.bs.put_commit_meta(key.clone(), meta);
                available.insert(key);
            }
        }
        available
    }

    pub fn on_reveal(&mut self, rs: RevealShred) -> bool {
        let Some(meta) = self.bs.get_commit_meta(&rs.key) else { return false; };
        if !meta.finalized || !meta.phi_ok { return false; }
        let C = rs.opt_commitment.unwrap_or_else(|| meta.C.clone());

        if !V::verify(&C, rs.index as usize, &rs.c_i, &rs.r_i, &rs.w_i) {
            return false;
        }

        let count = self.bs.reveal_insert(&rs.key, rs.index, rs.c_i, rs.r_i, rs.w_i);
        count >= (meta.K + meta.T)
    }

    pub fn reconstruct_bytes(&self, key: &BatchKey) -> Option<Vec<u8>> {
        let meta = self.bs.get_commit_meta(key)?;
        let reveals = self.bs.get_reveals(key)?;
        if reveals.present.len() < (meta.K + meta.T) { return None; }
        // Collect first two distinct indices to interpolate (degree-1).
        let mut pts: Vec<(u8, Vec<u8>)> = Vec::new();
        for (ix, (c_i, _r_i, _w_i)) in reveals.present.iter() {
            let x = (*ix as u8) + 1; // x = index+1
            pts.push((x, c_i.clone()));
            if pts.len() == 2 { break; }
        }
        if pts.len() < 2 { return None; }
        let (a0, a1) = decode_degree1(&pts);
        let mut out = Vec::new();
        out.extend_from_slice(&a0);
        out.extend_from_slice(&a1);
        Some(out)
    }
}
