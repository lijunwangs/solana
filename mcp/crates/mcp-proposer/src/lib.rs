// mcp-proposer/src/lib.rs
use mcp_wire::{EncryptedReveal, FinalKeyCapsule, RelayAttestation};
use {
    blsttc::PublicKeySet,
    crossbeam_channel::{Receiver, Sender},
    mcp_crypto_te as te,
    mcp_crypto_vc::{MerkleVC, VectorCommitment},
    mcp_types::{BatchKey, CommitmentRoot, Params, ProposerId},
    solana_transaction::versioned::VersionedTransaction,
};

mod batcher;
mod encode;
mod network;
mod te_wrap;

pub struct ProposerConfig {
    pub params: Params,
    pub pk_set: PublicKeySet,
    pub relayers: Vec<([u8; 32], std::net::SocketAddr)>, // (relay_id, udp addr)
    pub batch_max_txs: usize,
    pub batch_max_bytes: usize,
    pub batch_max_millis: u64,
}

pub struct Proposer {
    pub id: ProposerId,
    cfg: ProposerConfig,
    tx_relay_encrypted: Sender<EncryptedReveal>,
    tx_attest: Sender<RelayAttestation>,
    tx_capsule: Sender<FinalKeyCapsule>,
}

impl Proposer {
    pub fn new(
        id: ProposerId,
        cfg: ProposerConfig,
        tx_relay_encrypted: Sender<EncryptedReveal>,
        tx_attest: Sender<RelayAttestation>,
        tx_capsule: Sender<FinalKeyCapsule>,
    ) -> Self {
        Self {
            id,
            cfg,
            tx_relay_encrypted,
            tx_attest,
            tx_capsule,
        }
    }

    /// Main loop: consume verified txs, build batches, encode+commit+encrypt, emit.
    pub fn run(
        &self,
        rx_verified_txs: Receiver<VersionedTransaction>,
        slot: u64,
        proposer_index_nonce: u32,
    ) {
        let mut batcher = crate::batcher::Batcher::new(
            self.cfg.batch_max_txs,
            self.cfg.batch_max_bytes,
            std::time::Duration::from_millis(self.cfg.batch_max_millis),
        );

        let sym_key = te::sym_keygen();

        while let Ok(vtx) = rx_verified_txs.recv() {
            if let Some(batch) = batcher.push_and_maybe_cut(vtx) {
                // 1) Serialize batch into bytes
                let batch_bytes = crate::encode::serialize_batch(&batch);

                // 2) HECC encode: produces coded symbols
                let (coded_symbols, eval_points) = crate::encode::hecc_degree1_encode(&batch_bytes);

                // 3) VC commit over coded symbol bytes
                let (commitment_root, leaf_randomizers, merkle_paths) =
                    crate::encode::vc_commit::<MerkleVC>(&coded_symbols);

                // 4) TE symmetric encrypt each coded symbol (AAD binds identity)
                let batch_key = BatchKey {
                    slot,
                    proposer: self.id,
                    batch_id: proposer_index_nonce, // or increment per batch
                };
                for (j, symbol) in coded_symbols.iter().enumerate() {
                    let aad = crate::encode::make_aad(&batch_key, j as u32, &commitment_root);
                    let encrypted_symbol =
                        te_wrap::sym_encrypt_with_aad(&sym_key, symbol, &aad).expect("aes-gcm");

                    // 5) Emit EncryptedReveal to relayer set (any policy)
                    for (relay_id, addr) in &self.cfg.relayers {
                        let msg = EncryptedReveal {
                            key: batch_key.clone(),
                            index: j as u32,
                            encrypted_symbol: encrypted_symbol.clone(), // {nonce, ciphertext}
                            leaf_randomizer: leaf_randomizers[j].clone(),
                            merkle_path: merkle_paths[j].clone(),
                            opt_commitment_root: Some(commitment_root.clone()),
                            relay_id: *relay_id,
                        };
                        let _ = self.tx_relay_encrypted.send(msg);
                        // network.rs will UDP-send it to *addr*
                    }
                }

                // 6) Build FinalKeyCapsule = TE encrypt (tail || sym_key) once per block/slot
                let tail_bytes: Vec<u8> = crate::encode::tail_bytes(); // can be empty at POC
                let final_payload = te::pack_final_batch(&tail_bytes, &sym_key);
                let threshold_ciphertext = te::te_encrypt_final(&self.cfg.pk_set, &final_payload);

                let capsule = FinalKeyCapsule {
                    slot,
                    proposer: self.id,
                    threshold_ciphertext: threshold_ciphertext.0, // serialized blsttc::Ciphertext
                                                                  // optional: include descriptor for (batches, commitment roots)
                };
                let _ = self.tx_capsule.send(capsule);

                // 7) Build proposer->leader attestation stub (one per relay if required)
                let entries = vec![(batch_key.clone(), commitment_root.clone(), vec![])];
                for (relay_id, _addr) in &self.cfg.relayers {
                    let att = RelayAttestation {
                        relay: *relay_id,
                        entries: entries.clone(),
                        sig_relay: vec![], // leader verifies later (stub for POC)
                    };
                    let _ = self.tx_attest.send(att);
                }
            }
        }
        // zeroize happens on SymKey Drop
        drop(sym_key);
    }
}
