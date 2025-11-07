use mcp_types::{Params, BatchKey, CommitmentRoot};
use mcp_wire::{RevealShred, RelayAttestation};
use mcp_consensus::{Leader, PiAb};
use mcp_reconstructor::Reconstructor;
use mcp_crypto_vc::{MerkleVC, VectorCommitment};
use mcp_hecc_mock::encode_degree1;

fn mk_batch_commitment(symbols: &[Vec<u8>]) -> (CommitmentRoot, Vec<Vec<u8>>, Vec<Vec<[u8;32]>>) {
    // commit returns C and r_i; we also compute w_i per leaf via open()
    let (C, r_vec) = MerkleVC::commit(symbols);
    let mut paths = Vec::with_capacity(symbols.len());
    for (i, r_i) in r_vec.iter().enumerate() {
        let w_i = MerkleVC::open(symbols, i, r_i);
        paths.push(w_i);
    }
    (C, r_vec, paths)
}

fn main() {
    let params = Params { gamma: 1.0, tau: 0.0, mu: 0.5, phi: 0.5, n_relays: 2 };

    // Four batches (P1x2, P2x2). Each with 2 symbols (indices 0,1) for the demo.
    let key_a1 = BatchKey { slot: 1, proposer: [1u8;32], batch_id: 1 };
    let key_a2 = BatchKey { slot: 1, proposer: [1u8;32], batch_id: 2 };
    let key_b1 = BatchKey { slot: 1, proposer: [2u8;32], batch_id: 1 };
    let key_b2 = BatchKey { slot: 1, proposer: [2u8;32], batch_id: 2 };

    // Original coefficients (two 32-byte symbols per batch)
    let orig_a1 = vec![vec![0x11; 32], vec![0x22; 32]];
    let orig_a2 = vec![vec![0x33; 32], vec![0x44; 32]];
    let orig_b1 = vec![vec![0x55; 32], vec![0x66; 32]];
    let orig_b2 = vec![vec![0x77; 32], vec![0x88; 32]];

    // Encode to N=2 coded symbols using degree-1 evaluations at x=1,2
    let xs = [1u8, 2u8];
    let code_a1 = encode_degree1(&orig_a1, &xs);
    let code_a2 = encode_degree1(&orig_a2, &xs);
    let code_b1 = encode_degree1(&orig_b1, &xs);
    let code_b2 = encode_degree1(&orig_b2, &xs);

    // VC commit over the coded symbols (two leaves per batch)
    let (C_a1, r_a1, w_a1) = mk_batch_commitment(&code_a1);
    let (C_a2, r_a2, w_a2) = mk_batch_commitment(&code_a2);
    let (C_b1, r_b1, w_b1) = mk_batch_commitment(&code_b1);
    let (C_b2, r_b2, w_b2) = mk_batch_commitment(&code_b2);

    // Two relays attest all four batches (φ satisfied)
    let att_r1 = RelayAttestation { relay: [0xA1;32], entries: vec![
        (key_a1.clone(), C_a1.clone(), vec![]),
        (key_a2.clone(), C_a2.clone(), vec![]),
        (key_b1.clone(), C_b1.clone(), vec![]),
        (key_b2.clone(), C_b2.clone(), vec![]),
    ], sig_relay: vec![] };
    let att_r2 = RelayAttestation { relay: [0xA2;32], entries: att_r1.entries.clone(), sig_relay: vec![] };

    // Aggregate and finalize
    let payload = Leader::aggregate(1, vec![att_r1, att_r2], &params).expect("µ ok");
    assert!(PiAb::finalize(&payload));

    // Reconstruction node
    let mut R: Reconstructor<MerkleVC> = Reconstructor::new(params);
    let available = R.on_block_finalized(&payload);
    println!("Available batches: {}", available.len());

    // For each batch, emit two reveal shreds (index 0 and 1) with real (c_i, r_i, w_i)
    let mut emit = |key: &BatchKey, syms: &Vec<Vec<u8>>, rset: &Vec<Vec<u8>>, wset: &Vec<Vec<[u8;32]>>, C: &CommitmentRoot| {
        for i in 0..syms.len() {
            let rs = RevealShred {
                key: key.clone(),
                index: i as u32,
                c_i: syms[i].clone(),
                r_i: rset[i].clone(),
                w_i: wset[i].clone(),
                opt_commitment: Some(C.clone()),
            };
            let hit = R.on_reveal(rs);
            if hit {
                if let Some(bytes) = R.reconstruct_bytes(key) {
                    println!("Reconstructed {} bytes for batch {:?} (threshold reached).", bytes.len(), key);
                }
            }
        }
    };

    emit(&key_a1, &syms_a1, &r_a1, &w_a1, &C_a1);
    emit(&key_a2, &syms_a2, &r_a2, &w_a2, &C_a2);
    emit(&key_b1, &syms_b1, &r_b1, &w_b1, &C_b1);
    emit(&key_b2, &syms_b2, &r_b2, &w_b2, &C_b2);

    println!("POC done.");
}
