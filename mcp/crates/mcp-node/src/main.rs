use std::net::UdpSocket;
use mcp_types::{Params, BatchKey, CommitmentRoot};
use mcp_wire::{RevealShred, RelayAttestation, McpEnvelope, LeaderBlockPayload};
use mcp_crypto_vc::{MerkleVC, VectorCommitment};
use mcp_hecc_mock::encode_degree1;
use mcp_blockstore::mem::MemStore;
use mcp_reconstructor::Reconstructor;

fn mk_batch_commitment(symbols: &[Vec<u8>]) -> (CommitmentRoot, Vec<Vec<u8>>, Vec<Vec<[u8;32]>>) {
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

    let key_a1 = BatchKey { slot: 1, proposer: [1u8;32], batch_id: 1 };
    let key_a2 = BatchKey { slot: 1, proposer: [1u8;32], batch_id: 2 };
    let key_b1 = BatchKey { slot: 1, proposer: [2u8;32], batch_id: 1 };
    let key_b2 = BatchKey { slot: 1, proposer: [2u8;32], batch_id: 2 };

    let orig_a1 = vec![vec![0x11; 32], vec![0x22; 32]];
    let orig_a2 = vec![vec![0x33; 32], vec![0x44; 32]];
    let orig_b1 = vec![vec![0x55; 32], vec![0x66; 32]];
    let orig_b2 = vec![vec![0x77; 32], vec![0x88; 32]];

    let xs = [1u8, 2u8];
    let code_a1 = encode_degree1(&orig_a1, &xs);
    let code_a2 = encode_degree1(&orig_a2, &xs);
    let code_b1 = encode_degree1(&orig_b1, &xs);
    let code_b2 = encode_degree1(&orig_b2, &xs);

    let (C_a1, r_a1, w_a1) = mk_batch_commitment(&code_a1);
    let (C_a2, r_a2, w_a2) = mk_batch_commitment(&code_a2);
    let (C_b1, r_b1, w_b1) = mk_batch_commitment(&code_b1);
    let (C_b2, r_b2, w_b2) = mk_batch_commitment(&code_b2);

    let att_r1 = RelayAttestation { relay: [0xA1;32], entries: vec![
        (key_a1.clone(), C_a1.clone(), vec![]),
        (key_a2.clone(), C_a2.clone(), vec![]),
        (key_b1.clone(), C_b1.clone(), vec![]),
        (key_b2.clone(), C_b2.clone(), vec![]),
    ], sig_relay: vec![] };
    let att_r2 = RelayAttestation { relay: [0xA2;32], entries: att_r1.entries.clone(), sig_relay: vec![] };
    let payload = LeaderBlockPayload { slot: 1, relay_attestations: vec![att_r1, att_r2], sig_leader: vec![] };

    let store = MemStore::new();
    let mut R: Reconstructor<MerkleVC, MemStore> = Reconstructor::new(params, store);
    let available = R.on_block_finalized(&payload);
    println!("Available batches: {}", available.len());

    let recv = UdpSocket::bind("127.0.0.1:0").expect("bind recv");
    let send = UdpSocket::bind("127.0.0.1:0").expect("bind send");
    send.connect(recv.local_addr().unwrap()).expect("connect");

    let mut send_reveals = |key: &BatchKey, code: &Vec<Vec<u8>>, rset: &Vec<Vec<u8>>, wset: &Vec<Vec<[u8;32]>>, C: &CommitmentRoot| {
        for i in 0..code.len() {
            let rs = RevealShred { key: key.clone(), index: i as u32, c_i: code[i].clone(), r_i: rset[i].clone(), w_i: wset[i].clone(), opt_commitment: Some(C.clone()) };
            let env = McpEnvelope::reveal(&rs);
            let bytes = bincode::serialize(&env).unwrap();
            send.send(&bytes).unwrap();

            let _hit = R.on_reveal(rs);
            if let Some(bytes) = R.reconstruct_bytes(key) {
                println!("Reconstructed {} bytes for batch {:?}", bytes.len(), key);
            }
        }
    };

    send_reveals(&key_a1, &code_a1, &r_a1, &w_a1, &C_a1);
    send_reveals(&key_a2, &code_a2, &r_a2, &w_a2, &C_a2);
    send_reveals(&key_b1, &code_b1, &r_b1, &w_b1, &C_b1);
    send_reveals(&key_b2, &code_b2, &r_b2, &w_b2, &C_b2);

    println!("POC done.");
}
