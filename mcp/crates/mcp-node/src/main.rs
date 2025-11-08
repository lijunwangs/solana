use std::net::UdpSocket;
use mcp_types::{Params, BatchKey, CommitmentRoot};
use mcp_wire::{RevealShred, RelayAttestation, McpEnvelope, LeaderBlockPayload};
use mcp_crypto_vc::{MerkleVC, VectorCommitment};
use mcp_hecc_mock::encode_degree1;
use mcp_blockstore::mem::MemStore;
use mcp_reconstructor::Reconstructor;

fn mk_batch_commitment(symbols: &[Vec<u8>]) -> (CommitmentRoot, Vec<Vec<u8>>, Vec<Vec<[u8;32]>>) {
    let (commitment_root, leaf_randomizers) = MerkleVC::commit(symbols);
    let mut merkle_paths = Vec::with_capacity(symbols.len());
    for (i, leaf_randomizer) in leaf_randomizers.iter().enumerate() {
        let mp = MerkleVC::open(symbols, i, leaf_randomizer);
        merkle_paths.push(mp);
    }
    (commitment_root, leaf_randomizers, merkle_paths)
}

fn main() {
    let params = Params { gamma: 1.0, tau: 0.0, mu: 0.5, phi: 0.5, n_relays: 2 };

    let key_a1 = BatchKey { slot: 1, proposer: [1u8;32], batch_id: 1 };
    let key_a2 = BatchKey { slot: 1, proposer: [1u8;32], batch_id: 2 };
    let key_b1 = BatchKey { slot: 1, proposer: [2u8;32], batch_id: 1 };
    let key_b2 = BatchKey { slot: 1, proposer: [2u8;32], batch_id: 2 };

    let coeffs_a1 = vec![vec![0x11; 32], vec![0x22; 32]];
    let coeffs_a2 = vec![vec![0x33; 32], vec![0x44; 32]];
    let coeffs_b1 = vec![vec![0x55; 32], vec![0x66; 32]];
    let coeffs_b2 = vec![vec![0x77; 32], vec![0x88; 32]];

    let eval_points = [1u8, 2u8];
    let code_a1 = encode_degree1(&coeffs_a1, &eval_points);
    let code_a2 = encode_degree1(&coeffs_a2, &eval_points);
    let code_b1 = encode_degree1(&coeffs_b1, &eval_points);
    let code_b2 = encode_degree1(&coeffs_b2, &eval_points);

    let (commitment_root_a1, leaf_randomizers_a1, merkle_paths_a1) = mk_batch_commitment(&code_a1);
    let (commitment_root_a2, leaf_randomizers_a2, merkle_paths_a2) = mk_batch_commitment(&code_a2);
    let (commitment_root_b1, leaf_randomizers_b1, merkle_paths_b1) = mk_batch_commitment(&code_b1);
    let (commitment_root_b2, leaf_randomizers_b2, merkle_paths_b2) = mk_batch_commitment(&code_b2);

    let att_r1 = RelayAttestation { relay: [0xA1;32], entries: vec![
        (key_a1.clone(), commitment_root_a1.clone(), vec![]),
        (key_a2.clone(), commitment_root_a2.clone(), vec![]),
        (key_b1.clone(), commitment_root_b1.clone(), vec![]),
        (key_b2.clone(), commitment_root_b2.clone(), vec![]),
    ], sig_relay: vec![] };
    let att_r2 = RelayAttestation { relay: [0xA2;32], entries: att_r1.entries.clone(), sig_relay: vec![] };
    let payload = LeaderBlockPayload { slot: 1, relay_attestations: vec![att_r1, att_r2], sig_leader: vec![] };

    let store = MemStore::new();
    let mut recon: Reconstructor<MerkleVC, MemStore> = Reconstructor::new(params, store);
    let available = recon.on_block_finalized(&payload);
    println!("Available batches: {}", available.len());

    let recv = UdpSocket::bind("127.0.0.1:0").expect("bind recv");
    let send = UdpSocket::bind("127.0.0.1:0").expect("bind send");
    send.connect(recv.local_addr().unwrap()).expect("connect");

    let mut send_reveals = |key: &BatchKey, code: &Vec<Vec<u8>>, leaf_randomizers: &Vec<Vec<u8>>, merkle_paths: &Vec<Vec<[u8;32]>>, commitment_root: &CommitmentRoot| {
        for i in 0..code.len() {
            let rs = RevealShred { key: key.clone(), index: i as u32, coded_symbol: code[i].clone(), leaf_randomizer: leaf_randomizers[i].clone(), merkle_path: merkle_paths[i].clone(), opt_commitment_root: Some(commitment_root.clone()) };
            let env = McpEnvelope::reveal(&rs);
            let bytes = bincode::serialize(&env).unwrap();
            send.send(&bytes).unwrap();

            let _hit = recon.on_reveal(rs);
            if let Some(bytes) = recon.reconstruct_bytes(key) {
                println!("Reconstructed {} bytes for batch {:?}", bytes.len(), key);
            }
        }
    };

    send_reveals(&key_a1, &code_a1, &leaf_randomizers_a1, &merkle_paths_a1, &commitment_root_a1);
    send_reveals(&key_a2, &code_a2, &leaf_randomizers_a2, &merkle_paths_a2, &commitment_root_a2);
    send_reveals(&key_b1, &code_b1, &leaf_randomizers_b1, &merkle_paths_b1, &commitment_root_b1);
    send_reveals(&key_b2, &code_b2, &leaf_randomizers_b2, &merkle_paths_b2, &commitment_root_b2);

    println!("POC done.");
}
