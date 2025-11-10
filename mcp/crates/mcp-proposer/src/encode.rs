// mcp-proposer/src/encode.rs
use {
    mcp_crypto_vc::{MerkleVC, VectorCommitment},
    mcp_types::CommitmentRoot,
    solana_transaction::versioned::VersionedTransaction,
};

pub fn serialize_batch(v: &Vec<VersionedTransaction>) -> Vec<u8> {
    bincode::serialize(v).expect("serialize batch")
}

// POC degree-1
pub fn hecc_degree1_encode(batch: &[u8]) -> (Vec<Vec<u8>>, Vec<u8>) {
    // Split into two equal chunks as "coeffs" and produce 2 code symbols for demo
    let mid = (batch.len() + 1) / 2;
    let coeff0 = batch[..mid].to_vec();
    let coeff1 = batch[mid..].to_vec();
    let eval_points = vec![1u8, 2u8];
    let coded = mcp_hecc_mock::encode_degree1(&[coeff0, coeff1], &eval_points);
    (coded, eval_points)
}

pub fn vc_commit<V: VectorCommitment>(
    coded: &[Vec<u8>],
) -> (CommitmentRoot, Vec<Vec<u8>>, Vec<Vec<[u8; 32]>>) {
    let (commitment_root, leaf_randomizers) = V::commit(coded);
    let mut merkle_paths = Vec::with_capacity(coded.len());
    for (i, lr) in leaf_randomizers.iter().enumerate() {
        merkle_paths.push(V::open(coded, i, lr));
    }
    (commitment_root, leaf_randomizers, merkle_paths)
}

// Bind ciphertext to identity
use mcp_types::BatchKey;
pub fn make_aad(key: &BatchKey, index: u32, root: &CommitmentRoot) -> Vec<u8> {
    let mut aad = Vec::with_capacity(8 + 32 + 4 + 32);
    aad.extend_from_slice(&key.slot.to_le_bytes());
    aad.extend_from_slice(&key.proposer);
    aad.extend_from_slice(&key.batch_id.to_le_bytes());
    aad.extend_from_slice(&root.0);
    aad.extend_from_slice(&index.to_le_bytes());
    aad
}

// optional
pub fn tail_bytes() -> Vec<u8> {
    Vec::new()
}
