use mcp_types::CommitmentRoot;
use rand::RngCore;
use sha2::{Digest, Sha256};

pub trait VectorCommitment {
    fn commit(leaves: &[Vec<u8>]) -> (CommitmentRoot, Vec<Vec<u8>>);
    fn open(leaves: &[Vec<u8>], index: usize, leaf_randomizer: &[u8]) -> Vec<[u8;32]>;
    fn verify(commitment_root: &CommitmentRoot, index: usize, leaf_bytes: &[u8], leaf_randomizer: &[u8], merkle_path: &[[u8;32]]) -> bool;
}

fn hash_leaf(index: usize, leaf_randomizer: &[u8], leaf: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([0x00]);
    h.update((index as u32).to_be_bytes());
    h.update(leaf_randomizer);
    h.update(leaf);
    h.finalize().into()
}
fn hash_node(left: &[u8;32], right: &[u8;32]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([0x01]);
    h.update(left);
    h.update(right);
    h.finalize().into()
}
fn build_tree(leaf_hashes: &[[u8;32]]) -> Vec<Vec<[u8;32]>> {
    let mut cur = leaf_hashes.to_vec();
    let mut tree = vec![cur.clone()];
    while cur.len() > 1 {
        let mut next = Vec::with_capacity((cur.len()+1)/2);
        for i in (0..cur.len()).step_by(2) {
            let l = cur[i];
            let r = if i+1 < cur.len() { cur[i+1] } else { cur[i] };
            next.push(hash_node(&l, &r));
        }
        tree.push(next.clone());
        cur = next;
    }
    tree
}
fn root_of(tree: &Vec<Vec<[u8;32]>>) -> [u8;32] { tree.last().unwrap()[0] }
fn path_for(mut index: usize, tree: &Vec<Vec<[u8;32]>>) -> Vec<[u8;32]> {
    let mut path = Vec::new();
    for level in &tree[..tree.len()-1] {
        let sib = if index % 2 == 0 {
            if index+1 < level.len() { level[index+1] } else { level[index] }
        } else { level[index-1] };
        path.push(sib);
        index /= 2;
    }
    path
}

pub struct MerkleVC;
impl VectorCommitment for MerkleVC {
    fn commit(leaves: &[Vec<u8>]) -> (CommitmentRoot, Vec<Vec<u8>>) {
        let mut rng = rand::thread_rng();
        let mut leaf_randomizers = Vec::with_capacity(leaves.len());
        let mut leaf_hashes: Vec<[u8;32]> = Vec::with_capacity(leaves.len());
        for (i, leaf) in leaves.iter().enumerate() {
            let mut leaf_randomizer = vec![0u8; 16];
            rng.fill_bytes(&mut leaf_randomizer);
            let h = hash_leaf(i, &leaf_randomizer, leaf);
            leaf_randomizers.push(leaf_randomizer);
            leaf_hashes.push(h);
        }
        let tree = build_tree(&leaf_hashes);
        (CommitmentRoot(root_of(&tree)), leaf_randomizers)
    }
    fn open(leaves: &[Vec<u8>], index: usize, leaf_randomizer: &[u8]) -> Vec<[u8;32]> {
        let mut leaf_hashes: Vec<[u8;32]> = Vec::with_capacity(leaves.len());
        for (i, leaf) in leaves.iter().enumerate() {
            let h = if i == index { hash_leaf(i, leaf_randomizer, leaf) } else { hash_leaf(i, &vec![0u8;16], leaf) };
            leaf_hashes.push(h);
        }
        let tree = build_tree(&leaf_hashes);
        path_for(index, &tree)
    }
    fn verify(commitment_root: &CommitmentRoot, index: usize, leaf_bytes: &[u8], leaf_randomizer: &[u8], merkle_path: &[[u8;32]]) -> bool {
        let mut cur = hash_leaf(index, leaf_randomizer, leaf_bytes);
        let mut idx = index;
        for sib in merkle_path {
            cur = if idx % 2 == 0 { hash_node(&cur, sib) } else { hash_node(sib, &cur) };
            idx /= 2
        }
        cur == commitment_root.0
    }
}
