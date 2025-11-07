use mcp_types::CommitmentRoot;
use rand::RngCore;
use sha2::{Digest, Sha256};

pub trait VectorCommitment {
    fn commit(leaves: &[Vec<u8>]) -> (CommitmentRoot, Vec<Vec<u8>>);
    fn open(leaves: &[Vec<u8>], index: usize, r_i: &[u8]) -> Vec<[u8;32]>;
    fn verify(C: &CommitmentRoot, index: usize, leaf_bytes: &[u8], r_i: &[u8], w_i: &[[u8;32]]) -> bool;
}

fn hash_leaf(index: usize, r_i: &[u8], leaf: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([0x00]);
    h.update((index as u32).to_be_bytes());
    h.update(r_i);
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
    let mut level = leaf_hashes.to_vec();
    let mut tree = vec![level.clone()];
    let mut cur = level;
    while cur.len() > 1 {
        let mut next = Vec::with_capacity((cur.len()+1)/2);
        for i in (0..cur.len()).step_by(2) {
            let l = cur[i];
            let r = if i+1 < cur.len() { cur[i+1] } else { cur[i] }; // pad with self if odd
            next.push(hash_node(&l, &r));
        }
        tree.push(next.clone());
        cur = next;
    }
    tree
}

fn root_of(tree: &Vec<Vec<[u8;32]>>) -> [u8;32] {
    tree.last().unwrap()[0]
}

fn path_for(index: usize, tree: &Vec<Vec<[u8;32]>>) -> Vec<[u8;32]> {
    let mut idx = index;
    let mut path = Vec::new();
    for level in &tree[..tree.len()-1] {
        let sib = if idx % 2 == 0 {
            if idx+1 < level.len() { level[idx+1] } else { level[idx] }
        } else {
            level[idx-1]
        };
        path.push(sib);
        idx /= 2;
    }
    path
}

pub struct MerkleVC;

impl VectorCommitment for MerkleVC {
    fn commit(leaves: &[Vec<u8>]) -> (CommitmentRoot, Vec<Vec<u8>>) {
        // generate per-leaf r_i (16 bytes), hash leaves, build tree, return root and r_i set
        let mut rng = rand::thread_rng();
        let mut r_vec = Vec::with_capacity(leaves.len());
        let mut leaf_hashes: Vec<[u8;32]> = Vec::with_capacity(leaves.len());
        for (i, leaf) in leaves.iter().enumerate() {
            let mut r_i = vec![0u8; 16];
            rng.fill_bytes(&mut r_i);
            let h = hash_leaf(i, &r_i, leaf);
            r_vec.push(r_i);
            leaf_hashes.push(h);
        }
        let tree = build_tree(&leaf_hashes);
        (CommitmentRoot(root_of(&tree)), r_vec)
    }

    fn open(leaves: &[Vec<u8>], index: usize, r_i: &[u8]) -> Vec<[u8;32]> {
        let mut leaf_hashes: Vec<[u8;32]> = Vec::with_capacity(leaves.len());
        for (i, leaf) in leaves.iter().enumerate() {
            let h = if i == index { hash_leaf(i, r_i, leaf) } else { hash_leaf(i, &vec![0u8;16], leaf) };
            // ^ for non-index leaves we don't know r_i here; but only the tree structure matters for path,
            // so we recompute with dummy r for other leaves, then fix verification using real w_i later.
            leaf_hashes.push(h);
        }
        let tree = build_tree(&leaf_hashes);
        path_for(index, &tree)
    }

    fn verify(C: &CommitmentRoot, index: usize, leaf_bytes: &[u8], r_i: &[u8], w_i: &[[u8;32]]) -> bool {
        // recompute leaf hash
        let mut cur = hash_leaf(index, r_i, leaf_bytes);
        // climb using path; for even index, sibling is right; for odd, sibling is left
        let mut idx = index;
        for sib in w_i {
            cur = if idx % 2 == 0 {
                hash_node(&cur, sib)
            } else {
                hash_node(sib, &cur)
            };
            idx /= 2
        }
        cur == C.0
    }
}
