//! Threshold Encryption (TE) helper crate for MCP:
//! - AES-256-GCM for streaming batches
//! - blsttc (BLS12-381) (t,n) encryption for final batch (tail || sym_key)
//! Maps to Fast MCP Algorithms 1–6.

#[cfg(test)]
use rand::RngCore;
use {
    aes_gcm::{
        aead::{Aead, KeyInit},
        Aes256Gcm, Nonce,
    },
    blsttc::{Ciphertext, DecryptionShare as BlstShare, PublicKeySet, SecretKeyShare},
    mcp_wire::EncryptedSymbol,
    thiserror::Error,
    zeroize::Zeroize,
};

#[derive(Error, Debug)]
pub enum TeError {
    #[error("crypto error")]
    Crypto,
    #[error("not enough shares")]
    NotEnoughShares,
    #[error("invalid share")]
    InvalidShare,
    #[error("deserialize error")]
    Deserialize,
}

/// Global TE parameters (validators). In production each validator persists only its share;
/// in tests we generate a whole SecretKeySet to simulate a cluster.
#[derive(Clone)]
pub struct TeParams {
    pub t: usize,
    pub n: usize,
    pub pk_set: PublicKeySet,
}

#[cfg(test)]
impl TeParams {
    pub fn setup_for_test(t: usize, n: usize) -> (Self, SecretKeySet) {
        let sk_set = SecretKeySet::random(t, &mut rand::thread_rng());
        let pk_set = sk_set.public_keys();
        (Self { t, n, pk_set }, sk_set)
    }
}

/// Wire types that can cross crate boundaries safely via serialization.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ThresholdCiphertext(pub Vec<u8>); // serialized blsttc::Ciphertext

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct DecryptionShare {
    pub index: u32,     // validator index [0..n)
    pub share: Vec<u8>, // serialized blsttc::DecryptionShare
}

// #[derive(Clone, serde::Serialize, serde::Deserialize)]
// pub struct EncryptedSymbol {
//     pub nonce: [u8; 12],
//     pub ciphertext: Vec<u8>,
// }

/// Zeroizing 32-byte symmetric key
#[derive(Zeroize)]
#[zeroize(drop)]
pub struct SymKey(pub [u8; 32]);

/// Generate random 32B key
pub fn sym_keygen() -> SymKey {
    let mut k = [0u8; 32];
    getrandom::getrandom(&mut k).expect("rng");
    SymKey(k)
}

/// Encrypt a batch with AES-256-GCM and a random 96-bit nonce
pub fn sym_encrypt(key: &SymKey, plaintext: &[u8]) -> Result<EncryptedSymbol, TeError> {
    let cipher = Aes256Gcm::new_from_slice(&key.0).map_err(|_| TeError::Crypto)?;
    let mut nonce = [0u8; 12];
    getrandom::getrandom(&mut nonce).map_err(|_| TeError::Crypto)?;
    let ct = cipher
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(|_| TeError::Crypto)?;
    Ok(EncryptedSymbol {
        nonce,
        ciphertext: ct,
    })
}

/// Decrypt a batch with AES-256-GCM
pub fn sym_decrypt(key: &SymKey, enc: &EncryptedSymbol) -> Result<Vec<u8>, TeError> {
    let cipher = Aes256Gcm::new_from_slice(&key.0).map_err(|_| TeError::Crypto)?;
    cipher
        .decrypt(Nonce::from_slice(&enc.nonce), enc.ciphertext.as_ref())
        .map_err(|_| TeError::Crypto)
}

/// TE-encrypt the final batch (tail || sym_key)
pub fn te_encrypt_final(pk_set: &PublicKeySet, final_batch: &[u8]) -> ThresholdCiphertext {
    let ct: Ciphertext = pk_set.public_key().encrypt(final_batch);
    ThresholdCiphertext(bincode::serialize(&ct).expect("serialize ct"))
}

/// Validator i creates a partial decryption share
pub fn te_share_decrypt(
    i: usize,
    sk_share: &SecretKeyShare,
    ct: &ThresholdCiphertext,
) -> Result<DecryptionShare, TeError> {
    let ct: Ciphertext = bincode::deserialize(&ct.0).map_err(|_| TeError::Deserialize)?;
    let share = sk_share.decrypt_share(&ct).ok_or(TeError::InvalidShare)?;
    Ok(DecryptionShare {
        index: i as u32,
        share: bincode::serialize(&share).map_err(|_| TeError::Crypto)?,
    })
}

/// Combine ≥ t+1 shares to recover the final batch plaintext
pub fn te_combine(
    pk_set: &PublicKeySet,
    ct: &ThresholdCiphertext,
    shares: &[DecryptionShare],
) -> Result<Vec<u8>, TeError> {
    if shares.is_empty() {
        return Err(TeError::NotEnoughShares);
    }
    let ct: Ciphertext = bincode::deserialize(&ct.0).map_err(|_| TeError::Deserialize)?;
    let mut parts = Vec::<(usize, BlstShare)>::new();
    for s in shares {
        let sh: BlstShare = bincode::deserialize(&s.share).map_err(|_| TeError::Deserialize)?;
        parts.push((s.index as usize, sh));
    }
    pk_set
        .decrypt(parts.iter().map(|(i, s)| (*i, s)), &ct)
        .map_err(|_| TeError::InvalidShare)
}

/// Pack (tail || sym_key) as: [u32 tail_len][tail bytes][32B key]
pub fn pack_final_batch(tail_txns: &[u8], sym_key: &SymKey) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + tail_txns.len() + 32);
    let len = tail_txns.len() as u32;
    v.extend_from_slice(&len.to_le_bytes());
    v.extend_from_slice(tail_txns);
    v.extend_from_slice(&sym_key.0);
    v
}

/// Unpack (tail, sym_key)
pub fn unpack_final_batch(bytes: &[u8]) -> Option<(Vec<u8>, SymKey)> {
    if bytes.len() < 4 + 32 {
        return None;
    }
    let mut len_bytes = [0u8; 4];
    len_bytes.copy_from_slice(&bytes[..4]);
    let tail_len = u32::from_le_bytes(len_bytes) as usize;
    if bytes.len() < 4 + tail_len + 32 {
        return None;
    }
    let tail = bytes[4..4 + tail_len].to_vec();
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes[4 + tail_len..4 + tail_len + 32]);
    Some((tail, SymKey(key)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip() {
        let t = 2usize;
        let n = 4usize;
        let (params, sk_set) = TeParams::setup_for_test(t, n);
        // Proposer
        let sym = sym_keygen();
        let pt1 = b"batch-1";
        let enc1 = sym_encrypt(&sym, pt1).unwrap();
        let final_plain = pack_final_batch(b"tail", &sym);
        let enc_final = te_encrypt_final(&params.pk_set, &final_plain);
        // Voters produce t+1 shares
        let mut shares = Vec::new();
        for i in 0..=t {
            shares.push(te_share_decrypt(i, &sk_set.secret_key_share(i), &enc_final).unwrap());
        }
        // Combine -> recover (tail||key), decrypt batch
        let final_plain2 = te_combine(&params.pk_set, &enc_final, &shares).unwrap();
        let (_tail, sym2) = unpack_final_batch(&final_plain2).unwrap();
        let pt1_out = sym_decrypt(&sym2, &enc1).unwrap();
        assert_eq!(pt1_out, pt1);
    }
}
