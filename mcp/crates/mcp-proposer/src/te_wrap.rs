// mcp-proposer/src/te_wrap.rs
use {
    aes_gcm::{
        aead::{Aead, KeyInit},
        Aes256Gcm, Nonce,
    },
    mcp_crypto_te as te,
    mcp_wire::EncryptedSymbol,
};

pub fn sym_encrypt_with_aad(
    k: &te::SymKey,
    pt: &[u8],
    aad: &[u8],
) -> Result<EncryptedSymbol, te::TeError> {
    let cipher = Aes256Gcm::new_from_slice(&k.0).map_err(|_| te::TeError::Crypto)?;
    let mut nonce = [0u8; 12];
    getrandom::fill(&mut nonce).map_err(|_| te::TeError::Crypto)?;
    let ct = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            aes_gcm::aead::Payload { msg: pt, aad },
        )
        .map_err(|_| te::TeError::Crypto)?;
    Ok(EncryptedSymbol {
        nonce,
        ciphertext: ct,
    })
}
