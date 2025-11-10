use {
    mcp_types::{BatchKey, CommitmentRoot, ProposerId, RelayId},
    serde::{Deserialize, Serialize},
};

/// ---------------------------------------------------------------------------
/// Relay attestations (unchanged)
/// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelayAttestation {
    pub relay: RelayId,
    /// (batch_key, commitment_root, sigma_q) — sigma_q is placeholder for relay-side evidence
    pub entries: Vec<(BatchKey, CommitmentRoot, Vec<u8>)>,
    pub sig_relay: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaderBlockPayload {
    pub slot: u64,
    pub relay_attestations: Vec<RelayAttestation>,
    pub sig_leader: Vec<u8>,
}

/// ---------------------------------------------------------------------------
/// Plaintext reveal (legacy for POC compatibility)
/// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RevealShred {
    pub key: BatchKey,
    pub index: u32,
    pub coded_symbol: Vec<u8>,
    pub leaf_randomizer: Vec<u8>,
    pub merkle_path: Vec<[u8; 32]>,
    pub opt_commitment_root: Option<CommitmentRoot>,
}

/// ---------------------------------------------------------------------------
/// Threshold Encryption wire types
/// ---------------------------------------------------------------------------

/// Matches AES-256-GCM usage: 96-bit nonce + ciphertext (tag is appended internally by GCM)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EncryptedSymbol {
    pub nonce: [u8; 12],
    pub ciphertext: Vec<u8>,
}

/// Proposer -> Relayers: encrypted coded symbol + VC data
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EncryptedReveal {
    pub key: BatchKey,
    pub index: u32,
    pub encrypted_symbol: EncryptedSymbol,
    pub leaf_randomizer: Vec<u8>,
    pub merkle_path: Vec<[u8; 32]>,
    /// Optional echo of the batch commitment root (lets verifiers avoid an extra lookup)
    pub opt_commitment_root: Option<CommitmentRoot>,
    /// The intended relay (useful for logging/routing; message can still be fanned out)
    pub relay_id: RelayId,
}

/// Proposer (once per block/slot): threshold-encrypted (tail || sym_key)
/// This is a serialized blsttc::Ciphertext; kept as Vec<u8> to avoid a direct dep from mcp-wire.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FinalKeyCapsule {
    pub slot: u64,
    pub proposer: ProposerId,
    pub threshold_ciphertext: Vec<u8>,
    // Optionally: add a summary (e.g., batch keys / roots) for convenience
    // pub descriptors: Vec<(BatchKey, CommitmentRoot)>,
}

/// Relayer -> Network: decryption share for the final capsule
/// For the capsule, `for_index` is None; if you later support per-symbol TE, you can
/// carry an index here as Some(j).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecryptionShareMsg {
    pub slot: u64,
    pub proposer: ProposerId,
    pub relay_id: RelayId,
    pub for_index: Option<u32>,
    pub share_bytes: Vec<u8>,
    // Optional: attach a proof bytes if you export proofs from TE lib
    // pub proof_bytes: Vec<u8>,
}

/// ---------------------------------------------------------------------------
/// Envelopes
/// ---------------------------------------------------------------------------

#[repr(u8)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum McpDiscriminant {
    RevealShred = 0xA5,
    EncryptedReveal = 0xA6,
    FinalKeyCapsule = 0xA7,
    DecryptionShare = 0xA8,
    RelayAttestation = 0xA9,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct McpEnvelope {
    pub kind: u8,
    pub payload: Vec<u8>,
}

impl McpEnvelope {
    pub fn reveal(rs: &RevealShred) -> Self {
        Self {
            kind: McpDiscriminant::RevealShred as u8,
            payload: bincode::serialize(rs).expect("serialize reveal"),
        }
    }
    pub fn encrypted_reveal(er: &EncryptedReveal) -> Self {
        Self {
            kind: McpDiscriminant::EncryptedReveal as u8,
            payload: bincode::serialize(er).expect("serialize encrypted reveal"),
        }
    }
    pub fn final_key_capsule(capsule: &FinalKeyCapsule) -> Self {
        Self {
            kind: McpDiscriminant::FinalKeyCapsule as u8,
            payload: bincode::serialize(capsule).expect("serialize capsule"),
        }
    }
    pub fn decryption_share(share: &DecryptionShareMsg) -> Self {
        Self {
            kind: McpDiscriminant::DecryptionShare as u8,
            payload: bincode::serialize(share).expect("serialize share"),
        }
    }
    pub fn relay_attestation(att: &RelayAttestation) -> Self {
        Self {
            kind: McpDiscriminant::RelayAttestation as u8,
            payload: bincode::serialize(att).expect("serialize attestation"),
        }
    }
}
