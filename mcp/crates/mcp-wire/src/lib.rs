use {
    mcp_types::{BatchKey, CommitmentRoot, RelayId},
    serde::{Deserialize, Serialize},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelayAttestation {
    pub relay: RelayId,
    pub entries: Vec<(BatchKey, CommitmentRoot, Vec<u8>)>,
    pub sig_relay: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaderBlockPayload {
    pub slot: u64,
    pub relay_attestations: Vec<RelayAttestation>,
    pub sig_leader: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RevealShred {
    pub key: BatchKey,
    pub index: u32,
    pub coded_symbol: Vec<u8>,
    pub leaf_randomizer: Vec<u8>,
    pub merkle_path: Vec<[u8; 32]>,
    pub opt_commitment_root: Option<CommitmentRoot>,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum McpDiscriminant {
    RevealShred = 0xA5,
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
}
