use mcp_types::{BatchKey, CommitmentRoot, ProposerId};
use mcp_wire::RelayAttestation;

pub struct Proposer {
    pub id: ProposerId,
}

impl Proposer {
    pub fn build_attestation_stub(&self, key: &BatchKey, C: CommitmentRoot, sigma_q: Vec<u8>) -> (BatchKey, CommitmentRoot, Vec<u8>) {
        (key.clone(), C, sigma_q)
    }
    pub fn package_attestation(&self, relay: [u8;32], entries: Vec<(BatchKey, CommitmentRoot, Vec<u8>)>, sig_relay: Vec<u8>) -> RelayAttestation {
        RelayAttestation { relay, entries, sig_relay }
    }
}
