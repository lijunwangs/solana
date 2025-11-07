use mcp_types::{BatchKey, CommitmentRoot, RelayId};
use mcp_wire::{RelayAttestation, RevealShred};

pub struct Relay {
    pub id: RelayId,
}

impl Relay {
    pub fn attest(&self, entries: Vec<(BatchKey, CommitmentRoot, Vec<u8>)>, sig_relay: Vec<u8>) -> RelayAttestation {
        RelayAttestation { relay: self.id, entries, sig_relay }
    }

    pub fn broadcast_reveal(&self, reveal: RevealShred) -> RevealShred {
        reveal
    }
}
