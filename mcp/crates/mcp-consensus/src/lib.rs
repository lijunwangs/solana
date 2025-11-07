use mcp_types::Params;
use mcp_wire::{RelayAttestation, LeaderBlockPayload};

pub struct Leader;

impl Leader {
    pub fn aggregate(slot: u64, attestations: Vec<RelayAttestation>, params: &Params) -> Option<LeaderBlockPayload> {
        let need = (params.mu * params.n_relays as f32).ceil() as usize;
        if attestations.len() < need { return None; }
        Some(LeaderBlockPayload { slot, relay_attestations: attestations, sig_leader: vec![] })
    }
}

pub struct PiAb;

impl PiAb {
    pub fn finalize(_payload: &LeaderBlockPayload) -> bool { true }
}
