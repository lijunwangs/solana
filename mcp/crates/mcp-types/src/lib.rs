use serde::{Deserialize, Serialize};

pub type Slot = u64;
pub type ProposerId = [u8; 32];
pub type RelayId = [u8; 32];

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Params {
    pub gamma: f32, // total coded ratio = data + tolerance
    pub tau: f32,   // adversarial tolerance ratio
    pub mu: f32,    // leader->relay attestation threshold fraction
    pub phi: f32,   // per-batch relay quorum fraction
    pub n_relays: u16,
}
impl Params {
    /// Returns (min_symbols_to_decode, adversary_tolerance)
    pub fn k_t(&self) -> (usize, usize) {
        let n = self.n_relays as f32;
        let min_symbols_to_decode = ((self.gamma - self.tau) * n).round() as usize;
        let adversary_tolerance = (self.tau * n).round() as usize;
        (min_symbols_to_decode, adversary_tolerance)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BatchKey {
    pub slot: Slot,
    pub proposer: ProposerId,
    pub batch_id: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitmentRoot(pub [u8; 32]);
