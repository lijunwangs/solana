use serde::{Deserialize, Serialize};

pub type Slot = u64;
pub type ProposerId = [u8; 32];
pub type RelayId = [u8; 32];

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Params {
    pub gamma: f32,
    pub tau:   f32,
    pub mu:    f32,
    pub phi:   f32,
    pub n_relays: u16,
}

impl Params {
    pub fn k_t(&self) -> (usize, usize) {
        let n = self.n_relays as f32;
        let k = ((self.gamma - self.tau) * n).round() as usize;
        let t = (self.tau * n).round() as usize;
        (k, t)
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
