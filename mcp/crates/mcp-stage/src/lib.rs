use crossbeam_channel::{Receiver, Sender};
use mcp_wire::{McpEnvelope, McpDiscriminant, RevealShred};
use mcp_reconstructor::Reconstructor;
use mcp_crypto_vc::MerkleVC;
use mcp_blockstore::McpStore;

pub struct McpPacketForwarder {
    pub in_envelopes: Receiver<Vec<u8>>,
    pub out_reveals: Sender<RevealShred>,
}
impl McpPacketForwarder {
    pub fn run(self) {
        for buf in self.in_envelopes.iter() {
            if let Ok(env) = bincode::deserialize::<McpEnvelope>(&buf) {
                if env.kind == McpDiscriminant::RevealShred as u8 {
                    if let Ok(rs) = bincode::deserialize::<RevealShred>(&env.payload) {
                        let _ = self.out_reveals.send(rs);
                    }
                }
            }
        }
    }
}

pub struct McpVcVerifyStage<'a, S: McpStore> {
    pub in_reveals: Receiver<RevealShred>,
    pub reconstructor: &'a mut Reconstructor<MerkleVC, S>,
}
impl<'a, S: McpStore> McpVcVerifyStage<'a, S> {
    pub fn run(&mut self) {
        for rs in self.in_reveals.iter() {
            let _ = self.reconstructor.on_reveal(rs);
        }
    }
}
