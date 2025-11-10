// mcp-proposer/src/network.rs
use {
    crossbeam_channel::Receiver,
    mcp_wire::{EncryptedReveal, FinalKeyCapsule, McpEnvelope, RelayAttestation},
    std::net::UdpSocket,
};

pub fn run_udp_out(
    rs_encrypted: Receiver<EncryptedReveal>,
    rs_attest: Receiver<RelayAttestation>,
    rs_capsule: Receiver<FinalKeyCapsule>,
    relayer_addrs: Vec<std::net::SocketAddr>,
) {
    let sock = UdpSocket::bind("0.0.0.0:0").expect("bind");
    loop {
        crossbeam_channel::select! {
            recv(rs_encrypted) -> msg => {
                if let Ok(m) = msg {
                    let env = McpEnvelope::encrypted_reveal(&m); // add helper in mcp-wire
                    for addr in &relayer_addrs {
                        let _ = sock.send_to(&bincode::serialize(&env).unwrap(), addr);
                    }
                }
            }
            recv(rs_attest) -> msg => {
                if let Ok(m) = msg {
                    let env = McpEnvelope::relay_attestation(&m);
                    // send to leader’s addr (configure separately)
                    // sock.send_to(...);
                }
            }
            recv(rs_capsule) -> msg => {
                if let Ok(m) = msg {
                    let env = McpEnvelope::final_key_capsule(&m);
                    // broadcast or send to a well-known gossip addr
                }
            }
        }
    }
}
