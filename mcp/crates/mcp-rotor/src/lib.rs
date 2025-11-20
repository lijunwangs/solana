//! MCP rotor using Solana's sendmmsg path.
//!
//! - Deterministic μ-redundant dispatch: each symbol index j is sent to μ relayers
//!   chosen as (j + o) % num_relayers for o in 0..μ.
//! - Uses solana_streamer::sendmmsg::batch_send to batch UDP sends.

use bincode;
use mcp_wire::McpEnvelope;
use solana_packet::{Packet, PACKET_DATA_SIZE};
use solana_streamer::sendmmsg::{batch_send, SendPktsError};
use std::net::UdpSocket;

pub struct McpRotor {
    sock: UdpSocket,
    relayer_addrs: Vec<std::net::SocketAddr>,
    redundancy_mu: usize,
}

impl McpRotor {
    pub fn new(sock: UdpSocket, relayer_addrs: Vec<std::net::SocketAddr>, redundancy_mu: usize) -> Self {
        assert!(redundancy_mu >= 1, "redundancy_mu must be >= 1");
        assert!(!relayer_addrs.is_empty(), "need at least one relayer addr");
        Self { sock, relayer_addrs, redundancy_mu }
    }

    #[inline]
    fn encode_packet(env: &McpEnvelope, dest: std::net::SocketAddr) -> Packet {
        let bytes = bincode::serialize(env).expect("serialize envelope");
        let len = bytes.len().min(PACKET_DATA_SIZE);
        let mut pkt = Packet::default();
        let meta = pkt.meta_mut();
        meta.set_socket_addr(&dest);
        meta.size = len;

        // NOTE: If your solana-sdk uses `data_mut()` instead of `buffer_mut()`, change this line accordingly.
        pkt.buffer_mut()[..len].copy_from_slice(&bytes[..len]);
        pkt
    }

    /// Send a single envelope to μ relayers based on the symbol index.
    pub fn send_envelope_for_symbol(
        &self,
        symbol_index: usize,
        env: &McpEnvelope,
    ) -> Result<(), SendPktsError> {
        let mut pkts = Vec::with_capacity(self.redundancy_mu);
        let n = self.relayer_addrs.len();
        for o in 0..self.redundancy_mu {
            let idx = (symbol_index + o) % n;
            let packet = Self::encode_packet(env, self.relayer_addrs[idx]);
            pkts.push(packet);
        }

        let packets_and_addrs = pkts.iter().map(|packet| {
            let addr = packet.meta().socket_addr();
            let data = packet.data(..).unwrap();
            (data, addr)
        }); 
        batch_send(&self.sock, packets_and_addrs)
    }

    /// Send a batch of envelopes; each tuple carries the symbol index used for mapping.
    pub fn send_envelope_batch(
        &self,
        items: &[(usize, McpEnvelope)],
    ) -> Result<(), SendPktsError> {
        let n = self.relayer_addrs.len();
        let mut pkts = Vec::with_capacity(items.len() * self.redundancy_mu);
        for (symbol_index, env) in items {
            for o in 0..self.redundancy_mu {
                let idx = (symbol_index + o) % n;
                pkts.push(Self::encode_packet(env, self.relayer_addrs[idx]));
            }
        }
        let packets_and_addrs = pkts.iter().map(|packet| {
            let addr = packet.meta().socket_addr();
            let data = packet.data(..).unwrap();
            (data, addr)
        }); 
        batch_send(&self.sock, packets_and_addrs)
    }
}
