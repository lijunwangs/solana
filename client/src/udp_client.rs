//! Simple TPU client that communicates with the given UDP port with UDP and provides
//! an interface for sending transactions

pub use solana_udp_client::udp_client::UdpClientConnection;
use {
    crate::client_connection::ClientConnection, core::iter::repeat,
    solana_sdk::transport::Result as TransportResult, solana_streamer::sendmmsg::batch_send,
    std::net::SocketAddr,
};

impl ClientConnection for UdpClientConnection {
    fn server_addr(&self) -> &SocketAddr {
        &self.addr
    }

    fn send_data_async(&self, wire_transaction: Vec<u8>) -> TransportResult<()> {
        self.socket.send_to(wire_transaction.as_ref(), self.addr)?;
        Ok(())
    }

    fn send_data_batch<T>(&self, buffers: &[T]) -> TransportResult<()>
    where
        T: AsRef<[u8]> + Send + Sync,
    {
        let pkts: Vec<_> = buffers.iter().zip(repeat(self.server_addr())).collect();
        batch_send(&self.socket, &pkts)?;
        Ok(())
    }

    fn send_data_batch_async(&self, buffers: Vec<Vec<u8>>) -> TransportResult<()> {
        let pkts: Vec<_> = buffers.into_iter().zip(repeat(self.server_addr())).collect();
        batch_send(&self.socket, &pkts)?;
        Ok(())
    }
}
