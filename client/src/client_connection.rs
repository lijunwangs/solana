use {
    enum_dispatch::enum_dispatch, solana_quic_client::quic_client::QuicClientConnection,
    solana_sdk::transport::Result as TransportResult,
    solana_udp_client::udp_client::UdpClientConnection, std::net::SocketAddr,
};

#[enum_dispatch]
pub enum BlockingConnection {
    UdpClientConnection,
    QuicClientConnection,
}

#[enum_dispatch(BlockingConnection)]
pub trait ClientConnection {
    fn server_addr(&self) -> &SocketAddr;

    fn send_data<T>(&self, data: T) -> TransportResult<()>
    where
        T: AsRef<[u8]> + Send + Sync,
    {
        self.send_data_batch(&[data])
    }

    fn send_data_async(&self, data: Vec<u8>) -> TransportResult<()>;

    fn send_data_batch<T>(&self, buffers: &[T]) -> TransportResult<()>
    where
        T: AsRef<[u8]> + Send + Sync;

    fn send_data_batch_async(&self, buffers: Vec<Vec<u8>>) -> TransportResult<()>;
}
