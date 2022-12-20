//! Trait defining async send functions, to be used for UDP or QUIC sending

use {
    async_trait::async_trait,
    enum_dispatch::enum_dispatch,
    solana_quic_client::nonblocking::quic_client::QuicClientConnection,
    solana_sdk::{transport::Result as TransportResult},
    solana_udp_client::nonblocking::udp_client::UdpClientConnection,
    std::net::SocketAddr,
};

// Due to the existence of `crate::connection_cache::Connection`, if this is named
// `Connection`, enum_dispatch gets confused between the two and throws errors when
// trying to convert later.
#[enum_dispatch]
pub enum NonblockingConnection {
    QuicClientConnection,
    UdpClientConnection,
}

#[async_trait]
#[enum_dispatch(NonblockingConnection)]
pub trait ClientConnection {
    fn server_addr(&self) -> &SocketAddr;

    async fn send_data<T>(&self, data: T) -> TransportResult<()>
    where
        T: AsRef<[u8]> + Send + Sync;

    async fn send_data_batch<T>(&self, buffers: &[T]) -> TransportResult<()>
    where
        T: AsRef<[u8]> + Send + Sync;
}
