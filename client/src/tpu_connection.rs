pub use solana_tpu_client::tpu_connection::ClientStats;
use {
    enum_dispatch::enum_dispatch,
    rayon::iter::{IntoParallelIterator, ParallelIterator},
    solana_quic_client::quic_client::QuicClientConnection,
    solana_sdk::{transaction::VersionedTransaction, transport::Result as TransportResult},
    solana_udp_client::udp_client::UdpClientConnection,
    std::net::SocketAddr,
};

#[enum_dispatch]
pub enum BlockingConnection {
    UdpClientConnection,
    QuicClientConnection,
}

#[enum_dispatch(BlockingConnection)]
pub trait TpuConnection {
    fn tpu_addr(&self) -> &SocketAddr;

    fn serialize_and_send_transaction(
        &self,
        transaction: &VersionedTransaction,
    ) -> TransportResult<()> {
        let wire_transaction =
            bincode::serialize(transaction).expect("serialize Transaction in send_batch");
        self.send_data(wire_transaction)
    }

    fn send_data<T>(&self, data: T) -> TransportResult<()>
    where
        T: AsRef<[u8]> + Send + Sync,
    {
        self.send_data_batch(&[data])
    }

    fn send_data_async(&self, data: Vec<u8>) -> TransportResult<()>;

    fn par_serialize_and_send_transaction_batch(
        &self,
        transactions: &[VersionedTransaction],
    ) -> TransportResult<()> {
        let buffers = transactions
            .into_par_iter()
            .map(|tx| bincode::serialize(&tx).expect("serialize Transaction in send_batch"))
            .collect::<Vec<_>>();

        self.send_data_batch(&buffers)
    }

    fn send_data_batch<T>(&self, buffers: &[T]) -> TransportResult<()>
    where
        T: AsRef<[u8]> + Send + Sync;

    fn send_data_batch_async(&self, buffers: Vec<Vec<u8>>) -> TransportResult<()>;
}
