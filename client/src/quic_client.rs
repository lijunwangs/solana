//! Simple client that connects to a given UDP port with the QUIC protocol and provides
//! an interface for sending transactions which is restricted by the server's flow control.

pub use solana_quic_client::quic_client::QuicClientConnection;
use {
    crate::{
        nonblocking::tpu_connection::ClientConnection as NonblockingTpuConnection,
        tpu_connection::ClientConnection,
    },
    solana_quic_client::quic_client::temporary_pub::*,
    solana_sdk::transport::Result as TransportResult,
    std::net::SocketAddr,
};

impl ClientConnection for QuicClientConnection {
    fn server_addr(&self) -> &SocketAddr {
        self.inner.server_addr()
    }

    fn send_data_batch<T>(&self, buffers: &[T]) -> TransportResult<()>
    where
        T: AsRef<[u8]> + Send + Sync,
    {
        RUNTIME.block_on(self.inner.send_data_batch(buffers))?;
        Ok(())
    }

    fn send_data_async(&self, wire_transaction: Vec<u8>) -> TransportResult<()> {
        let _lock = ASYNC_TASK_SEMAPHORE.acquire();
        let inner = self.inner.clone();

        let _handle = RUNTIME
            .spawn(async move { send_wire_transaction_async(inner, wire_transaction).await });
        Ok(())
    }

    fn send_data_batch_async(&self, buffers: Vec<Vec<u8>>) -> TransportResult<()> {
        let _lock = ASYNC_TASK_SEMAPHORE.acquire();
        let inner = self.inner.clone();
        let _handle =
            RUNTIME.spawn(async move { send_wire_transaction_batch_async(inner, buffers).await });
        Ok(())
    }
}
