use tonic;

tonic::include_proto!("accountsdb_repl");

use {solana_client::rpc_sender::RpcSender, std::net::SocketAddr};

pub struct ReplicaRpcClient {
    sender: Box<dyn RpcSender + Send + Sync + 'static>,
}

impl ReplicaRpcClient {
    pub fn new(rpc_peer: &SocketAddr) {}
}
