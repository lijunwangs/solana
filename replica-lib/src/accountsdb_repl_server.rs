use tonic::{self, transport::Endpoint, Request};

tonic::include_proto!("accountsdb_repl");

trait ReplicaUpdatedSlotsServer {
    fn get_updated_slots(self, request: &ReplicaUpdatedSlotsRequest) -> Result<ReplicaUpdatedSlotsResponse, tonic::Status>;
}

trait ReplicaAccountsServer {
    fn get_slot_accounts(self, request: &ReplicaAccountsRequest) -> Result<ReplicaAccountsResponse, tonic::Status>;
}


pub struct AccountsDbReplServer {
    updated_slots_server: dyn ReplicaUpdatedSlotsServer,
    accounts_server: dyn ReplicaAccountsServer,

}


#[tonic::async_trait]
impl accounts_db_repl_server::AccountsDbRepl for AccountsDbReplServer {

    async fn get_updated_slots(
        &self,
        request: tonic::Request<ReplicaUpdatedSlotsRequest>,
    ) -> Result<tonic::Response<ReplicaUpdatedSlotsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet."))
    }

    async fn get_slot_accounts(
        &self,
        request: tonic::Request<ReplicaAccountsRequest>,
    ) -> Result<tonic::Response<ReplicaAccountsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet."))
    }

}