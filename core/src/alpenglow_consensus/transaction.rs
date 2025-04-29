use solana_transaction::versioned::VersionedTransaction;

pub trait AlpenglowVoteTransaction: Clone + Default {}

impl AlpenglowVoteTransaction for VersionedTransaction {}
