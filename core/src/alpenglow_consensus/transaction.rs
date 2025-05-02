use solana_transaction::versioned::VersionedTransaction;

pub trait AlpenglowVoteTransaction: Clone + std::fmt::Debug {
    fn new_for_test() -> Self;
}

impl AlpenglowVoteTransaction for VersionedTransaction {
    fn new_for_test() -> Self {
        Self::default()
    }
}
