use {solana_sdk::transaction::SanitizedTransaction, std::time::Instant};

/// Sanitized transaction with optional start_time
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExtendedSanitizedTransaction {
    pub transaction: SanitizedTransaction,
    pub start_time: Option<Instant>,
}

impl From<SanitizedTransaction> for ExtendedSanitizedTransaction {
    fn from(value: SanitizedTransaction) -> Self {
        Self {
            transaction: value,
            start_time: None,
        }
    }
}
