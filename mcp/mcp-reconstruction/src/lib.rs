use {
    agave_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView,
        transaction_view::SanitizedTransactionView,
    },
    solana_core::banking_stage::transaction_scheduler::receive_and_buffer::calculate_priority_and_cost,
    solana_fee_structure::FeeBudgetLimits,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_meta::StaticMeta,
    },
    solana_svm_transaction::svm_transaction::SVMTransaction,
    std::cmp::Ordering,
};

// Helper struct to avoid complex tuple types and satisfy clippy
struct SortableTx<'a> {
    priority: f64, // reward / (cost + 1)
    tx: RuntimeTransaction<ResolvedTransactionView<&'a [u8]>>,
}

/// Consume and sort transactions by priority fee (desc), compute units (desc),
/// then by first signature bytes (asc) as a deterministic tie-breaker.
///
/// Takes ownership of the input Vec and returns a new Vec sorted.
pub fn sort_transactions<'a>(
    txs: Vec<RuntimeTransaction<SanitizedTransactionView<&'a [u8]>>>,
    bank: &Bank,
) -> Vec<RuntimeTransaction<ResolvedTransactionView<&'a [u8]>>> {
    let mut keyed: Vec<SortableTx<'a>> = Vec::with_capacity(txs.len());

    for tx in txs.into_iter() {
        let resolved_tx = RuntimeTransaction::<ResolvedTransactionView<_>>::try_from(
            tx,
            None,
            bank.get_reserved_account_keys(),
        )
        .expect("resolve tx");

        let compute_budget_limits = resolved_tx
            .compute_budget_instruction_details()
            .sanitize_and_convert_to_compute_budget_limits(&bank.feature_set)
            .expect("compute budget limits");

        let fee_budget_limits = FeeBudgetLimits::from(compute_budget_limits);

        // calculate priority & cost (immutable reference)
        let (reward_u64, cost_u64) =
            calculate_priority_and_cost(&resolved_tx, &fee_budget_limits, bank);

        // compute ratio as double, avoid division by zero by adding 1
        let ratio = (reward_u64 as f64) / ((cost_u64 as f64) + 1.0);

        keyed.push(SortableTx {
            priority: ratio,
            tx: resolved_tx,
        });
    }

    // Sort by ratio desc, then by pubkey bytes asc (lexicographic)
    keyed.sort_by(|a, b| {
        let ord = b
            .priority
            .partial_cmp(&a.priority)
            .unwrap_or(Ordering::Equal);
        if ord != Ordering::Equal {
            return ord;
        }
        a.tx.signature().cmp(b.tx.signature())
    });

    keyed.into_iter().map(|k| k.tx).collect()
}

pub fn merge_shreds<'a>(
    bank: &Bank,
    mut left: Vec<RuntimeTransaction<SanitizedTransactionView<&'a [u8]>>>,
    mut right: Vec<RuntimeTransaction<SanitizedTransactionView<&'a [u8]>>>,
) -> Vec<RuntimeTransaction<ResolvedTransactionView<&'a [u8]>>> {
    left.append(&mut right);
    sort_transactions(left, bank)
}
