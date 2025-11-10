// mcp-proposer/src/batcher.rs
use {
    solana_transaction::versioned::VersionedTransaction,
    std::time::{Duration, Instant},
};

pub struct Batcher {
    max_txs: usize,
    max_bytes: usize,
    max_wait: Duration,
    cur: Vec<VersionedTransaction>,
    cur_bytes: usize,
    start: Instant,
}
impl Batcher {
    pub fn new(max_txs: usize, max_bytes: usize, max_wait: Duration) -> Self {
        Self {
            max_txs,
            max_bytes,
            max_wait,
            cur: Vec::new(),
            cur_bytes: 0,
            start: Instant::now(),
        }
    }
    pub fn push_and_maybe_cut(
        &mut self,
        vtx: VersionedTransaction,
    ) -> Option<Vec<VersionedTransaction>> {
        let sz = bincode::serialize(&vtx).map(|v| v.len()).unwrap_or(0);
        self.cur_bytes += sz;
        self.cur.push(vtx);
        let timeup = self.start.elapsed() >= self.max_wait;
        let full = self.cur.len() >= self.max_txs || self.cur_bytes >= self.max_bytes;
        if timeup || full {
            let mut out = Vec::new();
            std::mem::swap(&mut out, &mut self.cur);
            self.cur_bytes = 0;
            self.start = Instant::now();
            Some(out)
        } else {
            None
        }
    }
}
