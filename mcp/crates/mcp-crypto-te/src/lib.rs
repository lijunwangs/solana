pub trait ThresholdKms {
    fn split_key(aes_key: &[u8], t: usize, n: usize) -> Vec<Vec<u8>>;
    fn combine_key(shares: &[Vec<u8>], t: usize) -> Option<Vec<u8>>;
}

pub struct ShamirKms;

impl ThresholdKms for ShamirKms {
    fn split_key(_aes_key: &[u8], _t: usize, _n: usize) -> Vec<Vec<u8>> { vec![] }
    fn combine_key(_shares: &[Vec<u8>], _t: usize) -> Option<Vec<u8>> { None }
}
