fn gf_mul(mut a: u8, mut b: u8) -> u8 {
    let mut p = 0u8;
    for _ in 0..8 {
        if (b & 1) != 0 { p ^= a; }
        let hi = a & 0x80;
        a <<= 1;
        if hi != 0 { a ^= 0x1b; }
        b >>= 1;
    }
    p
}
fn gf_inv(a: u8) -> u8 {
    let mut y: u8 = 1;
    let mut x = a;
    for _ in 0..254 { y = gf_mul(y, x); }
    y
}
pub fn encode_degree1(orig_syms: &[Vec<u8>], xs: &[u8]) -> Vec<Vec<u8>> {
    assert_eq!(orig_syms.len(), 2, "K=2 required");
    let (a0, a1) = (&orig_syms[0], &orig_syms[1]);
    xs.iter().map(|&x| {
        a0.iter().zip(a1.iter()).map(|(&b0, &b1)| b0 ^ gf_mul(b1, x)).collect()
    }).collect()
}
pub fn decode_degree1(points: &[(u8, Vec<u8>)]) -> (Vec<u8>, Vec<u8>) {
    assert!(points.len() >= 2);
    let (x1, ref y1) = points[0];
    let (x2, ref y2) = points[1];
    assert_ne!(x1, x2);
    let dx = x1 ^ x2;
    let inv_dx = gf_inv(dx);
    let a1: Vec<u8> = y2.iter().zip(y1.iter()).map(|(&b2, &b1)| (b2 ^ b1)).map(|d| gf_mul(d, inv_dx)).collect();
    let a0: Vec<u8> = y1.iter().zip(a1.iter()).map(|(&b1, &aa1)| b1 ^ gf_mul(aa1, x1)).collect();
    (a0, a1)
}
