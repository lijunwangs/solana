// A tiny GF(256) with AES polynomial x^8 + x^4 + x^3 + x + 1 (0x11b).
// Provides degree-1 encode and decode per byte across symbols.
// K=2 only, N arbitrary (we'll use N=2 in the demo).

fn gf_add(a: u8, b: u8) -> u8 { a ^ b }

fn gf_mul(mut a: u8, mut b: u8) -> u8 {
    let mut p = 0u8;
    for _ in 0..8 {
        if (b & 1) != 0 { p ^= a; }
        let hi = a & 0x80;
        a <<= 1;
        if hi != 0 { a ^= 0x1b; } // 0x11b without the leading 1 bit
        b >>= 1;
    }
    p
}

fn gf_inv(a: u8) -> u8 {
    // a^(254) in GF(2^8) gives inverse (Fermat's little theorem). Fast enough for POC.
    let mut x = a;
    // exponentiation by squaring, but simple loop is fine here
    let mut y: u8 = 1;
    for _ in 0..254 {
        y = gf_mul(y, x);
    }
    y
}

pub fn encode_degree1(orig_syms: &[Vec<u8>], xs: &[u8]) -> Vec<Vec<u8>> {
    assert!(orig_syms.len() == 2, "K=2 required");
    let (a0, a1) = (&orig_syms[0], &orig_syms[1]);
    xs.iter().map(|&x| {
        a0.iter().zip(a1.iter()).map(|(&b0, &b1)| {
            gf_add(b0, gf_mul(b1, x))
        }).collect::<Vec<u8>>()
    }).collect::<Vec<_>>()
}

pub fn decode_degree1(points: &[(u8, Vec<u8>)]) -> (Vec<u8>, Vec<u8>) {
    assert!(points.len() >= 2, "need at least 2 points");
    let (x1, ref y1) = points[0];
    let (x2, ref y2) = points[1];
    assert!(x1 != x2, "distinct x required");
    // For degree-1 f(x) = a0 + a1*x
    // a1 = (y2 - y1) * (x2 - x1)^-1 ; a0 = y1 - a1*x1
    let dx = gf_add(x2, x1);           // subtraction == addition in GF(2^8)
    let inv_dx = gf_inv(dx);
    let a1: Vec<u8> = y2.iter().zip(y1.iter()).map(|(&b2, &b1)| gf_mul(gf_add(b2, b1), inv_dx)).collect();
    let a0: Vec<u8> = y1.iter().zip(a1.iter()).map(|(&b1, &aa1)| gf_add(b1, gf_mul(aa1, x1))).collect();
    (a0, a1)
}
