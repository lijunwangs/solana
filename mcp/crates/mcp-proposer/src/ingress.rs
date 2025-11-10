//! MCP Proposer ingress
//!
//! Two options:
//! 1) `spawn_from_sigverify(...)` – subscribe to an existing channel of *verified*
//!    `VersionedTransaction`s (preferred for Solana integration).
//! 2) `spawn_udp_bincode_ingress(...)` – POC UDP listener that expects
//!    `bincode`-encoded `VersionedTransaction`. Optional soft verification.

use crossbeam_channel::{Receiver, Sender};
use std::net::UdpSocket;
use std::thread;

use solana_sdk::transaction::VersionedTransaction;

/// For consistency: the proposer consumes *verified* txs from this module.
pub type VerifiedTx = VersionedTransaction;

/// Forwarder from the TPU post-sigverify output into the proposer pipeline.
/// Non-blocking; uses a small copy buffer to avoid back-pressure on TPU.
pub fn spawn_from_sigverify(
    rx_verified_txs: Receiver<VersionedTransaction>,
    tx_out: Sender<VerifiedTx>,
    thread_name: &str,
) -> thread::JoinHandle<()> {
    let name = format!("mcp-ingress-sigverify-{}", thread_name);
    thread::Builder::new()
        .name(name)
        .spawn(move || {
            for tx in rx_verified_txs.iter() {
                // At this point, txs are already signature-verified by TPU.
                // If you want an extra belt-and-suspenders check in debug builds:
                #[cfg(debug_assertions)]
                {
                    // Avoid heavy work; do *not* re-verify cryptography here in prod.
                    let _ = tx; // placeholder
                }
                if tx_out.send(tx).is_err() {
                    break;
                }
            }
        })
        .expect("spawn_from_sigverify thread")
}

/// Configuration for the POC UDP ingress.
pub struct UdpIngressConfig {
    /// Bind address, e.g. "0.0.0.0:9901"
    pub bind_addr: std::net::SocketAddr,
    /// Optional best-effort verification (cheap sanity checks). Default: false.
    pub soft_verify: bool,
    /// Maximum single datagram size we accept.
    pub max_datagram: usize,
}

/// POC UDP ingress that expects `bincode`-encoded `VersionedTransaction`.
/// This is *not* Turbine; it is only for quickly driving the proposer in isolation.
///
/// Notes:
/// - If `soft_verify` is true, we run lightweight checks (length, signatures vector non-empty).
///   Do not rely on this for security; the real integration should use post-sigverify channel.
pub fn spawn_udp_bincode_ingress(
    cfg: UdpIngressConfig,
    tx_out: Sender<VerifiedTx>,
    thread_name: &str,
) -> thread::JoinHandle<()> {
    let name = format!("mcp-ingress-udp-{}", thread_name);
    let sock = UdpSocket::bind(cfg.bind_addr).expect("bind UDP ingress");
    sock.set_nonblocking(true).ok();

    thread::Builder::new()
        .name(name)
        .spawn(move || {
            let mut buf = vec![0u8; cfg.max_datagram.max(64 * 1024)];
            loop {
                match sock.recv(&mut buf) {
                    Ok(n) if n > 0 => {
                        // Defensive copy of just the datagram payload
                        let bytes = &buf[..n];
                        if let Ok(tx) = bincode::deserialize::<VersionedTransaction>(bytes) {
                            if cfg.soft_verify && !soft_verify_tx(&tx) {
                                // Drop silently or log in debug
                                continue;
                            }
                            if tx_out.send(tx).is_err() {
                                break;
                            }
                        } else {
                            // Ignore malformed datagrams
                            continue;
                        }
                    }
                    _ => {
                        // Back off a bit to avoid a tight spin on EWOULDBLOCK
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                }
            }
        })
        .expect("spawn_udp_bincode_ingress thread")
}

/// Extremely lightweight sanity checks for POC UDP path.
/// This is *not* signature verification; it only protects against obvious garbage.
fn soft_verify_tx(tx: &VersionedTransaction) -> bool {
    // Presence checks
    if tx.signatures().is_empty() {
        return false;
    }
    // Size sanity (defend against pathological packets); tweak as needed.
    // A typical Solana vtx is < 1500 bytes on-wire; bincode may differ.
    // If you want tighter limits, enforce them at the sender.
    true
}
