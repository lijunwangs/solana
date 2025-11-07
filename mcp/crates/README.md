# MCP POC Workspace

- `mcp-blockstore`: storage trait with in-memory backend and optional Solana adapter (feature `solana-ledger`).
- `mcp-reconstructor`: generic over store + VC; performs VC.Verify and HECC decode.
- `mcp-crypto-vc`: Merkle VC (SHA-256).
- `mcp-hecc-mock`: GF(256) degree-1 encoder/decoder.
- `mcp-wire`: wire structs + `McpEnvelope`/`RevealShred`.
- `mcp-stage`: forwarder stage skeletons.
- `mcp-node`: runnable demo using UDP loopback.

Build POC:
```bash
cd mcp_ws
cargo run -p mcp-node
```
