# MCP POC Workspace

This is a phase-structured Rust workspace for an MCP-style commit-then-reveal pipeline.

- `mcp-proposer` → Phase 1 (stubbed here)
- `mcp-relay` → Phase 2 (attest & reveal stubs)
- `mcp-consensus` → Phase 3 (leader aggregation µ-check; Π_ab shim)
- `mcp-reconstructor` → Phase 4 (Alg. 4 POC: ingest reveal shreds, VC verify, threshold reconstruct)
- `mcp-blockstore` → in-memory KV; swap with RocksDB later
- `mcp-crypto-vc` → Vector commitment trait (+ Merkle VC stub)
- `mcp-crypto-te` → Threshold encryption facade (stub)
- `mcp-wire` → On-the-wire structs
- `mcp-types` → Common IDs, params

Run the demo:

```bash
cd mcp
cargo run -p mcp-node
```
