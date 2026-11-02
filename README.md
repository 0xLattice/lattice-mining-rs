# lattice-mining (Rust)

Node-facing mining coordinator for the 0xLattice platform. It fetches immutable
work, partitions the nonce space, runs CPU or external workers, checks freshness,
and submits work IDs plus nonces. It supports merged targets: a child-only hit
is submitted once and its remaining range continues toward harder targets.

```sh
cargo build --release
cargo test --locked
cargo clippy --all-targets -- -D warnings
```

The binary is `lattice-mining-coordinator`. `--help` lists its options.
`--worker-executable /path/to/lattice-miner-gpu` uses the existing separate Rust
GPU worker; repeat `--worker-arg=VALUE` for worker-specific options, for example
`--worker-arg=--backend --worker-arg=cpu`. Without an external executable the
coordinator uses its canonical-block CPU worker.

Configuration follows the pinned Swift tool: `--node`, `--rpc-cookie-file`,
`--workers`, `--batch-size`, repeated `--recipient Nexus/Child=address`, repeated
`--min-work Nexus/Child=2^32`, `--once`, and `--no-stale-probe`. In `--once` mode
batch size is the total span; continuous mode uses a per-worker span. Node
cookies are reread for every request. Construction and `--help` make no requests.
Actually running with `--node` fetches work and submits found solutions.

The library provides `Coordinator`, injectable `NodeClient` and `Worker` traits,
`HttpNodeClient`, `CpuWorker`, `ProcessWorker`, and parsing/range helpers. Tokio
cancellation tokens stop batches and the owned continuous runner. One mutable
coordinator serializes batches; no overlapping assignment mutation is exposed.
Continuous-run results use a bounded channel. CLI output is one JSON result per
cycle; fatal node authentication or worker failures exit nonzero.

See [PORTING.md](PORTING.md) for exact source pins, evidence and differences.

A real disposable-node integration is available in
[`tools/node-integration.py`](tools/node-integration.py). It exercises authenticated
external CPU mining, stale-work/cancellation cleanup, and durable node restart.
See [reproduction and scope](PORTING.md#real-nodeprocess-integration).
