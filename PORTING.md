# Mining coordinator coverage

Source: `0xLattice/lattice-node@3df9e0a786c7b103a3da88d1c3881efc81706255`:
`LatticeMinerCore/MinerLoopLogic.swift`, `ProofOfWork.swift`,
`LatticeMiningCoordinator/MiningCoordinator.swift`, `MiningWorkerProcess.swift`,
and `LatticeMiningCoordinatorTool/main.swift`. MIT license preserved. The Rust
consensus prefix/canonical block representation delegates to sibling
`lattice-primitives`, `lattice-pow`, and `lattice-cashew` ports of pinned Lattice.
The external GPU worker remains its own repository; it is not duplicated here.

## Implemented

- Balanced contiguous nonce ranges, remainder distribution, UInt64 wrapping,
  same-work cursor advancement and changed-work cursor reset.
- Canonical template block decoding and consensus prefix derivation, normalized
  descending unique target ladder, recipients and bounded minimum-work parsing.
- Immutable per-task Arc assignments; concurrent worker fanout; wrong-work-ID
  results ignored; first final outcome cancels other searches.
- Immediate/repeating freshness probes, pre-submit freshness check, template
  lifetime search cancellation, and caller cancellation during requests/backoff.
- Locally recomputed nonce hashes for merged-target continuation; child-only
  submissions continue the remainder of the same range toward harder targets;
  redundant already-cleared shares are skipped. The node remains authoritative
  for hits missing all targets, matching Swift.
- Bounded same-solution retries; 401/403 fatal; clean refusal never retried;
  stale work aborts retry. Metrics match actual Swift cycle fixtures.
- Actual CPU search and external process workers. Process stdout is bounded,
  stderr is concurrently drained into a 4-KiB tail, and both drains have a
  deadline. Unix workers use isolated process groups; cancellation/exit cleans
  descendants as well as the direct child.
- `/mining/templates`, `/status`, `/mining/work` JSON HTTP client, fresh cookie
  authentication, status digest fallback to tipCID, definitive 4xx refusal
  decoding, deadlines, response byte limits, no redirects, and loopback HTTP or
  HTTPS URL policy.
- Useful library and CLI, continuous and once modes, CPU/external selection,
  operator rewards/min-work request fields, JSON cycle output and fatal exits.

## Evidence

`tools/swift-fixtures.py` compiles the actual unchanged pinned Swift miner core
and coordinator in the full cached Lattice package. Its node and worker are
explicit injected test doubles. `tests/swift-evidence.json` records source,
lockfile and corpus hashes. The differential test compares 72 range cases,
18 target/minimum-work parsing cases and 13 coordinator cycle transcripts,
including assignments, submissions, outcomes and six metrics. Cycles include
merged child-only continuation, cursor reset, expiry, stale retry, transient and
fatal failures. This is finite execution evidence, not a whole-system proof.

Native tests additionally cover concurrent freshness/cancellation, process
wrapping ranges and identity mismatches, invalid/flooded stdout, 200-KiB stderr,
nonzero exits, a child closing stdout before its deadline, forked descendant
teardown, HTTP status/body/cookie contracts, bounded replies, and a real CLI
round using the CPU worker against a local fixture server. These unit tests use
no live-network submission; the separate real-process integration below uses a
disposable node with an easy genesis fixture.

The separately built external worker is tested explicitly, never silently
skipped as a successful hardware test:

```sh
LATTICE_TEST_WORKER=/path/to/lattice-miner-gpu \
  cargo test --test external_worker -- --ignored
```

This tests CPU and automatic backends, a wrapping range and target exhaustion,
using a fixture prefix and no network. In this session the automatic backend
was the built worker's available platform backend; detailed GPU kernel evidence
belongs to `lattice-miner-gpu`.

## Deliberate differences and remaining limits

The process adapter fails closed on unknown status, wrong work ID/range,
missing/out-of-range found nonce, or an exhausted result with a nonce. Swift's
process decoder accepts some of these and lets later layers discard/refuse.
Rust bounds stdout in memory and continuously drains stderr rather than using
an unbounded temporary stderr file. Unix process-group teardown is implemented;
non-Unix builds kill only the direct child and do not claim subtree teardown.

HTTP transport adds explicit byte/deadline and redirect/URL restrictions. CLI
worker count is capped at 1024 to prevent accidental unbounded task allocation.
Rust has an explicit cancelled cycle and owned `run` lifecycle instead of Swift
actor start/stop methods; CLI emits JSON continuously rather than mixed logs.

The expiry event bounds searching. As in Swift, submission retry checks current
staleness and uses bounded request deadlines; it does not prove that no request
can cross the node's expiry instant. The node makes the final expiry decision.
A manually constructed work item without a lifetime relies on owner cancellation.
Arbitrary injected workers/transports must cooperate with their futures' drop or
cancellation; the shipped process/CPU/HTTP implementations do.

Tests do not establish every scheduling interleaving, distributed mining fairness,
GPU-driver fault containment, or production performance. Network ingress and
consensus validity remain node responsibilities. No remote repository or push
has been created.

## Real node/process integration

`tools/node-integration.py` connects the built Rust node, mining coordinator and
external `lattice-miner-gpu --backend cpu` through authenticated localhost HTTP.
It creates a fresh private data directory, imports the pinned Swift genesis
volumes, configures no peers and copies all three binaries into the run directory
before executing them. `tests/node-integration-evidence.json` records the binary,
fixture and harness hashes from the successful run.

The run rejects a bad cookie without mining, accepts an external CPU result into
the canonical chain, changes the node tip during an outstanding worker assignment
and observes `stale`, then sends SIGINT during another assignment and observes
`cancelled`. Both interrupted worker processes disappear and neither case adds a
block. The stale/cancellation timing fixture first executes the real worker on
its unchanged assignment, then withholds its output: it verifies coordinator
freshness and process cleanup, not interruption inside a long CPU kernel.

The node restarts without bootstrap files and recovers its exact durable tip.
The coordinator mines another accepted block using the restarted node's cookie;
a second node restart recovers the final height 3 and exact CID. Two of these
three blocks come from the coordinator/external worker; the middle block advances
the tip deliberately through the real node API for the stale-work check.
No public network, production chain, GPU kernel or mixed Swift node is involved.

Reproduce from this repository with sibling repositories present:

```sh
cargo build --locked --bins
(cd ../lattice-node-rs && cargo build --locked --bins)
(cd ../lattice-miner-gpu && cargo build --locked --bins)
python3 tools/node-integration.py
```

The default evidence path is `target/node-integration.json`; use `--output` to
retain another run. Logs contain node startup/shutdown data, never cookie values.
