# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-11-17

### Added
- Raft core with no I/O: randomized elections, pre-vote, check-quorum, vote lease, retransmitted vote
  requests, pipelined log replication with conflict-term hints, the current-term commit rule with a
  leader no-op, snapshots with InstallSnapshot, and ReadIndex reads.
- Key-value state machine with put, get, delete and compare-and-set, and per-client sessions that make
  retried writes exactly-once.
- Deterministic simulator: virtual clock, xoshiro256** seeded through SplitMix64, a network with delay,
  loss, duplication, reordering, partitions and one-way links, crash, restart and bounce faults, an
  online invariant monitor, closed-loop clients and a replayable trace.
- Linearizability checker (Wing and Gong search with Lowe's memoization) for per-key KV histories,
  including operations whose outcome is unknown.
- `concord-sim` command line: parallel seed sweeps, single-seed replay with a full trace, timeline SVG
  export, and two injected bugs to show the harness catches them.
- ASP.NET Core node: `/kv/{key}` GET, PUT, DELETE, `/kv/{key}/cas`, `/cluster/status`; binary Raft
  transport over HTTP; file storage with CRC-framed log records and crash-safe snapshot installation.
- Dockerfile, three-node compose file, and a smoke test that runs against local processes or compose.
- CI: formatting, analyzers as errors, 277 tests, a 20,000-seed sweep, an injected-bug self-check, the
  local smoke test, and the Docker image build plus compose smoke test.

[1.0.0]: https://github.com/TIENDUNG21/concord-raft-cluster/releases/tag/v1.0.0
