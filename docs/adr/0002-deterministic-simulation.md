# 2. Deterministic simulation instead of real-time fault injection

Status: accepted

## Context

Jepsen-style testing (real processes, real network faults, wall-clock time) finds real bugs but is slow
and cannot replay a failure exactly. FoundationDB showed the alternative: run the whole system in a
single-threaded simulator where every source of nondeterminism is controlled, so one seed reproduces one
run.

## Decision

`Concord.Simulation` is a discrete-event simulator. Events are ordered by virtual time and then by
insertion order. All randomness comes from xoshiro256** (Blackman and Vigna) seeded via SplitMix64,
implemented in the project so the stream does not depend on the .NET version's `System.Random`. The
generator is forked into separate streams for the network, faults, election timers and clients.

The trace keeps a running FNV-1a hash of every event; a test asserts that the same seed produces the
same hash, which catches accidental nondeterminism (iterating a hash set, reading the wall clock).

## Consequences

- A 12-second run with five nodes, five clients and around 1,300 operations takes on the order of 10 ms,
  so CI can afford tens of thousands of seeds.
- Any failure is reported with its seed and replays exactly with `concord-sim run --seed N --trace`.
- What is not simulated is not tested here: real HTTP, real fsync behaviour, clock skew, and process
  scheduling. The server has its own integration tests and an end-to-end smoke test for that reason.
