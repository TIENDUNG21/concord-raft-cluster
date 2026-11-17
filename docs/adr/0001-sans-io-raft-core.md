# 1. Raft core without I/O, driven by the host

Status: accepted

## Context

The goal is a Raft implementation that can be tested under thousands of fault schedules and run as a
real server. Tests that start threads, sockets and timers are slow and do not reproduce: a failure seen
once in CI may never be seen again.

## Decision

`RaftNode` is a single-threaded state machine. It reads time only through `IClock`, randomness through
`IRandomSource`, and talks to the world only through `ITransport`, `IRaftStorage` and `IStateMachine`.
The host decides when to call `Step`, `Tick`, `Propose` and `RequestRead`, and guarantees calls do not
overlap. `NextDeadline` tells the host when the next timer is due, so a host can sleep exactly that long
(the server) or jump virtual time there (the simulator).

Completions (applied entries, ReadIndex results) are callbacks invoked synchronously on the caller's
thread rather than `Task`s, because task continuations would bring the thread pool back into the
simulator and with it nondeterminism.

## Consequences

- The simulator runs the production Raft code, not a model of it.
- The server needs a loop that serializes all access to the node (`RaftHost`). Throughput is bounded by
  one core for Raft bookkeeping, which is far from the bottleneck for this store.
- Storage calls are synchronous and must be durable when they return. The file storage fsyncs on every
  append; batching appends across requests is possible later without changing the interface.
