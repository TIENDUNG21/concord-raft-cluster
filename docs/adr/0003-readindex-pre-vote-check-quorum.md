# 3. ReadIndex reads, with pre-vote and check-quorum

Status: accepted

## Context

Reads must be linearizable. Serving them from the leader's local state is wrong once a newer leader has
been elected elsewhere: the old leader does not know it has been deposed. The options are to put reads
in the log (simple, but every read costs a disk write on a majority), to use leader leases (cheap, but
correctness depends on bounded clock drift), or ReadIndex (Raft thesis 6.4).

## Decision

Reads use ReadIndex: the leader waits for an entry of its own term to commit, takes its commit index,
confirms leadership with one heartbeat round answered by a majority, and serves the read once that index
is applied. Heartbeats carry a round number that followers echo, so one round confirms every read that
was waiting for it.

Elections use pre-vote and check-quorum (thesis 9.6, 6.2): a node must learn it could win before it
increments its term, followers that recently heard from a leader refuse votes, and a leader without a
majority steps down after an election timeout.

## Consequences

- No clock assumptions anywhere in the safety argument.
- A read costs one round trip to a majority but no disk write.
- A leader cut off from the majority stops serving reads at once (they wait and then fail when it steps
  down) and stops accepting writes within an election timeout.
- The simulator's `unsafe-reads` switch removes the quorum round. The linearizability checker catches the
  resulting stale reads, which shows the round is load-bearing rather than decorative.
