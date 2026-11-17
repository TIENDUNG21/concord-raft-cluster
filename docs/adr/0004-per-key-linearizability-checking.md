# 4. Per-key linearizability checking of client histories

Status: accepted

## Context

The invariant monitor checks Raft's internal properties, but users only see the API. The property that
matters to them is linearizability (Herlihy and Wing, 1990) of the operations they observe, including
operations that timed out with an unknown outcome. Checking linearizability is NP-complete in general,
so the checker needs to be both correct and fast enough for every simulated run.

## Decision

`Concord.Checker` implements the Wing and Gong (1993) backtracking search with Lowe's (2017) memoization
of (linearized set, state), the algorithm Porcupine uses. Histories are split by key, which is valid
because linearizability is a local property, and each key is checked as a register with compare-and-set.
Operations the client gave up on have an infinite return time and an unknown output.

The checker has no dependency on the store: it takes operations with call/return times and a sequential
model. It is tested on hand-written histories (stale reads, values never written, two successful
compare-and-sets, pending writes that may or may not have happened) and on generated histories that are
linearizable by construction and become illegal when one observed value is corrupted.

## Consequences

- Every simulated run gets a linearizability verdict, not only the Raft invariants.
- Exactly-once writes matter for the verdict: without client sessions a retried compare-and-set could
  apply twice and be reported as a violation.
- A step budget returns "inconclusive" instead of hanging on a pathological history. The sweep reports
  how many runs were inconclusive.
