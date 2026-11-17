namespace Concord.Core;

/// <summary>
/// Server states. <see cref="PreCandidate"/> is the pre-vote phase (Raft thesis 9.6): the node asks
/// whether it could win before it increments its term, so a partitioned node cannot inflate terms.
/// </summary>
public enum RaftRole
{
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}
