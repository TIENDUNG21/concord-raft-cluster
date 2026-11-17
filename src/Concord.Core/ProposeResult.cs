namespace Concord.Core;

/// <summary>
/// Outcome of <see cref="RaftNode.Propose"/>. Accepted means appended to the leader's log at
/// (<see cref="Index"/>, <see cref="Term"/>), not committed: the entry is committed only if the entry
/// later applied at that index carries the same term.
/// </summary>
public readonly record struct ProposeResult(bool Accepted, long Index, long Term, int? LeaderHint)
{
    public static ProposeResult NotLeader(int? leaderHint) => new(false, 0, 0, leaderHint);
}

/// <summary>Raised when the node detects that a Raft safety property would be broken.</summary>
public sealed class RaftSafetyException : Exception
{
    public RaftSafetyException()
    {
    }

    public RaftSafetyException(string message)
        : base(message)
    {
    }

    public RaftSafetyException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
