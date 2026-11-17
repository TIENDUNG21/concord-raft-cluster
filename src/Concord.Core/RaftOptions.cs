namespace Concord.Core;

/// <summary>Tuning knobs for a node. Defaults suit a LAN; the simulator uses the same defaults.</summary>
public sealed class RaftOptions
{
    /// <summary>How often a leader sends AppendEntries when it has nothing new to say.</summary>
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Election timeouts are drawn uniformly from [min, max).</summary>
    public TimeSpan ElectionTimeoutMin { get; init; } = TimeSpan.FromMilliseconds(150);

    public TimeSpan ElectionTimeoutMax { get; init; } = TimeSpan.FromMilliseconds(300);

    /// <summary>Upper bound on entries in one AppendEntries message.</summary>
    public int MaxEntriesPerMessage { get; init; } = 64;

    /// <summary>Take a snapshot once this many entries were applied since the last one. 0 disables.</summary>
    public int SnapshotThreshold { get; init; } = 1024;

    /// <summary>Run a pre-vote round before incrementing the term (thesis 9.6).</summary>
    public bool PreVote { get; init; } = true;

    /// <summary>
    /// A leader that has not heard from a quorum within an election timeout steps down, and followers
    /// that recently heard from a leader ignore vote requests (thesis 4.2.3 and 6.2).
    /// </summary>
    public bool CheckQuorum { get; init; } = true;

    /// <summary>
    /// DELIBERATELY UNSAFE, only reachable from the simulator: grant votes without the "candidate's log
    /// is at least as up-to-date" check, so a leader can be elected without committed entries.
    /// </summary>
    internal bool UnsafeIgnoreLogInVotes { get; init; }

    public void Validate()
    {
        if (HeartbeatInterval <= TimeSpan.Zero)
        {
            throw new ArgumentException("HeartbeatInterval must be positive.");
        }

        if (ElectionTimeoutMin <= HeartbeatInterval)
        {
            throw new ArgumentException("ElectionTimeoutMin must be greater than HeartbeatInterval.");
        }

        if (ElectionTimeoutMax <= ElectionTimeoutMin)
        {
            throw new ArgumentException("ElectionTimeoutMax must be greater than ElectionTimeoutMin.");
        }

        if (MaxEntriesPerMessage < 1)
        {
            throw new ArgumentException("MaxEntriesPerMessage must be at least 1.");
        }

        if (SnapshotThreshold < 0)
        {
            throw new ArgumentException("SnapshotThreshold must not be negative.");
        }
    }
}
