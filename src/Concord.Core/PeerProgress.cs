namespace Concord.Core;

/// <summary>How the leader is currently feeding one follower.</summary>
public enum ReplicationState
{
    /// <summary>Searching for the point where the logs match; one AppendEntries in flight at a time.</summary>
    Probe,

    /// <summary>Logs match up to MatchIndex; new entries are pipelined optimistically.</summary>
    Replicate,

    /// <summary>The follower needs entries we compacted; a snapshot is on its way.</summary>
    Snapshot,
}

/// <summary>What a leader knows about one follower.</summary>
internal sealed class PeerProgress(long nextIndex)
{
    /// <summary>Index of the next entry to send.</summary>
    public long NextIndex { get; set; } = nextIndex;

    /// <summary>Highest index known to be replicated on the follower.</summary>
    public long MatchIndex { get; set; }

    public ReplicationState State { get; set; } = ReplicationState.Probe;

    /// <summary>The follower answered since the last check-quorum round.</summary>
    public bool RecentActive { get; set; } = true;

    /// <summary>When the last InstallSnapshot was sent, to retry it if it got lost.</summary>
    public TimeSpan SnapshotSentAt { get; set; }
}
