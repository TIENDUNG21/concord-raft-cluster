namespace Concord.Core;

/// <summary>What a leader knows about one follower, for diagnostics.</summary>
public sealed record PeerStatus(int Id, long MatchIndex, long NextIndex, ReplicationState State);

/// <summary>A point-in-time view of a node, for status endpoints and tooling.</summary>
public sealed record NodeStatus(
    int Id,
    RaftRole Role,
    long Term,
    int? LeaderId,
    int? VotedFor,
    long CommitIndex,
    long LastApplied,
    long LastLogIndex,
    long LastLogTerm,
    long SnapshotIndex,
    IReadOnlyList<int> Members,
    IReadOnlyList<PeerStatus> Peers);

public sealed partial class RaftNode
{
    public NodeStatus Status() => new(
        Id,
        _role,
        _currentTerm,
        _leaderId,
        _votedFor,
        _commitIndex,
        _lastApplied,
        _storage.LastIndex,
        LastLogTerm,
        _storage.SnapshotIndex,
        _members,
        _role == RaftRole.Leader
            ? _peers.Select(p => new PeerStatus(p, _progress[p].MatchIndex, _progress[p].NextIndex, _progress[p].State)).ToArray()
            : Array.Empty<PeerStatus>());
}
