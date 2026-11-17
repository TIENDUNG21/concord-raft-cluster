namespace Concord.Core.Messages;

/// <summary>Base of every Raft RPC. Requests and responses are one-way messages; there are no calls.</summary>
public abstract record RaftMessage(int From, int To, long Term);

/// <summary>
/// Vote request. With <see cref="PreVote"/> set, <see cref="RaftMessage.Term"/> is the term the sender
/// would campaign in; receivers neither change their term nor record a vote.
/// </summary>
public sealed record RequestVote(int From, int To, long Term, long LastLogIndex, long LastLogTerm, bool PreVote)
    : RaftMessage(From, To, Term);

public sealed record RequestVoteResponse(int From, int To, long Term, bool Granted, bool PreVote)
    : RaftMessage(From, To, Term);

/// <summary>
/// Replication and heartbeat. <see cref="Round"/> is echoed by the follower; the leader uses it to
/// confirm leadership for ReadIndex reads.
/// </summary>
public sealed record AppendEntries(
    int From,
    int To,
    long Term,
    long PrevLogIndex,
    long PrevLogTerm,
    IReadOnlyList<LogEntry> Entries,
    long LeaderCommit,
    long Round) : RaftMessage(From, To, Term);

/// <summary>
/// On success <see cref="MatchIndex"/> is the last index known to match the leader. On rejection
/// <see cref="ConflictTerm"/>/<see cref="ConflictIndex"/> let the leader skip a whole term at once
/// (0 term means the follower's log is too short and <see cref="ConflictIndex"/> is its length + 1).
/// </summary>
public sealed record AppendEntriesResponse(
    int From,
    int To,
    long Term,
    bool Success,
    long MatchIndex,
    long ConflictIndex,
    long ConflictTerm,
    long Round) : RaftMessage(From, To, Term);

/// <summary>Sends a whole snapshot in one message (the KV snapshots are small).</summary>
public sealed record InstallSnapshot(int From, int To, long Term, Snapshot Snapshot, long Round)
    : RaftMessage(From, To, Term);

/// <summary><see cref="MatchIndex"/> is the index through which the follower now matches the leader.</summary>
public sealed record InstallSnapshotResponse(int From, int To, long Term, long MatchIndex, long Round)
    : RaftMessage(From, To, Term);
