namespace Concord.Core.Storage;

/// <summary>The state Raft must persist before answering any RPC: the current term and the vote in it.</summary>
public readonly record struct HardState(long Term, int? VotedFor)
{
    public static HardState Initial => new(0, null);
}

/// <summary>
/// Durable Raft state: hard state, the log, and the latest snapshot. Every mutating call must be
/// durable when it returns; the node relies on that to answer RPCs safely.
/// The log holds entries (<see cref="SnapshotIndex"/>, <see cref="LastIndex"/>]; earlier entries are
/// represented by the snapshot.
/// </summary>
public interface IRaftStorage
{
    HardState LoadHardState();

    void SaveHardState(HardState state);

    /// <summary>Index covered by the latest snapshot, 0 when there is none.</summary>
    long SnapshotIndex { get; }

    /// <summary>Term of the entry at <see cref="SnapshotIndex"/>, 0 when there is none.</summary>
    long SnapshotTerm { get; }

    /// <summary>Index of the last entry, or <see cref="SnapshotIndex"/> when the log is empty.</summary>
    long LastIndex { get; }

    /// <summary>
    /// Term of the entry at <paramref name="index"/>; the snapshot term for <see cref="SnapshotIndex"/>;
    /// null when the index is compacted away or beyond the end of the log.
    /// </summary>
    long? TermAt(long index);

    /// <summary>Entries in [from, toInclusive], at most <paramref name="maxCount"/>.</summary>
    IReadOnlyList<LogEntry> GetEntries(long fromIndex, long toInclusive, int maxCount);

    /// <summary>Appends entries whose first index is <see cref="LastIndex"/> + 1.</summary>
    void Append(IReadOnlyList<LogEntry> entries);

    /// <summary>Removes the entry at <paramref name="index"/> and every later one.</summary>
    void TruncateFrom(long index);

    Snapshot? LoadSnapshot();

    /// <summary>
    /// Stores a snapshot newer than the current one. If the log holds the entry at the snapshot's last
    /// index with the same term, the entries after it are retained; otherwise the whole log is discarded
    /// (Raft paper, section 7).
    /// </summary>
    void InstallSnapshot(Snapshot snapshot);
}
