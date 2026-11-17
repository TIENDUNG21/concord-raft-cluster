namespace Concord.Core.Storage;

/// <summary>
/// Storage kept in memory. The simulator keeps one instance per node across crashes, which models a
/// disk where every write is synced before the call returns.
/// </summary>
public sealed class InMemoryRaftStorage : IRaftStorage
{
    private readonly List<LogEntry> _entries = new();
    private HardState _hardState = HardState.Initial;
    private Snapshot? _snapshot;

    public long SnapshotIndex => _snapshot?.Metadata.LastIncludedIndex ?? 0;

    public long SnapshotTerm => _snapshot?.Metadata.LastIncludedTerm ?? 0;

    public long LastIndex => SnapshotIndex + _entries.Count;

    public HardState LoadHardState() => _hardState;

    public void SaveHardState(HardState state)
    {
        if (state.Term < _hardState.Term)
        {
            throw new InvalidOperationException($"Term must not decrease ({_hardState.Term} -> {state.Term}).");
        }

        _hardState = state;
    }

    public long? TermAt(long index)
    {
        if (index == SnapshotIndex)
        {
            return SnapshotTerm;
        }

        if (index < SnapshotIndex || index > LastIndex)
        {
            return null;
        }

        return _entries[(int)(index - SnapshotIndex - 1)].Term;
    }

    public IReadOnlyList<LogEntry> GetEntries(long fromIndex, long toInclusive, int maxCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxCount);
        if (fromIndex <= SnapshotIndex)
        {
            throw new ArgumentOutOfRangeException(nameof(fromIndex), fromIndex, $"Entries up to {SnapshotIndex} are compacted.");
        }

        var last = Math.Min(toInclusive, LastIndex);
        if (fromIndex > last || maxCount == 0)
        {
            return Array.Empty<LogEntry>();
        }

        var count = (int)Math.Min(last - fromIndex + 1, maxCount);
        return _entries.GetRange((int)(fromIndex - SnapshotIndex - 1), count);
    }

    public void Append(IReadOnlyList<LogEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        var expected = LastIndex + 1;
        foreach (var entry in entries)
        {
            if (entry.Index != expected)
            {
                throw new ArgumentException($"Entry index {entry.Index} is not contiguous (expected {expected}).", nameof(entries));
            }

            expected++;
        }

        _entries.AddRange(entries);
    }

    public void TruncateFrom(long index)
    {
        if (index <= SnapshotIndex)
        {
            throw new ArgumentOutOfRangeException(nameof(index), index, $"Cannot truncate into the snapshot (index {SnapshotIndex}).");
        }

        if (index > LastIndex)
        {
            return;
        }

        var keep = (int)(index - SnapshotIndex - 1);
        _entries.RemoveRange(keep, _entries.Count - keep);
    }

    public Snapshot? LoadSnapshot() => _snapshot;

    public void InstallSnapshot(Snapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        var meta = snapshot.Metadata;
        if (meta.LastIncludedIndex < SnapshotIndex)
        {
            throw new ArgumentException($"Snapshot at {meta.LastIncludedIndex} is older than the current one at {SnapshotIndex}.", nameof(snapshot));
        }

        if (TermAt(meta.LastIncludedIndex) == meta.LastIncludedTerm)
        {
            var drop = (int)(meta.LastIncludedIndex - SnapshotIndex);
            _entries.RemoveRange(0, drop);
        }
        else
        {
            _entries.Clear();
        }

        _snapshot = snapshot;
    }
}
