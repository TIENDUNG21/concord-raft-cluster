using Concord.Core.Storage;

namespace Concord.Core.Tests.Storage;

public sealed class InMemoryRaftStorageTests
{
    private static LogEntry Entry(long index, long term, byte payload = 0) =>
        LogEntry.CreateCommand(index, term, new[] { payload });

    private static InMemoryRaftStorage WithTerms(params long[] terms)
    {
        var storage = new InMemoryRaftStorage();
        storage.Append(terms.Select((term, i) => Entry(i + 1, term)).ToArray());
        return storage;
    }

    [Fact]
    public void EmptyStorage_HasNoEntriesAndTermZeroAtIndexZero()
    {
        var storage = new InMemoryRaftStorage();

        Assert.Equal(0, storage.LastIndex);
        Assert.Equal(0, storage.TermAt(0));
        Assert.Null(storage.TermAt(1));
        Assert.Equal(HardState.Initial, storage.LoadHardState());
        Assert.Null(storage.LoadSnapshot());
    }

    [Fact]
    public void Append_RejectsNonContiguousEntries()
    {
        var storage = WithTerms(1, 1);

        Assert.Throws<ArgumentException>(() => storage.Append(new[] { Entry(4, 1) }));
        Assert.Equal(2, storage.LastIndex);
    }

    [Fact]
    public void GetEntries_RespectsRangeAndMaxCount()
    {
        var storage = WithTerms(1, 1, 2, 2, 3);

        var entries = storage.GetEntries(2, 5, 2);

        Assert.Equal(new long[] { 2, 3 }, entries.Select(e => e.Index));
        Assert.Empty(storage.GetEntries(6, 10, 5));
        Assert.Equal(3, storage.GetEntries(3, 100, 100).Count);
    }

    [Fact]
    public void TruncateFrom_RemovesSuffix()
    {
        var storage = WithTerms(1, 1, 2, 2);

        storage.TruncateFrom(3);

        Assert.Equal(2, storage.LastIndex);
        Assert.Null(storage.TermAt(3));
        storage.Append(new[] { Entry(3, 5) });
        Assert.Equal(5, storage.TermAt(3));
    }

    [Fact]
    public void SaveHardState_RejectsDecreasingTerm()
    {
        var storage = new InMemoryRaftStorage();
        storage.SaveHardState(new HardState(4, 2));

        Assert.Throws<InvalidOperationException>(() => storage.SaveHardState(new HardState(3, null)));
        Assert.Equal(new HardState(4, 2), storage.LoadHardState());
    }

    [Fact]
    public void InstallSnapshot_WithMatchingEntry_RetainsFollowingEntries()
    {
        var storage = WithTerms(1, 1, 2, 2, 3);

        storage.InstallSnapshot(new Snapshot(new SnapshotMetadata(3, 2), new byte[] { 9 }));

        Assert.Equal(3, storage.SnapshotIndex);
        Assert.Equal(5, storage.LastIndex);
        Assert.Equal(2, storage.TermAt(3));
        Assert.Null(storage.TermAt(2));
        Assert.Equal(new long[] { 4, 5 }, storage.GetEntries(4, 5, 10).Select(e => e.Index));
        Assert.Throws<ArgumentOutOfRangeException>(() => storage.GetEntries(3, 5, 10));
    }

    [Fact]
    public void InstallSnapshot_WithConflictingEntry_DiscardsWholeLog()
    {
        var storage = WithTerms(1, 1, 2, 2, 3);

        storage.InstallSnapshot(new Snapshot(new SnapshotMetadata(4, 7), new byte[] { 1 }));

        Assert.Equal(4, storage.SnapshotIndex);
        Assert.Equal(4, storage.LastIndex);
        Assert.Equal(7, storage.TermAt(4));
        Assert.Null(storage.TermAt(5));
    }

    [Fact]
    public void InstallSnapshot_BeyondLog_DiscardsWholeLog()
    {
        var storage = WithTerms(1, 1);

        storage.InstallSnapshot(new Snapshot(new SnapshotMetadata(10, 4), ReadOnlyMemory<byte>.Empty));

        Assert.Equal(10, storage.LastIndex);
        storage.Append(new[] { Entry(11, 4) });
        Assert.Equal(11, storage.LastIndex);
    }

    [Fact]
    public void InstallSnapshot_OlderThanCurrent_Throws()
    {
        var storage = WithTerms(1, 1, 1);
        storage.InstallSnapshot(new Snapshot(new SnapshotMetadata(3, 1), ReadOnlyMemory<byte>.Empty));

        Assert.Throws<ArgumentException>(() => storage.InstallSnapshot(new Snapshot(new SnapshotMetadata(2, 1), ReadOnlyMemory<byte>.Empty)));
    }

    [Fact]
    public void TruncateFrom_IntoSnapshot_Throws()
    {
        var storage = WithTerms(1, 1, 1);
        storage.InstallSnapshot(new Snapshot(new SnapshotMetadata(2, 1), ReadOnlyMemory<byte>.Empty));

        Assert.Throws<ArgumentOutOfRangeException>(() => storage.TruncateFrom(2));
    }
}
