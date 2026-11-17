using System.Text;
using Concord.Core;
using Concord.Core.Storage;
using Concord.Server.Storage;

namespace Concord.Server.Tests;

public sealed class FileRaftStorageTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "concord-storage-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    private static LogEntry Entry(long index, long term) => LogEntry.CreateCommand(index, term, Encoding.UTF8.GetBytes($"cmd-{index}"));

    private static LogEntry[] Entries(long from, long to, long term) =>
        Enumerable.Range((int)from, (int)(to - from + 1)).Select(i => Entry(i, term)).ToArray();

    private FileRaftStorage Reopen(FileRaftStorage? previous = null)
    {
        previous?.Dispose();
        return FileRaftStorage.Open(_directory);
    }

    private static IReadOnlyList<LogEntry> All(FileRaftStorage storage) =>
        storage.LastIndex > storage.SnapshotIndex
            ? storage.GetEntries(storage.SnapshotIndex + 1, storage.LastIndex, int.MaxValue)
            : Array.Empty<LogEntry>();

    [Fact]
    public void Crc32_MatchesTheStandardCheckValue()
    {
        Assert.Equal(0xCBF43926u, Crc32.Compute("123456789"u8));
    }

    [Fact]
    public void FreshDirectory_StartsEmpty()
    {
        using var storage = Reopen();

        Assert.Equal(HardState.Initial, storage.LoadHardState());
        Assert.Equal(0, storage.LastIndex);
        Assert.Null(storage.LoadSnapshot());
    }

    [Fact]
    public void HardStateAndEntries_SurviveReopen()
    {
        var storage = Reopen();
        storage.SaveHardState(new HardState(3, 2));
        storage.Append(Entries(1, 5, 3));

        storage = Reopen(storage);

        Assert.Equal(new HardState(3, 2), storage.LoadHardState());
        Assert.Equal(Entries(1, 5, 3), All(storage));
        storage.Dispose();
    }

    [Fact]
    public void Truncation_IsDurable()
    {
        var storage = Reopen();
        storage.Append(Entries(1, 5, 1));
        storage.TruncateFrom(3);
        storage.Append(new[] { Entry(3, 2) });

        storage = Reopen(storage);

        Assert.Equal(new long[] { 1, 1, 2 }, All(storage).Select(e => e.Term));
        storage.Dispose();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(13)]
    public void TornFinalRecord_IsDroppedOnOpen(int bytesCut)
    {
        var storage = Reopen();
        storage.Append(Entries(1, 3, 1));
        storage.Dispose();
        var log = Path.Combine(_directory, "log");
        using (var stream = new FileStream(log, FileMode.Open))
        {
            stream.SetLength(stream.Length - bytesCut);
        }

        storage = Reopen();

        Assert.Equal(2, storage.LastIndex);
        storage.Append(new[] { Entry(3, 1) });
        storage = Reopen(storage);
        Assert.Equal(Entries(1, 3, 1), All(storage));
        storage.Dispose();
    }

    [Fact]
    public void CorruptedRecordBeforeTheEnd_RefusesToOpen()
    {
        var storage = Reopen();
        storage.Append(Entries(1, 3, 1));
        storage.Dispose();
        var log = Path.Combine(_directory, "log");
        var bytes = File.ReadAllBytes(log);
        bytes[13 + 8 + 3] ^= 0xFF;
        File.WriteAllBytes(log, bytes);

        Assert.Throws<InvalidDataException>(() => FileRaftStorage.Open(_directory));
    }

    [Fact]
    public void CorruptedHardState_RefusesToOpen()
    {
        var storage = Reopen();
        storage.SaveHardState(new HardState(9, 1));
        storage.Dispose();
        var path = Path.Combine(_directory, "hardstate");
        var bytes = File.ReadAllBytes(path);
        bytes[6] ^= 0x01;
        File.WriteAllBytes(path, bytes);

        Assert.Throws<InvalidDataException>(() => FileRaftStorage.Open(_directory));
    }

    [Fact]
    public void Snapshot_CompactsTheLogAndKeepsTheMatchingSuffix()
    {
        var storage = Reopen();
        storage.Append(Entries(1, 10, 2));

        storage.InstallSnapshot(new Snapshot(new SnapshotMetadata(6, 2), new byte[] { 42 }));
        storage = Reopen(storage);

        Assert.Equal(6, storage.SnapshotIndex);
        Assert.Equal(new byte[] { 42 }, storage.LoadSnapshot()!.Data.ToArray());
        Assert.Equal(Entries(7, 10, 2), All(storage));
        storage.Append(new[] { Entry(11, 2) });
        storage = Reopen(storage);
        Assert.Equal(11, storage.LastIndex);
        storage.Dispose();
    }

    [Fact]
    public void Snapshot_FromLeaderWithConflictingTerm_DiscardsTheLog()
    {
        var storage = Reopen();
        storage.Append(Entries(1, 4, 1));

        storage.InstallSnapshot(new Snapshot(new SnapshotMetadata(8, 3), ReadOnlyMemory<byte>.Empty));
        storage = Reopen(storage);

        Assert.Equal(8, storage.LastIndex);
        Assert.Empty(All(storage));
        storage.Dispose();
    }

    [Fact]
    public void CrashAfterSnapshotCommit_IsRolledForwardOnOpen()
    {
        var storage = Reopen();
        storage.Append(Entries(1, 10, 2));
        storage.CrashPoint = point =>
        {
            if (point == "snapshot-committed")
            {
                throw new IOException("simulated crash");
            }
        };

        Assert.Throws<IOException>(() => storage.InstallSnapshot(new Snapshot(new SnapshotMetadata(6, 2), new byte[] { 1 })));
        Assert.True(File.Exists(Path.Combine(_directory, "log.tmp")));

        storage = Reopen();

        Assert.Equal(6, storage.SnapshotIndex);
        Assert.Equal(Entries(7, 10, 2), All(storage));
        Assert.False(File.Exists(Path.Combine(_directory, "log.tmp")));
        storage.Dispose();
    }

    [Fact]
    public void CrashBeforeSnapshotCommit_IsRolledBackOnOpen()
    {
        var storage = Reopen();
        storage.Append(Entries(1, 10, 2));
        storage.CrashPoint = point =>
        {
            if (point == "log-written")
            {
                throw new IOException("simulated crash");
            }
        };

        Assert.Throws<IOException>(() => storage.InstallSnapshot(new Snapshot(new SnapshotMetadata(6, 2), new byte[] { 1 })));

        storage = Reopen();

        Assert.Equal(0, storage.SnapshotIndex);
        Assert.Equal(Entries(1, 10, 2), All(storage));
        storage.Dispose();
    }
}
