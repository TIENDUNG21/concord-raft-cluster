using Concord.Core;

namespace Concord.Kv.Tests;

public sealed class KvStateMachineTests
{
    private static long _index;

    private static KvResult? Apply(KvStateMachine machine, KvCommand command)
    {
        KvResult? captured = null;
        machine.Applied = (_, _, result) => captured = result;
        machine.Apply(LogEntry.CreateCommand(Interlocked.Increment(ref _index), 1, KvCommandCodec.Encode(command)));
        return captured;
    }

    [Fact]
    public void PutGetDelete_BehaveLikeAMap()
    {
        var machine = new KvStateMachine();

        Assert.Equal(KvResult.Ok, Apply(machine, KvCommand.Put(0, 0, "a", "1")));
        Assert.Equal("1", machine.Get("a"));
        Assert.Equal(new KvResult(true, null), Apply(machine, KvCommand.Delete(0, 0, "a")));
        Assert.Equal(new KvResult(false, null), Apply(machine, KvCommand.Delete(0, 0, "a")));
        Assert.Null(machine.Get("a"));
    }

    [Fact]
    public void CompareAndSet_SwapsOnlyOnMatchAndReportsCurrentValue()
    {
        var machine = new KvStateMachine();

        Assert.Equal(new KvResult(true, "v1"), Apply(machine, KvCommand.CompareAndSet(0, 0, "k", null, "v1")));
        Assert.Equal(new KvResult(false, "v1"), Apply(machine, KvCommand.CompareAndSet(0, 0, "k", null, "v2")));
        Assert.Equal(new KvResult(false, "v1"), Apply(machine, KvCommand.CompareAndSet(0, 0, "k", "zzz", "v2")));
        Assert.Equal(new KvResult(true, "v2"), Apply(machine, KvCommand.CompareAndSet(0, 0, "k", "v1", "v2")));
        Assert.Equal("v2", machine.Get("k"));
    }

    [Fact]
    public void RetriedCommand_IsExecutedOnceAndAnsweredFromSession()
    {
        var machine = new KvStateMachine();
        var swap = KvCommand.CompareAndSet(5, 1, "k", null, "first");

        var first = Apply(machine, swap);
        Apply(machine, KvCommand.Put(6, 1, "k", "other"));
        var retry = Apply(machine, swap);

        Assert.Equal(new KvResult(true, "first"), first);
        Assert.Equal(first, retry);
        Assert.Equal("other", machine.Get("k"));
    }

    [Fact]
    public void OlderSequence_IsStaleAndHasNoEffect()
    {
        var machine = new KvStateMachine();
        Apply(machine, KvCommand.Put(5, 2, "k", "new"));

        var stale = Apply(machine, KvCommand.Put(5, 1, "k", "old"));

        Assert.True(stale!.Stale);
        Assert.Equal("new", machine.Get("k"));
    }

    [Fact]
    public void ClientZero_HasNoSessionAndNoDeduplication()
    {
        var machine = new KvStateMachine();
        Apply(machine, KvCommand.CompareAndSet(0, 0, "k", null, "a"));

        Assert.Equal(new KvResult(false, "a"), Apply(machine, KvCommand.CompareAndSet(0, 0, "k", null, "a")));
        Assert.Equal(0, machine.Sessions);
    }

    [Fact]
    public void NoOpAndUndecodableEntries_AreReportedWithoutChangingState()
    {
        var machine = new KvStateMachine();
        var seen = new List<(LogEntry Entry, KvResult? Result)>();
        machine.Applied = (entry, _, result) => seen.Add((entry, result));

        machine.Apply(LogEntry.CreateNoOp(1, 1));
        machine.Apply(LogEntry.CreateCommand(2, 1, new byte[] { 1, 2, 3 }));

        Assert.Equal(2, seen.Count);
        Assert.Null(seen[0].Result);
        Assert.False(seen[1].Result!.Succeeded);
        Assert.Equal(0, machine.Count);
    }

    [Fact]
    public void Snapshot_RoundTripsDataAndSessions()
    {
        var source = new KvStateMachine();
        Apply(source, KvCommand.Put(1, 1, "a", "1"));
        Apply(source, KvCommand.Put(2, 1, "b", "2"));
        Apply(source, KvCommand.CompareAndSet(1, 2, "a", "1", "3"));

        var restored = new KvStateMachine();
        restored.RestoreSnapshot(new Snapshot(new SnapshotMetadata(3, 1), source.TakeSnapshot()));

        Assert.Equal("3", restored.Get("a"));
        Assert.Equal("2", restored.Get("b"));
        Assert.Equal(new KvResult(true, "3"), Apply(restored, KvCommand.CompareAndSet(1, 2, "a", "1", "3")));
        Assert.Equal("3", restored.Get("a"));
    }

    [Fact]
    public void Snapshot_IsCanonicalRegardlessOfInsertionOrder()
    {
        var left = new KvStateMachine();
        var right = new KvStateMachine();
        Apply(left, KvCommand.Put(1, 1, "x", "1"));
        Apply(left, KvCommand.Put(2, 1, "y", "2"));
        Apply(right, KvCommand.Put(2, 1, "y", "2"));
        Apply(right, KvCommand.Put(1, 1, "x", "1"));

        Assert.Equal(left.TakeSnapshot().ToArray(), right.TakeSnapshot().ToArray());
    }

    [Fact]
    public void MalformedSnapshot_IsRejectedAndStateIsKept()
    {
        var machine = new KvStateMachine();
        Apply(machine, KvCommand.Put(1, 1, "keep", "me"));
        var good = machine.TakeSnapshot().ToArray();

        Assert.Throws<KvFormatException>(() => machine.RestoreSnapshot(new Snapshot(new SnapshotMetadata(1, 1), good.AsMemory(0, good.Length - 2))));
        Assert.Throws<KvFormatException>(() => machine.RestoreSnapshot(new Snapshot(new SnapshotMetadata(1, 1), new byte[] { 9 })));
        Assert.Equal("me", machine.Get("keep"));
    }
}
