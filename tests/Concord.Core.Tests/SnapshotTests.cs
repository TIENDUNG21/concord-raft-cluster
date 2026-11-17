using Concord.Core.Messages;
using Concord.Core.Storage;
using Concord.Core.Tests.Harness;

namespace Concord.Core.Tests;

public sealed class SnapshotTests
{
    private static readonly RaftOptions SnapshotEvery10 = new() { SnapshotThreshold = 10 };

    private static byte[] Cmd(int value) => new[] { (byte)value };

    private static void ProposeMany(TestCluster cluster, RaftNode leader, int count)
    {
        for (var i = 1; i <= count; i++)
        {
            Assert.True(leader.Propose(Cmd(i)).Accepted);
            cluster.Advance(TimeSpan.FromMilliseconds(2));
        }

        cluster.Advance(TimeSpan.FromMilliseconds(100));
    }

    private static byte[] Commands(RecordingStateMachine machine) => machine.Commands.Select(c => c[0]).ToArray();

    [Fact]
    public void Leader_CompactsLogAfterThreshold()
    {
        var cluster = new TestCluster(3, SnapshotEvery10);
        var leader = cluster.ElectLeader();

        ProposeMany(cluster, leader, 25);

        var storage = cluster.Storages[leader.Id];
        Assert.True(storage.SnapshotIndex >= 20);
        Assert.True(storage.LastIndex - storage.SnapshotIndex < 10);
        Assert.NotNull(storage.LoadSnapshot());
    }

    [Fact]
    public void LaggingFollower_IsBroughtUpToDateWithInstallSnapshot()
    {
        var cluster = new TestCluster(3, SnapshotEvery10);
        var leader = cluster.ElectLeader();
        var follower = cluster.Ids.First(id => id != leader.Id);
        cluster.Isolate(follower);
        ProposeMany(cluster, leader, 40);
        var installs = 0;
        cluster.DropFilter = m =>
        {
            installs += m is InstallSnapshot ? 1 : 0;
            return false;
        };

        cluster.Heal();
        cluster.Advance(TimeSpan.FromMilliseconds(300));

        Assert.True(installs >= 1);
        Assert.Equal(1, cluster.Machines[follower].Restores);
        Assert.Equal(Commands(cluster.Machines[leader.Id]), Commands(cluster.Machines[follower]));
        Assert.Equal(leader.CommitIndex, cluster[follower].LastApplied);
    }

    [Fact]
    public void LostInstallSnapshot_IsRetried()
    {
        var cluster = new TestCluster(3, SnapshotEvery10);
        var leader = cluster.ElectLeader();
        var follower = cluster.Ids.First(id => id != leader.Id);
        cluster.Isolate(follower);
        ProposeMany(cluster, leader, 30);
        var sent = 0;
        cluster.DropFilter = m => m is InstallSnapshot && ++sent <= 2;

        cluster.Heal();
        cluster.Advance(TimeSpan.FromMilliseconds(800));

        Assert.True(sent >= 3, $"expected a third attempt after two losses, saw {sent}");
        Assert.Equal(leader.CommitIndex, cluster[follower].LastApplied);
    }

    [Fact]
    public void RestartedNode_RestoresSnapshotThenReplaysCommittedLog()
    {
        var cluster = new TestCluster(3, SnapshotEvery10);
        var leader = cluster.ElectLeader();
        var follower = cluster.Ids.First(id => id != leader.Id);
        ProposeMany(cluster, leader, 25);
        var before = Commands(cluster.Machines[follower]);

        cluster.Crash(follower);
        var restarted = cluster.Restart(follower);

        Assert.Equal(1, cluster.Machines[follower].Restores);
        Assert.Equal(cluster.Storages[follower].SnapshotIndex, restarted.LastApplied);
        cluster.Advance(TimeSpan.FromMilliseconds(200));
        Assert.Equal(before, Commands(cluster.Machines[follower]));
    }

    [Fact]
    public void InstallSnapshot_ReplacesStateAndDiscardsConflictingLog()
    {
        var single = new SingleNode(1, storage: SingleNode.StorageWithTerms(1, 1, 1));
        var snapshot = new Snapshot(new SnapshotMetadata(5, 2), new RecordingStateMachine().TakeSnapshot());

        single.Node.Step(new InstallSnapshot(2, 1, 2, snapshot, 4));

        Assert.Equal(5, single.Node.CommitIndex);
        Assert.Equal(5, single.Node.LastApplied);
        Assert.Equal(5, single.Storage.LastIndex);
        Assert.Equal(1, single.Machine.Restores);
        var response = single.Transport.Single<InstallSnapshotResponse>();
        Assert.Equal(5, response.MatchIndex);
        Assert.Equal(4, response.Round);
    }

    [Fact]
    public void InstallSnapshot_OlderThanCommitIndex_IsAcknowledgedButIgnored()
    {
        var single = new SingleNode(1, storage: SingleNode.StorageWithTerms(1, 1, 1, 1));
        single.Node.Step(new AppendEntries(2, 1, 1, 4, 1, Array.Empty<LogEntry>(), 4, 0));
        var snapshot = new Snapshot(new SnapshotMetadata(2, 1), ReadOnlyMemory<byte>.Empty);

        single.Node.Step(new InstallSnapshot(2, 1, 1, snapshot, 0));

        Assert.Equal(0, single.Machine.Restores);
        Assert.Equal(4, single.Storage.LastIndex);
        Assert.Equal(2, single.Transport.Sent.OfType<InstallSnapshotResponse>().Single().MatchIndex);
    }

    [Fact]
    public void AppendEntries_OverlappingSnapshot_AppendsOnlyTheNewSuffix()
    {
        var storage = new InMemoryRaftStorage();
        storage.InstallSnapshot(new Snapshot(new SnapshotMetadata(5, 2), new RecordingStateMachine().TakeSnapshot()));
        storage.SaveHardState(new HardState(2, null));
        var single = new SingleNode(1, storage: storage);
        var entries = Enumerable.Range(4, 4).Select(i => LogEntry.CreateCommand(i, 2, Cmd(i))).ToArray();

        single.Node.Step(new AppendEntries(2, 1, 2, 3, 2, entries, 7, 0));

        Assert.Equal(7, storage.LastIndex);
        Assert.Equal(7, single.Transport.Single<AppendEntriesResponse>().MatchIndex);
        Assert.Equal(new byte[] { 6, 7 }, Commands(single.Machine));
    }

    [Fact]
    public void AppendEntries_EntirelyInsideSnapshot_ReportsSnapshotAsMatched()
    {
        var storage = new InMemoryRaftStorage();
        storage.InstallSnapshot(new Snapshot(new SnapshotMetadata(5, 2), ReadOnlyMemory<byte>.Empty));
        var single = new SingleNode(1, storage: storage);

        single.Node.Step(new AppendEntries(2, 1, 2, 1, 1, new[] { LogEntry.CreateCommand(2, 1, Cmd(2)) }, 2, 0));

        var response = single.Transport.Single<AppendEntriesResponse>();
        Assert.True(response.Success);
        Assert.Equal(5, response.MatchIndex);
    }
}
