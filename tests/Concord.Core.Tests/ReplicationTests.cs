using Concord.Core.Messages;
using Concord.Core.Storage;
using Concord.Core.Tests.Harness;

namespace Concord.Core.Tests;

public sealed class ReplicationTests
{
    private static byte[] Cmd(byte value) => new[] { value };

    private static long[] Terms(InMemoryRaftStorage storage) =>
        storage.GetEntries(storage.SnapshotIndex + 1, storage.LastIndex, int.MaxValue).Select(e => e.Term).ToArray();

    private static void AssertSameLogs(TestCluster cluster)
    {
        var reference = cluster.Storages[cluster.Ids[0]];
        var expected = reference.GetEntries(1, reference.LastIndex, int.MaxValue);
        foreach (var id in cluster.Ids.Skip(1))
        {
            var storage = cluster.Storages[id];
            Assert.Equal(expected, storage.GetEntries(1, storage.LastIndex, int.MaxValue));
        }
    }

    [Fact]
    public void Leader_ReplicatesProposalsToEveryFollower()
    {
        var cluster = new TestCluster(3);
        var leader = cluster.ElectLeader();

        for (byte i = 1; i <= 5; i++)
        {
            Assert.True(leader.Propose(Cmd(i)).Accepted);
        }

        cluster.Advance(TimeSpan.FromMilliseconds(100));

        AssertSameLogs(cluster);
        Assert.Equal(leader.LastLogIndex, cluster.Storages[cluster.Ids[2]].LastIndex);
    }

    [Fact]
    public void Propose_OnFollower_ReturnsNotLeaderWithHint()
    {
        var cluster = new TestCluster(3);
        var leader = cluster.ElectLeader();
        var follower = cluster.LiveNodes.First(n => n.Id != leader.Id);

        var result = follower.Propose(Cmd(1));

        Assert.False(result.Accepted);
        Assert.Equal(leader.Id, result.LeaderHint);
    }

    [Fact]
    public void Follower_TruncatesDivergentSuffixAndAppendsLeaderEntries()
    {
        var single = new SingleNode(1, storage: SingleNode.StorageWithTerms(1, 1, 2, 2));
        var entries = new[] { LogEntry.CreateCommand(3, 3, Cmd(30)), LogEntry.CreateCommand(4, 3, Cmd(40)) };

        single.Node.Step(new AppendEntries(2, 1, 3, 2, 1, entries, 0, 0));

        Assert.Equal(new long[] { 1, 1, 3, 3 }, Terms(single.Storage));
        var response = single.Transport.Single<AppendEntriesResponse>();
        Assert.True(response.Success);
        Assert.Equal(4, response.MatchIndex);
    }

    [Fact]
    public void Follower_RejectsMismatchWithFirstIndexOfConflictingTerm()
    {
        var single = new SingleNode(1, storage: SingleNode.StorageWithTerms(1, 1, 2, 2, 2));

        single.Node.Step(new AppendEntries(2, 1, 3, 4, 3, Array.Empty<LogEntry>(), 0, 0));

        var response = single.Transport.Single<AppendEntriesResponse>();
        Assert.False(response.Success);
        Assert.Equal(2, response.ConflictTerm);
        Assert.Equal(3, response.ConflictIndex);
        Assert.Equal(5, single.Storage.LastIndex);
    }

    [Fact]
    public void Follower_WithShortLog_AsksForEntriesAfterItsEnd()
    {
        var single = new SingleNode(1, storage: SingleNode.StorageWithTerms(1));

        single.Node.Step(new AppendEntries(2, 1, 2, 5, 2, Array.Empty<LogEntry>(), 0, 0));

        var response = single.Transport.Single<AppendEntriesResponse>();
        Assert.False(response.Success);
        Assert.Equal(0, response.ConflictTerm);
        Assert.Equal(2, response.ConflictIndex);
    }

    [Fact]
    public void Follower_NeverTruncatesOnStaleOrDuplicateAppend()
    {
        var single = new SingleNode(1, storage: SingleNode.StorageWithTerms(1, 1, 1));
        var old = new[] { LogEntry.CreateCommand(1, 1, Cmd(1)) };

        single.Node.Step(new AppendEntries(2, 1, 1, 0, 0, old, 0, 0));

        Assert.Equal(3, single.Storage.LastIndex);
        Assert.Equal(1, single.Transport.Single<AppendEntriesResponse>().MatchIndex);
    }

    [Fact]
    public void Leader_RespectsMaxEntriesPerMessage()
    {
        var cluster = new TestCluster(3, new RaftOptions { MaxEntriesPerMessage = 4 });
        var leader = cluster.ElectLeader();
        var follower = cluster.Ids.First(id => id != leader.Id);
        cluster.Isolate(follower);
        for (byte i = 0; i < 20; i++)
        {
            leader.Propose(Cmd(i));
        }

        cluster.Advance(TimeSpan.FromMilliseconds(20));
        var largest = 0;
        cluster.DropFilter = m =>
        {
            if (m is AppendEntries append)
            {
                largest = Math.Max(largest, append.Entries.Count);
            }

            return false;
        };
        cluster.Heal();
        cluster.Advance(TimeSpan.FromMilliseconds(200));

        Assert.Equal(4, largest);
        AssertSameLogs(cluster);
    }

    [Fact]
    public void LaggingFollower_CatchesUpAfterHeal()
    {
        var cluster = new TestCluster(3);
        var leader = cluster.ElectLeader();
        var follower = cluster.Ids.First(id => id != leader.Id);
        cluster.Isolate(follower);

        for (byte i = 0; i < 50; i++)
        {
            leader.Propose(Cmd(i));
            cluster.Advance(TimeSpan.FromMilliseconds(2));
        }

        cluster.Heal();
        cluster.Advance(TimeSpan.FromMilliseconds(300));

        AssertSameLogs(cluster);
    }

    [Fact]
    public void DeposedLeader_LosesUncommittedEntriesToNewLeader()
    {
        var cluster = new TestCluster(5);
        var oldLeader = cluster.ElectLeader();
        cluster.Advance(TimeSpan.FromMilliseconds(50));

        cluster.Isolate(oldLeader.Id);
        oldLeader.Propose(Cmd(99));
        oldLeader.Propose(Cmd(98));

        Assert.True(cluster.RunUntil(() => cluster.LiveNodes.Any(n => n.Role == RaftRole.Leader && n.Id != oldLeader.Id)));
        var newLeader = cluster.LiveNodes.Single(n => n.Role == RaftRole.Leader && n.Id != oldLeader.Id);
        newLeader.Propose(Cmd(7));
        cluster.Advance(TimeSpan.FromMilliseconds(100));

        cluster.Heal();
        cluster.Advance(TimeSpan.FromMilliseconds(500));

        AssertSameLogs(cluster);
        var log = cluster.Storages[oldLeader.Id].GetEntries(1, cluster.Storages[oldLeader.Id].LastIndex, int.MaxValue);
        Assert.DoesNotContain(log, e => e.Kind == EntryKind.Command && e.Data.Span[0] == 99);
        Assert.Contains(log, e => e.Kind == EntryKind.Command && e.Data.Span[0] == 7);
    }
}
