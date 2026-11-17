using Concord.Core.Messages;
using Concord.Core.Storage;
using Concord.Core.Tests.Harness;

namespace Concord.Core.Tests;

public sealed class CommitTests
{
    private static readonly RaftOptions NoPreVote = new() { PreVote = false, CheckQuorum = false };

    private static byte[] Cmd(byte value) => new[] { value };

    [Fact]
    public void NewLeader_AppendsNoOpInItsTermAndCommitsIt()
    {
        var cluster = new TestCluster(3);
        var leader = cluster.ElectLeader();
        cluster.Advance(TimeSpan.FromMilliseconds(20));

        var last = cluster.Storages[leader.Id].GetEntries(leader.LastLogIndex, leader.LastLogIndex, 1).Single();
        Assert.Equal(EntryKind.NoOp, last.Kind);
        Assert.Equal(leader.CurrentTerm, last.Term);
        Assert.Equal(last.Index, leader.CommitIndex);
    }

    [Fact]
    public void Entries_AreAppliedInOrderOnEveryNode()
    {
        var cluster = new TestCluster(5);
        var leader = cluster.ElectLeader();

        for (byte i = 1; i <= 30; i++)
        {
            Assert.True(leader.Propose(Cmd(i)).Accepted);
            cluster.Advance(TimeSpan.FromMilliseconds(3));
        }

        cluster.Advance(TimeSpan.FromMilliseconds(200));

        var expected = Enumerable.Range(1, 30).Select(i => (byte)i).ToArray();
        foreach (var id in cluster.Ids)
        {
            Assert.Equal(expected, cluster.Machines[id].Commands.Select(c => c[0]).ToArray());
            Assert.Equal(leader.CommitIndex, cluster[id].CommitIndex);
            Assert.Equal(cluster[id].CommitIndex, cluster[id].LastApplied);
        }
    }

    [Fact]
    public void Commit_RequiresAMajority()
    {
        var cluster = new TestCluster(5);
        var leader = cluster.ElectLeader();
        cluster.Advance(TimeSpan.FromMilliseconds(20));
        var committed = leader.CommitIndex;
        var followers = cluster.Ids.Where(id => id != leader.Id).ToArray();
        cluster.Isolate(followers[0]);
        cluster.Isolate(followers[1]);
        cluster.Isolate(followers[2]);

        var proposal = leader.Propose(Cmd(42));
        cluster.Advance(TimeSpan.FromMilliseconds(100));
        Assert.Equal(committed, leader.CommitIndex);

        cluster.Heal();
        Assert.True(cluster.RunUntil(() => leader.CommitIndex >= proposal.Index, TimeSpan.FromMilliseconds(200)));
    }

    [Fact]
    public void Leader_NeverCommitsEntryFromEarlierTermByCountingReplicas()
    {
        // Figure 8 of the Raft paper: entry 2 (term 2) sits on a majority but must not be committed by
        // the term-4 leader until an entry of term 4 is replicated on a majority too.
        var storage = SingleNode.StorageWithTerms(1, 2);
        storage.SaveHardState(new HardState(3, null));
        var node = new SingleNode(1, storage: storage, options: NoPreVote);
        node.TimeOut();
        node.Node.Step(new RequestVoteResponse(2, 1, 4, true, false));
        Assert.Equal(RaftRole.Leader, node.Node.Role);
        Assert.Equal(3, node.Node.LastLogIndex);

        node.Node.Step(new AppendEntriesResponse(2, 1, 4, true, 2, 0, 0, 0));
        Assert.Equal(0, node.Node.CommitIndex);

        node.Node.Step(new AppendEntriesResponse(2, 1, 4, true, 3, 0, 0, 0));
        Assert.Equal(3, node.Node.CommitIndex);
        Assert.Equal(new long[] { 1, 2, 3 }, node.Machine.Applied.Select(e => e.Index));
    }

    [Fact]
    public void Follower_CommitsOnlyEntriesVerifiedByTheLeader()
    {
        // The follower holds a stale suffix (indexes 2..4 of term 1) the leader has not vouched for.
        var single = new SingleNode(1, storage: SingleNode.StorageWithTerms(1, 1, 1, 1));

        single.Node.Step(new AppendEntries(2, 1, 2, 1, 1, Array.Empty<LogEntry>(), LeaderCommit: 4, 0));

        Assert.Equal(1, single.Node.CommitIndex);
        Assert.Single(single.Machine.Applied);
    }

    [Fact]
    public void Follower_RefusesToOverwriteCommittedEntry()
    {
        var single = new SingleNode(1, storage: SingleNode.StorageWithTerms(1, 1));
        single.Node.Step(new AppendEntries(2, 1, 1, 2, 1, Array.Empty<LogEntry>(), LeaderCommit: 2, 0));
        Assert.Equal(2, single.Node.CommitIndex);

        var conflicting = new[] { LogEntry.CreateCommand(2, 2, Cmd(9)) };

        Assert.Throws<RaftSafetyException>(() => single.Node.Step(new AppendEntries(3, 1, 2, 1, 1, conflicting, 2, 0)));
    }

    [Fact]
    public void SingleNodeCluster_CommitsProposalsImmediately()
    {
        var single = new SingleNode(1, new[] { 1 });
        single.TimeOut();

        var result = single.Node.Propose(Cmd(5));

        Assert.True(result.Accepted);
        Assert.Equal(result.Index, single.Node.CommitIndex);
        Assert.Equal(new byte[] { 5 }, single.Machine.Commands.Single());
    }
}
