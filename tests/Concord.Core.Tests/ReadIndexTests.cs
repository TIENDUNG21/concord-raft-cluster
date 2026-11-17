using Concord.Core.Messages;
using Concord.Core.Tests.Harness;

namespace Concord.Core.Tests;

public sealed class ReadIndexTests
{
    private static readonly RaftOptions NoPreVote = new() { PreVote = false, CheckQuorum = false };

    /// <summary>Node 1 of {1,2,3} as a fresh term-1 leader whose no-op (index 1) is not yet committed.</summary>
    private static SingleNode NewLeader()
    {
        var single = new SingleNode(1, options: NoPreVote);
        single.TimeOut();
        single.Node.Step(new RequestVoteResponse(2, 1, 1, true, false));
        Assert.Equal(RaftRole.Leader, single.Node.Role);
        single.Transport.Clear();
        return single;
    }

    [Fact]
    public void SingleNodeLeader_ServesReadImmediately()
    {
        var single = new SingleNode(1, new[] { 1 });
        single.TimeOut();
        ReadResult? result = null;

        single.Node.RequestRead(r => result = r);

        Assert.True(result?.Success);
        Assert.Equal(single.Node.CommitIndex, result?.Index);
    }

    [Fact]
    public void Follower_RejectsReadWithLeaderHint()
    {
        var single = new SingleNode(1);
        single.Node.Step(new AppendEntries(3, 1, 1, 0, 0, Array.Empty<LogEntry>(), 0, 0));
        ReadResult? result = null;

        single.Node.RequestRead(r => result = r);

        Assert.False(result?.Success);
        Assert.Equal(3, result?.LeaderHint);
    }

    [Fact]
    public void Leader_WaitsForCurrentTermCommitAndQuorumAck()
    {
        var single = NewLeader();
        ReadResult? result = null;

        single.Node.RequestRead(r => result = r);
        var round = single.Transport.Sent.OfType<AppendEntries>().Select(a => a.Round).Max();
        Assert.Null(result);
        Assert.Equal(2, single.Transport.Sent.OfType<AppendEntries>().Count());

        // An ack from an older round proves nothing about leadership at read time.
        single.Node.Step(new AppendEntriesResponse(2, 1, 1, true, 1, 0, 0, round - 1));
        Assert.Equal(1, single.Node.CommitIndex);
        Assert.Null(result);

        single.Node.Step(new AppendEntriesResponse(3, 1, 1, true, 1, 0, 0, round));
        Assert.True(result?.Success);
        Assert.Equal(1, result?.Index);
    }

    [Fact]
    public void Read_IsNotServedBeforeTheNoOpCommits()
    {
        var single = NewLeader();
        ReadResult? result = null;
        single.Node.RequestRead(r => result = r);
        var round = single.Transport.Sent.OfType<AppendEntries>().Select(a => a.Round).Max();

        // Quorum acknowledged the round, but the follower has not stored the no-op yet.
        single.Node.Step(new AppendEntriesResponse(2, 1, 1, false, 0, 1, 0, round));
        Assert.Null(result);
        Assert.Equal(1, single.Node.PendingReads);

        single.Node.Step(new AppendEntriesResponse(2, 1, 1, true, 1, 0, 0, round));
        Assert.True(result?.Success);
    }

    [Fact]
    public void StepDown_FailsPendingReads()
    {
        var single = NewLeader();
        ReadResult? result = null;
        single.Node.RequestRead(r => result = r);

        single.Node.Step(new AppendEntries(3, 1, 2, 0, 0, Array.Empty<LogEntry>(), 0, 0));

        Assert.False(result?.Success);
        Assert.Equal(3, result?.LeaderHint);
        Assert.Equal(0, single.Node.PendingReads);
    }

    [Fact]
    public void IsolatedLeader_CannotServeReads()
    {
        var cluster = new TestCluster(3);
        var leader = cluster.ElectLeader();
        cluster.Advance(TimeSpan.FromMilliseconds(50));
        cluster.Isolate(leader.Id);
        ReadResult? result = null;

        leader.RequestRead(r => result = r);
        cluster.Advance(TimeSpan.FromMilliseconds(100));
        Assert.Null(result);

        // Check-quorum eventually deposes it and the read fails instead of returning stale data.
        cluster.Advance(TimeSpan.FromMilliseconds(600));
        Assert.False(result?.Success);
    }

    [Fact]
    public void Read_ReflectsEveryWriteCommittedBeforeIt()
    {
        var cluster = new TestCluster(3);
        var leader = cluster.ElectLeader();
        var proposal = leader.Propose(new byte[] { 42 });
        Assert.True(cluster.RunUntil(() => leader.CommitIndex >= proposal.Index));
        ReadResult? result = null;

        leader.RequestRead(r => result = r);
        cluster.Advance(TimeSpan.FromMilliseconds(20));

        Assert.True(result?.Success);
        Assert.True(result?.Index >= proposal.Index);
        Assert.Contains(cluster.Machines[leader.Id].Commands, c => c[0] == 42);
    }
}
