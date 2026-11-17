using Concord.Core.Messages;
using Concord.Core.Tests.Harness;

namespace Concord.Core.Tests;

public sealed class PreVoteAndCheckQuorumTests
{
    [Fact]
    public void Timeout_StartsPreVoteWithoutIncrementingTerm()
    {
        var single = new SingleNode(1);

        single.TimeOut();

        Assert.Equal(RaftRole.PreCandidate, single.Node.Role);
        Assert.Equal(0, single.Node.CurrentTerm);
        Assert.Null(single.Node.VotedFor);
        var requests = single.Transport.Sent.OfType<RequestVote>().ToArray();
        Assert.Equal(new[] { 2, 3 }, requests.Select(r => r.To));
        Assert.All(requests, r => Assert.True(r.PreVote));
        Assert.All(requests, r => Assert.Equal(1, r.Term));
    }

    [Fact]
    public void PreCandidate_StartsRealElectionAfterMajorityOfPreVotes()
    {
        var single = new SingleNode(1);
        single.TimeOut();
        single.Transport.Clear();

        single.Node.Step(new RequestVoteResponse(2, 1, 1, Granted: true, PreVote: true));

        Assert.Equal(RaftRole.Candidate, single.Node.Role);
        Assert.Equal(1, single.Node.CurrentTerm);
        Assert.Equal(1, single.Node.VotedFor);
        Assert.All(single.Transport.Sent.OfType<RequestVote>(), r => Assert.False(r.PreVote));
    }

    [Fact]
    public void PreVote_IsGrantedWithoutChangingTermOrVote()
    {
        var single = new SingleNode(1);

        single.Node.Step(new RequestVote(2, 1, 5, 0, 0, PreVote: true));

        var response = single.Transport.Single<RequestVoteResponse>();
        Assert.True(response.Granted);
        Assert.True(response.PreVote);
        Assert.Equal(5, response.Term);
        Assert.Equal(0, single.Node.CurrentTerm);
        Assert.Null(single.Node.VotedFor);
    }

    [Fact]
    public void PreVote_IsRejectedForStaleLog()
    {
        var single = new SingleNode(1, storage: SingleNode.StorageWithTerms(1, 3));

        single.Node.Step(new RequestVote(2, 1, 4, 10, 2, PreVote: true));

        var response = single.Transport.Single<RequestVoteResponse>();
        Assert.False(response.Granted);
        Assert.Equal(3, response.Term);
    }

    [Fact]
    public void RejectedPreVoteWithHigherTerm_TurnsPreCandidateIntoFollower()
    {
        var single = new SingleNode(1);
        single.TimeOut();

        single.Node.Step(new RequestVoteResponse(2, 1, 6, Granted: false, PreVote: true));

        Assert.Equal(RaftRole.Follower, single.Node.Role);
        Assert.Equal(6, single.Node.CurrentTerm);
    }

    [Fact]
    public void FollowerInLease_IgnoresDisruptiveVoteRequest()
    {
        var single = new SingleNode(1);
        single.Node.Step(new AppendEntries(2, 1, 1, 0, 0, Array.Empty<LogEntry>(), 0, 0));
        single.Transport.Clear();

        single.Node.Step(new RequestVote(3, 1, 2, 0, 0, PreVote: false));
        single.Node.Step(new RequestVote(3, 1, 2, 0, 0, PreVote: true));

        Assert.Equal(1, single.Node.CurrentTerm);
        Assert.Equal(2, single.Node.LeaderId);
        Assert.DoesNotContain(single.Transport.Sent.OfType<RequestVoteResponse>(), r => r.Granted);
    }

    [Fact]
    public void FollowerAfterLease_GrantsVoteAgain()
    {
        var single = new SingleNode(1);
        single.Node.Step(new AppendEntries(2, 1, 1, 0, 0, Array.Empty<LogEntry>(), 0, 0));
        single.Clock.Now += TimeSpan.FromMilliseconds(151);

        single.Node.Step(new RequestVote(3, 1, 2, 0, 0, PreVote: false));

        Assert.True(single.Transport.Sent.OfType<RequestVoteResponse>().Last().Granted);
        Assert.Equal(2, single.Node.CurrentTerm);
    }

    [Fact]
    public void IsolatedFollower_DoesNotInflateTermAndRejoinsWithoutDisruption()
    {
        var cluster = new TestCluster(3);
        var leader = cluster.ElectLeader();
        var term = leader.CurrentTerm;
        var follower = cluster.Ids.First(id => id != leader.Id);

        cluster.Isolate(follower);
        cluster.Advance(TimeSpan.FromSeconds(5));
        Assert.Equal(term, cluster[follower].CurrentTerm);

        cluster.Heal();
        cluster.Advance(TimeSpan.FromSeconds(2));

        Assert.Equal(RaftRole.Leader, leader.Role);
        Assert.All(cluster.LiveNodes, n => Assert.Equal(term, n.CurrentTerm));
        Assert.Equal(leader.Id, cluster[follower].LeaderId);
    }

    [Fact]
    public void WithoutPreVote_IsolatedFollowerDisruptsLeaderOnRejoin()
    {
        var cluster = new TestCluster(3, new RaftOptions { PreVote = false, CheckQuorum = false });
        var leader = cluster.ElectLeader();
        var term = leader.CurrentTerm;
        var follower = cluster.Ids.First(id => id != leader.Id);

        cluster.Isolate(follower);
        cluster.Advance(TimeSpan.FromSeconds(5));
        Assert.True(cluster[follower].CurrentTerm > term + 5);

        cluster.Heal();
        cluster.Advance(TimeSpan.FromSeconds(2));

        // The returning node's inflated term forces the old leader to step down.
        Assert.True(leader.CurrentTerm > term + 5);
    }

    [Fact]
    public void LeaderWithoutQuorum_StepsDownAndMajorityElectsNewLeader()
    {
        var cluster = new TestCluster(5);
        var leader = cluster.ElectLeader();

        cluster.Isolate(leader.Id);
        Assert.True(cluster.RunUntil(() => leader.Role != RaftRole.Leader, TimeSpan.FromMilliseconds(700)));
        Assert.True(cluster.RunUntil(
            () => cluster.LiveNodes.Any(n => n.Role == RaftRole.Leader && n.Id != leader.Id),
            TimeSpan.FromSeconds(2)));

        Assert.Equal(RaftRole.Follower, leader.Role);
    }
}
