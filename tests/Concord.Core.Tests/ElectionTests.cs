using Concord.Core.Messages;
using Concord.Core.Storage;
using Concord.Core.Tests.Harness;

namespace Concord.Core.Tests;

public sealed class ElectionTests
{
    private static readonly RaftOptions NoPreVote = new() { PreVote = false, CheckQuorum = false };

    [Fact]
    public void SingleNodeCluster_ElectsItselfAfterTimeout()
    {
        var single = new SingleNode(1, new[] { 1 }, options: NoPreVote);
        Assert.Equal(RaftRole.Follower, single.Node.Role);

        single.TimeOut();

        Assert.Equal(RaftRole.Leader, single.Node.Role);
        Assert.Equal(1, single.Node.CurrentTerm);
        Assert.Equal(1, single.Node.LeaderId);
    }

    [Fact]
    public void ThreeNodes_ElectExactlyOneLeaderThatOthersFollow()
    {
        var cluster = new TestCluster(3, NoPreVote);

        var leader = cluster.ElectLeader();

        Assert.Single(cluster.LiveNodes, n => n.Role == RaftRole.Leader);
        Assert.All(cluster.LiveNodes, n => Assert.Equal(leader.CurrentTerm, n.CurrentTerm));
        Assert.All(cluster.LiveNodes, n => Assert.Equal(leader.Id, n.LeaderId));
    }

    [Fact]
    public void StableLeader_KeepsTermWhileHeartbeatsFlow()
    {
        var cluster = new TestCluster(5, NoPreVote);
        var leader = cluster.ElectLeader();
        var term = leader.CurrentTerm;

        cluster.Advance(TimeSpan.FromSeconds(5));

        Assert.Equal(RaftRole.Leader, leader.Role);
        Assert.All(cluster.LiveNodes, n => Assert.Equal(term, n.CurrentTerm));
    }

    [Fact]
    public void LeaderCrash_ElectsNewLeaderInHigherTerm()
    {
        var cluster = new TestCluster(3, NoPreVote);
        var first = cluster.ElectLeader();

        cluster.Crash(first.Id);
        var second = cluster.ElectLeader();

        Assert.NotEqual(first.Id, second.Id);
        Assert.True(second.CurrentTerm > first.CurrentTerm);
    }

    [Fact]
    public void Vote_DeniedToCandidateWithStaleLog()
    {
        var single = new SingleNode(1, storage: SingleNode.StorageWithTerms(1, 2));

        single.Node.Step(new RequestVote(2, 1, 3, LastLogIndex: 5, LastLogTerm: 1, PreVote: false));

        var response = single.Transport.Single<RequestVoteResponse>();
        Assert.False(response.Granted);
        Assert.Equal(3, response.Term);
        Assert.Null(single.Node.VotedFor);
    }

    [Fact]
    public void Vote_GrantedToCandidateWithLongerLogInSameLastTerm()
    {
        var single = new SingleNode(1, storage: SingleNode.StorageWithTerms(1, 2));

        single.Node.Step(new RequestVote(2, 1, 3, LastLogIndex: 3, LastLogTerm: 2, PreVote: false));

        Assert.True(single.Transport.Single<RequestVoteResponse>().Granted);
        Assert.Equal(new HardState(3, 2), single.Storage.LoadHardState());
    }

    [Fact]
    public void Vote_GrantedAtMostOncePerTerm()
    {
        var single = new SingleNode(1);

        single.Node.Step(new RequestVote(2, 1, 1, 0, 0, PreVote: false));
        single.Node.Step(new RequestVote(3, 1, 1, 0, 0, PreVote: false));
        single.Node.Step(new RequestVote(2, 1, 1, 0, 0, PreVote: false));

        var grants = single.Transport.Sent.OfType<RequestVoteResponse>().Select(r => (r.To, r.Granted)).ToArray();
        Assert.Equal(new[] { (2, true), (3, false), (2, true) }, grants);
    }

    [Fact]
    public void Vote_IsPersistedAcrossRestart()
    {
        var storage = new InMemoryRaftStorage();
        var before = new SingleNode(1, storage: storage);
        before.Node.Step(new RequestVote(2, 1, 4, 0, 0, PreVote: false));

        var after = new SingleNode(1, storage: storage);
        after.Node.Step(new RequestVote(3, 1, 4, 0, 0, PreVote: false));

        Assert.Equal(2, after.Node.VotedFor);
        Assert.False(after.Transport.Single<RequestVoteResponse>().Granted);
    }

    [Fact]
    public void Leader_StepsDownOnHigherTerm()
    {
        var single = new SingleNode(1, new[] { 1 }, options: NoPreVote);
        single.TimeOut();
        Assert.Equal(RaftRole.Leader, single.Node.Role);

        // A message from outside the membership is ignored; a member's higher term is not.
        single.Node.Step(new AppendEntriesResponse(9, 1, 10, false, 0, 0, 0, 0));
        Assert.Equal(RaftRole.Leader, single.Node.Role);

        var three = new SingleNode(1, options: NoPreVote);
        three.TimeOut();
        three.Node.Step(new RequestVoteResponse(2, 1, three.Node.CurrentTerm, true, PreVote: false));
        Assert.Equal(RaftRole.Leader, three.Node.Role);

        three.Node.Step(new AppendEntriesResponse(3, 1, 7, false, 0, 0, 0, 0));

        Assert.Equal(RaftRole.Follower, three.Node.Role);
        Assert.Equal(7, three.Node.CurrentTerm);
        Assert.Null(three.Node.LeaderId);
    }

    [Fact]
    public void StaleAppendEntries_IsRejectedWithCurrentTerm()
    {
        var single = new SingleNode(1, storage: SingleNode.StorageWithTerms(1, 5));

        single.Node.Step(new AppendEntries(2, 1, 3, 0, 0, Array.Empty<LogEntry>(), 0, 7));

        var response = single.Transport.Single<AppendEntriesResponse>();
        Assert.False(response.Success);
        Assert.Equal(5, response.Term);
        Assert.Equal(7, response.Round);
    }

    [Fact]
    public void Candidate_BecomesFollowerWhenLeaderOfSameTermAppears()
    {
        var single = new SingleNode(1, options: NoPreVote);
        single.TimeOut();
        Assert.Equal(RaftRole.Candidate, single.Node.Role);

        single.Node.Step(new AppendEntries(2, 1, single.Node.CurrentTerm, 0, 0, Array.Empty<LogEntry>(), 0, 0));

        Assert.Equal(RaftRole.Follower, single.Node.Role);
        Assert.Equal(2, single.Node.LeaderId);
    }
}
