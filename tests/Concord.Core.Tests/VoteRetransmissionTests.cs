using Concord.Core.Messages;
using Concord.Core.Tests.Harness;

namespace Concord.Core.Tests;

public sealed class VoteRetransmissionTests
{
    [Fact]
    public void Candidate_ResendsVoteRequestsOnlyToSilentPeers()
    {
        var single = new SingleNode(1, new[] { 1, 2, 3, 4, 5 }, options: new RaftOptions { PreVote = false });
        single.TimeOut();
        single.Node.Step(new RequestVoteResponse(2, 1, 1, false, false));
        single.Node.Step(new RequestVoteResponse(3, 1, 1, true, false));
        single.Transport.Clear();

        single.Clock.Now += TimeSpan.FromMilliseconds(50);
        Assert.True(single.Node.NextDeadline <= single.Clock.Now);
        single.Node.Tick();

        var resent = single.Transport.Sent.OfType<RequestVote>().Select(r => r.To).ToArray();
        Assert.Equal(new[] { 4, 5 }, resent);
        Assert.Equal(RaftRole.Candidate, single.Node.Role);
        Assert.Equal(1, single.Node.CurrentTerm);
    }

    [Fact]
    public void LostVoteRequests_DoNotCostAWholeElectionTimeout()
    {
        var cluster = new TestCluster(3, new RaftOptions { PreVote = false });
        var sent = 0;
        cluster.DropFilter = m => m is RequestVote && ++sent <= 2;

        var leader = cluster.ElectLeader();

        // Both first requests were lost, yet the first candidate still wins term 1 via retransmission.
        Assert.Equal(1, leader.CurrentTerm);
        Assert.True(sent > 2);
    }
}
