using Concord.Core;
using Concord.Core.Messages;
using Concord.Core.Storage;

namespace Concord.Kv.Tests;

public sealed class KvServiceTests
{
    private sealed class Clock : IClock
    {
        public TimeSpan Now { get; set; }
    }

    private sealed class FixedRandom : IRandomSource
    {
        public int NextInt(int minInclusive, int maxExclusive) => minInclusive;
    }

    private sealed class Outbox : ITransport
    {
        public List<RaftMessage> Sent { get; } = new();

        public void Send(RaftMessage message) => Sent.Add(message);
    }

    private sealed class Fixture
    {
        public Fixture(int[] members, KvServiceOptions? options = null)
        {
            Node = new RaftNode(1, members, new RaftOptions { PreVote = false }, new InMemoryRaftStorage(), Machine, Outbox, Clock, new FixedRandom());
            Service = new KvService(Node, Machine, options);
        }

        public Clock Clock { get; } = new();

        public Outbox Outbox { get; } = new();

        public KvStateMachine Machine { get; } = new();

        public RaftNode Node { get; }

        public KvService Service { get; }

        public void ElectSelf()
        {
            Clock.Now = Node.NextDeadline;
            Node.Tick();
            if (Node.Role != RaftRole.Leader)
            {
                Node.Step(new RequestVoteResponse(2, 1, Node.CurrentTerm, true, false));
            }

            Assert.Equal(RaftRole.Leader, Node.Role);
        }
    }

    [Fact]
    public void Write_CompletesWhenApplied()
    {
        var f = new Fixture(new[] { 1 });
        f.ElectSelf();
        KvResponse? response = null;

        f.Service.Write(KvCommand.Put(1, 1, "k", "v"), r => response = r);

        Assert.Equal(KvStatus.Ok, response?.Status);
        Assert.Equal(KvResult.Ok, response?.Result);
        Assert.Equal("v", f.Machine.Get("k"));
    }

    [Fact]
    public void Read_UsesReadIndexAndReturnsCommittedValue()
    {
        var f = new Fixture(new[] { 1 });
        f.ElectSelf();
        f.Service.Write(KvCommand.Put(1, 1, "k", "v"), _ => { });
        KvResponse? found = null;
        KvResponse? missing = null;

        f.Service.Read("k", r => found = r);
        f.Service.Read("nope", r => missing = r);

        Assert.Equal(new KvResult(true, "v"), found?.Result);
        Assert.Equal(new KvResult(false, null), missing?.Result);
    }

    [Fact]
    public void Follower_AnswersNotLeaderWithHint()
    {
        var f = new Fixture(new[] { 1, 2, 3 });
        f.Node.Step(new AppendEntries(3, 1, 1, 0, 0, Array.Empty<LogEntry>(), 0, 0));
        KvResponse? write = null;
        KvResponse? read = null;

        f.Service.Write(KvCommand.Put(1, 1, "k", "v"), r => write = r);
        f.Service.Read("k", r => read = r);

        Assert.Equal(KvResponse.NotLeader(3), write);
        Assert.Equal(KvResponse.NotLeader(3), read);
    }

    [Fact]
    public void LeaderStepsDown_PendingWriteBecomesUnknown()
    {
        var f = new Fixture(new[] { 1, 2, 3 });
        f.ElectSelf();
        KvResponse? response = null;
        f.Service.Write(KvCommand.Put(1, 1, "k", "v"), r => response = r);
        Assert.Null(response);
        Assert.Equal(1, f.Service.PendingWrites);

        f.Node.Step(new AppendEntries(2, 1, f.Node.CurrentTerm + 1, 0, 0, Array.Empty<LogEntry>(), 0, 0));

        Assert.Equal(KvStatus.Unknown, response?.Status);
        Assert.Equal(2, response?.LeaderHint);
        Assert.Equal(0, f.Service.PendingWrites);
    }

    [Fact]
    public void InvalidRequests_AreRejectedBeforeReplication()
    {
        var f = new Fixture(new[] { 1 });
        f.ElectSelf();
        var before = f.Node.LastLogIndex;
        KvResponse? write = null;
        KvResponse? read = null;

        f.Service.Write(KvCommand.Put(1, 1, string.Empty, "v"), r => write = r);
        f.Service.Read(new string('k', 1000), r => read = r);

        Assert.Equal(KvStatus.Rejected, write?.Status);
        Assert.Equal(KvStatus.Rejected, read?.Status);
        Assert.Equal(before, f.Node.LastLogIndex);
    }

    [Fact]
    public void UnsafeLocalReads_SkipTheQuorumRound()
    {
        var f = new Fixture(new[] { 1, 2, 3 }, new KvServiceOptions { UnsafeLocalReads = true });
        f.ElectSelf();
        f.Outbox.Sent.Clear();
        KvResponse? read = null;

        f.Service.Read("k", r => read = r);

        Assert.Equal(KvStatus.Ok, read?.Status);
        Assert.Empty(f.Outbox.Sent);
    }
}
