namespace Concord.Simulation.Tests;

public sealed class NetworkTests
{
    private sealed class Fixture
    {
        public Fixture(NetworkOptions? options = null)
        {
            Trace = new SimTrace(Sim);
            Network = new SimNetwork(Sim, new DeterministicRandom(11), options, Trace);
            foreach (var id in new[] { 1, 2, 3 })
            {
                var node = id;
                Network.RegisterNode(node, (from, payload) => Received.Add((from, node, payload, Sim.Now)));
            }

            Network.RegisterClient(100, (from, payload) => Received.Add((from, 100, payload, Sim.Now)));
        }

        public Simulator Sim { get; } = new();

        public SimTrace Trace { get; }

        public SimNetwork Network { get; }

        public List<(int From, int To, object Payload, TimeSpan At)> Received { get; } = new();

        public void Drain() => Sim.RunUntil(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void Messages_ArriveWithinLatencyBounds()
    {
        var f = new Fixture(new NetworkOptions { SlowRate = 0 });
        for (var i = 0; i < 200; i++)
        {
            f.Network.Send(1, 2, i);
        }

        f.Drain();

        Assert.Equal(200, f.Received.Count);
        Assert.All(f.Received, r => Assert.InRange(r.At.TotalMilliseconds, 1, 8));
        Assert.Equal(200, f.Trace.MessagesDelivered);
    }

    [Fact]
    public void JitterAndSlowMessages_ReorderDelivery()
    {
        var f = new Fixture(new NetworkOptions { SlowRate = 0.2 });
        for (var i = 0; i < 100; i++)
        {
            f.Network.Send(1, 2, i);
        }

        f.Drain();

        var order = f.Received.Select(r => (int)r.Payload).ToArray();
        Assert.Equal(Enumerable.Range(0, 100), order.Order());
        Assert.NotEqual(Enumerable.Range(0, 100), order);
    }

    [Fact]
    public void Partition_DropsCrossGroupMessagesOnly()
    {
        var f = new Fixture();
        f.Network.Partition(new IReadOnlyCollection<int>[] { new[] { 1 }, new[] { 2, 3 } });

        f.Network.Send(1, 2, "cut");
        f.Network.Send(2, 1, "cut");
        f.Network.Send(2, 3, "inside");
        f.Network.Send(100, 1, "client");
        f.Drain();

        Assert.Equal(new object[] { "inside", "client" }.Order(), f.Received.Select(r => r.Payload).Order());
        Assert.Equal(2, f.Trace.MessagesDropped);
    }

    [Fact]
    public void MessageInFlight_IsDroppedWhenLinkFailsBeforeArrival()
    {
        var f = new Fixture(new NetworkOptions { SlowRate = 0, MinLatency = TimeSpan.FromMilliseconds(5), MaxLatency = TimeSpan.FromMilliseconds(5) });
        f.Network.Send(1, 2, "late");
        f.Sim.RunUntil(TimeSpan.FromMilliseconds(2));

        f.Network.Isolate(1);
        f.Drain();

        Assert.Empty(f.Received);
    }

    [Fact]
    public void Heal_RestoresAllLinks()
    {
        var f = new Fixture();
        f.Network.Isolate(3);
        f.Network.Block(1, 2);
        Assert.False(f.Network.IsHealed);

        f.Network.Heal();
        f.Network.Send(1, 2, "a");
        f.Network.Send(3, 1, "b");
        f.Drain();

        Assert.True(f.Network.IsHealed);
        Assert.Equal(2, f.Received.Count);
    }

    [Fact]
    public void AsymmetricBlock_CutsOneDirection()
    {
        var f = new Fixture();
        f.Network.Block(1, 2);

        f.Network.Send(1, 2, "x");
        f.Network.Send(2, 1, "y");
        f.Drain();

        Assert.Equal("y", Assert.Single(f.Received).Payload);
    }

    [Fact]
    public void DropAndDuplicateRates_AreApplied()
    {
        var f = new Fixture(new NetworkOptions { DropRate = 0.3, DuplicateRate = 0.3 });
        for (var i = 0; i < 1000; i++)
        {
            f.Network.Send(1, 2, i);
        }

        f.Drain();

        var distinct = f.Received.Select(r => (int)r.Payload).Distinct().Count();
        Assert.InRange(distinct, 620, 780);
        Assert.True(f.Received.Count > distinct);
        Assert.Equal(f.Received.Count - distinct, f.Network.Duplicated);
    }
}
