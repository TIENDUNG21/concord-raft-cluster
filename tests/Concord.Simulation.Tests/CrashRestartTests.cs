using Concord.Core;

namespace Concord.Simulation.Tests;

public sealed class CrashRestartTests
{
    private sealed class Harness
    {
        public Harness(int nodes = 3, ulong seed = 1)
        {
            var random = new DeterministicRandom(seed);
            Trace = new SimTrace(Sim);
            Network = new SimNetwork(Sim, random.Fork(), new NetworkOptions(), Trace);
            Cluster = new SimCluster(Sim, Network, Trace, nodes, new RaftOptions { SnapshotThreshold = 20 }, random.Fork(), id => Apps[id] = new ChecksumApplication());
            Monitor = new InvariantMonitor(id => Cluster[id].Storage, () => Sim.Now);
            Cluster.AddObserver(Monitor);
            Cluster.OnRestart(Monitor.NodeRestarted);
            Cluster.Start();
        }

        public Simulator Sim { get; } = new();

        public SimTrace Trace { get; }

        public SimNetwork Network { get; }

        public SimCluster Cluster { get; }

        public InvariantMonitor Monitor { get; }

        public Dictionary<int, ChecksumApplication> Apps { get; } = new();

        public void Run(int milliseconds) => Sim.RunUntil(Sim.Now + TimeSpan.FromMilliseconds(milliseconds));

        public RaftNode Leader()
        {
            Run(1000);
            return Cluster.Leader() ?? throw new InvalidOperationException("no leader");
        }

        public void Propose(int count)
        {
            for (var i = 0; i < count; i++)
            {
                if (Cluster.Leader() is { } leader)
                {
                    Cluster.WithNode(leader.Id, n => n.Propose(BitConverter.GetBytes(i)));
                }

                Run(5);
            }

            Run(300);
        }
    }

    [Fact]
    public void RestartedFollower_RecoversStateFromStorageAndCatchesUp()
    {
        var h = new Harness();
        var leader = h.Leader();
        var follower = h.Cluster.Ids.First(id => id != leader.Id);
        h.Propose(30);
        var termBefore = h.Cluster[follower].Raft!.CurrentTerm;

        h.Cluster.Crash(follower);
        h.Propose(30);
        Assert.False(h.Cluster[follower].IsUp);
        h.Cluster.Restart(follower);
        h.Run(500);

        var node = h.Cluster[follower].Raft!;
        Assert.True(node.CurrentTerm >= termBefore);
        Assert.Equal(h.Apps[leader.Id].Checksum, h.Apps[follower].Checksum);
        Assert.Equal(60, h.Apps[follower].Count);
        Assert.Null(h.Monitor.Violation);
    }

    [Fact]
    public void CrashedLeader_IsReplacedAndRejoinsAsFollower()
    {
        var h = new Harness(5);
        var first = h.Leader();
        h.Propose(10);

        h.Cluster.Crash(first.Id);
        h.Run(1000);
        var second = h.Cluster.Leader();
        Assert.NotNull(second);
        Assert.NotEqual(first.Id, second.Id);

        h.Cluster.Restart(first.Id);
        h.Propose(10);

        Assert.Equal(RaftRole.Follower, h.Cluster[first.Id].Raft!.Role);
        Assert.Equal(second.Id, h.Cluster[first.Id].Raft!.LeaderId);
        Assert.Equal(20, h.Apps[first.Id].Count);
        Assert.Null(h.Monitor.Violation);
        Assert.Contains(h.Trace.Events, e => e.Kind == TraceKind.Fault && e.Text == "crash (was leader)");
    }

    [Fact]
    public void Crash_BumpsIncarnationSoOldTimersAndRepliesAreIgnored()
    {
        var h = new Harness();
        h.Leader();
        var node = h.Cluster[1];
        var incarnation = node.Incarnation;

        h.Cluster.Crash(1);
        h.Run(2000);

        Assert.Equal(incarnation + 1, node.Incarnation);
        Assert.Null(node.Raft);
        Assert.Null(h.Monitor.Violation);
    }

    [Fact]
    public void CrashFaults_AreInjectedAndEveryNodeIsBackUpAfterTheWindow()
    {
        var result = SimulationRunner.Run(new SimulationConfig { Seed = 21, Faults = FaultOptions.None with { Crashes = true } });

        Assert.True(result.Passed, SimulationRunner.FormatReport(result));
        Assert.Contains(result.Trace.Events, e => e.Kind == TraceKind.Fault && e.Text.StartsWith("crash", StringComparison.Ordinal));
        Assert.Contains(result.Trace.Events, e => e.Kind == TraceKind.Fault && e.Text.StartsWith("restart", StringComparison.Ordinal));
    }
}
