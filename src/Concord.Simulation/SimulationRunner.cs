using System.Text;
using Concord.Core;

namespace Concord.Simulation;

/// <summary>Everything that defines a run. Together with the seed it fully determines the outcome.</summary>
public sealed record SimulationConfig
{
    public ulong Seed { get; init; } = 1;

    public int Nodes { get; init; } = 5;

    /// <summary>Total virtual time.</summary>
    public TimeSpan Duration { get; init; } = TimeSpan.FromSeconds(12);

    /// <summary>Faults are injected during [0, FaultWindow); then the network heals and the cluster must recover.</summary>
    public TimeSpan FaultWindow { get; init; } = TimeSpan.FromSeconds(8);

    public RaftOptions Raft { get; init; } = new() { SnapshotThreshold = 200 };

    public NetworkOptions Network { get; init; } = new();

    public FaultOptions Faults { get; init; } = new();

    /// <summary>How often the Raft-level workload proposes a command at the leader.</summary>
    public TimeSpan ProposalInterval { get; init; } = TimeSpan.FromMilliseconds(15);
}

public enum FailureKind
{
    Invariant,
    Liveness,
    Exception,
}

public sealed record SimulationStats(
    long Events,
    long MessagesDelivered,
    long MessagesDropped,
    int Elections,
    long MaxTerm,
    long CommittedEntries,
    int Faults,
    TimeSpan VirtualTime);

public sealed record SimulationResult(ulong Seed, FailureKind? Failure, string? Message, SimulationStats Stats, SimTrace Trace)
{
    public bool Passed => Failure is null;
}

/// <summary>Runs one seeded Raft-level simulation: random faults, a steady proposal stream, invariants, recovery.</summary>
public static class SimulationRunner
{
    public static SimulationResult Run(SimulationConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        var root = new DeterministicRandom(config.Seed);
        var networkRandom = root.Fork();
        var faultRandom = root.Fork();
        var nodeRandom = root.Fork();
        var workloadRandom = root.Fork();

        var sim = new Simulator();
        var trace = new SimTrace(sim);
        var network = new SimNetwork(sim, networkRandom, config.Network, trace);
        var cluster = new SimCluster(sim, network, trace, config.Nodes, config.Raft, nodeRandom, _ => new ChecksumApplication());
        var monitor = new InvariantMonitor(id => cluster[id].Storage, () => sim.Now);
        cluster.AddObserver(monitor);
        cluster.OnRestart(monitor.NodeRestarted);
        cluster.Start();

        var faults = new FaultInjector(sim, cluster, network, trace, faultRandom, config.Faults);
        faults.Schedule(config.FaultWindow);
        ScheduleProposals(sim, cluster, workloadRandom, config);
        var probe = new LivenessProbe(sim, cluster, trace, config.FaultWindow);

        FailureKind? failure = null;
        string? message = null;
        try
        {
            sim.RunUntil(config.Duration, () => monitor.Violation is not null);
        }
        catch (RaftSafetyException ex)
        {
            failure = FailureKind.Invariant;
            message = ex.Message;
        }
#pragma warning disable CA1031 // Any exception from a node is a finding of the run, reported with its seed.
        catch (Exception ex)
#pragma warning restore CA1031
        {
            failure = FailureKind.Exception;
            message = $"{ex.GetType().Name}: {ex.Message}";
        }

        if (failure is null && monitor.Violation is not null)
        {
            failure = FailureKind.Invariant;
            message = monitor.Violation;
        }

        if (failure is null && probe.Check(cluster) is { } livenessProblem)
        {
            failure = FailureKind.Liveness;
            message = livenessProblem;
        }

        if (message is not null)
        {
            trace.Add(TraceKind.Violation, null, message);
        }

        var stats = new SimulationStats(
            sim.Steps,
            trace.MessagesDelivered,
            trace.MessagesDropped,
            monitor.Elections,
            monitor.MaxTerm,
            monitor.MaxApplied,
            faults.Injected,
            sim.Now);
        return new SimulationResult(config.Seed, failure, message, stats, trace);
    }

    /// <summary>A compact, human-readable failure report: seed, verdict and the events that matter.</summary>
    public static string FormatReport(SimulationResult result, int maxEvents = 60)
    {
        ArgumentNullException.ThrowIfNull(result);
        var builder = new StringBuilder();
        builder.Append("seed ").Append(result.Seed).Append(": ")
            .Append(result.Passed ? "passed" : $"FAILED ({result.Failure}) {result.Message}").Append('\n');
        builder.Append("-- faults, elections and verdict --\n");
        builder.Append(result.Trace.Format(
            e => e.Kind is TraceKind.Fault or TraceKind.Violation || (e.Kind == TraceKind.Role && e.Text.StartsWith("Leader", StringComparison.Ordinal)),
            maxEvents));
        builder.Append("-- last messages --\n");
        foreach (var record in result.Trace.RecentMessageRecords().TakeLast(20))
        {
            builder.Append(record).Append('\n');
        }

        return builder.ToString();
    }

    private static void ScheduleProposals(Simulator sim, SimCluster cluster, DeterministicRandom random, SimulationConfig config)
    {
        var stopAt = config.Duration - TimeSpan.FromSeconds(1);
        void Tick()
        {
            if (cluster.Leader() is { } leader)
            {
                var data = new byte[8];
                for (var i = 0; i < data.Length; i++)
                {
                    data[i] = (byte)random.NextInt(0, 256);
                }

                cluster.WithNode(leader.Id, node => node.Propose(data));
            }

            if (sim.Now + config.ProposalInterval < stopAt)
            {
                sim.After(config.ProposalInterval, Tick);
            }
        }

        sim.After(config.ProposalInterval, Tick);
    }

    /// <summary>
    /// After the fault window the cluster must elect a leader, commit a fresh entry, and bring every node
    /// up to date before the run ends.
    /// </summary>
    private sealed class LivenessProbe
    {
        private static readonly byte[] Marker = "liveness-probe"u8.ToArray();
        private readonly Simulator _sim;
        private readonly SimTrace _trace;
        private readonly TimeSpan _healAt;
        private (long Index, long Term)? _attempt;
        private TimeSpan _attemptAt;

        public LivenessProbe(Simulator sim, SimCluster cluster, SimTrace trace, TimeSpan healAt)
        {
            _sim = sim;
            _trace = trace;
            _healAt = healAt;
            sim.Schedule(healAt, () => Poll(cluster));
        }

        public (long Index, long Term)? Committed { get; private set; }

        public string? Check(SimCluster cluster)
        {
            if (Committed is not { } committed)
            {
                return $"no new entry was committed between the heal at {_healAt.TotalMilliseconds}ms and the end of the run";
            }

            var behind = cluster.Nodes.Where(n => n.Raft is null || n.Raft.LastApplied < committed.Index).Select(n => "n" + n.Id).ToArray();
            return behind.Length == 0 ? null : $"nodes {string.Join(",", behind)} did not catch up to index {committed.Index} after heal";
        }

        private void Poll(SimCluster cluster)
        {
            if (Committed is not null)
            {
                return;
            }

            var leader = cluster.Leader();
            if (_attempt is { } attempt && leader is not null && leader.CommitIndex >= attempt.Index)
            {
                var term = cluster[leader.Id].Storage.TermAt(attempt.Index);
                if (term == attempt.Term)
                {
                    Committed = attempt;
                    _trace.Add(TraceKind.Commit, leader.Id, $"liveness probe committed at {attempt.Index}");
                    return;
                }
            }

            if (leader is not null && (_attempt is null || _sim.Now - _attemptAt > TimeSpan.FromMilliseconds(300)))
            {
                ProposeResult result = default;
                cluster.WithNode(leader.Id, node => result = node.Propose(Marker));
                if (result.Accepted)
                {
                    _attempt = (result.Index, result.Term);
                    _attemptAt = _sim.Now;
                }
            }

            _sim.After(TimeSpan.FromMilliseconds(20), () => Poll(cluster));
        }
    }
}
