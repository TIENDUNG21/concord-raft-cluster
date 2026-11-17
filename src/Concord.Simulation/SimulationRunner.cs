using System.Globalization;
using System.Text;
using Concord.Checker;
using Concord.Core;
using Concord.Kv;

namespace Concord.Simulation;

/// <summary>
/// Known-bad behaviours that can be switched on to prove the harness catches them. A test suite that
/// has never failed proves little; these make it fail on purpose.
/// </summary>
public sealed record InjectedBugs
{
    /// <summary>Leaders answer reads from local state without ReadIndex: a deposed leader serves stale data.</summary>
    public bool UnsafeLocalReads { get; init; }

    /// <summary>Votes are granted without comparing logs, so a node missing committed entries can lead.</summary>
    public bool IgnoreLogInVotes { get; init; }

    public static InjectedBugs None { get; } = new();

    public bool Any => UnsafeLocalReads || IgnoreLogInVotes;
}

/// <summary>Everything that defines a run. Together with the seed it fully determines the outcome.</summary>
public sealed record SimulationConfig
{
    public ulong Seed { get; init; } = 1;

    public int Nodes { get; init; } = 5;

    /// <summary>Total virtual time.</summary>
    public TimeSpan Duration { get; init; } = TimeSpan.FromSeconds(12);

    /// <summary>Faults are injected during [0, FaultWindow); then everything heals and the cluster must recover.</summary>
    public TimeSpan FaultWindow { get; init; } = TimeSpan.FromSeconds(8);

    /// <summary>Clients stop issuing new operations this long before the end, so the cluster can settle.</summary>
    public TimeSpan SettleTime { get; init; } = TimeSpan.FromSeconds(1.5);

    public RaftOptions Raft { get; init; } = new() { SnapshotThreshold = 200 };

    public NetworkOptions Network { get; init; } = new();

    public FaultOptions Faults { get; init; } = new();

    public WorkloadOptions Workload { get; init; } = new();

    public InjectedBugs Bugs { get; init; } = InjectedBugs.None;
}

public enum FailureKind
{
    Invariant,
    Linearizability,
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
    int Operations,
    int OperationsGivenUp,
    TimeSpan VirtualTime);

public sealed record SimulationResult(
    ulong Seed,
    FailureKind? Failure,
    string? Message,
    SimulationStats Stats,
    SimTrace Trace,
    IReadOnlyList<Timed<KvOperation>> History,
    CheckResult? Linearizability)
{
    public bool Passed => Failure is null;
}

/// <summary>
/// Runs one seeded simulation of a Concord cluster: random faults, KV clients, online Raft invariants,
/// recovery after the fault window, and a linearizability check of everything the clients observed.
/// </summary>
public static class SimulationRunner
{
    public const int FirstClientEndpoint = 101;

    public static SimulationResult Run(SimulationConfig config, CheckOptions? checkOptions = null)
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
        var serviceOptions = new KvServiceOptions { UnsafeLocalReads = config.Bugs.UnsafeLocalReads };
        var raftOptions = config.Bugs.IgnoreLogInVotes ? CopyWithUnsafeVotes(config.Raft) : config.Raft;
        var cluster = new SimCluster(sim, network, trace, config.Nodes, raftOptions, nodeRandom, _ => new KvApplication(serviceOptions));
        var monitor = new InvariantMonitor(id => cluster[id].Storage, () => sim.Now);
        cluster.AddObserver(monitor);
        cluster.OnRestart(monitor.NodeRestarted);
        cluster.Start();

        var faults = new FaultInjector(sim, cluster, network, trace, faultRandom, config.Faults);
        faults.Schedule(config.FaultWindow);

        var history = new HistoryRecorder();
        var stopIssuing = config.Duration - config.SettleTime;
        var clients = Enumerable.Range(0, config.Workload.Clients)
            .Select(i => new SimClient(FirstClientEndpoint + i, sim, network, cluster.Ids, workloadRandom.Fork(), config.Workload, history, trace))
            .ToArray();
        foreach (var client in clients)
        {
            client.Start(TimeSpan.FromMilliseconds(workloadRandom.NextInt(0, 50)), stopIssuing);
        }

        var (failure, message) = Execute(sim, config, monitor);
        if (failure is null)
        {
            (failure, message) = Liveness(cluster, clients, config);
        }

        CheckResult? check = null;
        if (failure is null)
        {
            check = KvLinearizability.Check(history.Operations, checkOptions);
            if (check.Outcome == CheckOutcome.Illegal)
            {
                failure = FailureKind.Linearizability;
                message = $"history of key {check.Partition} is not linearizable:\n{check.Explanation}";
            }
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
            history.Operations.Count,
            clients.Sum(c => c.GaveUp),
            sim.Now);
        return new SimulationResult(config.Seed, failure, message, stats, trace, history.Operations, check);
    }

    /// <summary>A compact, human-readable report: seed, verdict and the events that matter.</summary>
    public static string FormatReport(SimulationResult result, int maxEvents = 60)
    {
        ArgumentNullException.ThrowIfNull(result);
        var builder = new StringBuilder();
        builder.Append("seed ").Append(result.Seed).Append(": ")
            .Append(result.Passed ? "passed" : $"FAILED ({result.Failure})").Append('\n');
        if (result.Message is not null)
        {
            builder.Append(result.Message).Append('\n');
        }

        builder.Append("-- faults, elections and verdict (virtual time) --\n");
        builder.Append(result.Trace.Format(
            e => e.Kind is TraceKind.Fault or TraceKind.Violation || (e.Kind == TraceKind.Role && e.Text.StartsWith("Leader", StringComparison.Ordinal)),
            maxEvents));
        builder.Append("-- last messages --\n");
        foreach (var record in result.Trace.RecentMessageRecords().TakeLast(20))
        {
            builder.Append(record).Append('\n');
        }

        builder.Append("replay: concord-sim run --seed ").Append(result.Seed).Append(" --trace\n");
        return builder.ToString();
    }

    private static RaftOptions CopyWithUnsafeVotes(RaftOptions options) => new()
    {
        HeartbeatInterval = options.HeartbeatInterval,
        ElectionTimeoutMin = options.ElectionTimeoutMin,
        ElectionTimeoutMax = options.ElectionTimeoutMax,
        MaxEntriesPerMessage = options.MaxEntriesPerMessage,
        SnapshotThreshold = options.SnapshotThreshold,
        PreVote = options.PreVote,
        CheckQuorum = options.CheckQuorum,
        UnsafeIgnoreLogInVotes = true,
    };

    private static (FailureKind? Failure, string? Message) Execute(Simulator sim, SimulationConfig config, InvariantMonitor monitor)
    {
        try
        {
            sim.RunUntil(config.Duration, () => monitor.Violation is not null);
        }
        catch (RaftSafetyException ex)
        {
            return (FailureKind.Invariant, ex.Message);
        }
#pragma warning disable CA1031 // Any exception from a node is a finding of the run, reported with its seed.
        catch (Exception ex)
#pragma warning restore CA1031
        {
            return (FailureKind.Exception, $"{ex.GetType().Name}: {ex.Message}");
        }

        return monitor.Violation is null ? (null, null) : (FailureKind.Invariant, monitor.Violation);
    }

    /// <summary>
    /// After the fault window: every client must have finished its last operation, every node must be
    /// up and have applied the same prefix, and some client operation must have succeeded.
    /// </summary>
    private static (FailureKind? Failure, string? Message) Liveness(SimCluster cluster, SimClient[] clients, SimulationConfig config)
    {
        if (clients.Select(c => c.ProtocolError).FirstOrDefault(e => e is not null) is { } protocolError)
        {
            return (FailureKind.Invariant, protocolError);
        }

        var busy = clients.Count(c => !c.Idle);
        if (busy > 0)
        {
            return (FailureKind.Liveness, string.Create(CultureInfo.InvariantCulture, $"{busy} client(s) still waiting {config.SettleTime.TotalSeconds:0.0}s after they stopped issuing operations"));
        }

        if (cluster.Nodes.Select(n => n.Raft?.LastApplied ?? -1).Distinct().Count() != 1)
        {
            var applied = cluster.Nodes.Select(n => $"n{n.Id}:{(n.Raft is null ? "down" : n.Raft.LastApplied.ToString(CultureInfo.InvariantCulture))}");
            return (FailureKind.Liveness, "nodes did not converge after the heal: last applied " + string.Join(", ", applied));
        }

        return clients.Sum(c => c.Completed) == 0
            ? (FailureKind.Liveness, "no client operation completed during the whole run")
            : (null, null);
    }
}
