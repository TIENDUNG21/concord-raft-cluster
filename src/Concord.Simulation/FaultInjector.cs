namespace Concord.Simulation;

/// <summary>Which faults a run may inject, and how often.</summary>
public sealed record FaultOptions
{
    /// <summary>Faults are drawn at uniformly random gaps in [MinGap, MaxGap].</summary>
    public TimeSpan MinGap { get; init; } = TimeSpan.FromMilliseconds(150);

    public TimeSpan MaxGap { get; init; } = TimeSpan.FromMilliseconds(900);

    public bool Partitions { get; init; } = true;

    public bool LossBursts { get; init; } = true;

    /// <summary>Crash and restart nodes (at least one node always stays up).</summary>
    public bool Crashes { get; init; } = true;

    public static FaultOptions None => new() { Partitions = false, LossBursts = false, Crashes = false };

    internal bool Any => Partitions || LossBursts || Crashes;
}

/// <summary>
/// Draws a random fault schedule from the run's seed: partitions (random splits, isolating the leader,
/// one-way links), heals, and bursts of loss and duplication. After the fault window everything is healed.
/// </summary>
public sealed class FaultInjector
{
    private readonly Simulator _sim;
    private readonly SimCluster _cluster;
    private readonly SimNetwork _network;
    private readonly SimTrace _trace;
    private readonly DeterministicRandom _random;
    private readonly FaultOptions _options;
    private readonly NetworkOptions _baseline;

    public FaultInjector(Simulator sim, SimCluster cluster, SimNetwork network, SimTrace trace, DeterministicRandom random, FaultOptions options)
    {
        _sim = sim;
        _cluster = cluster;
        _network = network;
        _trace = trace;
        _random = random;
        _options = options;
        _baseline = network.Options;
    }

    public int Injected { get; private set; }

    /// <summary>Schedules faults until <paramref name="until"/>, then a full heal at that time.</summary>
    public void Schedule(TimeSpan until)
    {
        if (_options.Any)
        {
            ScheduleNext(until);
        }

        _sim.Schedule(until, HealAll);
    }

    public void HealAll()
    {
        _network.Heal();
        _network.Options = _baseline;
        _trace.Add(TraceKind.Fault, null, "heal: all links up, loss back to baseline");
        foreach (var node in _cluster.Nodes.Where(n => !n.IsUp).ToArray())
        {
            _cluster.Restart(node.Id);
        }
    }

    private void ScheduleNext(TimeSpan until)
    {
        var gap = _random.NextInt((int)_options.MinGap.TotalMilliseconds, (int)_options.MaxGap.TotalMilliseconds + 1);
        var at = _sim.Now + TimeSpan.FromMilliseconds(gap);
        if (at >= until)
        {
            return;
        }

        _sim.Schedule(at, () =>
        {
            InjectOne();
            ScheduleNext(until);
        });
    }

    private void InjectOne()
    {
        var choices = new List<Action>();
        if (_options.Partitions)
        {
            choices.Add(RandomSplit);
            choices.Add(IsolateLeader);
            choices.Add(OneWayLink);
            choices.Add(HealNetwork);
            choices.Add(HealNetwork);
        }

        if (_options.LossBursts)
        {
            choices.Add(LossBurst);
        }

        if (_options.Crashes)
        {
            choices.Add(CrashRandom);
            choices.Add(CrashLeader);
            choices.Add(RestartRandom);
            choices.Add(RestartRandom);
        }

        Injected++;
        _random.Pick(choices)();
    }

    private void RandomSplit()
    {
        var ids = _cluster.Ids.ToArray();
        Shuffle(ids);
        var cut = _random.NextInt(1, ids.Length);
        var left = ids[..cut].Order().ToArray();
        var right = ids[cut..].Order().ToArray();
        _network.Heal();
        _network.Partition(new IReadOnlyCollection<int>[] { left, right });
        _trace.Add(TraceKind.Fault, null, $"partition {{{string.Join(",", left)}}} | {{{string.Join(",", right)}}}");
    }

    private void IsolateLeader()
    {
        var leader = _cluster.Leader();
        if (leader is null)
        {
            RandomSplit();
            return;
        }

        _network.Heal();
        _network.Isolate(leader.Id);
        _trace.Add(TraceKind.Fault, leader.Id, "isolate leader");
    }

    private void OneWayLink()
    {
        var from = _random.Pick(_cluster.Ids);
        var to = _random.Pick(_cluster.Ids.Where(id => id != from).ToArray());
        _network.Block(from, to);
        _trace.Add(TraceKind.Fault, null, $"cut link n{from} -> n{to}");
    }

    private void HealNetwork()
    {
        _network.Heal();
        _trace.Add(TraceKind.Fault, null, "heal partitions");
    }

    private void LossBurst()
    {
        var drop = 0.05 + (_random.NextDouble() * 0.25);
        var duplicate = _random.NextDouble() * 0.1;
        _network.Options = _baseline with { DropRate = drop, DuplicateRate = duplicate };
        _trace.Add(TraceKind.Fault, null, $"loss burst drop={drop:0.00} dup={duplicate:0.00}");
        _sim.After(TimeSpan.FromMilliseconds(_random.NextInt(100, 600)), () =>
        {
            _network.Options = _baseline;
            _trace.Add(TraceKind.Fault, null, "loss burst over");
        });
    }

    private void CrashRandom()
    {
        var up = _cluster.Nodes.Where(n => n.IsUp).Select(n => n.Id).ToArray();
        if (up.Length > 1)
        {
            _cluster.Crash(_random.Pick(up));
        }
    }

    private void CrashLeader()
    {
        if (_cluster.Leader() is { } leader && _cluster.UpCount > 1)
        {
            _cluster.Crash(leader.Id);
        }
        else
        {
            CrashRandom();
        }
    }

    private void RestartRandom()
    {
        var down = _cluster.Nodes.Where(n => !n.IsUp).Select(n => n.Id).ToArray();
        if (down.Length > 0)
        {
            _cluster.Restart(_random.Pick(down));
        }
    }

    private void Shuffle(int[] items)
    {
        for (var i = items.Length - 1; i > 0; i--)
        {
            var j = _random.NextInt(0, i + 1);
            (items[i], items[j]) = (items[j], items[i]);
        }
    }
}
