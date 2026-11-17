namespace Concord.Simulation;

/// <summary>Link behaviour. Latency is uniform in [min, max]; a slow fraction takes up to 10x longer.</summary>
public sealed record NetworkOptions
{
    public TimeSpan MinLatency { get; init; } = TimeSpan.FromMilliseconds(1);

    public TimeSpan MaxLatency { get; init; } = TimeSpan.FromMilliseconds(8);

    /// <summary>Probability that a message is slow; slow messages overtake nothing but get overtaken (reordering).</summary>
    public double SlowRate { get; init; } = 0.02;

    public double DropRate { get; init; }

    public double DuplicateRate { get; init; }
}

/// <summary>
/// An unreliable network between endpoints. Raft nodes can be partitioned from each other; clients
/// are never partitioned (a client next to a deposed leader keeps talking to it, which is exactly the
/// situation a stale read needs). A link is checked both when a message is sent and when it arrives.
/// </summary>
public sealed class SimNetwork
{
    private readonly Simulator _sim;
    private readonly DeterministicRandom _random;
    private readonly SimTrace? _trace;
    private readonly Dictionary<int, Action<int, object>> _endpoints = new();
    private readonly SortedSet<int> _nodes = new();
    private readonly HashSet<(int From, int To)> _blocked = new();

    public SimNetwork(Simulator sim, DeterministicRandom random, NetworkOptions? options = null, SimTrace? trace = null)
    {
        _sim = sim ?? throw new ArgumentNullException(nameof(sim));
        _random = random ?? throw new ArgumentNullException(nameof(random));
        _trace = trace;
        Options = options ?? new NetworkOptions();
    }

    public NetworkOptions Options { get; set; }

    public long Sent { get; private set; }

    public long Duplicated { get; private set; }

    public IReadOnlyCollection<int> Nodes => _nodes;

    /// <summary>Registers a Raft node; it takes part in partitions.</summary>
    public void RegisterNode(int id, Action<int, object> receive)
    {
        _endpoints[id] = receive;
        _nodes.Add(id);
    }

    /// <summary>Registers a client endpoint; partitions never cut it off.</summary>
    public void RegisterClient(int id, Action<int, object> receive) => _endpoints[id] = receive;

    public void Send(int from, int to, object payload)
    {
        Sent++;
        if (!CanReach(from, to) || _random.Chance(Options.DropRate))
        {
            _trace?.Message(MessageFate.Dropped, from, to, payload);
            return;
        }

        ScheduleDelivery(from, to, payload);
        if (_random.Chance(Options.DuplicateRate))
        {
            Duplicated++;
            ScheduleDelivery(from, to, payload);
        }
    }

    public bool CanReach(int from, int to) => !_blocked.Contains((from, to));

    /// <summary>Cuts every link between the given groups; links inside a group stay up.</summary>
    public void Partition(IReadOnlyList<IReadOnlyCollection<int>> groups)
    {
        ArgumentNullException.ThrowIfNull(groups);
        for (var i = 0; i < groups.Count; i++)
        {
            for (var j = 0; j < groups.Count; j++)
            {
                if (i == j)
                {
                    continue;
                }

                foreach (var a in groups[i])
                {
                    foreach (var b in groups[j])
                    {
                        _blocked.Add((a, b));
                    }
                }
            }
        }
    }

    public void Isolate(int node) =>
        Partition(new IReadOnlyCollection<int>[] { new[] { node }, _nodes.Where(n => n != node).ToArray() });

    /// <summary>Cuts one direction of one link (asymmetric failures).</summary>
    public void Block(int from, int to) => _blocked.Add((from, to));

    public void Heal() => _blocked.Clear();

    public bool IsHealed => _blocked.Count == 0;

    private void ScheduleDelivery(int from, int to, object payload)
    {
        var min = (int)Options.MinLatency.TotalMilliseconds;
        var max = (int)Options.MaxLatency.TotalMilliseconds;
        var latency = _random.NextInt(min, max + 1);
        if (_random.Chance(Options.SlowRate))
        {
            latency = _random.NextInt(max, (max * 10) + 1);
        }

        _sim.After(TimeSpan.FromMilliseconds(latency), () =>
        {
            if (!CanReach(from, to) || !_endpoints.TryGetValue(to, out var receive))
            {
                _trace?.Message(MessageFate.Dropped, from, to, payload);
                return;
            }

            _trace?.Message(MessageFate.Delivered, from, to, payload);
            receive(from, payload);
        });
    }
}
