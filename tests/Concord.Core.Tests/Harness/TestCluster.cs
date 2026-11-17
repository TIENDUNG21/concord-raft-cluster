using Concord.Core.Messages;
using Concord.Core.Storage;

namespace Concord.Core.Tests.Harness;

internal sealed class ManualClock : IClock
{
    public TimeSpan Now { get; set; }
}

internal sealed class SeededRandom(int seed) : IRandomSource
{
    private readonly Random _random = new(seed);

    public int NextInt(int minInclusive, int maxExclusive) => _random.Next(minInclusive, maxExclusive);
}

internal sealed class CapturingTransport : ITransport
{
    public List<RaftMessage> Sent { get; } = new();

    public void Send(RaftMessage message) => Sent.Add(message);

    public T Single<T>()
        where T : RaftMessage => Assert.Single(Sent.OfType<T>());

    public void Clear() => Sent.Clear();
}

/// <summary>
/// A small synchronous cluster for unit tests: zero network latency, 1 ms time steps, explicit link
/// blocking and crashes. The simulator in Concord.Simulation is the realistic (latency, loss) version.
/// </summary>
internal sealed class TestCluster
{
    private readonly Queue<RaftMessage> _inflight = new();
    private readonly HashSet<(int From, int To)> _blocked = new();
    private readonly Dictionary<int, RaftNode?> _nodes = new();
    private readonly RaftOptions _options;
    private readonly int[] _ids;
    private int _randomSeed;

    public TestCluster(int size, RaftOptions? options = null, int seed = 1)
    {
        _options = options ?? new RaftOptions();
        _ids = Enumerable.Range(1, size).ToArray();
        _randomSeed = seed * 1000;
        foreach (var id in _ids)
        {
            Storages[id] = new InMemoryRaftStorage();
            _nodes[id] = CreateNode(id);
        }
    }

    public ManualClock Clock { get; } = new();

    public Dictionary<int, InMemoryRaftStorage> Storages { get; } = new();

    public Dictionary<int, RecordingStateMachine> Machines { get; } = new();

    public IReadOnlyList<int> Ids => _ids;

    /// <summary>Drops a message when it returns true. Applied at delivery time.</summary>
    public Func<RaftMessage, bool>? DropFilter { get; set; }

    public RaftNode this[int id] => _nodes[id] ?? throw new InvalidOperationException($"Node {id} is down.");

    public IEnumerable<RaftNode> LiveNodes => _ids.Select(id => _nodes[id]).OfType<RaftNode>();

    public bool IsUp(int id) => _nodes[id] is not null;

    public void DeliverAll()
    {
        var budget = 1_000_000;
        while (_inflight.Count > 0)
        {
            if (--budget == 0)
            {
                throw new InvalidOperationException("Message storm: the cluster did not quiesce.");
            }

            var message = _inflight.Dequeue();
            if (_blocked.Contains((message.From, message.To)) || DropFilter?.Invoke(message) == true)
            {
                continue;
            }

            _nodes[message.To]?.Step(message);
        }
    }

    /// <summary>Advances virtual time in 1 ms steps, firing due timers and delivering messages.</summary>
    public void Advance(TimeSpan duration)
    {
        var end = Clock.Now + duration;
        while (Clock.Now < end)
        {
            StepOneMillisecond();
        }
    }

    public bool RunUntil(Func<bool> condition, TimeSpan? limit = null)
    {
        var end = Clock.Now + (limit ?? TimeSpan.FromSeconds(10));
        while (!condition())
        {
            if (Clock.Now >= end)
            {
                return false;
            }

            StepOneMillisecond();
        }

        return true;
    }

    public RaftNode ElectLeader()
    {
        Assert.True(RunUntil(() => CurrentLeader() is not null), "No leader was elected.");
        return CurrentLeader()!;
    }

    /// <summary>The leader with the highest term among live nodes, if any.</summary>
    public RaftNode? CurrentLeader() =>
        LiveNodes.Where(n => n.Role == RaftRole.Leader).OrderByDescending(n => n.CurrentTerm).FirstOrDefault();

    public void Isolate(int id)
    {
        foreach (var other in _ids.Where(o => o != id))
        {
            _blocked.Add((id, other));
            _blocked.Add((other, id));
        }
    }

    public void Block(int from, int to) => _blocked.Add((from, to));

    public void Heal() => _blocked.Clear();

    public void Crash(int id) => _nodes[id] = null;

    public RaftNode Restart(int id)
    {
        var node = CreateNode(id);
        _nodes[id] = node;
        return node;
    }

    public void Send(RaftMessage message) => _inflight.Enqueue(message);

    private void StepOneMillisecond()
    {
        Clock.Now += TimeSpan.FromMilliseconds(1);
        foreach (var id in _ids)
        {
            var node = _nodes[id];
            if (node is not null && Clock.Now >= node.NextDeadline)
            {
                node.Tick();
                DeliverAll();
            }
        }

        DeliverAll();
    }

    private RaftNode CreateNode(int id)
    {
        var machine = new RecordingStateMachine();
        Machines[id] = machine;
        return new RaftNode(
            id,
            _ids,
            _options,
            Storages[id],
            machine,
            new ClusterTransport(this),
            Clock,
            new SeededRandom(++_randomSeed));
    }

    private sealed class ClusterTransport(TestCluster cluster) : ITransport
    {
        public void Send(RaftMessage message) => cluster.Send(message);
    }
}
