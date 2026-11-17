using Concord.Core;
using Concord.Core.Messages;
using Concord.Core.Storage;

namespace Concord.Simulation;

/// <summary>What runs on top of Raft on one node: the state machine and any client-facing service.</summary>
public interface INodeApplication
{
    IStateMachine StateMachine { get; }

    /// <summary>Called once the node of this incarnation exists.</summary>
    void Attach(RaftNode node);

    /// <summary>A non-Raft message (a client request) arrived; <paramref name="reply"/> sends one back.</summary>
    void Receive(int from, object payload, Action<int, object> reply);
}

/// <summary>One simulated server: durable storage that survives crashes, plus the current incarnation.</summary>
public sealed class SimNode
{
    internal SimNode(int id, InMemoryRaftStorage storage)
    {
        Id = id;
        Storage = storage;
    }

    public int Id { get; }

    public InMemoryRaftStorage Storage { get; }

    public RaftNode? Raft { get; internal set; }

    public INodeApplication? Application { get; internal set; }

    public bool IsUp => Raft is not null;

    /// <summary>Bumped on every crash, so timers and replies of a dead incarnation are ignored.</summary>
    public int Incarnation { get; internal set; }

    internal TimeSpan? ScheduledWakeup { get; set; }
}

/// <summary>
/// Wires Raft nodes to the simulated network and virtual clock. Each node's timers are scheduled at
/// exactly <see cref="RaftNode.NextDeadline"/>, so virtual time jumps from event to event.
/// </summary>
public sealed class SimCluster
{
    private readonly Simulator _sim;
    private readonly SimNetwork _network;
    private readonly SimTrace _trace;
    private readonly RaftOptions _options;
    private readonly DeterministicRandom _random;
    private readonly Func<int, INodeApplication> _applicationFactory;
    private readonly List<IRaftObserver> _observers = new();
    private readonly List<Action<int>> _restartHooks = new();
    private readonly List<Action<int>> _crashHooks = new();
    private readonly SimNode[] _nodes;
    private readonly int[] _ids;

    public SimCluster(
        Simulator sim,
        SimNetwork network,
        SimTrace trace,
        int nodeCount,
        RaftOptions options,
        DeterministicRandom random,
        Func<int, INodeApplication> applicationFactory)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(nodeCount, 1);
        _sim = sim ?? throw new ArgumentNullException(nameof(sim));
        _network = network ?? throw new ArgumentNullException(nameof(network));
        _trace = trace ?? throw new ArgumentNullException(nameof(trace));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _random = random ?? throw new ArgumentNullException(nameof(random));
        _applicationFactory = applicationFactory ?? throw new ArgumentNullException(nameof(applicationFactory));
        _ids = Enumerable.Range(1, nodeCount).ToArray();
        _nodes = _ids.Select(id => new SimNode(id, new InMemoryRaftStorage())).ToArray();
        foreach (var node in _nodes)
        {
            _network.RegisterNode(node.Id, (from, payload) => Receive(node, from, payload));
        }

        _observers.Add(new TraceObserver(trace));
    }

    public IReadOnlyList<SimNode> Nodes => _nodes;

    public IReadOnlyList<int> Ids => _ids;

    public SimNode this[int id] => _nodes[id - 1];

    public void AddObserver(IRaftObserver observer) => _observers.Add(observer);

    /// <summary>Runs before a crashed node boots again (the invariant monitor resets its counters here).</summary>
    public void OnRestart(Action<int> hook) => _restartHooks.Add(hook);

    public void OnCrash(Action<int> hook) => _crashHooks.Add(hook);

    public int UpCount => _nodes.Count(n => n.IsUp);

    /// <summary>
    /// Kills a node: its volatile state is gone, its storage survives (every write was synced), and
    /// timers or replies scheduled by this incarnation become no-ops.
    /// </summary>
    public void Crash(int id)
    {
        var node = this[id];
        if (!node.IsUp)
        {
            return;
        }

        var wasLeader = node.Raft!.Role == RaftRole.Leader;
        node.Raft = null;
        node.Application = null;
        node.ScheduledWakeup = null;
        node.Incarnation++;
        _trace.Add(TraceKind.Fault, id, wasLeader ? "crash (was leader)" : "crash");
        foreach (var hook in _crashHooks)
        {
            hook(id);
        }
    }

    /// <summary>Boots a crashed node from its durable storage.</summary>
    public void Restart(int id)
    {
        var node = this[id];
        if (node.IsUp)
        {
            return;
        }

        foreach (var hook in _restartHooks)
        {
            hook(id);
        }

        Boot(node);
        _trace.Add(TraceKind.Fault, id, $"restart (term {node.Raft!.CurrentTerm}, log {node.Raft.LastLogIndex}, snapshot {node.Storage.SnapshotIndex})");
    }

    /// <summary>Starts every node. Call once, after observers are registered.</summary>
    public void Start()
    {
        foreach (var node in _nodes)
        {
            Boot(node);
        }
    }

    /// <summary>The live leader with the highest term, if any.</summary>
    public RaftNode? Leader() =>
        _nodes.Select(n => n.Raft)
            .OfType<RaftNode>()
            .Where(r => r.Role == RaftRole.Leader)
            .OrderByDescending(r => r.CurrentTerm)
            .FirstOrDefault();

    /// <summary>Runs <paramref name="action"/> against a live node, then re-arms its timer.</summary>
    public void WithNode(int id, Action<RaftNode> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        var node = this[id];
        if (node.Raft is null)
        {
            return;
        }

        action(node.Raft);
        ArmTimer(node);
    }

    private void Boot(SimNode node)
    {
        var application = _applicationFactory(node.Id);
        var raft = new RaftNode(
            node.Id,
            _ids,
            _options,
            node.Storage,
            application.StateMachine,
            new NodeTransport(_network, node.Id),
            _sim.Clock,
            _random.Fork());
        foreach (var observer in _observers)
        {
            raft.AddObserver(observer);
        }

        node.Raft = raft;
        node.Application = application;
        node.ScheduledWakeup = null;
        application.Attach(raft);
        ArmTimer(node);
    }

    private void Receive(SimNode node, int from, object payload)
    {
        if (node.Raft is null)
        {
            return;
        }

        if (payload is RaftMessage message)
        {
            node.Raft.Step(message);
        }
        else
        {
            var incarnation = node.Incarnation;
            node.Application!.Receive(from, payload, (to, reply) =>
            {
                if (node.Incarnation == incarnation && node.IsUp)
                {
                    _network.Send(node.Id, to, reply);
                }
            });
        }

        ArmTimer(node);
    }

    private void ArmTimer(SimNode node)
    {
        if (node.Raft is null)
        {
            return;
        }

        var deadline = node.Raft.NextDeadline;
        if (node.ScheduledWakeup is { } pending && pending <= deadline && pending >= _sim.Now)
        {
            return;
        }

        node.ScheduledWakeup = deadline;
        var incarnation = node.Incarnation;
        _sim.Schedule(deadline, () =>
        {
            if (node.Incarnation != incarnation || node.Raft is null)
            {
                return;
            }

            if (node.ScheduledWakeup == deadline)
            {
                node.ScheduledWakeup = null;
            }

            node.Raft.Tick();
            ArmTimer(node);
        });
    }

    private sealed class NodeTransport(SimNetwork network, int id) : ITransport
    {
        public void Send(RaftMessage message) => network.Send(id, message.To, message);
    }

    private sealed class TraceObserver(SimTrace trace) : IRaftObserver
    {
        public void RoleChanged(int nodeId, RaftRole role, long term, int? leaderId) =>
            trace.Add(TraceKind.Role, nodeId, $"{role} term={term} leader={(leaderId is { } l ? "n" + l : "?")}");

        public void VoteGranted(int nodeId, long term, int candidateId) =>
            trace.Add(TraceKind.Vote, nodeId, $"votes for n{candidateId} in term {term}");

        public void SnapshotTaken(int nodeId, SnapshotMetadata metadata) =>
            trace.Add(TraceKind.Snapshot, nodeId, $"snapshot taken up to {metadata.LastIncludedIndex}@{metadata.LastIncludedTerm}");

        public void SnapshotInstalled(int nodeId, SnapshotMetadata metadata) =>
            trace.Add(TraceKind.Snapshot, nodeId, $"snapshot installed up to {metadata.LastIncludedIndex}@{metadata.LastIncludedTerm}");
    }
}
