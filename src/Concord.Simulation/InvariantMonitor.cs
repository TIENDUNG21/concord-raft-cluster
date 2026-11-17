using Concord.Core;
using Concord.Core.Storage;

namespace Concord.Simulation;

/// <summary>
/// Checks Raft's safety properties (paper, Figure 3) online, as events happen:
/// Election Safety (one leader per term), one vote per node per term (even across restarts),
/// State Machine Safety (no two nodes apply different entries at one index), Leader Completeness
/// (a new leader holds every entry already applied anywhere) and per-node apply/commit monotonicity.
/// The first violation is kept; the runner stops there.
/// </summary>
public sealed class InvariantMonitor : IRaftObserver
{
    private readonly Func<int, IRaftStorage> _storageOf;
    private readonly Func<TimeSpan> _now;
    private readonly Dictionary<long, int> _leaders = new();
    private readonly Dictionary<(int Node, long Term), int> _votes = new();
    private readonly Dictionary<long, LogEntry> _applied = new();
    private readonly Dictionary<int, long> _lastApplied = new();
    private readonly Dictionary<int, long> _commitIndex = new();

    public InvariantMonitor(Func<int, IRaftStorage> storageOf, Func<TimeSpan> now)
    {
        _storageOf = storageOf ?? throw new ArgumentNullException(nameof(storageOf));
        _now = now ?? throw new ArgumentNullException(nameof(now));
    }

    public string? Violation { get; private set; }

    public TimeSpan? ViolationTime { get; private set; }

    /// <summary>Terms in which some node became leader.</summary>
    public int Elections => _leaders.Count;

    public long MaxTerm => _leaders.Count == 0 ? 0 : _leaders.Keys.Max();

    /// <summary>Highest index applied by any node.</summary>
    public long MaxApplied { get; private set; }

    public void RoleChanged(int nodeId, RaftRole role, long term, int? leaderId)
    {
        if (role != RaftRole.Leader)
        {
            return;
        }

        if (_leaders.TryGetValue(term, out var existing) && existing != nodeId)
        {
            Fail($"Election Safety: n{existing} and n{nodeId} are both leader in term {term}.");
            return;
        }

        _leaders[term] = nodeId;
        var storage = _storageOf(nodeId);
        foreach (var (index, entry) in _applied)
        {
            if (index <= storage.SnapshotIndex)
            {
                continue;
            }

            var held = storage.TermAt(index);
            if (held != entry.Term)
            {
                Fail($"Leader Completeness: new leader n{nodeId} (term {term}) has {(held is null ? "no entry" : "term " + held)} at {index}, but {entry} was committed.");
                return;
            }
        }
    }

    public void VoteGranted(int nodeId, long term, int candidateId)
    {
        if (_votes.TryGetValue((nodeId, term), out var earlier) && earlier != candidateId)
        {
            Fail($"Double vote: n{nodeId} voted for n{earlier} and n{candidateId} in term {term}.");
            return;
        }

        _votes[(nodeId, term)] = candidateId;
    }

    public void Committed(int nodeId, long commitIndex)
    {
        if (_commitIndex.TryGetValue(nodeId, out var previous) && commitIndex < previous)
        {
            Fail($"Commit index of n{nodeId} went backwards ({previous} -> {commitIndex}).");
        }

        _commitIndex[nodeId] = commitIndex;
    }

    public void Applied(int nodeId, LogEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        var last = _lastApplied.GetValueOrDefault(nodeId);
        if (entry.Index != last + 1)
        {
            Fail($"n{nodeId} applied {entry.Index} right after {last}.");
            return;
        }

        _lastApplied[nodeId] = entry.Index;
        if (_applied.TryGetValue(entry.Index, out var first))
        {
            if (!first.Equals(entry))
            {
                Fail($"State Machine Safety: n{nodeId} applied {entry} at {entry.Index}, another node applied {first}.");
            }

            return;
        }

        _applied[entry.Index] = entry;
        MaxApplied = Math.Max(MaxApplied, entry.Index);
    }

    public void SnapshotInstalled(int nodeId, SnapshotMetadata metadata)
    {
        _lastApplied[nodeId] = metadata.LastIncludedIndex;
        _commitIndex[nodeId] = Math.Max(_commitIndex.GetValueOrDefault(nodeId), metadata.LastIncludedIndex);
    }

    /// <summary>A restarted node re-applies from its snapshot; its volatile counters start over.</summary>
    public void NodeRestarted(int nodeId)
    {
        var snapshotIndex = _storageOf(nodeId).SnapshotIndex;
        _lastApplied[nodeId] = snapshotIndex;
        _commitIndex[nodeId] = snapshotIndex;
    }

    public void Fail(string message)
    {
        if (Violation is not null)
        {
            return;
        }

        Violation = message;
        ViolationTime = _now();
    }
}
