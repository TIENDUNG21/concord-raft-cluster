using Concord.Core;

namespace Concord.Kv;

public enum KvStatus
{
    /// <summary>Done; <see cref="KvResponse.Result"/> holds the outcome.</summary>
    Ok,

    /// <summary>This node cannot serve the request; try <see cref="KvResponse.LeaderHint"/>.</summary>
    NotLeader,

    /// <summary>Leadership changed before the outcome was known. The write may or may not have happened; retry with the same sequence number.</summary>
    Unknown,

    /// <summary>The request itself is invalid.</summary>
    Rejected,
}

public sealed record KvResponse(KvStatus Status, KvResult? Result, int? LeaderHint, long Index, string? Error = null)
{
    public static KvResponse NotLeader(int? hint) => new(KvStatus.NotLeader, null, hint, 0);

    public static KvResponse Unknown(int? hint) => new(KvStatus.Unknown, null, hint, 0);
}

public sealed class KvServiceOptions
{
    /// <summary>
    /// DELIBERATELY UNSAFE, for testing the test harness: a node that believes it is leader answers reads
    /// from local state without ReadIndex. A deposed leader then serves stale data, which the simulator's
    /// linearizability checker must catch.
    /// </summary>
    public bool UnsafeLocalReads { get; init; }
}

/// <summary>
/// Client-facing KV API on one node. Writes go through the Raft log and complete when applied; reads
/// use ReadIndex. Completion is reported through callbacks on the node's thread, which keeps the
/// simulator deterministic; the server wraps them in tasks.
/// </summary>
public sealed class KvService : IRaftObserver
{
    private readonly RaftNode _node;
    private readonly KvStateMachine _machine;
    private readonly KvServiceOptions _options;
    private readonly SortedDictionary<long, PendingWrite> _pending = new();

    public KvService(RaftNode node, KvStateMachine machine, KvServiceOptions? options = null)
    {
        _node = node ?? throw new ArgumentNullException(nameof(node));
        _machine = machine ?? throw new ArgumentNullException(nameof(machine));
        _options = options ?? new KvServiceOptions();
        _machine.Applied = OnApplied;
        _node.AddObserver(this);
    }

    public int PendingWrites => _pending.Count;

    public void Write(KvCommand command, Action<KvResponse> done)
    {
        ArgumentNullException.ThrowIfNull(command);
        ArgumentNullException.ThrowIfNull(done);
        if (command.Validate() is { } problem)
        {
            done(new KvResponse(KvStatus.Rejected, null, null, 0, problem));
            return;
        }

        if (_node.Role != RaftRole.Leader)
        {
            done(KvResponse.NotLeader(_node.LeaderId));
            return;
        }

        // Register before proposing: in a single-node cluster the entry commits and applies inside Propose.
        var index = _node.LastLogIndex + 1;
        _pending[index] = new PendingWrite(_node.CurrentTerm, done);
        var proposal = _node.Propose(KvCommandCodec.Encode(command));
        if (!proposal.Accepted || proposal.Index != index)
        {
            throw new InvalidOperationException($"Leader appended at {proposal.Index}, expected {index}.");
        }
    }

    public void Read(string key, Action<KvResponse> done)
    {
        ArgumentNullException.ThrowIfNull(done);
        if (KvLimits.ValidateKey(key) is { } problem)
        {
            done(new KvResponse(KvStatus.Rejected, null, null, 0, problem));
            return;
        }

        if (_options.UnsafeLocalReads)
        {
            done(_node.Role == RaftRole.Leader ? Found(key, _node.LastApplied) : KvResponse.NotLeader(_node.LeaderId));
            return;
        }

        _node.RequestRead(read => done(read.Success ? Found(key, read.Index) : KvResponse.NotLeader(read.LeaderHint)));
    }

    void IRaftObserver.RoleChanged(int nodeId, RaftRole role, long term, int? leaderId)
    {
        if (role == RaftRole.Leader || _pending.Count == 0)
        {
            return;
        }

        // We may never learn whether these entries commit (a new leader decides); callers must retry.
        var orphaned = _pending.Values.ToArray();
        _pending.Clear();
        foreach (var write in orphaned)
        {
            write.Done(KvResponse.Unknown(leaderId));
        }
    }

    private KvResponse Found(string key, long index)
    {
        var value = _machine.Get(key);
        return new KvResponse(KvStatus.Ok, new KvResult(value is not null, value), _node.Id, index);
    }

    private void OnApplied(LogEntry entry, KvCommand? command, KvResult? result)
    {
        if (!_pending.Remove(entry.Index, out var write))
        {
            return;
        }

        // Same index, different term: our entry was overwritten by another leader's.
        write.Done(write.Term == entry.Term && result is not null
            ? new KvResponse(KvStatus.Ok, result, _node.Id, entry.Index)
            : KvResponse.Unknown(_node.LeaderId));
    }

    private sealed record PendingWrite(long Term, Action<KvResponse> Done);
}
