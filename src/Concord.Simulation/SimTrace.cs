using System.Globalization;
using System.Text;
using Concord.Core.Messages;

namespace Concord.Simulation;

public enum TraceKind
{
    Fault,
    Role,
    Vote,
    Commit,
    Snapshot,
    Client,
    Violation,
    Note,
}

/// <summary>One notable event. Messages are kept separately because there are so many of them.</summary>
public readonly record struct TraceEvent(TimeSpan Time, TraceKind Kind, int? Node, string Text)
{
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"{Time.TotalMilliseconds,8:0}ms  {Kind,-9} {(Node is { } n ? "n" + n : "-"),-4} {Text}");
}

public enum MessageFate
{
    Delivered,
    Dropped,
}

public readonly record struct MessageRecord(TimeSpan Time, MessageFate Fate, int From, int To, object Payload)
{
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"{Time.TotalMilliseconds,8:0}ms  {(Fate == MessageFate.Delivered ? "deliver" : "drop"),-9} {From}->{To} {SimTrace.Describe(Payload)}");
}

/// <summary>
/// The record of a run: every notable event, the last messages, and a running hash used to prove
/// that a seed always produces the same run.
/// </summary>
public sealed class SimTrace
{
    private const int RecentMessages = 40;
    private const ulong FnvOffset = 14695981039346656037UL;
    private const ulong FnvPrime = 1099511628211UL;

    private readonly Simulator _sim;
    private readonly List<TraceEvent> _events = new();
    private readonly MessageRecord[] _recent = new MessageRecord[RecentMessages];
    private int _recentNext;

    public SimTrace(Simulator sim)
    {
        _sim = sim ?? throw new ArgumentNullException(nameof(sim));
    }

    public IReadOnlyList<TraceEvent> Events => _events;

    public long MessagesDelivered { get; private set; }

    public long MessagesDropped { get; private set; }

    /// <summary>FNV-1a over every event and message endpoint, in order.</summary>
    public ulong Hash { get; private set; } = FnvOffset;

    public void Add(TraceKind kind, int? node, string text)
    {
        var item = new TraceEvent(_sim.Now, kind, node, text);
        _events.Add(item);
        Mix((ulong)_sim.Now.Ticks);
        Mix((ulong)kind);
        Mix((ulong)(node ?? -1));
        foreach (var ch in text)
        {
            Mix(ch);
        }
    }

    public void Message(MessageFate fate, int from, int to, object payload)
    {
        if (fate == MessageFate.Delivered)
        {
            MessagesDelivered++;
        }
        else
        {
            MessagesDropped++;
        }

        Mix((ulong)_sim.Now.Ticks);
        Mix((ulong)fate + 100);
        Mix((ulong)from);
        Mix((ulong)to);
        _recent[_recentNext % RecentMessages] = new MessageRecord(_sim.Now, fate, from, to, payload);
        _recentNext++;
    }

    /// <summary>The most recent messages, oldest first.</summary>
    public IEnumerable<MessageRecord> RecentMessageRecords()
    {
        var count = Math.Min(_recentNext, RecentMessages);
        for (var i = _recentNext - count; i < _recentNext; i++)
        {
            yield return _recent[i % RecentMessages];
        }
    }

    public static string Describe(object payload) => payload switch
    {
        RequestVote m => $"{(m.PreVote ? "PreVote" : "RequestVote")} t{m.Term} last={m.LastLogIndex}@{m.LastLogTerm}",
        RequestVoteResponse m => $"{(m.PreVote ? "PreVoteResp" : "VoteResp")} t{m.Term} granted={m.Granted}",
        AppendEntries m => $"AppendEntries t{m.Term} prev={m.PrevLogIndex}@{m.PrevLogTerm} n={m.Entries.Count} commit={m.LeaderCommit}",
        AppendEntriesResponse m => $"AppendResp t{m.Term} ok={m.Success} match={m.MatchIndex} conflict={m.ConflictIndex}@{m.ConflictTerm}",
        InstallSnapshot m => $"InstallSnapshot t{m.Term} upto={m.Snapshot.Metadata.LastIncludedIndex}@{m.Snapshot.Metadata.LastIncludedTerm}",
        InstallSnapshotResponse m => $"SnapshotResp t{m.Term} match={m.MatchIndex}",
        _ => payload.ToString() ?? payload.GetType().Name,
    };

    public string Format(Func<TraceEvent, bool>? filter = null, int maxEvents = int.MaxValue)
    {
        var builder = new StringBuilder();
        foreach (var item in _events.Where(e => filter?.Invoke(e) ?? true).TakeLast(maxEvents))
        {
            builder.Append(item).Append('\n');
        }

        return builder.ToString();
    }

    private void Mix(ulong value)
    {
        var hash = Hash;
        for (var i = 0; i < 8; i++)
        {
            hash ^= (value >> (i * 8)) & 0xFF;
            hash *= FnvPrime;
        }

        Hash = hash;
    }
}
