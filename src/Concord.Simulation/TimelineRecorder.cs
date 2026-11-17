using Concord.Core;

namespace Concord.Simulation;

public enum LaneState
{
    Follower,
    PreCandidate,
    Candidate,
    Leader,
    Down,
}

/// <summary>One stretch of time during which a node stayed in one state and term.</summary>
public readonly record struct LaneSegment(int Node, LaneState State, long Term, TimeSpan From, TimeSpan To);

/// <summary>
/// Records what each node was doing over virtual time (role, term, crashed or not) and how the
/// cluster's highest commit index grew. <see cref="TimelineSvg"/> draws it.
/// </summary>
public sealed class TimelineRecorder : IRaftObserver
{
    private readonly Func<TimeSpan> _now;
    private readonly Dictionary<int, (LaneState State, long Term, TimeSpan Since)> _open = new();
    private readonly List<LaneSegment> _segments = new();
    private readonly List<(TimeSpan Time, long Index)> _commits = new();
    private long _maxCommit;

    public TimelineRecorder(IEnumerable<int> nodes, Func<TimeSpan> now)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        _now = now ?? throw new ArgumentNullException(nameof(now));
        foreach (var node in nodes)
        {
            Nodes.Add(node);
            _open[node] = (LaneState.Follower, 0, TimeSpan.Zero);
        }
    }

    public List<int> Nodes { get; } = new();

    public IReadOnlyList<LaneSegment> Segments => _segments;

    /// <summary>(time, highest index committed anywhere) each time it grew.</summary>
    public IReadOnlyList<(TimeSpan Time, long Index)> Commits => _commits;

    public TimeSpan End { get; private set; }

    public void RoleChanged(int nodeId, RaftRole role, long term, int? leaderId)
    {
        var state = role switch
        {
            RaftRole.Leader => LaneState.Leader,
            RaftRole.Candidate => LaneState.Candidate,
            RaftRole.PreCandidate => LaneState.PreCandidate,
            _ => LaneState.Follower,
        };
        Switch(nodeId, state, term);
    }

    public void Committed(int nodeId, long commitIndex)
    {
        if (commitIndex > _maxCommit)
        {
            _maxCommit = commitIndex;
            _commits.Add((_now(), commitIndex));
        }
    }

    public void Crashed(int nodeId) => Switch(nodeId, LaneState.Down, _open[nodeId].Term);

    public void Restarted(int nodeId) => Switch(nodeId, LaneState.Follower, _open[nodeId].Term);

    /// <summary>Closes every open segment at the end of the run.</summary>
    public void Finish()
    {
        End = _now();
        foreach (var (node, open) in _open.OrderBy(o => o.Key))
        {
            _segments.Add(new LaneSegment(node, open.State, open.Term, open.Since, End));
        }

        _open.Clear();
    }

    private void Switch(int node, LaneState state, long term)
    {
        if (!_open.TryGetValue(node, out var open) || (open.State == state && open.Term == term))
        {
            return;
        }

        var now = _now();
        if (now > open.Since)
        {
            _segments.Add(new LaneSegment(node, open.State, open.Term, open.Since, now));
        }

        _open[node] = (state, term, now);
    }
}
