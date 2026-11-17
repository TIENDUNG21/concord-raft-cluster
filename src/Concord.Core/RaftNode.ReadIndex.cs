namespace Concord.Core;

/// <summary>Outcome of a ReadIndex request: on success, reads may be served from the local state machine.</summary>
public readonly record struct ReadResult(bool Success, long Index, int? LeaderHint);

public sealed partial class RaftNode
{
    private readonly List<PendingRead> _pendingReads = new();
    private long _round;

    /// <summary>
    /// Linearizable read without writing to the log (Raft thesis 6.4, ReadIndex). The leader
    /// 1) waits until it has committed an entry of its own term (its no-op), so its commit index is at
    ///    least that of any earlier leader,
    /// 2) records that commit index as the read index,
    /// 3) confirms it is still leader by a heartbeat round answered by a quorum, and
    /// 4) waits until the state machine has applied the read index.
    /// Then <paramref name="callback"/> runs and the caller reads local state. On any other node, or if
    /// leadership is lost first, the callback reports failure with the best leader hint.
    /// </summary>
    public void RequestRead(Action<ReadResult> callback)
    {
        ArgumentNullException.ThrowIfNull(callback);
        if (_role != RaftRole.Leader)
        {
            callback(new ReadResult(false, 0, _leaderId));
            return;
        }

        _round++;
        _pendingReads.Add(new PendingRead(callback, _round));
        if (_peers.Length > 0)
        {
            BroadcastHeartbeat();
            _heartbeatDeadline = _clock.Now + _options.HeartbeatInterval;
        }

        ServeReads();
    }

    public int PendingReads => _pendingReads.Count;

    private void ServeReads()
    {
        if (_pendingReads.Count == 0 || _role != RaftRole.Leader || _storage.TermAt(_commitIndex) != _currentTerm)
        {
            return;
        }

        List<(PendingRead Read, long Index)>? ready = null;
        for (var i = 0; i < _pendingReads.Count;)
        {
            var read = _pendingReads[i];
            read.ReadIndex ??= _commitIndex;
            if (!read.Confirmed)
            {
                var acks = 1;
                foreach (var peer in _peers)
                {
                    if (_progress[peer].AckedRound >= read.Round)
                    {
                        acks++;
                    }
                }

                read.Confirmed = acks >= _quorum;
            }

            if (read.Confirmed && _lastApplied >= read.ReadIndex)
            {
                (ready ??= new()).Add((read, read.ReadIndex.Value));
                _pendingReads.RemoveAt(i);
            }
            else
            {
                i++;
            }
        }

        // Callbacks run after the bookkeeping so they may safely call back into the node.
        if (ready is not null)
        {
            foreach (var (read, index) in ready)
            {
                read.Callback(new ReadResult(true, index, Id));
            }
        }
    }

    private void FailPendingReads()
    {
        if (_pendingReads.Count == 0)
        {
            return;
        }

        var failed = _pendingReads.ToArray();
        _pendingReads.Clear();
        foreach (var read in failed)
        {
            read.Callback(new ReadResult(false, 0, _leaderId));
        }
    }

    private sealed class PendingRead(Action<ReadResult> callback, long round)
    {
        public Action<ReadResult> Callback { get; } = callback;

        public long Round { get; } = round;

        public long? ReadIndex { get; set; }

        public bool Confirmed { get; set; }
    }
}
