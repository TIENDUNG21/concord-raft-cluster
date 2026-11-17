using Concord.Core.Messages;

namespace Concord.Core;

public sealed partial class RaftNode
{
    private readonly Dictionary<int, PeerProgress> _progress = new();
    private TimeSpan _lastLeaderContact;

    /// <summary>
    /// Appends a command to the leader's log and starts replicating it. Returns NotLeader (with the
    /// known leader, if any) on other nodes.
    /// </summary>
    public ProposeResult Propose(ReadOnlyMemory<byte> command)
    {
        if (_role != RaftRole.Leader)
        {
            return ProposeResult.NotLeader(_leaderId);
        }

        var entry = LogEntry.CreateCommand(_storage.LastIndex + 1, _currentTerm, command);
        AppendLocal(entry);
        return new ProposeResult(true, entry.Index, entry.Term, Id);
    }

    private void AppendLocal(LogEntry entry)
    {
        _storage.Append(new[] { entry });
        foreach (var peer in _peers)
        {
            if (_progress[peer].State == ReplicationState.Replicate)
            {
                SendAppend(peer);
            }
        }
    }

    /// <summary>Heartbeat to every follower; also retransmits whatever is still unacknowledged.</summary>
    private void BroadcastHeartbeat()
    {
        foreach (var peer in _peers)
        {
            var progress = _progress[peer];
            if (progress.State == ReplicationState.Replicate)
            {
                progress.NextIndex = progress.MatchIndex + 1;
            }

            SendAppend(peer);
        }
    }

    private void SendAppend(int peer)
    {
        var progress = _progress[peer];
        var prevIndex = progress.NextIndex - 1;
        var prevTerm = _storage.TermAt(prevIndex)
            ?? throw new InvalidOperationException($"No term for index {prevIndex} while replicating to {peer}.");
        var entries = _storage.GetEntries(progress.NextIndex, _storage.LastIndex, _options.MaxEntriesPerMessage);
        Send(new AppendEntries(Id, peer, _currentTerm, prevIndex, prevTerm, entries, _commitIndex, 0));
        if (progress.State == ReplicationState.Replicate && entries.Count > 0)
        {
            progress.NextIndex = entries[^1].Index + 1;
        }
    }

    private void HandleAppendEntries(AppendEntries append)
    {
        // append.Term == _currentTerm: the sender is the leader of our term.
        if (_role != RaftRole.Follower || _leaderId != append.From)
        {
            BecomeFollower(append.Term, append.From);
        }

        _lastLeaderContact = _clock.Now;
        ResetElectionTimer();

        var prevIndex = append.PrevLogIndex;
        IReadOnlyList<LogEntry> entries = append.Entries;
        var prevTerm = _storage.TermAt(prevIndex);
        if (prevTerm is null)
        {
            // Our log is too short: ask the leader to continue right after our last entry.
            Reject(append, _storage.LastIndex + 1, 0);
            return;
        }

        if (prevTerm != append.PrevLogTerm)
        {
            // Point the leader at the first entry of the conflicting term so it can skip the whole term.
            var conflictTerm = prevTerm.Value;
            var first = prevIndex;
            while (first - 1 > 0 && _storage.TermAt(first - 1) == conflictTerm)
            {
                first--;
            }

            Reject(append, first, conflictTerm);
            return;
        }

        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            var existing = _storage.TermAt(entry.Index);
            if (existing == entry.Term)
            {
                // Already have it (duplicate or reordered message): never truncate on a match.
                continue;
            }

            if (existing is not null)
            {
                _storage.TruncateFrom(entry.Index);
            }

            _storage.Append(entries.Skip(i).ToArray());
            break;
        }

        var lastNew = prevIndex + entries.Count;
        Send(new AppendEntriesResponse(Id, append.From, _currentTerm, true, lastNew, 0, 0, append.Round));
    }

    private void Reject(AppendEntries append, long conflictIndex, long conflictTerm) =>
        Send(new AppendEntriesResponse(Id, append.From, _currentTerm, false, 0, conflictIndex, conflictTerm, append.Round));

    private void HandleAppendEntriesResponse(AppendEntriesResponse response)
    {
        if (_role != RaftRole.Leader)
        {
            return;
        }

        var progress = _progress[response.From];
        progress.RecentActive = true;

        if (response.Success)
        {
            progress.MatchIndex = Math.Max(progress.MatchIndex, response.MatchIndex);
            if (progress.State == ReplicationState.Probe)
            {
                progress.State = ReplicationState.Replicate;
                progress.NextIndex = progress.MatchIndex + 1;
            }
            else
            {
                progress.NextIndex = Math.Max(progress.NextIndex, progress.MatchIndex + 1);
            }

            if (progress.NextIndex <= _storage.LastIndex)
            {
                SendAppend(response.From);
            }
        }
        else if (response.ConflictIndex > 0)
        {
            var next = response.ConflictIndex;
            if (response.ConflictTerm > 0)
            {
                var lastOfTerm = LastIndexOfTerm(response.ConflictTerm);
                if (lastOfTerm > 0)
                {
                    next = lastOfTerm + 1;
                }
            }

            progress.State = ReplicationState.Probe;
            progress.NextIndex = Math.Clamp(next, progress.MatchIndex + 1, _storage.LastIndex + 1);
            SendAppend(response.From);
        }
    }

    /// <summary>The last index holding <paramref name="term"/> in our log, or 0 when we have none.</summary>
    private long LastIndexOfTerm(long term)
    {
        for (var index = _storage.LastIndex; index >= _storage.SnapshotIndex && index > 0; index--)
        {
            var current = _storage.TermAt(index);
            if (current == term)
            {
                return index;
            }

            if (current < term)
            {
                break;
            }
        }

        return 0;
    }
}
