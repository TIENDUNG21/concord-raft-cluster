using Concord.Core.Messages;

namespace Concord.Core;

public sealed partial class RaftNode
{
    private readonly Dictionary<int, PeerProgress> _progress = new();
    private TimeSpan _lastLeaderContact;

    private void BroadcastHeartbeat()
    {
        var lastIndex = _storage.LastIndex;
        var lastTerm = LastLogTerm;
        foreach (var peer in _peers)
        {
            Send(new AppendEntries(Id, peer, _currentTerm, lastIndex, lastTerm, Array.Empty<LogEntry>(), _commitIndex, 0));
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
        Send(new AppendEntriesResponse(Id, append.From, _currentTerm, true, 0, 0, 0, append.Round));
    }

    private void HandleAppendEntriesResponse(AppendEntriesResponse response)
    {
        if (_role != RaftRole.Leader)
        {
            return;
        }

        _progress[response.From].RecentActive = true;
    }
}
