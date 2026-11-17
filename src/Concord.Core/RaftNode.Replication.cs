using Concord.Core.Messages;

namespace Concord.Core;

public sealed partial class RaftNode
{
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

    private static void HandleAppendEntriesResponse(AppendEntriesResponse response)
    {
        _ = response;
    }
}
