using Concord.Core.Messages;

namespace Concord.Core;

public sealed partial class RaftNode
{
    private readonly HashSet<int> _votes = new();
    private TimeSpan _electionDeadline;
    private TimeSpan _heartbeatDeadline;

    private void ResetElectionTimer()
    {
        var min = (int)_options.ElectionTimeoutMin.TotalMilliseconds;
        var max = (int)_options.ElectionTimeoutMax.TotalMilliseconds;
        _electionDeadline = _clock.Now + TimeSpan.FromMilliseconds(_random.NextInt(min, max));
    }

    private TimeSpan LeaderDeadline() => _heartbeatDeadline;

    private void TickLeader(TimeSpan now)
    {
        if (now >= _heartbeatDeadline)
        {
            BroadcastHeartbeat();
            _heartbeatDeadline = now + _options.HeartbeatInterval;
        }
    }

    private void StartElection() => BecomeCandidate();

    private void BecomeCandidate()
    {
        _currentTerm++;
        _votedFor = Id;
        PersistHardState();
        _role = RaftRole.Candidate;
        _leaderId = null;
        _votes.Clear();
        _votes.Add(Id);
        ResetElectionTimer();
        NotifyRole();
        NotifyVote(Id);

        if (_votes.Count >= _quorum)
        {
            BecomeLeader();
            return;
        }

        var lastIndex = _storage.LastIndex;
        var lastTerm = LastLogTerm;
        foreach (var peer in _peers)
        {
            Send(new RequestVote(Id, peer, _currentTerm, lastIndex, lastTerm, PreVote: false));
        }
    }

    private void BecomeLeader()
    {
        _role = RaftRole.Leader;
        _leaderId = Id;
        _votes.Clear();
        NotifyRole();
        BroadcastHeartbeat();
        _heartbeatDeadline = _clock.Now + _options.HeartbeatInterval;
    }

    private void HandleRequestVote(RequestVote request)
    {
        var upToDate = IsLogUpToDate(request.LastLogIndex, request.LastLogTerm);

        // After the term check in Step, request.Term == _currentTerm here.
        var canVote = _votedFor is null || _votedFor == request.From;
        var granted = canVote && upToDate;
        if (granted)
        {
            _votedFor = request.From;
            PersistHardState();
            ResetElectionTimer();
            NotifyVote(request.From);
        }

        Send(new RequestVoteResponse(Id, request.From, _currentTerm, granted, PreVote: false));
    }

    private void HandleVoteResponse(RequestVoteResponse response)
    {
        if (_role != RaftRole.Candidate || response.Term != _currentTerm || !response.Granted)
        {
            return;
        }

        _votes.Add(response.From);
        if (_votes.Count >= _quorum)
        {
            BecomeLeader();
        }
    }

    private static bool InLease() => false;

    private void NotifyVote(int candidate)
    {
        foreach (var observer in _observers)
        {
            observer.VoteGranted(Id, _currentTerm, candidate);
        }
    }
}
