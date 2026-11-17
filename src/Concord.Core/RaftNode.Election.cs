using Concord.Core.Messages;

namespace Concord.Core;

public sealed partial class RaftNode
{
    private readonly HashSet<int> _votes = new();
    private readonly HashSet<int> _answered = new();
    private TimeSpan _electionDeadline;
    private TimeSpan _voteRetryDeadline;
    private TimeSpan _heartbeatDeadline;
    private TimeSpan _checkQuorumDeadline;

    private void ResetElectionTimer()
    {
        var min = (int)_options.ElectionTimeoutMin.TotalMilliseconds;
        var max = (int)_options.ElectionTimeoutMax.TotalMilliseconds;
        _electionDeadline = _clock.Now + TimeSpan.FromMilliseconds(_random.NextInt(min, max));
    }

    private bool IsCampaigning => _role is RaftRole.PreCandidate or RaftRole.Candidate;

    private TimeSpan FollowerDeadline() =>
        IsCampaigning && _voteRetryDeadline < _electionDeadline ? _voteRetryDeadline : _electionDeadline;

    /// <summary>
    /// Vote requests lost on the way would otherwise cost a whole election timeout; resend them every
    /// heartbeat interval to the peers that have not answered this campaign.
    /// </summary>
    private void RetransmitVoteRequests()
    {
        SendVoteRequests(_role == RaftRole.PreCandidate, onlyUnanswered: true);
    }

    private void SendVoteRequests(bool preVote, bool onlyUnanswered)
    {
        var term = preVote ? _currentTerm + 1 : _currentTerm;
        var lastIndex = _storage.LastIndex;
        var lastTerm = LastLogTerm;
        foreach (var peer in _peers)
        {
            if (!onlyUnanswered || !_answered.Contains(peer))
            {
                Send(new RequestVote(Id, peer, term, lastIndex, lastTerm, preVote));
            }
        }

        _voteRetryDeadline = _clock.Now + _options.HeartbeatInterval;
    }

    private TimeSpan LeaderDeadline() =>
        _options.CheckQuorum && _checkQuorumDeadline < _heartbeatDeadline ? _checkQuorumDeadline : _heartbeatDeadline;

    private void TickLeader(TimeSpan now)
    {
        if (_options.CheckQuorum && now >= _checkQuorumDeadline)
        {
            // Step down if a quorum has not answered during the last election timeout: we may be on the
            // minority side of a partition, and clients should go find the real leader.
            var active = 1 + _peers.Count(p => _progress[p].RecentActive);
            if (active < _quorum)
            {
                BecomeFollower(_currentTerm, null);
                return;
            }

            foreach (var peer in _peers)
            {
                _progress[peer].RecentActive = false;
            }

            _checkQuorumDeadline = now + _options.ElectionTimeoutMax;
        }

        if (now >= _heartbeatDeadline)
        {
            BroadcastHeartbeat();
            _heartbeatDeadline = now + _options.HeartbeatInterval;
        }
    }

    private void StartElection()
    {
        if (_options.PreVote)
        {
            BecomePreCandidate();
        }
        else
        {
            BecomeCandidate();
        }
    }

    private void BecomePreCandidate()
    {
        _role = RaftRole.PreCandidate;
        _leaderId = null;
        _votes.Clear();
        _votes.Add(Id);
        _answered.Clear();
        ResetElectionTimer();
        NotifyRole();

        if (_votes.Count >= _quorum)
        {
            BecomeCandidate();
            return;
        }

        SendVoteRequests(preVote: true, onlyUnanswered: false);
    }

    private void BecomeCandidate()
    {
        _currentTerm++;
        _votedFor = Id;
        PersistHardState();
        _role = RaftRole.Candidate;
        _leaderId = null;
        _votes.Clear();
        _votes.Add(Id);
        _answered.Clear();
        ResetElectionTimer();
        NotifyRole();
        NotifyVote(Id);

        if (_votes.Count >= _quorum)
        {
            BecomeLeader();
            return;
        }

        SendVoteRequests(preVote: false, onlyUnanswered: false);
    }

    private void BecomeLeader()
    {
        _role = RaftRole.Leader;
        _leaderId = Id;
        _votes.Clear();
        _progress.Clear();
        foreach (var peer in _peers)
        {
            _progress[peer] = new PeerProgress(_storage.LastIndex + 1);
        }

        NotifyRole();

        // A leader may only commit entries of its own term by counting replicas (Raft paper 5.4.2).
        // Appending a no-op lets it commit everything earlier terms left behind, and is what makes
        // ReadIndex safe right after an election.
        AppendLocal(LogEntry.CreateNoOp(_storage.LastIndex + 1, _currentTerm));
        BroadcastHeartbeat();
        _heartbeatDeadline = _clock.Now + _options.HeartbeatInterval;
        _checkQuorumDeadline = _clock.Now + _options.ElectionTimeoutMax;
    }

    private void HandleRequestVote(RequestVote request)
    {
        var upToDate = IsLogUpToDate(request.LastLogIndex, request.LastLogTerm);
        if (request.PreVote)
        {
            // Would we vote for this node if it started an election now? Our own term and vote stay put.
            var wouldGrant = request.Term > _currentTerm && upToDate && !InLease();
            Send(new RequestVoteResponse(Id, request.From, wouldGrant ? request.Term : _currentTerm, wouldGrant, PreVote: true));
            return;
        }

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
        if (response.PreVote)
        {
            if (_role == RaftRole.PreCandidate && response.Term == _currentTerm + 1 && response.Granted)
            {
                _answered.Add(response.From);
                _votes.Add(response.From);
                if (_votes.Count >= _quorum)
                {
                    BecomeCandidate();
                }
            }

            return;
        }

        if (_role != RaftRole.Candidate || response.Term != _currentTerm)
        {
            return;
        }

        _answered.Add(response.From);
        if (!response.Granted)
        {
            return;
        }

        _votes.Add(response.From);
        if (_votes.Count >= _quorum)
        {
            BecomeLeader();
        }
    }

    /// <summary>
    /// True while we have reason to believe a leader is alive: we are the leader, or we heard from one
    /// less than the minimum election timeout ago. Votes are refused during the lease.
    /// </summary>
    private bool InLease() =>
        _options.CheckQuorum
        && (_role == RaftRole.Leader
            || (_leaderId is not null && _clock.Now - _lastLeaderContact < _options.ElectionTimeoutMin));

    private void NotifyVote(int candidate)
    {
        foreach (var observer in _observers)
        {
            observer.VoteGranted(Id, _currentTerm, candidate);
        }
    }
}
