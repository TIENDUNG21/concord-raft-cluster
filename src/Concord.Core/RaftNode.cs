using Concord.Core.Messages;
using Concord.Core.Storage;

namespace Concord.Core;

/// <summary>
/// One Raft server as a single-threaded, sans-IO state machine. The host drives it with
/// <see cref="Step"/> (an incoming message), <see cref="Tick"/> (time passed; call it at or after
/// <see cref="NextDeadline"/>) and client calls; the node talks back only through the interfaces it
/// was given. Calls must never overlap. Because nothing inside reads the wall clock or a global RNG,
/// a seeded host replays exactly the same run.
/// </summary>
public sealed partial class RaftNode
{
    private readonly RaftOptions _options;
    private readonly IRaftStorage _storage;
    private readonly IStateMachine _stateMachine;
    private readonly ITransport _transport;
    private readonly IClock _clock;
    private readonly IRandomSource _random;
    private readonly List<IRaftObserver> _observers = new();
    private readonly int[] _members;
    private readonly int[] _peers;
    private readonly int _quorum;

    private long _currentTerm;
    private int? _votedFor;
    private RaftRole _role = RaftRole.Follower;
    private int? _leaderId;
    private long _commitIndex;
    private long _lastApplied;
    private (RaftRole Role, long Term, int? Leader) _lastNotified = (RaftRole.Follower, -1, null);

    public RaftNode(
        int id,
        IEnumerable<int> members,
        RaftOptions options,
        IRaftStorage storage,
        IStateMachine stateMachine,
        ITransport transport,
        IClock clock,
        IRandomSource random)
    {
        ArgumentNullException.ThrowIfNull(members);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        _members = members.Distinct().Order().ToArray();
        if (!_members.Contains(id))
        {
            throw new ArgumentException($"Node {id} is not in the member list.", nameof(members));
        }

        Id = id;
        _peers = _members.Where(m => m != id).ToArray();
        _quorum = (_members.Length / 2) + 1;
        _options = options;
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _stateMachine = stateMachine ?? throw new ArgumentNullException(nameof(stateMachine));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        _random = random ?? throw new ArgumentNullException(nameof(random));

        var hardState = _storage.LoadHardState();
        _currentTerm = hardState.Term;
        _votedFor = hardState.VotedFor;

        // Everything in the snapshot was committed; the log after it is re-applied once committed again.
        var snapshot = _storage.LoadSnapshot();
        if (snapshot is not null)
        {
            _stateMachine.RestoreSnapshot(snapshot);
            _commitIndex = snapshot.Metadata.LastIncludedIndex;
            _lastApplied = snapshot.Metadata.LastIncludedIndex;
        }

        ResetElectionTimer();
    }

    public int Id { get; }

    public RaftRole Role => _role;

    public long CurrentTerm => _currentTerm;

    public int? LeaderId => _leaderId;

    public int? VotedFor => _votedFor;

    public long CommitIndex => _commitIndex;

    public long LastApplied => _lastApplied;

    public long LastLogIndex => _storage.LastIndex;

    public long LastLogTerm => _storage.TermAt(_storage.LastIndex) ?? 0;

    public IReadOnlyList<int> Members => _members;

    /// <summary>The earliest time at which <see cref="Tick"/> has work to do.</summary>
    public TimeSpan NextDeadline => _role == RaftRole.Leader ? LeaderDeadline() : _electionDeadline;

    public void AddObserver(IRaftObserver observer)
    {
        ArgumentNullException.ThrowIfNull(observer);
        _observers.Add(observer);
    }

    /// <summary>Fires the timers that are due.</summary>
    public void Tick()
    {
        var now = _clock.Now;
        if (_role == RaftRole.Leader)
        {
            TickLeader(now);
        }
        else if (now >= _electionDeadline)
        {
            StartElection();
        }
    }

    /// <summary>Handles one incoming message.</summary>
    public void Step(RaftMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (message.To != Id || message.From == Id || !_members.Contains(message.From))
        {
            return;
        }

        if (message.Term > _currentTerm)
        {
            switch (message)
            {
                case RequestVote { PreVote: true }:
                    // Pre-votes never change our term.
                    break;
                case RequestVoteResponse { PreVote: true, Granted: true }:
                    // A granted pre-vote echoes the term we would campaign in.
                    break;
                case RequestVote when InLease():
                    // We heard from a live leader recently: this candidate is disruptive, ignore it.
                    return;
                case AppendEntries or InstallSnapshot:
                    BecomeFollower(message.Term, message.From);
                    break;
                default:
                    BecomeFollower(message.Term, null);
                    break;
            }
        }
        else if (message.Term < _currentTerm)
        {
            RejectStale(message);
            return;
        }

        switch (message)
        {
            case RequestVote request:
                HandleRequestVote(request);
                break;
            case RequestVoteResponse response:
                HandleVoteResponse(response);
                break;
            case AppendEntries append:
                HandleAppendEntries(append);
                break;
            case AppendEntriesResponse response:
                HandleAppendEntriesResponse(response);
                break;
            case InstallSnapshot install:
                HandleInstallSnapshot(install);
                break;
            case InstallSnapshotResponse response:
                HandleInstallSnapshotResponse(response);
                break;
        }
    }

    private void RejectStale(RaftMessage message)
    {
        switch (message)
        {
            case AppendEntries append:
                // Tell the deposed leader about the newer term so it steps down.
                Send(new AppendEntriesResponse(Id, append.From, _currentTerm, false, 0, 0, 0, append.Round));
                break;
            case InstallSnapshot install:
                Send(new InstallSnapshotResponse(Id, install.From, _currentTerm, 0, install.Round));
                break;
            case RequestVote request:
                Send(new RequestVoteResponse(Id, request.From, _currentTerm, false, request.PreVote));
                break;
        }
    }

    private void BecomeFollower(long term, int? leaderId)
    {
        if (term > _currentTerm)
        {
            _currentTerm = term;
            _votedFor = null;
            PersistHardState();
        }

        _role = RaftRole.Follower;
        _leaderId = leaderId;
        _votes.Clear();
        _progress.Clear();
        ResetElectionTimer();
        NotifyRole();
        FailPendingReads();
    }

    private void PersistHardState() => _storage.SaveHardState(new HardState(_currentTerm, _votedFor));

    private void Send(RaftMessage message) => _transport.Send(message);

    private bool IsLogUpToDate(long lastLogIndex, long lastLogTerm)
    {
        var ourTerm = LastLogTerm;
        return lastLogTerm > ourTerm || (lastLogTerm == ourTerm && lastLogIndex >= _storage.LastIndex);
    }

    private void NotifyRole()
    {
        var current = (_role, _currentTerm, _leaderId);
        if (current == _lastNotified)
        {
            return;
        }

        _lastNotified = current;
        foreach (var observer in _observers)
        {
            observer.RoleChanged(Id, _role, _currentTerm, _leaderId);
        }
    }
}
