using Concord.Core.Messages;

namespace Concord.Core;

public sealed partial class RaftNode
{
    public long SnapshotIndex => _storage.SnapshotIndex;

    /// <summary>
    /// Log compaction (Raft paper, section 7): once enough entries were applied since the last snapshot,
    /// serialize the state machine and let storage drop the log prefix it replaces.
    /// </summary>
    private void MaybeTakeSnapshot()
    {
        if (_options.SnapshotThreshold == 0 || _lastApplied - _storage.SnapshotIndex < _options.SnapshotThreshold)
        {
            return;
        }

        var term = _storage.TermAt(_lastApplied)
            ?? throw new InvalidOperationException($"No term for applied index {_lastApplied}.");
        var metadata = new SnapshotMetadata(_lastApplied, term);
        _storage.InstallSnapshot(new Snapshot(metadata, _stateMachine.TakeSnapshot()));
        foreach (var observer in _observers)
        {
            observer.SnapshotTaken(Id, metadata);
        }
    }

    private void SendSnapshot(int peer)
    {
        var snapshot = _storage.LoadSnapshot()
            ?? throw new InvalidOperationException("Asked to send a snapshot, but none exists.");
        var progress = _progress[peer];
        progress.State = ReplicationState.Snapshot;
        progress.SnapshotSentAt = _clock.Now;
        Send(new InstallSnapshot(Id, peer, _currentTerm, snapshot, 0));
    }

    private void HandleInstallSnapshot(InstallSnapshot install)
    {
        if (_role != RaftRole.Follower || _leaderId != install.From)
        {
            BecomeFollower(install.Term, install.From);
        }

        _lastLeaderContact = _clock.Now;
        ResetElectionTimer();

        var metadata = install.Snapshot.Metadata;
        if (metadata.LastIncludedIndex > _commitIndex)
        {
            // Storage keeps any log suffix that matches the snapshot; the state machine is replaced.
            _storage.InstallSnapshot(install.Snapshot);
            _stateMachine.RestoreSnapshot(install.Snapshot);
            _commitIndex = metadata.LastIncludedIndex;
            _lastApplied = metadata.LastIncludedIndex;
            foreach (var observer in _observers)
            {
                observer.SnapshotInstalled(Id, metadata);
            }
        }

        Send(new InstallSnapshotResponse(Id, install.From, _currentTerm, metadata.LastIncludedIndex, install.Round));
    }

    private void HandleInstallSnapshotResponse(InstallSnapshotResponse response)
    {
        if (_role != RaftRole.Leader)
        {
            return;
        }

        var progress = _progress[response.From];
        progress.RecentActive = true;
        progress.MatchIndex = Math.Max(progress.MatchIndex, response.MatchIndex);
        progress.State = ReplicationState.Replicate;
        progress.NextIndex = progress.MatchIndex + 1;
        MaybeAdvanceCommit();
        if (_role == RaftRole.Leader && progress.NextIndex <= _storage.LastIndex)
        {
            SendAppend(response.From);
        }
    }
}
