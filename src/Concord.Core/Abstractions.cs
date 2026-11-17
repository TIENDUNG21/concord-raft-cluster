using Concord.Core.Messages;

namespace Concord.Core;

/// <summary>Monotonic time source. The simulator supplies virtual time; the server a stopwatch.</summary>
public interface IClock
{
    TimeSpan Now { get; }
}

/// <summary>Randomness used for election timeouts. Seeded in the simulator so runs are reproducible.</summary>
public interface IRandomSource
{
    /// <summary>Returns an integer in [<paramref name="minInclusive"/>, <paramref name="maxExclusive"/>).</summary>
    int NextInt(int minInclusive, int maxExclusive);
}

/// <summary>
/// Outbound network. <see cref="Send"/> must not block and may lose, delay, duplicate or reorder
/// messages: the protocol tolerates all of these.
/// </summary>
public interface ITransport
{
    void Send(RaftMessage message);
}

/// <summary>The replicated state machine. Entries arrive in log order, exactly once per incarnation.</summary>
public interface IStateMachine
{
    void Apply(LogEntry entry);

    /// <summary>Serializes the state as of the last applied entry.</summary>
    ReadOnlyMemory<byte> TakeSnapshot();

    /// <summary>Replaces the whole state with the snapshot contents.</summary>
    void RestoreSnapshot(Snapshot snapshot);
}

/// <summary>
/// Hooks for tracing, invariant checking and metrics. All members have empty default bodies so an
/// observer implements only what it needs. Calls happen synchronously on the node's thread.
/// </summary>
public interface IRaftObserver
{
    void RoleChanged(int nodeId, RaftRole role, long term, int? leaderId)
    {
    }

    void VoteGranted(int nodeId, long term, int candidateId)
    {
    }

    void Committed(int nodeId, long commitIndex)
    {
    }

    void Applied(int nodeId, LogEntry entry)
    {
    }

    void SnapshotTaken(int nodeId, SnapshotMetadata metadata)
    {
    }

    void SnapshotInstalled(int nodeId, SnapshotMetadata metadata)
    {
    }
}
