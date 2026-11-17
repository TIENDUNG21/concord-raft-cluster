using Concord.Core.Storage;

namespace Concord.Core.Tests.Harness;

/// <summary>One node whose outgoing messages are captured, for message-level protocol tests.</summary>
internal sealed class SingleNode
{
    public SingleNode(int id = 1, int[]? members = null, InMemoryRaftStorage? storage = null, RaftOptions? options = null)
    {
        Storage = storage ?? new InMemoryRaftStorage();
        Node = new RaftNode(
            id,
            members ?? new[] { 1, 2, 3 },
            options ?? new RaftOptions(),
            Storage,
            Machine,
            Transport,
            Clock,
            new SeededRandom(id));
    }

    public RaftNode Node { get; }

    public CapturingTransport Transport { get; } = new();

    public InMemoryRaftStorage Storage { get; }

    public ManualClock Clock { get; } = new();

    public RecordingStateMachine Machine { get; } = new();

    /// <summary>Moves time past the election deadline and ticks once.</summary>
    public void TimeOut()
    {
        Clock.Now = Node.NextDeadline;
        Node.Tick();
    }

    public static InMemoryRaftStorage StorageWithTerms(params long[] terms)
    {
        var storage = new InMemoryRaftStorage();
        storage.Append(terms.Select((term, i) => LogEntry.CreateCommand(i + 1, term, new[] { (byte)(i + 1) })).ToArray());
        if (terms.Length > 0)
        {
            storage.SaveHardState(new HardState(terms[^1], null));
        }

        return storage;
    }
}
