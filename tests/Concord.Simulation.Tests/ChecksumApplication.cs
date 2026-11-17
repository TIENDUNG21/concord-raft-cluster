using System.Buffers.Binary;
using Concord.Core;

namespace Concord.Simulation.Tests;

/// <summary>
/// The simplest replicated state machine: a count and a rolling hash of applied commands. Used for
/// Raft-level simulations where only log agreement matters, not KV semantics.
/// </summary>
internal sealed class ChecksumApplication : INodeApplication, IStateMachine
{
    public long Count { get; private set; }

    public ulong Checksum { get; private set; } = 14695981039346656037UL;

    public IStateMachine StateMachine => this;

    public void Attach(RaftNode node)
    {
    }

    public void Receive(int from, object payload, Action<int, object> reply)
    {
    }

    public void Apply(LogEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        if (entry.Kind != EntryKind.Command)
        {
            return;
        }

        Count++;
        var hash = Checksum;
        foreach (var b in entry.Data.Span)
        {
            hash = (hash ^ b) * 1099511628211UL;
        }

        Checksum = hash;
    }

    public ReadOnlyMemory<byte> TakeSnapshot()
    {
        var data = new byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(data, Count);
        BinaryPrimitives.WriteUInt64LittleEndian(data.AsSpan(8), Checksum);
        return data;
    }

    public void RestoreSnapshot(Snapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        var data = snapshot.Data.Span;
        if (data.Length != 16)
        {
            throw new FormatException($"Checksum snapshot must be 16 bytes, got {data.Length}.");
        }

        Count = BinaryPrimitives.ReadInt64LittleEndian(data);
        Checksum = BinaryPrimitives.ReadUInt64LittleEndian(data[8..]);
    }
}
