using System.Buffers.Binary;

namespace Concord.Core.Tests.Harness;

/// <summary>Records every applied command; its snapshot is the list of applied payloads.</summary>
internal sealed class RecordingStateMachine : IStateMachine
{
    public List<LogEntry> Applied { get; } = new();

    public List<byte[]> Commands { get; } = new();

    public int Restores { get; private set; }

    public void Apply(LogEntry entry)
    {
        Applied.Add(entry);
        if (entry.Kind == EntryKind.Command)
        {
            Commands.Add(entry.Data.ToArray());
        }
    }

    public ReadOnlyMemory<byte> TakeSnapshot()
    {
        using var stream = new MemoryStream();
        Span<byte> length = stackalloc byte[4];
        foreach (var command in Commands)
        {
            BinaryPrimitives.WriteInt32LittleEndian(length, command.Length);
            stream.Write(length);
            stream.Write(command);
        }

        return stream.ToArray();
    }

    public void RestoreSnapshot(Snapshot snapshot)
    {
        Restores++;
        Commands.Clear();
        var data = snapshot.Data.Span;
        while (!data.IsEmpty)
        {
            var length = BinaryPrimitives.ReadInt32LittleEndian(data);
            Commands.Add(data.Slice(4, length).ToArray());
            data = data[(4 + length)..];
        }
    }
}
