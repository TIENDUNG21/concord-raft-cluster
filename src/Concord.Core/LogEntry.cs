namespace Concord.Core;

/// <summary>What a log entry carries. No-op entries are appended by a new leader to commit its term.</summary>
public enum EntryKind : byte
{
    NoOp = 0,
    Command = 1,
}

/// <summary>
/// One entry of the replicated log. <see cref="Data"/> is an opaque command for the state machine;
/// the Raft core never interprets it. Equality compares the payload bytes, not the buffer identity.
/// </summary>
public sealed record LogEntry(long Index, long Term, EntryKind Kind, ReadOnlyMemory<byte> Data)
{
    public static LogEntry CreateNoOp(long index, long term) => new(index, term, EntryKind.NoOp, ReadOnlyMemory<byte>.Empty);

    public static LogEntry CreateCommand(long index, long term, ReadOnlyMemory<byte> data) => new(index, term, EntryKind.Command, data);

    public bool Equals(LogEntry? other) =>
        other is not null
        && Index == other.Index
        && Term == other.Term
        && Kind == other.Kind
        && Data.Span.SequenceEqual(other.Data.Span);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Index);
        hash.Add(Term);
        hash.Add(Kind);
        hash.AddBytes(Data.Span);
        return hash.ToHashCode();
    }

    public override string ToString() => $"{Index}@{Term}:{Kind}({Data.Length}B)";
}
