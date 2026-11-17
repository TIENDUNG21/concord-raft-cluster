namespace Concord.Core;

/// <summary>The last log position a snapshot replaces.</summary>
public readonly record struct SnapshotMetadata(long LastIncludedIndex, long LastIncludedTerm);

/// <summary>A point-in-time image of the state machine plus the log position it covers.</summary>
public sealed record Snapshot(SnapshotMetadata Metadata, ReadOnlyMemory<byte> Data)
{
    public bool Equals(Snapshot? other) =>
        other is not null && Metadata == other.Metadata && Data.Span.SequenceEqual(other.Data.Span);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Metadata);
        hash.AddBytes(Data.Span);
        return hash.ToHashCode();
    }
}
