using Concord.Core.Messages;

namespace Concord.Core.Serialization;

/// <summary>
/// Binary wire format for batches of Raft messages (used by the HTTP transport).
/// Batch: "CR" magic, version byte, int32 count, then messages: type byte, from, to, term, fields.
/// </summary>
public static class RaftMessageCodec
{
    public const int MaxEntryBytes = 1 << 20;
    public const int MaxSnapshotBytes = 64 << 20;
    public const int MaxBatch = 4096;

    private const byte Version = 1;
    private const byte MagicC = (byte)'C';
    private const byte MagicR = (byte)'R';

    private enum MessageType : byte
    {
        RequestVote = 1,
        RequestVoteResponse = 2,
        AppendEntries = 3,
        AppendEntriesResponse = 4,
        InstallSnapshot = 5,
        InstallSnapshotResponse = 6,
    }

    public static byte[] Encode(IReadOnlyCollection<RaftMessage> messages)
    {
        ArgumentNullException.ThrowIfNull(messages);
        var writer = new WireWriter();
        writer.WriteByte(MagicC);
        writer.WriteByte(MagicR);
        writer.WriteByte(Version);
        writer.WriteInt32(messages.Count);
        foreach (var message in messages)
        {
            Write(writer, message);
        }

        return writer.ToArray();
    }

    public static IReadOnlyList<RaftMessage> Decode(ReadOnlySpan<byte> data)
    {
        var reader = new WireReader(data);
        if (reader.ReadByte() != MagicC || reader.ReadByte() != MagicR)
        {
            throw new FormatException("Not a Concord message batch (bad magic).");
        }

        var version = reader.ReadByte();
        if (version != Version)
        {
            throw new FormatException($"Unsupported message batch version {version}.");
        }

        var count = reader.ReadInt32();
        if (count < 0 || count > MaxBatch)
        {
            throw new FormatException($"Invalid message count {count}.");
        }

        var messages = new List<RaftMessage>(count);
        for (var i = 0; i < count; i++)
        {
            messages.Add(Read(ref reader));
        }

        reader.ExpectEnd();
        return messages;
    }

    private static void Write(WireWriter w, RaftMessage message)
    {
        switch (message)
        {
            case RequestVote m:
                Header(w, MessageType.RequestVote, m);
                w.WriteInt64(m.LastLogIndex);
                w.WriteInt64(m.LastLogTerm);
                w.WriteBool(m.PreVote);
                break;
            case RequestVoteResponse m:
                Header(w, MessageType.RequestVoteResponse, m);
                w.WriteBool(m.Granted);
                w.WriteBool(m.PreVote);
                break;
            case AppendEntries m:
                Header(w, MessageType.AppendEntries, m);
                w.WriteInt64(m.PrevLogIndex);
                w.WriteInt64(m.PrevLogTerm);
                w.WriteInt64(m.LeaderCommit);
                w.WriteInt64(m.Round);
                w.WriteInt32(m.Entries.Count);
                foreach (var entry in m.Entries)
                {
                    w.WriteEntry(entry);
                }

                break;
            case AppendEntriesResponse m:
                Header(w, MessageType.AppendEntriesResponse, m);
                w.WriteBool(m.Success);
                w.WriteInt64(m.MatchIndex);
                w.WriteInt64(m.ConflictIndex);
                w.WriteInt64(m.ConflictTerm);
                w.WriteInt64(m.Round);
                break;
            case InstallSnapshot m:
                Header(w, MessageType.InstallSnapshot, m);
                w.WriteInt64(m.Snapshot.Metadata.LastIncludedIndex);
                w.WriteInt64(m.Snapshot.Metadata.LastIncludedTerm);
                w.WriteInt64(m.Round);
                w.WriteBytes(m.Snapshot.Data.Span);
                break;
            case InstallSnapshotResponse m:
                Header(w, MessageType.InstallSnapshotResponse, m);
                w.WriteInt64(m.MatchIndex);
                w.WriteInt64(m.Round);
                break;
            default:
                throw new ArgumentException($"Unknown message type {message.GetType().Name}.", nameof(message));
        }
    }

    private static void Header(WireWriter w, MessageType type, RaftMessage message)
    {
        w.WriteByte((byte)type);
        w.WriteInt32(message.From);
        w.WriteInt32(message.To);
        w.WriteInt64(message.Term);
    }

    private static RaftMessage Read(ref WireReader r)
    {
        var at = r.Offset;
        var type = (MessageType)r.ReadByte();
        var from = r.ReadInt32();
        var to = r.ReadInt32();
        var term = r.ReadInt64();
        if (term < 0)
        {
            throw new FormatException($"Negative term at offset {at}.");
        }

        switch (type)
        {
            case MessageType.RequestVote:
                return new RequestVote(from, to, term, r.ReadInt64(), r.ReadInt64(), r.ReadBool());
            case MessageType.RequestVoteResponse:
                return new RequestVoteResponse(from, to, term, r.ReadBool(), r.ReadBool());
            case MessageType.AppendEntries:
                var prevIndex = r.ReadInt64();
                var prevTerm = r.ReadInt64();
                var commit = r.ReadInt64();
                var round = r.ReadInt64();
                var count = r.ReadInt32();
                if (count < 0 || count > r.Remaining / 21)
                {
                    throw new FormatException($"Invalid entry count {count} at offset {at}.");
                }

                var entries = new LogEntry[count];
                for (var i = 0; i < count; i++)
                {
                    entries[i] = r.ReadEntry(MaxEntryBytes);
                }

                return new AppendEntries(from, to, term, prevIndex, prevTerm, entries, commit, round);
            case MessageType.AppendEntriesResponse:
                return new AppendEntriesResponse(from, to, term, r.ReadBool(), r.ReadInt64(), r.ReadInt64(), r.ReadInt64(), r.ReadInt64());
            case MessageType.InstallSnapshot:
                var metadata = new SnapshotMetadata(r.ReadInt64(), r.ReadInt64());
                var snapshotRound = r.ReadInt64();
                return new InstallSnapshot(from, to, term, new Snapshot(metadata, r.ReadBytes(MaxSnapshotBytes)), snapshotRound);
            case MessageType.InstallSnapshotResponse:
                return new InstallSnapshotResponse(from, to, term, r.ReadInt64(), r.ReadInt64());
            default:
                throw new FormatException($"Unknown message type {(byte)type} at offset {at}.");
        }
    }
}
