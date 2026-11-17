using Concord.Core;

namespace Concord.Kv;

/// <summary>Called for every applied entry (no-ops included) with the command and its result.</summary>
public delegate void KvAppliedHandler(LogEntry entry, KvCommand? command, KvResult? result);

/// <summary>
/// The replicated key-value map plus one session per client (last sequence number and its result),
/// which turns at-least-once client retries into exactly-once execution (Raft thesis 6.3).
/// Deterministic: the same entries in the same order give byte-identical snapshots on every node.
/// </summary>
public sealed class KvStateMachine : IStateMachine
{
    private const byte SnapshotVersion = 1;
    private readonly Dictionary<string, string> _data = new(StringComparer.Ordinal);
    private readonly Dictionary<long, Session> _sessions = new();

    public KvAppliedHandler? Applied { get; set; }

    public int Count => _data.Count;

    public int Sessions => _sessions.Count;

    public string? Get(string key) => _data.GetValueOrDefault(key);

    public void Apply(LogEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        if (entry.Kind != EntryKind.Command)
        {
            Applied?.Invoke(entry, null, null);
            return;
        }

        KvCommand command;
        try
        {
            command = KvCommandCodec.Decode(entry.Data.Span);
        }
        catch (KvFormatException)
        {
            // Every replica rejects the same bytes the same way, so skipping keeps them identical.
            Applied?.Invoke(entry, null, new KvResult(false, null));
            return;
        }

        Applied?.Invoke(entry, command, Execute(command));
    }

    /// <summary>Runs a command against the map, honouring the client's session.</summary>
    public KvResult Execute(KvCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        if (command.ClientId != 0 && _sessions.TryGetValue(command.ClientId, out var session))
        {
            if (command.Sequence == session.Sequence)
            {
                return session.Result;
            }

            if (command.Sequence < session.Sequence)
            {
                return KvResult.StaleRetry;
            }
        }

        var result = command.Operation switch
        {
            KvCommandType.Put => Put(command.Key, command.Value!),
            KvCommandType.Delete => new KvResult(_data.Remove(command.Key), null),
            KvCommandType.CompareAndSet => CompareAndSet(command.Key, command.Expected, command.Value!),
            _ => throw new InvalidOperationException($"Unknown operation {command.Operation}."),
        };

        if (command.ClientId != 0)
        {
            _sessions[command.ClientId] = new Session(command.Sequence, result);
        }

        return result;
    }

    /// <summary>
    /// Snapshot layout: version, entry count, (key, value) sorted ordinally, session count,
    /// (client, sequence, succeeded, value) sorted by client.
    /// </summary>
    public ReadOnlyMemory<byte> TakeSnapshot()
    {
        var builder = new BinaryBuilder();
        builder.WriteByte(SnapshotVersion);
        builder.WriteInt32(_data.Count);
        foreach (var key in _data.Keys.Order(StringComparer.Ordinal))
        {
            builder.WriteString(key);
            builder.WriteString(_data[key]);
        }

        builder.WriteInt32(_sessions.Count);
        foreach (var client in _sessions.Keys.Order())
        {
            var session = _sessions[client];
            builder.WriteInt64(client);
            builder.WriteInt64(session.Sequence);
            builder.WriteByte(session.Result.Succeeded ? (byte)1 : (byte)0);
            builder.WriteString(session.Result.Value);
        }

        return builder.ToArray();
    }

    public void RestoreSnapshot(Snapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        var data = new Dictionary<string, string>(StringComparer.Ordinal);
        var sessions = new Dictionary<long, Session>();
        var cursor = new BinaryCursor(snapshot.Data.Span);
        if (!snapshot.Data.IsEmpty)
        {
            var version = cursor.ReadByte();
            if (version != SnapshotVersion)
            {
                throw new KvFormatException($"Unsupported snapshot version {version}.");
            }

            var count = ReadCount(ref cursor);
            for (var i = 0; i < count; i++)
            {
                var key = cursor.ReadString(KvLimits.MaxKeyLength * 4) ?? throw new KvFormatException("Null key in snapshot.");
                data[key] = cursor.ReadString(KvLimits.MaxValueBytes) ?? throw new KvFormatException("Null value in snapshot.");
            }

            var sessionCount = ReadCount(ref cursor);
            for (var i = 0; i < sessionCount; i++)
            {
                var client = cursor.ReadInt64();
                var sequence = cursor.ReadInt64();
                var succeeded = cursor.ReadByte() == 1;
                var value = cursor.ReadString(KvLimits.MaxValueBytes);
                sessions[client] = new Session(sequence, new KvResult(succeeded, value));
            }

            cursor.ExpectEnd();
        }

        _data.Clear();
        _sessions.Clear();
        foreach (var (key, value) in data)
        {
            _data[key] = value;
        }

        foreach (var (client, session) in sessions)
        {
            _sessions[client] = session;
        }
    }

    private static int ReadCount(ref BinaryCursor cursor)
    {
        var count = cursor.ReadInt32();
        return count < 0 ? throw new KvFormatException($"Negative count {count} in snapshot.") : count;
    }

    private KvResult Put(string key, string value)
    {
        _data[key] = value;
        return KvResult.Ok;
    }

    private KvResult CompareAndSet(string key, string? expected, string value)
    {
        var current = _data.GetValueOrDefault(key);
        if (!string.Equals(current, expected, StringComparison.Ordinal))
        {
            return new KvResult(false, current);
        }

        _data[key] = value;
        return new KvResult(true, value);
    }

    private sealed record Session(long Sequence, KvResult Result);
}
