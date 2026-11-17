namespace Concord.Kv;

/// <summary>
/// Binary encoding of <see cref="KvCommand"/> for log entries:
/// version(1) op(1) clientId(8) sequence(8) key value [expected], strings length-prefixed UTF-8.
/// </summary>
public static class KvCommandCodec
{
    private const byte Version = 1;

    public static byte[] Encode(KvCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        var builder = new BinaryBuilder();
        builder.WriteByte(Version);
        builder.WriteByte((byte)command.Operation);
        builder.WriteInt64(command.ClientId);
        builder.WriteInt64(command.Sequence);
        builder.WriteString(command.Key);
        switch (command.Operation)
        {
            case KvOperation.Put:
                builder.WriteString(command.Value);
                break;
            case KvOperation.CompareAndSet:
                builder.WriteString(command.Value);
                builder.WriteString(command.Expected);
                break;
        }

        return builder.ToArray();
    }

    public static KvCommand Decode(ReadOnlySpan<byte> data)
    {
        var cursor = new BinaryCursor(data);
        var version = cursor.ReadByte();
        if (version != Version)
        {
            throw new KvFormatException($"Unsupported command version {version}.");
        }

        var operation = (KvOperation)cursor.ReadByte();
        var clientId = cursor.ReadInt64();
        var sequence = cursor.ReadInt64();
        var key = cursor.ReadString(KvLimits.MaxKeyLength * 4) ?? throw new KvFormatException("Command key is null.");
        KvCommand command = operation switch
        {
            KvOperation.Put => KvCommand.Put(clientId, sequence, key, cursor.ReadString(KvLimits.MaxValueBytes) ?? throw new KvFormatException("Put value is null.")),
            KvOperation.Delete => KvCommand.Delete(clientId, sequence, key),
            KvOperation.CompareAndSet => ReadCompareAndSet(ref cursor, clientId, sequence, key),
            _ => throw new KvFormatException($"Unknown operation {(byte)operation}."),
        };
        cursor.ExpectEnd();
        if (command.Validate() is { } problem)
        {
            throw new KvFormatException($"Invalid command: {problem}.");
        }

        return command;
    }

    private static KvCommand ReadCompareAndSet(ref BinaryCursor cursor, long clientId, long sequence, string key)
    {
        var value = cursor.ReadString(KvLimits.MaxValueBytes) ?? throw new KvFormatException("Compare-and-set value is null.");
        var expected = cursor.ReadString(KvLimits.MaxValueBytes);
        return KvCommand.CompareAndSet(clientId, sequence, key, expected, value);
    }
}
