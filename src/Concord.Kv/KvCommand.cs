using System.Text;

namespace Concord.Kv;

public enum KvOperation : byte
{
    Put = 1,
    Delete = 2,
    CompareAndSet = 3,
}

/// <summary>Size and encoding limits shared by the state machine, the server and the codecs.</summary>
public static class KvLimits
{
    public const int MaxKeyLength = 256;
    public const int MaxValueBytes = 64 * 1024;

    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>Returns why a key is invalid, or null if it is fine. Keys are compared ordinally (no Unicode normalization).</summary>
    public static string? ValidateKey(string? key) =>
        string.IsNullOrEmpty(key) ? "key must not be empty"
        : key.Length > MaxKeyLength ? $"key longer than {MaxKeyLength} characters"
        : Utf8Length(key) is null ? "key is not valid Unicode (lone surrogate)"
        : null;

    public static string? ValidateValue(string? value) =>
        value is null ? null
        : Utf8Length(value) is not { } length ? "value is not valid Unicode (lone surrogate)"
        : length > MaxValueBytes ? $"value larger than {MaxValueBytes} bytes"
        : null;

    private static int? Utf8Length(string text)
    {
        try
        {
            return StrictUtf8.GetByteCount(text);
        }
        catch (EncoderFallbackException)
        {
            return null;
        }
    }
}

/// <summary>
/// A write replicated through the Raft log. (<see cref="ClientId"/>, <see cref="Sequence"/>) identify
/// it for exactly-once execution: a retried command is answered from the client's session instead of
/// being applied twice. Client id 0 means "no session".
/// For <see cref="KvOperation.CompareAndSet"/>, a null <see cref="Expected"/> means "only if absent".
/// </summary>
public sealed record KvCommand(long ClientId, long Sequence, KvOperation Operation, string Key, string? Value, string? Expected)
{
    public static KvCommand Put(long clientId, long sequence, string key, string value) =>
        new(clientId, sequence, KvOperation.Put, key, value, null);

    public static KvCommand Delete(long clientId, long sequence, string key) =>
        new(clientId, sequence, KvOperation.Delete, key, null, null);

    public static KvCommand CompareAndSet(long clientId, long sequence, string key, string? expected, string value) =>
        new(clientId, sequence, KvOperation.CompareAndSet, key, value, expected);

    /// <summary>Returns why the command is invalid, or null if it can be proposed.</summary>
    public string? Validate()
    {
        if (KvLimits.ValidateKey(Key) is { } keyProblem)
        {
            return keyProblem;
        }

        if (Operation is KvOperation.Put or KvOperation.CompareAndSet && Value is null)
        {
            return $"{Operation} needs a value";
        }

        if (ClientId < 0 || Sequence < 0)
        {
            return "client id and sequence must not be negative";
        }

        return KvLimits.ValidateValue(Value) ?? KvLimits.ValidateValue(Expected);
    }
}

/// <summary>
/// Result of a command or read. Put: always succeeded. Delete: succeeded if the key existed.
/// Compare-and-set: succeeded if swapped; <see cref="Value"/> is the value after the operation.
/// Get: succeeded if found. <see cref="Stale"/> marks a retry older than the client's latest command.
/// </summary>
public sealed record KvResult(bool Succeeded, string? Value, bool Stale = false)
{
    public static readonly KvResult Ok = new(true, null);

    public static readonly KvResult StaleRetry = new(false, null, Stale: true);
}

public sealed class KvFormatException : FormatException
{
    public KvFormatException()
    {
    }

    public KvFormatException(string message)
        : base(message)
    {
    }

    public KvFormatException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
