using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Concord.Kv;

/// <summary>Little-endian writer for the KV wire and snapshot formats.</summary>
internal sealed class BinaryBuilder
{
    private readonly ArrayBufferWriter<byte> _buffer = new();

    public void WriteByte(byte value)
    {
        _buffer.GetSpan(1)[0] = value;
        _buffer.Advance(1);
    }

    public void WriteInt64(long value)
    {
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.GetSpan(8), value);
        _buffer.Advance(8);
    }

    public void WriteInt32(int value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.GetSpan(4), value);
        _buffer.Advance(4);
    }

    /// <summary>Length-prefixed UTF-8; length -1 encodes null.</summary>
    public void WriteString(string? value)
    {
        if (value is null)
        {
            WriteInt32(-1);
            return;
        }

        var length = Encoding.UTF8.GetByteCount(value);
        WriteInt32(length);
        Encoding.UTF8.GetBytes(value, _buffer.GetSpan(length));
        _buffer.Advance(length);
    }

    public byte[] ToArray() => _buffer.WrittenSpan.ToArray();
}

/// <summary>
/// Bounds-checked reader. Every malformed input ends in a <see cref="KvFormatException"/> naming the
/// offset, never in an IndexOutOfRange or a huge allocation.
/// </summary>
internal ref struct BinaryCursor
{
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
    private readonly ReadOnlySpan<byte> _data;
    private int _offset;

    public BinaryCursor(ReadOnlySpan<byte> data)
    {
        _data = data;
        _offset = 0;
    }

    public readonly bool AtEnd => _offset == _data.Length;

    public byte ReadByte() => Take(1)[0];

    public long ReadInt64() => BinaryPrimitives.ReadInt64LittleEndian(Take(8));

    public int ReadInt32() => BinaryPrimitives.ReadInt32LittleEndian(Take(4));

    public string? ReadString(int maxBytes)
    {
        var start = _offset;
        var length = ReadInt32();
        if (length == -1)
        {
            return null;
        }

        if (length < 0 || length > maxBytes)
        {
            throw new KvFormatException($"Invalid string length {length} at offset {start}.");
        }

        var bytes = Take(length);
        try
        {
            return StrictUtf8.GetString(bytes);
        }
        catch (DecoderFallbackException ex)
        {
            throw new KvFormatException($"Invalid UTF-8 in string at offset {start}.", ex);
        }
    }

    public readonly void ExpectEnd()
    {
        if (!AtEnd)
        {
            throw new KvFormatException($"{_data.Length - _offset} unexpected trailing bytes at offset {_offset}.");
        }
    }

    private ReadOnlySpan<byte> Take(int count)
    {
        if (count > _data.Length - _offset)
        {
            throw new KvFormatException($"Truncated input: needed {count} bytes at offset {_offset}, {_data.Length - _offset} left.");
        }

        var slice = _data.Slice(_offset, count);
        _offset += count;
        return slice;
    }
}
