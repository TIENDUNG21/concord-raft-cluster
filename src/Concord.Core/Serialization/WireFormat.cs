using System.Buffers;
using System.Buffers.Binary;

namespace Concord.Core.Serialization;

/// <summary>Little-endian binary writer used by the message codec and the file storage.</summary>
public sealed class WireWriter
{
    private readonly ArrayBufferWriter<byte> _buffer = new();

    public int Length => _buffer.WrittenCount;

    public void WriteByte(byte value)
    {
        _buffer.GetSpan(1)[0] = value;
        _buffer.Advance(1);
    }

    public void WriteBool(bool value) => WriteByte(value ? (byte)1 : (byte)0);

    public void WriteInt32(int value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.GetSpan(4), value);
        _buffer.Advance(4);
    }

    public void WriteUInt32(uint value)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(_buffer.GetSpan(4), value);
        _buffer.Advance(4);
    }

    public void WriteInt64(long value)
    {
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.GetSpan(8), value);
        _buffer.Advance(8);
    }

    /// <summary>Length-prefixed bytes.</summary>
    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        WriteInt32(value.Length);
        WriteRaw(value);
    }

    public void WriteRaw(ReadOnlySpan<byte> value) => _buffer.Write(value);

    public void WriteEntry(LogEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        WriteInt64(entry.Index);
        WriteInt64(entry.Term);
        WriteByte((byte)entry.Kind);
        WriteBytes(entry.Data.Span);
    }

    public ReadOnlySpan<byte> WrittenSpan => _buffer.WrittenSpan;

    public byte[] ToArray() => _buffer.WrittenSpan.ToArray();
}

/// <summary>
/// Bounds-checked reader: malformed or hostile input raises <see cref="FormatException"/> with the offset,
/// never an out-of-range access or an attempt to allocate an absurd buffer.
/// </summary>
public ref struct WireReader
{
    private readonly ReadOnlySpan<byte> _data;

    public WireReader(ReadOnlySpan<byte> data)
    {
        _data = data;
        Offset = 0;
    }

    public int Offset { get; private set; }

    public readonly int Remaining => _data.Length - Offset;

    public byte ReadByte() => Take(1)[0];

    public bool ReadBool()
    {
        var value = ReadByte();
        return value switch
        {
            0 => false,
            1 => true,
            _ => throw new FormatException($"Invalid boolean {value} at offset {Offset - 1}."),
        };
    }

    public int ReadInt32() => BinaryPrimitives.ReadInt32LittleEndian(Take(4));

    public uint ReadUInt32() => BinaryPrimitives.ReadUInt32LittleEndian(Take(4));

    public long ReadInt64() => BinaryPrimitives.ReadInt64LittleEndian(Take(8));

    public byte[] ReadBytes(int maxLength)
    {
        var at = Offset;
        var length = ReadInt32();
        if (length < 0 || length > maxLength || length > Remaining)
        {
            throw new FormatException($"Invalid length {length} at offset {at} ({Remaining} bytes left, limit {maxLength}).");
        }

        return Take(length).ToArray();
    }

    public ReadOnlySpan<byte> ReadRaw(int count) => Take(count);

    public LogEntry ReadEntry(int maxData)
    {
        var at = Offset;
        var index = ReadInt64();
        var term = ReadInt64();
        var kind = ReadByte();
        if (kind > (byte)EntryKind.Command || index < 1 || term < 0)
        {
            throw new FormatException($"Invalid log entry header at offset {at}.");
        }

        return new LogEntry(index, term, (EntryKind)kind, ReadBytes(maxData));
    }

    public readonly void ExpectEnd()
    {
        if (Remaining != 0)
        {
            throw new FormatException($"{Remaining} unexpected trailing bytes at offset {Offset}.");
        }
    }

    private ReadOnlySpan<byte> Take(int count)
    {
        if (count > Remaining)
        {
            throw new FormatException($"Truncated input: needed {count} bytes at offset {Offset}, {Remaining} left.");
        }

        var slice = _data.Slice(Offset, count);
        Offset += count;
        return slice;
    }
}
