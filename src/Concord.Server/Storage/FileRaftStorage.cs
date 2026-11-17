using System.Buffers.Binary;
using Concord.Core;
using Concord.Core.Serialization;
using Concord.Core.Storage;

namespace Concord.Server.Storage;

/// <summary>
/// Durable Raft storage in one directory:
/// <list type="bullet">
/// <item><c>hardstate</c>: term and vote, replaced atomically (write temp, fsync, rename).</item>
/// <item><c>snapshot</c>: the latest snapshot, replaced atomically.</item>
/// <item><c>log</c>: a header naming the snapshot index it follows, then length + CRC-32 framed entries.
/// Appends and truncations are fsynced before returning.</item>
/// </list>
/// Installing a snapshot rewrites the log into <c>log.tmp</c> first; renaming the snapshot is the commit
/// point, and <see cref="Open"/> rolls a leftover <c>log.tmp</c> forward or back accordingly.
/// A torn final record (a crash mid-append) is cut off on open; a bad record anywhere else is treated as
/// corruption and refuses to start, because silently dropping acknowledged entries would break Raft.
/// The log is also kept in memory, which is fine for a store that compacts every few thousand entries.
/// </summary>
public sealed class FileRaftStorage : IRaftStorage, IDisposable
{
    private const uint HardStateMagic = 0x53484E43; // "CNHS"
    private const uint SnapshotMagic = 0x4E534E43; // "CNSN"
    private const uint LogMagic = 0x474C4E43; // "CNLG"
    private const byte FormatVersion = 1;
    private const int LogHeaderSize = 4 + 1 + 8;
    private const int RecordHeaderSize = 8;

    private readonly string _directory;
    private readonly InMemoryRaftStorage _memory = new();
    private readonly List<long> _offsets = new();
    private FileStream? _log;

    private FileRaftStorage(string directory)
    {
        _directory = directory;
    }

    /// <summary>Test hook: called at named points of multi-file updates, so tests can simulate a crash there.</summary>
    internal Action<string>? CrashPoint { get; set; }

    public long SnapshotIndex => _memory.SnapshotIndex;

    public long SnapshotTerm => _memory.SnapshotTerm;

    public long LastIndex => _memory.LastIndex;

    private FileStream Log => _log ?? throw new ObjectDisposedException(nameof(FileRaftStorage));

    private string LogPath => Path.Combine(_directory, "log");

    private string LogTempPath => Path.Combine(_directory, "log.tmp");

    private string HardStatePath => Path.Combine(_directory, "hardstate");

    private string SnapshotPath => Path.Combine(_directory, "snapshot");

    public static FileRaftStorage Open(string directory)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        Directory.CreateDirectory(directory);
        var storage = new FileRaftStorage(directory);
        try
        {
            storage.Load();
        }
        catch
        {
            storage.Dispose();
            throw;
        }

        return storage;
    }

    public HardState LoadHardState() => _memory.LoadHardState();

    public void SaveHardState(HardState state)
    {
        _memory.SaveHardState(state);
        var writer = new WireWriter();
        writer.WriteUInt32(HardStateMagic);
        writer.WriteByte(FormatVersion);
        writer.WriteInt64(state.Term);
        writer.WriteBool(state.VotedFor.HasValue);
        writer.WriteInt32(state.VotedFor ?? 0);
        WriteAtomically(HardStatePath, WithChecksum(writer));
    }

    public long? TermAt(long index) => _memory.TermAt(index);

    public IReadOnlyList<LogEntry> GetEntries(long fromIndex, long toInclusive, int maxCount) =>
        _memory.GetEntries(fromIndex, toInclusive, maxCount);

    public void Append(IReadOnlyList<LogEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        _memory.Append(entries);
        Log.Seek(0, SeekOrigin.End);
        foreach (var entry in entries)
        {
            _offsets.Add(Log.Position);
            Log.Write(Record(entry));
        }

        Log.Flush(flushToDisk: true);
    }

    public void TruncateFrom(long index)
    {
        _memory.TruncateFrom(index);
        var keep = (int)(index - _memory.SnapshotIndex - 1);
        if (keep >= _offsets.Count)
        {
            return;
        }

        Log.SetLength(_offsets[keep]);
        _offsets.RemoveRange(keep, _offsets.Count - keep);
        Log.Flush(flushToDisk: true);
    }

    public Snapshot? LoadSnapshot() => _memory.LoadSnapshot();

    public void InstallSnapshot(Snapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        _memory.InstallSnapshot(snapshot);
        var retained = _memory.GetEntries(_memory.SnapshotIndex + 1, _memory.LastIndex, int.MaxValue);

        // 1. New log next to the old one.
        Log.Dispose();
        WriteLogFile(LogTempPath, snapshot.Metadata.LastIncludedIndex, retained);
        CrashPoint?.Invoke("log-written");

        // 2. Commit point: the new snapshot replaces the old one.
        var writer = new WireWriter();
        writer.WriteUInt32(SnapshotMagic);
        writer.WriteByte(FormatVersion);
        writer.WriteInt64(snapshot.Metadata.LastIncludedIndex);
        writer.WriteInt64(snapshot.Metadata.LastIncludedTerm);
        writer.WriteBytes(snapshot.Data.Span);
        WriteAtomically(SnapshotPath, WithChecksum(writer));
        CrashPoint?.Invoke("snapshot-committed");

        // 3. Roll the log forward (Open does the same if we crash before this).
        File.Move(LogTempPath, LogPath, overwrite: true);
        OpenLog();
    }

    public void Dispose() => _log?.Dispose();

    private static byte[] Record(LogEntry entry)
    {
        var payload = new WireWriter();
        payload.WriteEntry(entry);
        var record = new byte[RecordHeaderSize + payload.Length];
        BinaryPrimitives.WriteInt32LittleEndian(record, payload.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(4), Crc32.Compute(payload.WrittenSpan));
        payload.WrittenSpan.CopyTo(record.AsSpan(RecordHeaderSize));
        return record;
    }

    private static byte[] WithChecksum(WireWriter writer)
    {
        writer.WriteUInt32(Crc32.Compute(writer.WrittenSpan));
        return writer.ToArray();
    }

    /// <summary>Validates the trailing CRC and returns the body without it.</summary>
    private static ReadOnlySpan<byte> VerifyChecksum(byte[] data, string path)
    {
        if (data.Length < 4 || Crc32.Compute(data.AsSpan(0, data.Length - 4)) != BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(data.Length - 4)))
        {
            throw new InvalidDataException($"{path} is corrupted (checksum mismatch).");
        }

        return data.AsSpan(0, data.Length - 4);
    }

    private static void WriteAtomically(string path, byte[] data)
    {
        var temp = path + ".tmp";
        using (var stream = new FileStream(temp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            stream.Write(data);
            stream.Flush(flushToDisk: true);
        }

        File.Move(temp, path, overwrite: true);
    }

    private static void WriteLogFile(string path, long baseIndex, IReadOnlyList<LogEntry> entries)
    {
        using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
        stream.Write(LogHeader(baseIndex));
        foreach (var entry in entries)
        {
            stream.Write(Record(entry));
        }

        stream.Flush(flushToDisk: true);
    }

    private static byte[] LogHeader(long baseIndex)
    {
        var header = new byte[LogHeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(header, LogMagic);
        header[4] = FormatVersion;
        BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(5), baseIndex);
        return header;
    }

    private static long? ReadLogBase(string path)
    {
        Span<byte> header = stackalloc byte[LogHeaderSize];
        using var stream = File.OpenRead(path);
        if (stream.Read(header) != LogHeaderSize
            || BinaryPrimitives.ReadUInt32LittleEndian(header) != LogMagic
            || header[4] != FormatVersion)
        {
            return null;
        }

        return BinaryPrimitives.ReadInt64LittleEndian(header[5..]);
    }

    private void Load()
    {
        if (File.Exists(SnapshotPath))
        {
            var reader = new WireReader(VerifyChecksum(File.ReadAllBytes(SnapshotPath), SnapshotPath));
            if (reader.ReadUInt32() != SnapshotMagic || reader.ReadByte() != FormatVersion)
            {
                throw new InvalidDataException($"{SnapshotPath} is not a Concord snapshot.");
            }

            var metadata = new SnapshotMetadata(reader.ReadInt64(), reader.ReadInt64());
            _memory.InstallSnapshot(new Snapshot(metadata, reader.ReadBytes(int.MaxValue)));
            reader.ExpectEnd();
        }

        if (File.Exists(HardStatePath))
        {
            var reader = new WireReader(VerifyChecksum(File.ReadAllBytes(HardStatePath), HardStatePath));
            if (reader.ReadUInt32() != HardStateMagic || reader.ReadByte() != FormatVersion)
            {
                throw new InvalidDataException($"{HardStatePath} is not a Concord hard state file.");
            }

            var term = reader.ReadInt64();
            var hasVote = reader.ReadBool();
            var vote = reader.ReadInt32();
            reader.ExpectEnd();
            _memory.SaveHardState(new HardState(term, hasVote ? vote : null));
        }

        if (File.Exists(LogTempPath))
        {
            // A snapshot install was interrupted. If its snapshot was committed, finish the job; otherwise undo it.
            if (ReadLogBase(LogTempPath) == _memory.SnapshotIndex)
            {
                File.Move(LogTempPath, LogPath, overwrite: true);
            }
            else
            {
                File.Delete(LogTempPath);
            }
        }

        if (!File.Exists(LogPath))
        {
            WriteLogFile(LogPath, _memory.SnapshotIndex, Array.Empty<LogEntry>());
        }

        var logBase = ReadLogBase(LogPath)
            ?? throw new InvalidDataException($"{LogPath} has an invalid header.");
        if (logBase != _memory.SnapshotIndex)
        {
            throw new InvalidDataException($"{LogPath} follows index {logBase}, but the snapshot ends at {_memory.SnapshotIndex}.");
        }

        OpenLog();
    }

    private void OpenLog()
    {
        _log = new FileStream(LogPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
        _offsets.Clear();
        var length = Log.Length;
        var position = (long)LogHeaderSize;
        var entries = new List<LogEntry>();
        var header = new byte[RecordHeaderSize];
        while (position < length)
        {
            Log.Position = position;
            if (length - position < RecordHeaderSize)
            {
                break;
            }

            Log.ReadExactly(header);
            var size = BinaryPrimitives.ReadInt32LittleEndian(header);
            var end = position + RecordHeaderSize + size;
            if (size <= 0 || end > length)
            {
                break;
            }

            var payload = new byte[size];
            Log.ReadExactly(payload);
            if (Crc32.Compute(payload) != BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(4)))
            {
                if (end < length)
                {
                    throw new InvalidDataException($"{LogPath} has a corrupted record at offset {position} followed by more data.");
                }

                break;
            }

            var reader = new WireReader(payload);
            var entry = reader.ReadEntry(int.MaxValue);
            reader.ExpectEnd();
            var expected = _memory.SnapshotIndex + entries.Count + 1;
            if (entry.Index != expected)
            {
                throw new InvalidDataException($"{LogPath} holds entry {entry.Index} where {expected} was expected.");
            }

            _offsets.Add(position);
            entries.Add(entry);
            position = end;
        }

        if (position < length)
        {
            // Torn final record from a crash during append: it was never acknowledged, drop it.
            Log.SetLength(position);
            Log.Flush(flushToDisk: true);
        }

        _memory.TruncateFrom(_memory.SnapshotIndex + 1);
        _memory.Append(entries);
    }
}
