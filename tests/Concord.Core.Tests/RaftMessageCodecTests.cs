using Concord.Core.Messages;
using Concord.Core.Serialization;

namespace Concord.Core.Tests;

public sealed class RaftMessageCodecTests
{
    private static readonly RaftMessage[] AllKinds =
    {
        new RequestVote(1, 2, 7, 40, 6, PreVote: true),
        new RequestVoteResponse(2, 1, 7, Granted: true, PreVote: false),
        new AppendEntries(1, 3, 7, 39, 6, new[] { LogEntry.CreateNoOp(40, 7), LogEntry.CreateCommand(41, 7, new byte[] { 1, 2, 3 }) }, 38, 12),
        new AppendEntriesResponse(3, 1, 7, false, 0, 22, 5, 12),
        new InstallSnapshot(1, 2, 7, new Snapshot(new SnapshotMetadata(100, 6), new byte[] { 9, 8, 7 }), 13),
        new InstallSnapshotResponse(2, 1, 7, 100, 13),
    };

    private static void AssertSameMessage(RaftMessage expected, RaftMessage actual)
    {
        Assert.Equal(expected.GetType(), actual.GetType());
        if (expected is AppendEntries e && actual is AppendEntries a)
        {
            Assert.Equal(e with { Entries = Array.Empty<LogEntry>() }, a with { Entries = Array.Empty<LogEntry>() });
            Assert.Equal(e.Entries, a.Entries);
        }
        else
        {
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public void EveryMessageType_RoundTrips()
    {
        var decoded = RaftMessageCodec.Decode(RaftMessageCodec.Encode(AllKinds));

        Assert.Equal(AllKinds.Length, decoded.Count);
        for (var i = 0; i < AllKinds.Length; i++)
        {
            AssertSameMessage(AllKinds[i], decoded[i]);
        }
    }

    [Fact]
    public void EmptyBatch_RoundTrips()
    {
        Assert.Empty(RaftMessageCodec.Decode(RaftMessageCodec.Encode(Array.Empty<RaftMessage>())));
    }

    [Fact]
    public void EveryTruncation_IsAFormatError()
    {
        var bytes = RaftMessageCodec.Encode(AllKinds);

        for (var length = 0; length < bytes.Length; length++)
        {
            var prefix = bytes[..length];
            Assert.Throws<FormatException>(() => RaftMessageCodec.Decode(prefix));
        }
    }

    [Fact]
    public void BadMagicVersionCountTypeOrTrailingBytes_AreRejected()
    {
        var good = RaftMessageCodec.Encode(new RaftMessage[] { new RequestVoteResponse(2, 1, 7, true, false) });

        byte[] With(int offset, byte value)
        {
            var copy = (byte[])good.Clone();
            copy[offset] = value;
            return copy;
        }

        Assert.Throws<FormatException>(() => RaftMessageCodec.Decode(With(0, (byte)'X')));
        Assert.Throws<FormatException>(() => RaftMessageCodec.Decode(With(2, 9)));
        Assert.Throws<FormatException>(() => RaftMessageCodec.Decode(With(6, 0x7F)));
        Assert.Throws<FormatException>(() => RaftMessageCodec.Decode(With(7, 99)));
        Assert.Throws<FormatException>(() => RaftMessageCodec.Decode(With(good.Length - 1, 2)));
        Assert.Throws<FormatException>(() => RaftMessageCodec.Decode(good.Append((byte)0).ToArray()));
    }

    [Fact]
    public void HostileEntryCount_IsRejectedWithoutAllocating()
    {
        var bytes = RaftMessageCodec.Encode(new RaftMessage[] { new AppendEntries(1, 2, 3, 0, 0, Array.Empty<LogEntry>(), 0, 0) });
        BitConverter.GetBytes(int.MaxValue).CopyTo(bytes, bytes.Length - 4);

        var error = Assert.Throws<FormatException>(() => RaftMessageCodec.Decode(bytes));
        Assert.Contains("entry count", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Status_ReportsLeaderProgressPerPeer()
    {
        var single = new Harness.SingleNode(1, options: new RaftOptions { PreVote = false });
        Assert.Empty(single.Node.Status().Peers);
        single.TimeOut();
        single.Node.Step(new RequestVoteResponse(2, 1, 1, true, false));
        single.Node.Step(new AppendEntriesResponse(2, 1, 1, true, 1, 0, 0, 0));

        var status = single.Node.Status();

        Assert.Equal(RaftRole.Leader, status.Role);
        Assert.Equal(new[] { 1, 2, 3 }, status.Members);
        Assert.Equal(new[] { 2, 3 }, status.Peers.Select(p => p.Id));
        Assert.Equal(ReplicationState.Replicate, status.Peers[0].State);
        Assert.Equal(1, status.Peers[0].MatchIndex);
        Assert.Equal(ReplicationState.Probe, status.Peers[1].State);
    }
}
