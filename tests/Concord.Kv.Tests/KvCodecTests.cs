namespace Concord.Kv.Tests;

public sealed class KvCodecTests
{
    public static TheoryData<KvCommand> Commands => new()
    {
        KvCommand.Put(7, 1, "a", "1"),
        KvCommand.Put(0, 0, "empty-value", string.Empty),
        KvCommand.Delete(7, 2, "a"),
        KvCommand.CompareAndSet(9, 3, "lock", null, "owner-9"),
        KvCommand.CompareAndSet(9, 4, "lock", "owner-9", "owner-10"),
        KvCommand.Put(1, 5, "thành-phố/Hà Nội", "phở 🍜 \u0000 nul inside"),
    };

    [Theory]
    [MemberData(nameof(Commands))]
    public void Encode_ThenDecode_RoundTrips(KvCommand command)
    {
        var decoded = KvCommandCodec.Decode(KvCommandCodec.Encode(command));

        Assert.Equal(command, decoded);
    }

    [Fact]
    public void NfcAndNfdKeys_StayDistinctBytes()
    {
        // "Việt" precomposed (NFC) and decomposed (NFD): keys are ordinal byte strings, never normalized.
        const string nfc = "Vi\u1EC7t";
        const string nfd = "Vie\u0323\u0302t";

        var a = KvCommandCodec.Decode(KvCommandCodec.Encode(KvCommand.Put(1, 1, nfc, "x")));
        var b = KvCommandCodec.Decode(KvCommandCodec.Encode(KvCommand.Put(1, 1, nfd, "x")));

        Assert.Equal(nfc, a.Key);
        Assert.Equal(nfd, b.Key);
        Assert.NotEqual(a.Key, b.Key);
    }

    [Fact]
    public void EveryTruncation_IsRejectedWithFormatError()
    {
        var bytes = KvCommandCodec.Encode(KvCommand.CompareAndSet(3, 4, "key", "old", "new"));

        for (var length = 0; length < bytes.Length; length++)
        {
            var prefix = bytes[..length];
            Assert.Throws<KvFormatException>(() => KvCommandCodec.Decode(prefix));
        }
    }

    [Fact]
    public void TrailingBytes_AreRejected()
    {
        var bytes = KvCommandCodec.Encode(KvCommand.Delete(1, 1, "k")).Append((byte)0).ToArray();

        var error = Assert.Throws<KvFormatException>(() => KvCommandCodec.Decode(bytes));
        Assert.Contains("trailing", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(2, 1)]
    [InlineData(1, 99)]
    public void UnknownVersionOrOperation_IsRejected(byte version, byte operation)
    {
        var bytes = KvCommandCodec.Encode(KvCommand.Delete(1, 1, "k"));
        bytes[0] = version;
        bytes[1] = operation;

        Assert.Throws<KvFormatException>(() => KvCommandCodec.Decode(bytes));
    }

    [Fact]
    public void InvalidUtf8AndHostileLengths_AreRejected()
    {
        var bytes = KvCommandCodec.Encode(KvCommand.Delete(1, 1, "ab"));
        var keyStart = bytes.Length - 2;

        var badUtf8 = (byte[])bytes.Clone();
        badUtf8[keyStart] = 0xC3;
        badUtf8[keyStart + 1] = 0x28;
        Assert.Throws<KvFormatException>(() => KvCommandCodec.Decode(badUtf8));

        var hugeLength = (byte[])bytes.Clone();
        BitConverter.GetBytes(int.MaxValue).CopyTo(hugeLength, keyStart - 4);
        Assert.Throws<KvFormatException>(() => KvCommandCodec.Decode(hugeLength));

        var negative = (byte[])bytes.Clone();
        BitConverter.GetBytes(-5).CopyTo(negative, keyStart - 4);
        Assert.Throws<KvFormatException>(() => KvCommandCodec.Decode(negative));
    }

    [Fact]
    public void Validate_RejectsEmptyLongAndBrokenKeysAndOversizedValues()
    {
        Assert.NotNull(KvCommand.Put(1, 1, string.Empty, "v").Validate());
        Assert.NotNull(KvCommand.Put(1, 1, new string('k', KvLimits.MaxKeyLength + 1), "v").Validate());
        Assert.NotNull(KvCommand.Put(1, 1, "lone\uD800surrogate", "v").Validate());
        Assert.NotNull(KvCommand.Put(1, 1, "k", new string('v', KvLimits.MaxValueBytes + 1)).Validate());
        Assert.Null(KvCommand.Put(1, 1, new string('k', KvLimits.MaxKeyLength), new string('v', KvLimits.MaxValueBytes)).Validate());
    }
}
