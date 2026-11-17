namespace Concord.Core.Tests;

public sealed class RaftOptionsTests
{
    [Fact]
    public void Defaults_AreValid()
    {
        new RaftOptions().Validate();
    }

    [Theory]
    [InlineData(50, 300, 300)]
    [InlineData(50, 300, 200)]
    [InlineData(50, 50, 300)]
    [InlineData(0, 150, 300)]
    public void Validate_RejectsInconsistentTimeouts(int heartbeatMs, int minMs, int maxMs)
    {
        var options = new RaftOptions
        {
            HeartbeatInterval = TimeSpan.FromMilliseconds(heartbeatMs),
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(minMs),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(maxMs),
        };

        Assert.Throws<ArgumentException>(options.Validate);
    }

    [Fact]
    public void Validate_RejectsZeroBatchSize()
    {
        Assert.Throws<ArgumentException>(new RaftOptions { MaxEntriesPerMessage = 0 }.Validate);
    }

    [Fact]
    public void LogEntry_EqualityComparesPayloadBytes()
    {
        var a = LogEntry.CreateCommand(3, 2, new byte[] { 1, 2, 3 });
        var b = LogEntry.CreateCommand(3, 2, new byte[] { 1, 2, 3 });
        var c = LogEntry.CreateCommand(3, 2, new byte[] { 1, 2, 4 });

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, c);
    }
}
