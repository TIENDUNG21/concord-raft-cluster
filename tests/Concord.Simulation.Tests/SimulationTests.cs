using Concord.Core;
using Concord.Core.Storage;

namespace Concord.Simulation.Tests;

public sealed class SimulationTests
{
    private static SimulationConfig Config(ulong seed) => new() { Seed = seed };

    [Fact]
    public void SameSeed_ProducesIdenticalRun()
    {
        var first = SimulationRunner.Run(Config(5));
        var second = SimulationRunner.Run(Config(5));

        Assert.Equal(first.Trace.Hash, second.Trace.Hash);
        Assert.Equal(first.Stats, second.Stats);
        Assert.Equal(first.Trace.Events, second.Trace.Events);
    }

    [Fact]
    public void DifferentSeeds_ProduceDifferentRuns()
    {
        var hashes = Enumerable.Range(1, 5).Select(s => SimulationRunner.Run(Config((ulong)s)).Trace.Hash).ToHashSet();

        Assert.Equal(5, hashes.Count);
    }

    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    public void RandomPartitionsAndLoss_KeepSafetyAndRecover(int nodes)
    {
        for (ulong seed = 1; seed <= 25; seed++)
        {
            var result = SimulationRunner.Run(Config(seed) with { Nodes = nodes });

            Assert.True(result.Passed, SimulationRunner.FormatReport(result));
            Assert.True(result.Stats.Elections >= 1);
            Assert.True(result.Stats.CommittedEntries > 100);
        }
    }

    [Fact]
    public void QuietRun_ElectsOnceAndCommitsSteadily()
    {
        var result = SimulationRunner.Run(Config(9) with { Faults = FaultOptions.None });

        Assert.True(result.Passed, SimulationRunner.FormatReport(result));
        Assert.Equal(1, result.Stats.Elections);
        Assert.Equal(1, result.Stats.MaxTerm);
        Assert.Equal(0, result.Stats.MessagesDropped);
    }

    [Fact]
    public void FailureReport_ContainsSeedVerdictAndFaults()
    {
        var result = SimulationRunner.Run(Config(3));

        var report = SimulationRunner.FormatReport(result);

        Assert.StartsWith("seed 3: passed", report, StringComparison.Ordinal);
        Assert.Contains("Fault", report, StringComparison.Ordinal);
        Assert.Contains("last messages", report, StringComparison.Ordinal);
    }

    [Fact]
    public void Monitor_FlagsTwoLeadersInOneTerm()
    {
        var monitor = new InvariantMonitor(_ => new InMemoryRaftStorage(), () => TimeSpan.Zero);
        var observer = monitor;

        observer.RoleChanged(1, RaftRole.Leader, 4, 1);
        observer.RoleChanged(2, RaftRole.Leader, 5, 2);
        Assert.Null(monitor.Violation);

        observer.RoleChanged(3, RaftRole.Leader, 5, 3);

        Assert.Contains("Election Safety", monitor.Violation, StringComparison.Ordinal);
    }

    [Fact]
    public void Monitor_FlagsDoubleVoteAndDivergentApply()
    {
        var votes = new InvariantMonitor(_ => new InMemoryRaftStorage(), () => TimeSpan.Zero);
        var voteObserver = votes;
        voteObserver.VoteGranted(1, 3, 2);
        voteObserver.VoteGranted(1, 3, 2);
        Assert.Null(votes.Violation);
        voteObserver.VoteGranted(1, 3, 4);
        Assert.Contains("Double vote", votes.Violation, StringComparison.Ordinal);

        var applies = new InvariantMonitor(_ => new InMemoryRaftStorage(), () => TimeSpan.Zero);
        var applyObserver = applies;
        applyObserver.Applied(1, LogEntry.CreateCommand(1, 1, new byte[] { 1 }));
        applyObserver.Applied(2, LogEntry.CreateCommand(1, 1, new byte[] { 2 }));
        Assert.Contains("State Machine Safety", applies.Violation, StringComparison.Ordinal);
    }

    [Fact]
    public void Monitor_FlagsLeaderMissingCommittedEntry()
    {
        var storages = new Dictionary<int, InMemoryRaftStorage> { [1] = new(), [2] = new() };
        storages[1].Append(new[] { LogEntry.CreateCommand(1, 1, new byte[] { 7 }) });
        var monitor = new InvariantMonitor(id => storages[id], () => TimeSpan.Zero);
        var observer = monitor;
        observer.Applied(1, LogEntry.CreateCommand(1, 1, new byte[] { 7 }));

        observer.RoleChanged(2, RaftRole.Leader, 2, 2);

        Assert.Contains("Leader Completeness", monitor.Violation, StringComparison.Ordinal);
    }
}
