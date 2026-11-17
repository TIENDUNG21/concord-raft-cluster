using Concord.Checker;

namespace Concord.Simulation.Tests;

/// <summary>
/// A harness that never fails proves nothing. These runs switch on known bugs and require the
/// simulator to catch each one within a bounded number of seeds (deterministic, so this never flakes).
/// </summary>
public sealed class HarnessDetectsBugsTests
{
    private static SimulationResult? FirstFailure(InjectedBugs bugs, int maxSeeds)
    {
        for (ulong seed = 1; seed <= (ulong)maxSeeds; seed++)
        {
            var result = SimulationRunner.Run(new SimulationConfig { Seed = seed, Bugs = bugs });
            if (!result.Passed)
            {
                return result;
            }
        }

        return null;
    }

    [Fact]
    public void CorrectImplementation_PassesKvRunsWithLinearizableHistories()
    {
        for (ulong seed = 1; seed <= 15; seed++)
        {
            var result = SimulationRunner.Run(new SimulationConfig { Seed = seed });

            Assert.True(result.Passed, SimulationRunner.FormatReport(result));
            Assert.Equal(CheckOutcome.Ok, result.Linearizability?.Outcome);
            Assert.True(result.Stats.Operations > 500);
        }
    }

    [Fact]
    public void StaleReadsFromDeposedLeader_AreCaughtByTheLinearizabilityChecker()
    {
        var failure = FirstFailure(new InjectedBugs { UnsafeLocalReads = true }, maxSeeds: 60);

        Assert.NotNull(failure);
        Assert.Equal(FailureKind.Linearizability, failure.Failure);
        Assert.Contains("not linearizable", failure.Message, StringComparison.Ordinal);
        Assert.Contains("get(", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void VotesIgnoringLogs_AreCaughtByTheInvariantMonitor()
    {
        var failure = FirstFailure(new InjectedBugs { IgnoreLogInVotes = true }, maxSeeds: 60);

        Assert.NotNull(failure);
        Assert.Equal(FailureKind.Invariant, failure.Failure);
        Assert.Matches("Leader Completeness|State Machine Safety|overwrite committed", failure.Message!);
    }

    [Fact]
    public void FailureReport_NamesTheSeedAndHowToReplayIt()
    {
        var failure = FirstFailure(new InjectedBugs { IgnoreLogInVotes = true }, maxSeeds: 60)!;

        var report = SimulationRunner.FormatReport(failure);

        Assert.StartsWith($"seed {failure.Seed}: FAILED (Invariant)", report, StringComparison.Ordinal);
        Assert.Contains($"concord-sim run --seed {failure.Seed}", report, StringComparison.Ordinal);
        Assert.Contains("Violation", report, StringComparison.Ordinal);
    }
}
