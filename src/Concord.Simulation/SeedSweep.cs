using System.Diagnostics;
using Concord.Checker;

namespace Concord.Simulation;

/// <summary>Per-seed numbers kept for every run of a sweep (full traces are kept only for failures).</summary>
public sealed record SeedOutcome(ulong Seed, bool Passed, SimulationStats Stats, CheckOutcome? Linearizability);

public sealed record SweepSummary(
    ulong FirstSeed,
    int Seeds,
    IReadOnlyList<SeedOutcome> Outcomes,
    IReadOnlyList<SimulationResult> Failures,
    TimeSpan WallTime)
{
    public int Passed => Outcomes.Count(o => o.Passed);

    public long Operations => Outcomes.Sum(o => (long)o.Stats.Operations);

    public long OperationsGivenUp => Outcomes.Sum(o => (long)o.Stats.OperationsGivenUp);

    public long Elections => Outcomes.Sum(o => (long)o.Stats.Elections);

    public long Faults => Outcomes.Sum(o => (long)o.Stats.Faults);

    public long MessagesDelivered => Outcomes.Sum(o => o.Stats.MessagesDelivered);

    public long Events => Outcomes.Sum(o => o.Stats.Events);

    public int CheckerUnknown => Outcomes.Count(o => o.Linearizability == CheckOutcome.Unknown);

    public TimeSpan VirtualTime => TimeSpan.FromTicks(Outcomes.Sum(o => o.Stats.VirtualTime.Ticks));
}

/// <summary>
/// Runs many seeds. Each run is single-threaded and fully deterministic; seeds run in parallel only with
/// each other, and results are reported in seed order, so the summary is the same whatever the worker count.
/// </summary>
public static class SeedSweep
{
    public static SweepSummary Run(ulong firstSeed, int seeds, int workers, Func<ulong, SimulationConfig> configFor)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(seeds, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(workers, 1);
        ArgumentNullException.ThrowIfNull(configFor);
        var outcomes = new SeedOutcome[seeds];
        var failures = new SimulationResult?[seeds];
        var stopwatch = Stopwatch.StartNew();
        Parallel.For(0, seeds, new ParallelOptions { MaxDegreeOfParallelism = workers }, i =>
        {
            var seed = firstSeed + (ulong)i;
            var result = SimulationRunner.Run(configFor(seed));
            outcomes[i] = new SeedOutcome(seed, result.Passed, result.Stats, result.Linearizability?.Outcome);
            if (!result.Passed)
            {
                failures[i] = result;
            }
        });
        stopwatch.Stop();
        return new SweepSummary(firstSeed, seeds, outcomes, failures.OfType<SimulationResult>().ToArray(), stopwatch.Elapsed);
    }
}
