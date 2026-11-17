using System.Globalization;
using Concord.Checker;
using Concord.Simulation;

namespace Concord.Sim;

internal static class Program
{
    private const string Usage = """
        concord-sim: deterministic simulation of a Concord Raft cluster

        usage:
          concord-sim sweep [--seeds N] [--start S] [--workers W] [--nodes 3|5|7] [--seconds T] [--bug NAME] [--expect-failures]
          concord-sim run --seed S [--nodes 3|5|7] [--seconds T] [--bug NAME] [--trace]

        Every run is fully determined by its seed. A failing seed prints a compact report; replay it
        with "run --seed S --trace" to see every event.

        bugs (deliberately broken behaviour, to prove the harness catches it):
          unsafe-reads       leaders answer reads locally, without ReadIndex
          ignore-log-votes   votes are granted without comparing logs

        exit codes: 0 all seeds passed (or, with --expect-failures, at least one failed), 1 otherwise, 2 usage error
        """;

    private static readonly HashSet<string> Flags = new(StringComparer.Ordinal) { "trace", "expect-failures" };

    public static int Main(string[] args) => Execute(args, Console.Out);

    internal static int Execute(IReadOnlyList<string> args, TextWriter output)
    {
        try
        {
            var command = CommandLine.Parse(args, Flags);
            return command.Command switch
            {
                "sweep" => Sweep(command, output),
                "run" => RunOne(command, output),
                "help" => Help(output),
                _ => throw new UsageException($"unknown command '{command.Command}'"),
            };
        }
        catch (UsageException ex)
        {
            output.WriteLine("error: " + ex.Message);
            output.WriteLine();
            output.WriteLine(Usage);
            return 2;
        }
    }

    internal static SimulationConfig BuildConfig(CommandLine command, ulong seed)
    {
        var nodes = (int)command.Number("nodes", 5, 3, 7);
        if (nodes % 2 == 0)
        {
            throw new UsageException("--nodes must be odd (3, 5 or 7)");
        }

        var seconds = command.Number("seconds", 12, 5, 600);
        var bugs = command.String("bug") switch
        {
            null => InjectedBugs.None,
            "unsafe-reads" => new InjectedBugs { UnsafeLocalReads = true },
            "ignore-log-votes" => new InjectedBugs { IgnoreLogInVotes = true },
            var other => throw new UsageException($"unknown bug '{other}'"),
        };
        return new SimulationConfig
        {
            Seed = seed,
            Nodes = nodes,
            Duration = TimeSpan.FromSeconds(seconds),
            FaultWindow = TimeSpan.FromSeconds(seconds - 4),
            Bugs = bugs,
        };
    }

    private static int Help(TextWriter output)
    {
        output.WriteLine(Usage);
        return 0;
    }

    private static int Sweep(CommandLine command, TextWriter output)
    {
        command.AllowOnly("seeds", "start", "workers", "nodes", "seconds", "bug", "expect-failures");
        var seeds = (int)command.Number("seeds", 200, 1, 1_000_000);
        var start = (ulong)command.Number("start", 1, 0, long.MaxValue / 2);
        var workers = (int)command.Number("workers", Math.Min(4, Environment.ProcessorCount), 1, 64);
        var sample = BuildConfig(command, start);

        output.WriteLine(Invariant($"Concord simulation sweep: seeds {start}..{start + (ulong)seeds - 1}, {sample.Nodes} nodes, {sample.Duration.TotalSeconds:0} s virtual time each, {workers} workers"));
        if (sample.Bugs.Any)
        {
            output.WriteLine("injected bug: " + command.String("bug"));
        }

        var summary = SeedSweep.Run(start, seeds, workers, seed => BuildConfig(command, seed));
        WriteSummary(summary, output);
        foreach (var failure in summary.Failures.Take(3))
        {
            output.WriteLine();
            output.Write(SimulationRunner.FormatReport(failure));
        }

        if (summary.Failures.Count > 3)
        {
            output.WriteLine();
            output.WriteLine("more failing seeds: " + string.Join(", ", summary.Failures.Skip(3).Select(f => f.Seed)));
        }

        var anyFailed = summary.Failures.Count > 0;
        return command.Has("expect-failures") ? (anyFailed ? 0 : 1) : (anyFailed ? 1 : 0);
    }

    private static void WriteSummary(SweepSummary summary, TextWriter output)
    {
        output.WriteLine(Invariant($"passed: {summary.Passed} / {summary.Seeds}"));
        output.WriteLine(Invariant($"client operations: {summary.Operations:N0} (given up after retries: {summary.OperationsGivenUp:N0})"));
        output.WriteLine(Invariant($"elections: {summary.Elections:N0}, faults injected: {summary.Faults:N0}, messages delivered: {summary.MessagesDelivered:N0}"));
        output.WriteLine(Invariant($"linearizability: {summary.Outcomes.Count(o => o.Linearizability == CheckOutcome.Ok):N0} histories proven linearizable, {summary.CheckerUnknown} inconclusive"));
        output.WriteLine(Invariant($"virtual time simulated: {summary.VirtualTime.TotalHours:0.0} h in {summary.WallTime.TotalSeconds:0.0} s of wall time"));
    }

    private static int RunOne(CommandLine command, TextWriter output)
    {
        command.AllowOnly("seed", "nodes", "seconds", "bug", "trace");
        if (command.String("seed") is null)
        {
            throw new UsageException("run needs --seed");
        }

        var seed = (ulong)command.Number("seed", 1, 0, long.MaxValue / 2);
        var result = SimulationRunner.Run(BuildConfig(command, seed));
        var stats = result.Stats;
        output.WriteLine(Invariant($"seed {seed}: {(result.Passed ? "passed" : "FAILED")}"));
        output.WriteLine(Invariant($"events {stats.Events:N0}, messages delivered {stats.MessagesDelivered:N0}, dropped {stats.MessagesDropped:N0}"));
        output.WriteLine(Invariant($"elections {stats.Elections}, highest term {stats.MaxTerm}, entries committed {stats.CommittedEntries:N0}, faults {stats.Faults}"));
        output.WriteLine(Invariant($"client operations {stats.Operations:N0}, given up {stats.OperationsGivenUp}, linearizability {result.Linearizability?.Outcome.ToString() ?? "not checked"}"));
        output.WriteLine(Invariant($"trace hash {result.Trace.Hash:x16}"));
        output.WriteLine();
        if (command.Has("trace"))
        {
            output.Write(result.Trace.Format());
        }
        else
        {
            output.Write(SimulationRunner.FormatReport(result));
        }

        return result.Passed ? 0 : 1;
    }

    private static string Invariant(FormattableString text) => text.ToString(CultureInfo.InvariantCulture);
}
