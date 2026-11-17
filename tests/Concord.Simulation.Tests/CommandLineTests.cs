using Concord.Sim;

namespace Concord.Simulation.Tests;

public sealed class CommandLineTests
{
    private static (int Exit, string Output) Run(params string[] args)
    {
        using var output = new StringWriter();
        var exit = Program.Execute(args, output);
        return (exit, output.ToString());
    }

    [Theory]
    [InlineData("explode")]
    [InlineData("sweep", "--seeds")]
    [InlineData("sweep", "--seeds", "ten")]
    [InlineData("sweep", "--seeds", "0")]
    [InlineData("sweep", "--nodes", "4")]
    [InlineData("sweep", "--bug", "cosmic-rays")]
    [InlineData("sweep", "--colour", "blue")]
    [InlineData("sweep", "--seeds", "5", "--seeds", "6")]
    [InlineData("run")]
    [InlineData("run", "--seed", "-1")]
    [InlineData("run", "stray")]
    public void MalformedArguments_ExitWithUsageError(params string[] args)
    {
        var (exit, output) = Run(args);

        Assert.Equal(2, exit);
        Assert.StartsWith("error: ", output, StringComparison.Ordinal);
        Assert.Contains("usage:", output, StringComparison.Ordinal);
    }

    [Fact]
    public void NoArguments_PrintsHelp()
    {
        var (exit, output) = Run();

        Assert.Equal(0, exit);
        Assert.Contains("concord-sim sweep", output, StringComparison.Ordinal);
    }

    [Fact]
    public void Run_IsReproducibleFromTheSeed()
    {
        var first = Run("run", "--seed", "11", "--nodes", "3");
        var second = Run("run", "--seed", "11", "--nodes", "3");

        Assert.Equal(0, first.Exit);
        Assert.StartsWith("seed 11: passed", first.Output, StringComparison.Ordinal);
        Assert.Contains("trace hash ", first.Output, StringComparison.Ordinal);
        Assert.Equal(first.Output, second.Output);
    }

    [Fact]
    public void Sweep_ReportsPassedSeedsAndExitsZero()
    {
        var (exit, output) = Run("sweep", "--seeds", "4", "--start", "40", "--workers", "2");

        Assert.Equal(0, exit);
        Assert.Contains("seeds 40..43", output, StringComparison.Ordinal);
        Assert.Contains("passed: 4 / 4", output, StringComparison.Ordinal);
    }

    [Fact]
    public void Sweep_WithInjectedBug_FindsAFailingSeedAndPrintsItsReport()
    {
        var (exit, output) = Run("sweep", "--seeds", "20", "--bug", "ignore-log-votes", "--expect-failures");

        Assert.Equal(0, exit);
        Assert.Contains("FAILED (Invariant)", output, StringComparison.Ordinal);
        Assert.Contains("replay: concord-sim run --seed", output, StringComparison.Ordinal);
    }
}
