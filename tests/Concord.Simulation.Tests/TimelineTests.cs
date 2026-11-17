using System.Xml.Linq;
using Concord.Sim;

namespace Concord.Simulation.Tests;

public sealed class TimelineTests
{
    [Fact]
    public void LeaderCrashScenario_RecordsTheCrashAndARealReElection()
    {
        var result = SimulationRunner.Run(SimulationConfig.LeaderCrash(1));
        var timeline = result.Timeline;

        Assert.True(result.Passed, SimulationRunner.FormatReport(result));
        var leaders = timeline.Segments.Where(s => s.State == LaneState.Leader).OrderBy(s => s.From).ToArray();
        Assert.Equal(2, leaders.Length);
        Assert.NotEqual(leaders[0].Node, leaders[1].Node);
        Assert.Equal(TimeSpan.FromMilliseconds(1500), leaders[0].To);
        Assert.True(leaders[1].Term > leaders[0].Term);

        var down = Assert.Single(timeline.Segments, s => s.State == LaneState.Down);
        Assert.Equal(leaders[0].Node, down.Node);
        Assert.Equal(TimeSpan.FromMilliseconds(1500), down.From);
        Assert.Equal(TimeSpan.FromMilliseconds(3500), down.To);

        var gap = TimelineSvg.Leaderless(timeline).Single(g => g.From == TimeSpan.FromMilliseconds(1500));
        Assert.Equal(leaders[1].From, gap.To);
        Assert.True(timeline.Commits.Count > 10);
    }

    [Fact]
    public void Svg_IsWellFormedAndReproducibleFromTheSeed()
    {
        string Render()
        {
            var result = SimulationRunner.Run(SimulationConfig.LeaderCrash(4));
            return TimelineSvg.Render(result.Timeline, result.Trace, "title & <escaping>", "subtitle");
        }

        var first = Render();
        var document = XDocument.Parse(first);

        Assert.Equal(first, Render());
        Assert.Equal("svg", document.Root!.Name.LocalName);
        Assert.Contains("title &amp; &lt;escaping&gt;", first, StringComparison.Ordinal);
        Assert.Contains("leader, term", first, StringComparison.Ordinal);
        Assert.Contains("no leader for", first, StringComparison.Ordinal);
    }

    [Fact]
    public void TimelineCommand_WritesTheFile()
    {
        var path = Path.Combine(Path.GetTempPath(), $"concord-timeline-{Guid.NewGuid():N}.svg");
        try
        {
            using var output = new StringWriter();

            var exit = Program.Execute(new[] { "timeline", "--seed", "2", "--out", path }, output);

            Assert.Equal(0, exit);
            Assert.StartsWith("<svg", File.ReadAllText(path), StringComparison.Ordinal);
            Assert.Equal(2, Program.Execute(new[] { "timeline", "--seed", "2" }, new StringWriter()));
        }
        finally
        {
            File.Delete(path);
        }
    }
}
