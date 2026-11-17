using System.Globalization;
using System.Security;
using System.Text;

namespace Concord.Simulation;

/// <summary>
/// Renders a run as an SVG timeline: one lane per node coloured by role (follower, pre-candidate,
/// candidate, leader, down), fault markers taken from the trace, and the cluster's highest commit
/// index underneath, so a stall during an election is visible as a flat line.
/// </summary>
public static class TimelineSvg
{
    private const int Width = 960;
    private const int Left = 56;
    private const int Right = 24;
    private const int LaneTop = 84;
    private const int LaneHeight = 18;
    private const int LaneGap = 12;
    private const int ChartHeight = 72;

    private const string Background = "#0a0a0a";
    private const string Text = "#ededed";
    private const string Muted = "#8b949e";
    private const string Grid = "#21262d";
    private const string Accent = "#58a6ff";
    private const string FollowerFill = "#30363d";
    private const string CandidateFill = "#d29922";
    private const string PreCandidateFill = "#7d5a17";
    private const string CrashColour = "#f85149";
    private const string RestartColour = "#3fb950";

    public static string Render(TimelineRecorder timeline, SimTrace trace, string title, string subtitle)
    {
        ArgumentNullException.ThrowIfNull(timeline);
        ArgumentNullException.ThrowIfNull(trace);
        var end = Math.Max(1, timeline.End.TotalMilliseconds);
        var plotWidth = Width - Left - Right;
        double X(TimeSpan t) => Left + (t.TotalMilliseconds / end * plotWidth);
        var nodes = timeline.Nodes.Order().ToArray();
        var lanesBottom = LaneTop + (nodes.Length * (LaneHeight + LaneGap)) - LaneGap;
        var chartTop = lanesBottom + 48;
        var chartBottom = chartTop + ChartHeight;
        var axisY = chartBottom + 18;
        var legendY = axisY + 30;
        var height = legendY + 20;

        var svg = new StringBuilder();
        svg.Append(Invariant($"<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{Width}\" height=\"{height}\" viewBox=\"0 0 {Width} {height}\" font-family=\"ui-sans-serif, -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif\">\n"));
        svg.Append(Invariant($"  <rect width=\"{Width}\" height=\"{height}\" rx=\"8\" fill=\"{Background}\"/>\n"));
        svg.Append(Invariant($"  <text x=\"{Left}\" y=\"30\" font-size=\"15\" fill=\"{Text}\" font-weight=\"600\">{Escape(title)}</text>\n"));
        svg.Append(Invariant($"  <text x=\"{Left}\" y=\"50\" font-size=\"12\" fill=\"{Muted}\">{Escape(subtitle)}</text>\n"));

        // Time grid every 500 ms (every second for long runs).
        var step = end > 12_000 ? 2000 : end > 6_000 ? 1000 : 500;
        for (var ms = 0; ms <= end; ms += step)
        {
            var x = X(TimeSpan.FromMilliseconds(ms));
            svg.Append(Invariant($"  <line x1=\"{x:0.#}\" y1=\"{LaneTop - 6}\" x2=\"{x:0.#}\" y2=\"{chartBottom}\" stroke=\"{Grid}\" stroke-width=\"1\"/>\n"));
            svg.Append(Invariant($"  <text x=\"{x:0.#}\" y=\"{axisY}\" font-size=\"11\" fill=\"{Muted}\" text-anchor=\"middle\" font-family=\"ui-monospace, Menlo, Consolas, monospace\">{ms / 1000.0:0.#} s</text>\n"));
        }

        // Intervals with no leader anywhere: the cost of an election, made visible.
        foreach (var (from, to) in Leaderless(timeline))
        {
            var x1 = X(from);
            var x2 = X(to);
            svg.Append(Invariant($"  <rect x=\"{x1:0.##}\" y=\"{LaneTop - 6}\" width=\"{Math.Max(1, x2 - x1):0.##}\" height=\"{lanesBottom - LaneTop + 12}\" fill=\"{CrashColour}\" fill-opacity=\"0.12\"/>\n"));
            svg.Append(Invariant($"  <text x=\"{x1:0.##}\" y=\"{lanesBottom + 20}\" font-size=\"11\" fill=\"{Muted}\">no leader for {(to - from).TotalMilliseconds:0} ms</text>\n"));
        }

        // Lanes.
        for (var i = 0; i < nodes.Length; i++)
        {
            var y = LaneTop + (i * (LaneHeight + LaneGap));
            svg.Append(Invariant($"  <text x=\"{Left - 12}\" y=\"{y + 13}\" font-size=\"12\" fill=\"{Text}\" text-anchor=\"end\" font-family=\"ui-monospace, Menlo, Consolas, monospace\">n{nodes[i]}</text>\n"));
            foreach (var segment in timeline.Segments.Where(s => s.Node == nodes[i]))
            {
                AppendSegment(svg, segment, X(segment.From), X(segment.To), y);
            }
        }

        // Fault markers from the trace.
        var labelRow = 0;
        foreach (var fault in trace.Events.Where(e => e.Kind == TraceKind.Fault && e.Time > TimeSpan.Zero && e.Time < timeline.End))
        {
            var (colour, label) = Describe(fault);
            if (colour is null)
            {
                continue;
            }

            var x = X(fault.Time);
            var labelY = labelRow++ % 2 == 0 ? 66 : 78;
            svg.Append(Invariant($"  <line x1=\"{x:0.#}\" y1=\"{LaneTop - 10}\" x2=\"{x:0.#}\" y2=\"{lanesBottom + 4}\" stroke=\"{colour}\" stroke-width=\"1.5\" stroke-dasharray=\"4 3\"/>\n"));
            svg.Append(Invariant($"  <text x=\"{x + 4:0.#}\" y=\"{labelY}\" font-size=\"11\" fill=\"{colour}\">{Escape(label)}</text>\n"));
        }

        AppendCommitChart(svg, timeline, X, chartTop, chartBottom);
        AppendLegend(svg, legendY);
        svg.Append("</svg>\n");
        return svg.ToString();
    }

    /// <summary>Stretches of the run during which no node was leader.</summary>
    internal static IReadOnlyList<(TimeSpan From, TimeSpan To)> Leaderless(TimelineRecorder timeline)
    {
        var gaps = new List<(TimeSpan From, TimeSpan To)>();
        var cursor = TimeSpan.Zero;
        foreach (var lead in timeline.Segments.Where(s => s.State == LaneState.Leader).OrderBy(s => s.From))
        {
            if (lead.From > cursor)
            {
                gaps.Add((cursor, lead.From));
            }

            cursor = lead.To > cursor ? lead.To : cursor;
        }

        if (cursor < timeline.End)
        {
            gaps.Add((cursor, timeline.End));
        }

        return gaps;
    }

    private static void AppendSegment(StringBuilder svg, LaneSegment segment, double x1, double x2, int y)
    {
        var width = Math.Max(0.5, x2 - x1);
        switch (segment.State)
        {
            case LaneState.Down:
                svg.Append(Invariant($"  <rect x=\"{x1:0.##}\" y=\"{y + 0.5}\" width=\"{width:0.##}\" height=\"{LaneHeight - 1}\" fill=\"none\" stroke=\"{CrashColour}\" stroke-opacity=\"0.7\" stroke-dasharray=\"3 3\"/>\n"));
                if (width > 44)
                {
                    svg.Append(Invariant($"  <text x=\"{x1 + 6:0.##}\" y=\"{y + 13}\" font-size=\"11\" fill=\"{CrashColour}\">down</text>\n"));
                }

                break;
            case LaneState.Leader:
                svg.Append(Invariant($"  <rect x=\"{x1:0.##}\" y=\"{y}\" width=\"{width:0.##}\" height=\"{LaneHeight}\" fill=\"{Accent}\"/>\n"));
                if (width > 70)
                {
                    svg.Append(Invariant($"  <text x=\"{x1 + 6:0.##}\" y=\"{y + 13}\" font-size=\"11\" fill=\"{Background}\" font-weight=\"600\">leader, term {segment.Term}</text>\n"));
                }

                break;
            default:
                var (fill, inset) = segment.State switch
                {
                    LaneState.Candidate => (CandidateFill, 0),
                    LaneState.PreCandidate => (PreCandidateFill, 0),
                    _ => (FollowerFill, 5),
                };
                svg.Append(Invariant($"  <rect x=\"{x1:0.##}\" y=\"{y + inset}\" width=\"{width:0.##}\" height=\"{LaneHeight - (2 * inset)}\" fill=\"{fill}\"/>\n"));
                break;
        }
    }

    private static void AppendCommitChart(StringBuilder svg, TimelineRecorder timeline, Func<TimeSpan, double> x, int top, int bottom)
    {
        svg.Append(Invariant($"  <text x=\"{Left}\" y=\"{top - 8}\" font-size=\"11\" fill=\"{Muted}\">highest committed index</text>\n"));
        svg.Append(Invariant($"  <line x1=\"{Left}\" y1=\"{bottom}\" x2=\"{Width - Right}\" y2=\"{bottom}\" stroke=\"{Grid}\"/>\n"));
        if (timeline.Commits.Count == 0)
        {
            return;
        }

        var max = Math.Max(1, timeline.Commits[^1].Index);
        double Y(long index) => bottom - ((double)index / max * (bottom - top));
        var points = new StringBuilder();
        points.Append(Invariant($"{Left},{bottom} "));
        long last = 0;
        foreach (var (time, index) in timeline.Commits)
        {
            points.Append(Invariant($"{x(time):0.#},{Y(last):0.#} {x(time):0.#},{Y(index):0.#} "));
            last = index;
        }

        points.Append(Invariant($"{x(timeline.End):0.#},{Y(last):0.#}"));
        svg.Append(Invariant($"  <polyline points=\"{points}\" fill=\"none\" stroke=\"{Accent}\" stroke-width=\"1.5\"/>\n"));
        svg.Append(Invariant($"  <text x=\"{Width - Right}\" y=\"{top - 8}\" font-size=\"11\" fill=\"{Muted}\" text-anchor=\"end\" font-family=\"ui-monospace, Menlo, Consolas, monospace\">{last}</text>\n"));
    }

    private static void AppendLegend(StringBuilder svg, int y)
    {
        var x = Left;
        foreach (var (label, fill, dashed) in new[]
        {
            ("follower", FollowerFill, false),
            ("pre-candidate", PreCandidateFill, false),
            ("candidate", CandidateFill, false),
            ("leader", Accent, false),
            ("down", "none", true),
        })
        {
            var stroke = dashed ? Invariant($" stroke=\"{CrashColour}\" stroke-dasharray=\"3 2\"") : string.Empty;
            svg.Append(Invariant($"  <rect x=\"{x}\" y=\"{y - 9}\" width=\"14\" height=\"10\" fill=\"{fill}\"{stroke}/>\n"));
            svg.Append(Invariant($"  <text x=\"{x + 20}\" y=\"{y}\" font-size=\"11\" fill=\"{Muted}\">{label}</text>\n"));
            x += 30 + (label.Length * 7);
        }
    }

    private static (string? Colour, string Label) Describe(TraceEvent fault)
    {
        var node = fault.Node is { } n ? "n" + n.ToString(CultureInfo.InvariantCulture) : string.Empty;
        return fault.Text switch
        {
            var t when t.StartsWith("crash (was leader)", StringComparison.Ordinal) => (CrashColour, $"{node} (leader) crashes"),
            var t when t.StartsWith("crash", StringComparison.Ordinal) => (CrashColour, $"{node} crashes"),
            var t when t.StartsWith("restart", StringComparison.Ordinal) => (RestartColour, $"{node} restarts"),
            var t when t.StartsWith("isolate", StringComparison.Ordinal) => (CrashColour, $"{node} isolated"),
            var t when t.StartsWith("partition", StringComparison.Ordinal) => (CrashColour, t),
            _ => (null, string.Empty),
        };
    }

    private static string Escape(string text) => SecurityElement.Escape(text) ?? string.Empty;

    private static string Invariant(FormattableString text) => text.ToString(CultureInfo.InvariantCulture);
}
