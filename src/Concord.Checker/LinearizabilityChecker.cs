namespace Concord.Checker;

/// <summary>
/// A sequential specification. <see cref="TryApply"/> applies an operation (input and recorded output)
/// to a state and says whether the recorded output is what the specification allows.
/// </summary>
public interface ISequentialModel<TState, TOp>
    where TState : notnull
{
    TState Initial { get; }

    bool TryApply(TState state, TOp operation, out TState after);

    string Describe(TOp operation);
}

/// <summary>One operation in a history: invoked at <see cref="Call"/>, answered at <see cref="Return"/>.
/// <see cref="long.MaxValue"/> as return time means "never answered": it may have taken effect any time
/// after its call, or not at all.</summary>
public readonly record struct Timed<TOp>(TOp Operation, long Call, long Return)
{
    public bool Pending => Return == long.MaxValue;
}

public enum CheckOutcome
{
    Ok,
    Illegal,

    /// <summary>The search budget ran out before a verdict.</summary>
    Unknown,
}

public sealed record CheckResult(CheckOutcome Outcome, string? Partition, string? Explanation, int Operations)
{
    public static CheckResult Ok(int operations) => new(CheckOutcome.Ok, null, null, operations);
}

public sealed record CheckOptions
{
    /// <summary>Maximum search steps per partition before giving up with <see cref="CheckOutcome.Unknown"/>.</summary>
    public long MaxSteps { get; init; } = 5_000_000;
}

/// <summary>
/// Decides whether a concurrent history is linearizable: whether every operation can be given a single
/// point between its call and its return such that, in that order, the history is legal for the model.
/// Wing and Gong's backtracking search over a linked list of call/return events, with Lowe's
/// memoization of (set of linearized operations, model state) — the same algorithm as Porcupine.
/// </summary>
public static class LinearizabilityChecker
{
    public static CheckResult Check<TState, TOp>(
        ISequentialModel<TState, TOp> model,
        IReadOnlyList<Timed<TOp>> history,
        CheckOptions? options = null)
        where TState : notnull
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(history);
        options ??= new CheckOptions();
        foreach (var op in history)
        {
            if (op.Return < op.Call)
            {
                throw new ArgumentException($"Operation returns ({op.Return}) before it is called ({op.Call}).", nameof(history));
            }
        }

        if (history.Count == 0)
        {
            return CheckResult.Ok(0);
        }

        var head = BuildEventList(history);
        var cache = new HashSet<(Bitset Linearized, TState State)>();
        var calls = new Stack<(Event Call, TState State)>();
        var linearized = new Bitset(history.Count);
        var state = model.Initial;
        var entry = head.Next!;
        long steps = 0;
        var best = Array.Empty<int>();

        while (head.Next is not null)
        {
            if (++steps > options.MaxSteps)
            {
                return new CheckResult(CheckOutcome.Unknown, null, $"gave up after {options.MaxSteps} search steps", history.Count);
            }

            if (entry.IsCall)
            {
                if (model.TryApply(state, history[entry.Id].Operation, out var next))
                {
                    var candidate = linearized.With(entry.Id);
                    if (cache.Add((candidate, next)))
                    {
                        calls.Push((entry, state));
                        state = next;
                        linearized = candidate;
                        entry.Lift();
                        entry = head.Next!;
                        if (calls.Count > best.Length)
                        {
                            best = calls.Select(c => c.Call.Id).Reverse().ToArray();
                        }

                        continue;
                    }
                }

                entry = entry.Next!;
            }
            else
            {
                // A return whose call is not linearized yet: nothing can be placed before it. Backtrack.
                if (calls.Count == 0)
                {
                    return new CheckResult(CheckOutcome.Illegal, null, Explain(model, history, best), history.Count);
                }

                var (call, previous) = calls.Pop();
                state = previous;
                linearized = linearized.Without(call.Id);
                call.Unlift();
                entry = call.Next!;
            }
        }

        return CheckResult.Ok(history.Count);
    }

    private static Event BuildEventList<TOp>(IReadOnlyList<Timed<TOp>> history)
    {
        var events = new List<Event>(history.Count * 2);
        for (var i = 0; i < history.Count; i++)
        {
            var call = new Event(i, isCall: true, history[i].Call);
            var ret = new Event(i, isCall: false, history[i].Return);
            call.Match = ret;
            events.Add(call);
            events.Add(ret);
        }

        // Ties: calls first, so operations touching the same instant count as concurrent (never a false alarm).
        events.Sort((a, b) => a.Time != b.Time ? a.Time.CompareTo(b.Time) : b.IsCall.CompareTo(a.IsCall));
        var head = new Event(-1, isCall: false, long.MinValue);
        var previous = head;
        foreach (var item in events)
        {
            previous.Next = item;
            item.Prev = previous;
            previous = item;
        }

        return head;
    }

    private static string Explain<TState, TOp>(ISequentialModel<TState, TOp> model, IReadOnlyList<Timed<TOp>> history, int[] best)
        where TState : notnull
    {
        var placed = new HashSet<int>(best);
        var lines = new List<string>
        {
            $"no valid order exists; the longest legal prefix places {best.Length} of {history.Count} operations:",
        };
        lines.AddRange(best.Select((id, i) => $"  {i + 1,3}. {Format(model, history[id])}"));
        lines.Add("operations that could not be placed after it:");
        lines.AddRange(Enumerable.Range(0, history.Count)
            .Where(id => !placed.Contains(id))
            .OrderBy(id => history[id].Call)
            .Take(8)
            .Select(id => $"       {Format(model, history[id])}"));
        return string.Join('\n', lines);
    }

    private static string Format<TState, TOp>(ISequentialModel<TState, TOp> model, Timed<TOp> op)
        where TState : notnull =>
        $"[{op.Call}..{(op.Pending ? "pending" : op.Return.ToString(System.Globalization.CultureInfo.InvariantCulture))}] {model.Describe(op.Operation)}";

    private sealed class Event(int id, bool isCall, long time)
    {
        public int Id { get; } = id;

        public bool IsCall { get; } = isCall;

        public long Time { get; } = time;

        public Event? Match { get; set; }

        public Event? Prev { get; set; }

        public Event? Next { get; set; }

        /// <summary>Removes this call and its return from the list (dancing links: they keep their pointers).</summary>
        public void Lift()
        {
            Prev!.Next = Next;
            Next!.Prev = Prev;
            var ret = Match!;
            ret.Prev!.Next = ret.Next;
            if (ret.Next is not null)
            {
                ret.Next.Prev = ret.Prev;
            }
        }

        public void Unlift()
        {
            var ret = Match!;
            ret.Prev!.Next = ret;
            if (ret.Next is not null)
            {
                ret.Next.Prev = ret;
            }

            Prev!.Next = this;
            Next!.Prev = this;
        }
    }
}
