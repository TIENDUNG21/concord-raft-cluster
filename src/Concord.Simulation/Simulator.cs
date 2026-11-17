using Concord.Core;

namespace Concord.Simulation;

/// <summary>Virtual time. Only the simulator moves it, and only forward.</summary>
public sealed class VirtualClock : IClock
{
    public TimeSpan Now { get; private set; }

    internal void AdvanceTo(TimeSpan time)
    {
        if (time < Now)
        {
            throw new InvalidOperationException($"Time cannot go backwards ({Now} -> {time}).");
        }

        Now = time;
    }
}

/// <summary>
/// Discrete-event loop. Events are ordered by (time, insertion sequence), so two events at the same
/// virtual millisecond always run in the order they were scheduled: no hidden nondeterminism.
/// </summary>
public sealed class Simulator
{
    private readonly PriorityQueue<Action, (TimeSpan Time, long Sequence)> _queue = new();
    private long _sequence;

    public VirtualClock Clock { get; } = new();

    public TimeSpan Now => Clock.Now;

    /// <summary>Number of events executed so far; also a total order usable as a timestamp.</summary>
    public long Steps { get; private set; }

    public int Pending => _queue.Count;

    public void Schedule(TimeSpan at, Action action)
    {
        ArgumentNullException.ThrowIfNull(action);
        var time = at < Clock.Now ? Clock.Now : at;
        _queue.Enqueue(action, (time, _sequence++));
    }

    public void After(TimeSpan delay, Action action) => Schedule(Clock.Now + delay, action);

    /// <summary>Runs events up to and including <paramref name="end"/>, or until <paramref name="stop"/> returns true.</summary>
    public void RunUntil(TimeSpan end, Func<bool>? stop = null)
    {
        while (_queue.TryPeek(out _, out var key) && key.Time <= end)
        {
            if (stop?.Invoke() == true)
            {
                return;
            }

            var action = _queue.Dequeue();
            Clock.AdvanceTo(key.Time);
            Steps++;
            action();
        }

        if (stop?.Invoke() != true)
        {
            Clock.AdvanceTo(end > Clock.Now ? end : Clock.Now);
        }
    }
}
