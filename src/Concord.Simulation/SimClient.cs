using System.Globalization;
using Concord.Checker;
using Concord.Kv;

namespace Concord.Simulation;

/// <summary>The client workload: how many clients, how many keys, and how patiently they retry.</summary>
public sealed record WorkloadOptions
{
    public int Clients { get; init; } = 5;

    /// <summary>Few keys means more contention, which is what finds bugs.</summary>
    public int Keys { get; init; } = 4;

    public TimeSpan MaxThinkTime { get; init; } = TimeSpan.FromMilliseconds(20);

    /// <summary>An attempt not answered within this time is retried at another node.</summary>
    public TimeSpan AttemptTimeout { get; init; } = TimeSpan.FromMilliseconds(150);

    /// <summary>After this long the client gives up; the operation stays in the history as "never returned".</summary>
    public TimeSpan GiveUpAfter { get; init; } = TimeSpan.FromSeconds(2);
}

/// <summary>Collects the client-observed history. Timestamps are a strictly increasing counter, so no two events tie.</summary>
public sealed class HistoryRecorder
{
    private readonly List<Timed<KvOperation>> _operations = new();
    private long _clock;

    public IReadOnlyList<Timed<KvOperation>> Operations => _operations;

    public long Tick() => ++_clock;

    internal void Add(KvOperation operation, long call, long ret) => _operations.Add(new Timed<KvOperation>(operation, call, ret));
}

/// <summary>
/// A closed-loop client: one operation at a time, sent to the node it believes is leader, retried on
/// redirects, unknown outcomes and timeouts with the same sequence number (so the state machine runs it
/// at most once), abandoned after <see cref="WorkloadOptions.GiveUpAfter"/>.
/// </summary>
public sealed class SimClient
{
    private readonly int _endpoint;
    private readonly Simulator _sim;
    private readonly SimNetwork _network;
    private readonly IReadOnlyList<int> _nodes;
    private readonly DeterministicRandom _random;
    private readonly WorkloadOptions _options;
    private readonly HistoryRecorder _history;
    private readonly SimTrace _trace;
    private readonly Dictionary<string, string?> _lastSeen = new(StringComparer.Ordinal);
    private TimeSpan _stopAt;
    private long _sequence;
    private long _attempt;
    private int? _leaderGuess;
    private InFlight? _current;

    public SimClient(int endpoint, Simulator sim, SimNetwork network, IReadOnlyList<int> nodes, DeterministicRandom random, WorkloadOptions options, HistoryRecorder history, SimTrace trace)
    {
        _endpoint = endpoint;
        _sim = sim;
        _network = network;
        _nodes = nodes;
        _random = random;
        _options = options;
        _history = history;
        _trace = trace;
        network.RegisterClient(endpoint, Receive);
    }

    public int Completed { get; private set; }

    public int GaveUp { get; private set; }

    public bool Idle => _current is null;

    /// <summary>Set when a node answers in a way no correct node may (e.g. rejecting a valid request).</summary>
    public string? ProtocolError { get; private set; }

    public void Start(TimeSpan at, TimeSpan stopIssuingAt)
    {
        _stopAt = stopIssuingAt;
        _sim.Schedule(at, IssueNext);
    }

    private void IssueNext()
    {
        if (_sim.Now >= _stopAt)
        {
            return;
        }

        _sequence++;
        var key = "k" + _random.NextInt(0, _options.Keys).ToString(CultureInfo.InvariantCulture);
        var value = string.Create(CultureInfo.InvariantCulture, $"c{_endpoint}.{_sequence}");
        var roll = _random.NextInt(0, 100);
        var request = roll switch
        {
            < 40 => new ClientRequest(_endpoint, _sequence, KvOpKind.Get, key, null, null),
            < 70 => new ClientRequest(_endpoint, _sequence, KvOpKind.Put, key, value, null),
            < 80 => new ClientRequest(_endpoint, _sequence, KvOpKind.Delete, key, null, null),
            _ => new ClientRequest(_endpoint, _sequence, KvOpKind.CompareAndSet, key, value, _random.Chance(0.7) ? _lastSeen.GetValueOrDefault(key) : null),
        };
        _current = new InFlight(request, _history.Tick(), _sim.Now);
        SendAttempt();
    }

    private void SendAttempt()
    {
        var current = _current!;
        var target = _leaderGuess ?? _random.Pick(_nodes);
        var attempt = ++_attempt;
        current.Attempt = attempt;
        _network.Send(_endpoint, target, current.Request);
        _sim.After(_options.AttemptTimeout, () =>
        {
            if (_current == current && current.Attempt == attempt)
            {
                _leaderGuess = null;
                Retry();
            }
        });
    }

    private void Retry()
    {
        var current = _current!;
        if (_sim.Now - current.StartedAt >= _options.GiveUpAfter)
        {
            // The outcome is unknown: it may still be applied later, or never.
            var request = current.Request;
            _history.Add(new KvOperation(_endpoint, request.Kind, request.Key, request.Value, request.Expected, null), current.Call, long.MaxValue);
            _trace.Add(TraceKind.Client, null, $"c{_endpoint} gave up on {request.Kind} {request.Key} (seq {request.Sequence})");
            GaveUp++;
            _current = null;
            ScheduleNext();
            return;
        }

        SendAttempt();
    }

    private void Receive(int from, object payload)
    {
        if (payload is not ClientResponse response || _current is not { } current || response.Sequence != current.Request.Sequence)
        {
            return;
        }

        var status = response.Response.Status;
        if (status == KvStatus.Ok)
        {
            Complete(current, response.Response.Result!, from);
            return;
        }

        if (status == KvStatus.Rejected)
        {
            ProtocolError ??= $"node n{from} rejected a valid request {current.Request}: {response.Response.Error}";
            return;
        }

        // NotLeader or Unknown: follow the hint (or pick at random) after a short backoff.
        current.Attempt = ++_attempt;
        _leaderGuess = response.Response.LeaderHint is { } hint && hint != from ? hint : null;
        _sim.After(TimeSpan.FromMilliseconds(_random.NextInt(5, 25)), () =>
        {
            if (_current == current)
            {
                Retry();
            }
        });
    }

    private void Complete(InFlight current, KvResult result, int from)
    {
        var request = current.Request;
        if (result.Stale)
        {
            ProtocolError ??= $"node n{from} answered the current request {request} as a stale retry";
            return;
        }

        var outcome = request.Kind switch
        {
            KvOpKind.Get => new KvOutcome(result.Succeeded, result.Value),
            KvOpKind.Put => new KvOutcome(true, null),
            KvOpKind.Delete => new KvOutcome(result.Succeeded, null),
            _ => new KvOutcome(result.Succeeded, result.Value),
        };
        _lastSeen[request.Key] = request.Kind switch
        {
            KvOpKind.Put => request.Value,
            KvOpKind.Delete => null,
            _ => result.Value,
        };
        _history.Add(new KvOperation(_endpoint, request.Kind, request.Key, request.Value, request.Expected, outcome), current.Call, _history.Tick());
        Completed++;
        _leaderGuess = from;
        _current = null;
        ScheduleNext();
    }

    private void ScheduleNext()
    {
        var think = _random.NextInt(0, (int)_options.MaxThinkTime.TotalMilliseconds + 1);
        _sim.After(TimeSpan.FromMilliseconds(think), IssueNext);
    }

    private sealed class InFlight(ClientRequest request, long call, TimeSpan startedAt)
    {
        public ClientRequest Request { get; } = request;

        public long Call { get; } = call;

        public TimeSpan StartedAt { get; } = startedAt;

        public long Attempt { get; set; }
    }
}
