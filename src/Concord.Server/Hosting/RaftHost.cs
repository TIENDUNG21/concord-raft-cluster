using System.Diagnostics;
using System.Threading.Channels;
using Concord.Core;
using Concord.Core.Messages;
using Concord.Core.Storage;
using Concord.Kv;
using Concord.Server.Transport;

namespace Concord.Server.Hosting;

/// <summary>Monotonic wall-clock time for the real server.</summary>
internal sealed class MonotonicClock : IClock
{
    private readonly long _start = Stopwatch.GetTimestamp();

    public TimeSpan Now => Stopwatch.GetElapsedTime(_start);
}

internal sealed class SharedRandom : IRandomSource
{
    public int NextInt(int minInclusive, int maxExclusive) => Random.Shared.Next(minInclusive, maxExclusive);
}

/// <summary>
/// Owns the node. RaftNode is single-threaded by design, so every interaction (HTTP requests, incoming
/// Raft messages, timers) is posted to one inbox and executed by this loop. Request handlers await a
/// TaskCompletionSource completed from the loop with RunContinuationsAsynchronously, so no handler code
/// ever runs on the loop thread.
/// </summary>
public sealed partial class RaftHost : BackgroundService
{
    private readonly Channel<Action> _inbox = Channel.CreateUnbounded<Action>(new UnboundedChannelOptions { SingleReader = true });
    private readonly RaftNode _node;
    private readonly KvService _service;
    private readonly MonotonicClock _clock = new();
    private readonly ILogger<RaftHost> _logger;
    private readonly IHostApplicationLifetime _lifetime;

    public RaftHost(ConcordSettings settings, IRaftStorage storage, HttpTransport transport, ILogger<RaftHost> logger, IHostApplicationLifetime lifetime)
    {
        ArgumentNullException.ThrowIfNull(settings);
        _logger = logger;
        _lifetime = lifetime;
        var machine = new KvStateMachine();
        _node = new RaftNode(settings.NodeId, settings.Members.Keys, settings.Raft, storage, machine, transport, _clock, new SharedRandom());
        _node.AddObserver(new LoggingObserver(logger));
        _service = new KvService(_node, machine);
        LogStarting(settings.NodeId, _node.CurrentTerm, _node.LastLogIndex, _node.SnapshotIndex);
    }

    public Task<KvResponse> WriteAsync(KvCommand command) => Invoke<KvResponse>(done => _service.Write(command, done));

    public Task<KvResponse> ReadAsync(string key) => Invoke<KvResponse>(done => _service.Read(key, done));

    public Task<NodeStatus> StatusAsync() => Invoke<NodeStatus>(done => done(_node.Status()));

    public void Deliver(IReadOnlyList<RaftMessage> messages) =>
        _inbox.Writer.TryWrite(() =>
        {
            foreach (var message in messages)
            {
                _node.Step(message);
            }
        });

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var wait = _node.NextDeadline - _clock.Now;
                if (wait > TimeSpan.Zero && !_inbox.Reader.TryPeek(out _))
                {
                    using var timer = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                    timer.CancelAfter(wait);
                    try
                    {
                        await _inbox.Reader.WaitToReadAsync(timer.Token);
                    }
                    catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                    {
                        // Timer expired: fall through to Tick.
                    }
                }

                // Bounded batch so timers still fire under a flood of requests.
                for (var i = 0; i < 1024 && _inbox.Reader.TryRead(out var action); i++)
                {
                    action();
                }

                _node.Tick();
            }
        }
        catch (RaftSafetyException ex)
        {
            // Continuing after a safety violation could corrupt data on other nodes: stop the process.
            LogSafetyViolation(ex);
            _lifetime.StopApplication();
            throw;
        }
    }

    private Task<T> Invoke<T>(Action<Action<T>> work)
    {
        var completion = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_inbox.Writer.TryWrite(() => work(result => completion.TrySetResult(result))))
        {
            completion.TrySetException(new InvalidOperationException("The node is shutting down."));
        }

        return completion.Task;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Node {NodeId} starting at term {Term}, log up to {LastIndex}, snapshot at {SnapshotIndex}")]
    private partial void LogStarting(int nodeId, long term, long lastIndex, long snapshotIndex);

    [LoggerMessage(Level = LogLevel.Critical, Message = "Raft safety violation, stopping the node")]
    private partial void LogSafetyViolation(Exception exception);

    private sealed partial class LoggingObserver(ILogger logger) : IRaftObserver
    {
        public void RoleChanged(int nodeId, RaftRole role, long term, int? leaderId) => LogRole(logger, nodeId, role, term, leaderId);

        public void SnapshotTaken(int nodeId, SnapshotMetadata metadata) => LogSnapshot(logger, nodeId, metadata.LastIncludedIndex, "taken");

        public void SnapshotInstalled(int nodeId, SnapshotMetadata metadata) => LogSnapshot(logger, nodeId, metadata.LastIncludedIndex, "installed from leader");

        [LoggerMessage(Level = LogLevel.Information, Message = "Node {NodeId} is {Role} in term {Term} (leader {LeaderId})")]
        private static partial void LogRole(ILogger logger, int nodeId, RaftRole role, long term, int? leaderId);

        [LoggerMessage(Level = LogLevel.Information, Message = "Node {NodeId} snapshot up to {Index} {How}")]
        private static partial void LogSnapshot(ILogger logger, int nodeId, long index, string how);
    }
}
