using System.Net.Http.Headers;
using System.Threading.Channels;
using Concord.Core;
using Concord.Core.Messages;
using Concord.Core.Serialization;
using Concord.Server.Hosting;

namespace Concord.Server.Transport;

/// <summary>
/// Raft over HTTP. <see cref="Send"/> only enqueues; one pump per peer drains its queue in batches and
/// POSTs them to /raft/messages. Delivery is best effort, like the network Raft is designed for: a full
/// queue drops the oldest messages, a failed POST is dropped, and the protocol's own retries
/// (heartbeats, retransmitted vote requests) recover.
/// </summary>
public sealed partial class HttpTransport : BackgroundService, ITransport
{
    public const string ClientName = "concord-raft";
    private const int QueueCapacity = 4096;
    private const int BatchSize = 128;

    private readonly IHttpClientFactory _clients;
    private readonly ILogger<HttpTransport> _logger;
    private readonly Dictionary<int, Peer> _peers;

    public HttpTransport(ConcordSettings settings, IHttpClientFactory clients, ILogger<HttpTransport> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        _clients = clients;
        _logger = logger;
        _peers = settings.Members
            .Where(m => m.Key != settings.NodeId)
            .ToDictionary(
                m => m.Key,
                m => new Peer(new Uri(m.Value, "raft/messages"), Channel.CreateBounded<RaftMessage>(new BoundedChannelOptions(QueueCapacity)
                {
                    FullMode = BoundedChannelFullMode.DropOldest,
                    SingleReader = true,
                })));
    }

    public void Send(RaftMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (_peers.TryGetValue(message.To, out var peer))
        {
            peer.Queue.Writer.TryWrite(message);
        }
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.WhenAll(_peers.Select(p => PumpAsync(p.Key, p.Value, stoppingToken)));

    private async Task PumpAsync(int peerId, Peer peer, CancellationToken stoppingToken)
    {
        var client = _clients.CreateClient(ClientName);
        var batch = new List<RaftMessage>(BatchSize);
        var reachable = true;
        try
        {
            while (await peer.Queue.Reader.WaitToReadAsync(stoppingToken))
            {
                batch.Clear();
                while (batch.Count < BatchSize && peer.Queue.Reader.TryRead(out var message))
                {
                    batch.Add(message);
                }

                using var content = new ByteArrayContent(RaftMessageCodec.Encode(batch));
                content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
                try
                {
                    using var response = await client.PostAsync(peer.Url, content, stoppingToken);
                    if (!reachable)
                    {
                        LogReachable(peerId);
                        reachable = true;
                    }
                }
                catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException && !stoppingToken.IsCancellationRequested)
                {
                    if (reachable)
                    {
                        LogUnreachable(peerId, ex.Message);
                        reachable = false;
                    }

                    // Back off a little so a dead peer does not cost a busy loop.
                    await Task.Delay(TimeSpan.FromMilliseconds(100), stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Shutting down.
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Peer {PeerId} unreachable: {Reason}")]
    private partial void LogUnreachable(int peerId, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "Peer {PeerId} reachable again")]
    private partial void LogReachable(int peerId);

    private sealed record Peer(Uri Url, Channel<RaftMessage> Queue);
}
