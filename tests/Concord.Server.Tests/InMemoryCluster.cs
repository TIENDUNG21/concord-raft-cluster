using System.Collections.Concurrent;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Concord.Server.Api;
using Concord.Server.Transport;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace Concord.Server.Tests;

/// <summary>
/// Three real Concord nodes hosted in-process with TestServer. Their Raft traffic goes through a
/// routing HttpMessageHandler keyed by host name, so the whole HTTP transport is exercised without
/// opening sockets. A node can be cut off to simulate a crash.
/// </summary>
public sealed class InMemoryCluster : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, HttpMessageHandler> _routes = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, bool> _down = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<WebApplicationFactory<Program>> _factories = new();
    private readonly string _root = Path.Combine(Path.GetTempPath(), "concord-cluster-" + Guid.NewGuid().ToString("N"));

    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { Converters = { new JsonStringEnumConverter() } };

    public InMemoryCluster(int size = 3)
    {
        var peers = string.Join(",", Enumerable.Range(1, size).Select(i => $"{i}=http://n{i}.test"));
        for (var id = 1; id <= size; id++)
        {
            var nodeId = id;
            var factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
            {
                builder.UseSetting("Concord:NodeId", nodeId.ToString(System.Globalization.CultureInfo.InvariantCulture));
                builder.UseSetting("Concord:PeerList", peers);
                builder.UseSetting("Concord:DataDirectory", Path.Combine(_root, "n" + nodeId));
                builder.UseSetting("Concord:RequestTimeoutMs", "3000");
                builder.ConfigureServices(services => services
                    .AddHttpClient(HttpTransport.ClientName)
                    .ConfigurePrimaryHttpMessageHandler(() => new RoutingHandler($"n{nodeId}.test", _routes, _down)));
            });
            _factories.Add(factory);
        }

        for (var id = 1; id <= size; id++)
        {
            _routes[$"n{id}.test"] = _factories[id - 1].Server.CreateHandler();
        }
    }

    public int Size => _factories.Count;

    /// <summary>A client for one node that does not follow redirects, so tests can see them.</summary>
    public HttpClient Client(int id) =>
        _factories[id - 1].CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false, BaseAddress = new Uri($"http://n{id}.test") });

    /// <summary>Stops all Raft traffic to and from the node, as if its process had died.</summary>
    public void Disconnect(int id) => _down[$"n{id}.test"] = true;

    public async Task<StatusResponse> Status(int id) =>
        (await Client(id).GetFromJsonAsync<StatusResponse>("/cluster/status", Json))!;

    public async Task<int> WaitForLeader(IReadOnlyCollection<int>? among = null, TimeSpan? timeout = null)
    {
        var nodes = among ?? Enumerable.Range(1, Size).ToArray();
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(15));
        while (DateTime.UtcNow < deadline)
        {
            foreach (var id in nodes)
            {
                var status = await Status(id);
                if (status.Role == Core.RaftRole.Leader && status.CommitIndex > 0 && nodes.Contains(status.NodeId))
                {
                    return id;
                }
            }

            await Task.Delay(50);
        }

        throw new TimeoutException("no leader elected");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var factory in _factories)
        {
            await factory.DisposeAsync();
        }

        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    private sealed class RoutingHandler(
        string source,
        ConcurrentDictionary<string, HttpMessageHandler> routes,
        ConcurrentDictionary<string, bool> down) : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var host = request.RequestUri?.Host ?? string.Empty;
            if (down.ContainsKey(source) || down.ContainsKey(host) || !routes.TryGetValue(host, out var target))
            {
                throw new HttpRequestException($"{host} is unreachable from {source}");
            }

            using var invoker = new HttpMessageInvoker(target, disposeHandler: false);
            return await invoker.SendAsync(request, cancellationToken);
        }
    }
}
