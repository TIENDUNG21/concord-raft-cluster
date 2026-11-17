using System.Globalization;
using Concord.Core;

namespace Concord.Server.Hosting;

/// <summary>
/// Node configuration, read from the "Concord" section (command line, environment, appsettings).
/// <c>PeerList</c> names every member including this node: "1=http://host-a:5161,2=http://host-b:5162,...".
/// </summary>
public sealed class ConcordSettings
{
    public required int NodeId { get; init; }

    public required IReadOnlyDictionary<int, Uri> Members { get; init; }

    public required string DataDirectory { get; init; }

    public required RaftOptions Raft { get; init; }

    public required TimeSpan RequestTimeout { get; init; }

    public Uri? UrlOf(int? node) => node is { } id && Members.TryGetValue(id, out var url) ? url : null;

    public static ConcordSettings From(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var section = configuration.GetSection("Concord");
        var nodeId = Int(section, "NodeId", 0);
        var members = ParsePeers(section["PeerList"]);
        if (!members.ContainsKey(nodeId))
        {
            throw new InvalidOperationException($"Concord:NodeId {nodeId} is not in Concord:PeerList ({string.Join(", ", members.Keys)}).");
        }

        var raft = new RaftOptions
        {
            HeartbeatInterval = TimeSpan.FromMilliseconds(Int(section, "HeartbeatMs", 50)),
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(Int(section, "ElectionTimeoutMinMs", 300)),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(Int(section, "ElectionTimeoutMaxMs", 600)),
            SnapshotThreshold = Int(section, "SnapshotThreshold", 1000),
        };
        raft.Validate();

        return new ConcordSettings
        {
            NodeId = nodeId,
            Members = members,
            DataDirectory = section["DataDirectory"] is { Length: > 0 } dir ? dir : Path.Combine("data", "n" + nodeId.ToString(CultureInfo.InvariantCulture)),
            Raft = raft,
            RequestTimeout = TimeSpan.FromMilliseconds(Int(section, "RequestTimeoutMs", 5000)),
        };
    }

    internal static Dictionary<int, Uri> ParsePeers(string? peerList)
    {
        if (string.IsNullOrWhiteSpace(peerList))
        {
            throw new InvalidOperationException("Concord:PeerList is required, e.g. \"1=http://localhost:5161,2=http://localhost:5162,3=http://localhost:5163\".");
        }

        var members = new Dictionary<int, Uri>();
        foreach (var item in peerList.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var parts = item.Split('=', 2);
            if (parts.Length != 2
                || !int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var id)
                || id <= 0
                || !Uri.TryCreate(parts[1].TrimEnd('/') + "/", UriKind.Absolute, out var url)
                || (url.Scheme != Uri.UriSchemeHttp && url.Scheme != Uri.UriSchemeHttps))
            {
                throw new InvalidOperationException($"Invalid Concord:PeerList entry '{item}' (expected id=http://host:port).");
            }

            if (!members.TryAdd(id, url))
            {
                throw new InvalidOperationException($"Node {id} appears twice in Concord:PeerList.");
            }
        }

        return members;
    }

    private static int Int(IConfigurationSection section, string key, int fallback)
    {
        var text = section[key];
        if (string.IsNullOrEmpty(text))
        {
            return fallback;
        }

        return int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw new InvalidOperationException($"Concord:{key} must be an integer, got '{text}'.");
    }
}
