namespace Concord.Core;

/// <summary>What a leader knows about one follower.</summary>
internal sealed class PeerProgress
{
    /// <summary>The follower answered since the last check-quorum round.</summary>
    public bool RecentActive { get; set; } = true;
}
