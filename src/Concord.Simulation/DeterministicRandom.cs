using System.Numerics;
using Concord.Core;

namespace Concord.Simulation;

/// <summary>
/// xoshiro256** seeded through SplitMix64 (Blackman and Vigna). Implemented here instead of using
/// System.Random so a seed means the same run on every .NET version and platform.
/// </summary>
public sealed class DeterministicRandom : IRandomSource
{
    private ulong _s0;
    private ulong _s1;
    private ulong _s2;
    private ulong _s3;

    public DeterministicRandom(ulong seed)
    {
        var state = seed;
        _s0 = SplitMix64(ref state);
        _s1 = SplitMix64(ref state);
        _s2 = SplitMix64(ref state);
        _s3 = SplitMix64(ref state);
    }

    public static ulong SplitMix64(ref ulong state)
    {
        state += 0x9E3779B97F4A7C15UL;
        var z = state;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }

    public ulong NextUInt64()
    {
        var result = BitOperations.RotateLeft(_s1 * 5, 7) * 9;
        var t = _s1 << 17;
        _s2 ^= _s0;
        _s3 ^= _s1;
        _s1 ^= _s2;
        _s0 ^= _s3;
        _s2 ^= t;
        _s3 = BitOperations.RotateLeft(_s3, 45);
        return result;
    }

    /// <summary>Uniform integer in [min, maxExclusive) without modulo bias.</summary>
    public int NextInt(int minInclusive, int maxExclusive)
    {
        if (minInclusive >= maxExclusive)
        {
            throw new ArgumentOutOfRangeException(nameof(maxExclusive), "Range must not be empty.");
        }

        var range = (ulong)((long)maxExclusive - minInclusive);
        var threshold = (0UL - range) % range;
        ulong value;
        do
        {
            value = NextUInt64();
        }
        while (value < threshold);

        return (int)(minInclusive + (long)(value % range));
    }

    /// <summary>Uniform double in [0, 1) with 53 bits of precision.</summary>
    public double NextDouble() => (NextUInt64() >> 11) * (1.0 / (1UL << 53));

    public bool Chance(double probability) => probability > 0 && NextDouble() < probability;

    public T Pick<T>(IReadOnlyList<T> items)
    {
        ArgumentNullException.ThrowIfNull(items);
        return items[NextInt(0, items.Count)];
    }

    /// <summary>An independent stream, so one subsystem's draws do not shift another's.</summary>
    public DeterministicRandom Fork() => new(NextUInt64());
}
