namespace Concord.Simulation.Tests;

public sealed class EngineTests
{
    [Fact]
    public void SplitMix64_MatchesPublishedSequence()
    {
        ulong state = 1234567;

        Assert.Equal(6457827717110365317UL, DeterministicRandom.SplitMix64(ref state));
        Assert.Equal(3203168211198807973UL, DeterministicRandom.SplitMix64(ref state));
        Assert.Equal(9817491932198370423UL, DeterministicRandom.SplitMix64(ref state));
    }

    [Fact]
    public void Xoshiro256StarStar_MatchesReferenceImplementation()
    {
        // Values from an independent Python port of the reference C code, seeded through SplitMix64(42).
        var random = new DeterministicRandom(42);

        Assert.Equal(1546998764402558742UL, random.NextUInt64());
        Assert.Equal(6990951692964543102UL, random.NextUInt64());
        Assert.Equal(12544586762248559009UL, random.NextUInt64());
        Assert.Equal(17057574109182124193UL, random.NextUInt64());
        Assert.Equal(18295552978065317476UL, random.NextUInt64());
    }

    [Fact]
    public void SameSeed_GivesSameStream_DifferentSeedDoesNot()
    {
        var a = new DeterministicRandom(7);
        var b = new DeterministicRandom(7);
        var c = new DeterministicRandom(8);

        var first = Enumerable.Range(0, 100).Select(_ => a.NextInt(0, 1000)).ToArray();
        var second = Enumerable.Range(0, 100).Select(_ => b.NextInt(0, 1000)).ToArray();
        var third = Enumerable.Range(0, 100).Select(_ => c.NextInt(0, 1000)).ToArray();

        Assert.Equal(first, second);
        Assert.NotEqual(first, third);
    }

    [Fact]
    public void NextInt_StaysInRangeAndCoversIt()
    {
        var random = new DeterministicRandom(1);
        var seen = new HashSet<int>();
        for (var i = 0; i < 10_000; i++)
        {
            var value = random.NextInt(-3, 4);
            Assert.InRange(value, -3, 3);
            seen.Add(value);
        }

        Assert.Equal(7, seen.Count);
        Assert.Throws<ArgumentOutOfRangeException>(() => random.NextInt(5, 5));
    }

    [Fact]
    public void NextDouble_IsInUnitInterval()
    {
        var random = new DeterministicRandom(3);
        var values = Enumerable.Range(0, 10_000).Select(_ => random.NextDouble()).ToArray();

        Assert.All(values, v => Assert.InRange(v, 0.0, 0.9999999999));
        Assert.InRange(values.Average(), 0.48, 0.52);
    }

    [Fact]
    public void Simulator_RunsEventsInTimeThenInsertionOrder()
    {
        var sim = new Simulator();
        var log = new List<string>();
        sim.Schedule(TimeSpan.FromMilliseconds(5), () => log.Add("b"));
        sim.Schedule(TimeSpan.FromMilliseconds(1), () => log.Add("a"));
        sim.Schedule(TimeSpan.FromMilliseconds(5), () => log.Add("c"));
        sim.Schedule(TimeSpan.FromMilliseconds(5), () => sim.After(TimeSpan.Zero, () => log.Add("e")));
        sim.Schedule(TimeSpan.FromMilliseconds(5), () => log.Add("d"));

        sim.RunUntil(TimeSpan.FromMilliseconds(10));

        Assert.Equal("abcde", string.Concat(log));
        Assert.Equal(TimeSpan.FromMilliseconds(10), sim.Now);
        Assert.Equal(6, sim.Steps);
    }

    [Fact]
    public void Simulator_LeavesLaterEventsQueuedAndHonoursStop()
    {
        var sim = new Simulator();
        var count = 0;
        for (var i = 1; i <= 10; i++)
        {
            sim.Schedule(TimeSpan.FromMilliseconds(i), () => count++);
        }

        sim.RunUntil(TimeSpan.FromMilliseconds(4));
        Assert.Equal(4, count);
        Assert.Equal(6, sim.Pending);

        sim.RunUntil(TimeSpan.FromMilliseconds(100), stop: () => count == 7);
        Assert.Equal(7, count);
        Assert.Equal(TimeSpan.FromMilliseconds(7), sim.Now);
    }

    [Fact]
    public void Simulator_ClampsPastEventsToNow()
    {
        var sim = new Simulator();
        sim.RunUntil(TimeSpan.FromMilliseconds(50));
        var ranAt = TimeSpan.Zero;

        sim.Schedule(TimeSpan.FromMilliseconds(10), () => ranAt = sim.Now);
        sim.RunUntil(TimeSpan.FromMilliseconds(60));

        Assert.Equal(TimeSpan.FromMilliseconds(50), ranAt);
    }
}
