using System.Globalization;

namespace Concord.Checker.Tests;

/// <summary>
/// Property tests: histories generated from a real sequential execution, with each operation's interval
/// stretched randomly around its linearization point, must always pass; corrupting one observed value
/// must make them fail.
/// </summary>
public sealed class RandomHistoryTests
{
    private static List<Timed<KvOperation>> Generate(int seed, int operations, int keys)
    {
        var random = new Random(seed);
        var state = new Dictionary<string, string>();
        var history = new List<Timed<KvOperation>>();
        for (var i = 0; i < operations; i++)
        {
            var point = (i * 10) + 5;
            var key = "k" + random.Next(keys).ToString(CultureInfo.InvariantCulture);
            state.TryGetValue(key, out var current);
            var value = "v" + i.ToString(CultureInfo.InvariantCulture);
            var op = random.Next(4) switch
            {
                0 => new KvOperation(i, KvOpKind.Get, key, null, null, new KvOutcome(current is not null, current)),
                1 => new KvOperation(i, KvOpKind.Put, key, value, null, new KvOutcome(true, null)),
                2 => new KvOperation(i, KvOpKind.Delete, key, null, null, new KvOutcome(current is not null, null)),
                _ => Cas(key, current, random.Next(2) == 0 ? current : "other", value, i),
            };

            switch (op.Kind)
            {
                case KvOpKind.Put:
                    state[key] = value;
                    break;
                case KvOpKind.Delete:
                    state.Remove(key);
                    break;
                case KvOpKind.CompareAndSet when op.Outcome!.Succeeded:
                    state[key] = value;
                    break;
            }

            // Stretch the interval around the linearization point: overlaps with neighbours appear.
            var call = point - random.Next(0, 25);
            var ret = random.Next(10) == 0 ? long.MaxValue : point + random.Next(0, 25);
            if (ret == long.MaxValue)
            {
                op = op with { Outcome = null };
            }

            history.Add(new Timed<KvOperation>(op, call, ret));
        }

        return history;
    }

    private static KvOperation Cas(string key, string? current, string? expected, string value, int client)
    {
        var swapped = current == expected;
        return new KvOperation(client, KvOpKind.CompareAndSet, key, value, expected, new KvOutcome(swapped, swapped ? value : current));
    }

    public static TheoryData<int> Seeds()
    {
        var data = new TheoryData<int>();
        for (var seed = 1; seed <= 40; seed++)
        {
            data.Add(seed);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void HistoriesFromSequentialExecutions_AreLinearizable(int seed)
    {
        var history = Generate(seed, 150, 3);

        var result = KvLinearizability.Check(history);

        Assert.True(result.Outcome == CheckOutcome.Ok, result.Explanation);
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void CorruptingAnObservedValue_IsDetected(int seed)
    {
        var history = Generate(seed, 150, 3);
        var index = history.FindLastIndex(t => t.Operation.Kind == KvOpKind.Get && t.Operation.Outcome is { Succeeded: true });
        if (index < 0)
        {
            return;
        }

        var corrupted = history[index] with { Operation = history[index].Operation with { Outcome = new KvOutcome(true, "corrupt") } };
        history[index] = corrupted;

        Assert.Equal(CheckOutcome.Illegal, KvLinearizability.Check(history).Outcome);
    }
}
