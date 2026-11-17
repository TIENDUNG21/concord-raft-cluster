namespace Concord.Checker.Tests;

public sealed class KvLinearizabilityTests
{
    private const long Never = long.MaxValue;

    private static Timed<KvOperation> Put(int client, string key, string value, long call, long ret, bool? ok = true) =>
        new(new KvOperation(client, KvOpKind.Put, key, value, null, ok is null ? null : new KvOutcome(ok.Value, null)), call, ret);

    private static Timed<KvOperation> Get(int client, string key, string? seen, long call, long ret) =>
        new(new KvOperation(client, KvOpKind.Get, key, null, null, new KvOutcome(seen is not null, seen)), call, ret);

    private static Timed<KvOperation> Delete(int client, string key, bool existed, long call, long ret) =>
        new(new KvOperation(client, KvOpKind.Delete, key, null, null, new KvOutcome(existed, null)), call, ret);

    private static Timed<KvOperation> Cas(int client, string key, string? expected, string value, bool swapped, string? after, long call, long ret) =>
        new(new KvOperation(client, KvOpKind.CompareAndSet, key, value, expected, new KvOutcome(swapped, after)), call, ret);

    private static CheckOutcome Check(params Timed<KvOperation>[] history) => KvLinearizability.Check(history).Outcome;

    [Fact]
    public void EmptyHistory_IsLinearizable()
    {
        Assert.Equal(CheckOutcome.Ok, Check());
    }

    [Fact]
    public void SequentialHistory_IsLinearizable()
    {
        Assert.Equal(CheckOutcome.Ok, Check(
            Get(1, "x", null, 0, 1),
            Put(1, "x", "a", 2, 3),
            Get(2, "x", "a", 4, 5),
            Cas(2, "x", "a", "b", true, "b", 6, 7),
            Delete(1, "x", true, 8, 9),
            Get(1, "x", null, 10, 11)));
    }

    [Fact]
    public void ReadOfValueNeverWritten_IsRejected()
    {
        Assert.Equal(CheckOutcome.Illegal, Check(
            Put(1, "x", "a", 0, 10),
            Get(2, "x", "ghost", 20, 30)));
    }

    [Fact]
    public void StaleReadAfterCompletedWrite_IsRejected()
    {
        Assert.Equal(CheckOutcome.Illegal, Check(
            Put(1, "x", "a", 0, 10),
            Get(2, "x", null, 20, 30)));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("a")]
    public void ReadConcurrentWithWrite_MaySeeEitherValue(string? seen)
    {
        Assert.Equal(CheckOutcome.Ok, Check(
            Put(1, "x", "a", 0, 100),
            Get(2, "x", seen, 10, 20)));
    }

    [Fact]
    public void NewValueThenOldValue_IsRejected()
    {
        // Once one read observed the write, a later non-overlapping read cannot go back in time.
        Assert.Equal(CheckOutcome.Illegal, Check(
            Put(1, "x", "a", 0, 100),
            Get(2, "x", "a", 10, 20),
            Get(3, "x", null, 30, 40)));
    }

    [Fact]
    public void PorcupineRegisterExamples_GiveExpectedVerdicts()
    {
        Assert.Equal(CheckOutcome.Ok, Check(
            Put(0, "r", "100", 0, 100),
            Get(1, "r", "100", 25, 75),
            Get(2, "r", null, 30, 60)));

        Assert.Equal(CheckOutcome.Illegal, Check(
            Put(0, "r", "200", 0, 100),
            Get(1, "r", "200", 10, 30),
            Get(2, "r", null, 40, 90)));
    }

    [Fact]
    public void TwoConcurrentCompareAndSetsFromAbsent_CannotBothSucceed()
    {
        Assert.Equal(CheckOutcome.Illegal, Check(
            Cas(1, "lock", null, "a", true, "a", 0, 10),
            Cas(2, "lock", null, "b", true, "b", 5, 15)));

        Assert.Equal(CheckOutcome.Ok, Check(
            Cas(1, "lock", null, "a", true, "a", 0, 10),
            Cas(2, "lock", null, "b", false, "a", 5, 15)));
    }

    [Fact]
    public void FailedCompareAndSet_MustReportTheActualValue()
    {
        Assert.Equal(CheckOutcome.Illegal, Check(
            Put(1, "k", "real", 0, 10),
            Cas(2, "k", "other", "new", false, "imagined", 20, 30)));
    }

    [Fact]
    public void DeleteReportingExistence_MustMatchState()
    {
        Assert.Equal(CheckOutcome.Illegal, Check(Delete(1, "k", true, 0, 10)));
        Assert.Equal(CheckOutcome.Ok, Check(Put(1, "k", "v", 0, 10), Delete(2, "k", true, 20, 30), Delete(1, "k", false, 40, 50)));
    }

    [Theory]
    [InlineData("a")]
    [InlineData(null)]
    public void PendingWrite_MayOrMayNotHaveHappened(string? seen)
    {
        Assert.Equal(CheckOutcome.Ok, Check(
            Put(1, "x", "a", 0, Never, ok: null),
            Get(2, "x", seen, 50, 60)));
    }

    [Fact]
    public void PendingWrite_CannotBeUnseenOnceObserved()
    {
        Assert.Equal(CheckOutcome.Illegal, Check(
            Put(1, "x", "a", 0, Never, ok: null),
            Get(2, "x", "a", 50, 60),
            Get(2, "x", null, 70, 80)));
    }

    [Fact]
    public void Violation_IsReportedForTheRightKeyWithAnExplanation()
    {
        var result = KvLinearizability.Check(new[]
        {
            Put(1, "a", "1", 0, 10),
            Get(2, "a", "1", 20, 30),
            Put(1, "b", "1", 0, 10),
            Get(2, "b", null, 20, 30),
        });

        Assert.Equal(CheckOutcome.Illegal, result.Outcome);
        Assert.Equal("b", result.Partition);
        Assert.Contains("c2 get(b) => fail nil", result.Explanation, StringComparison.Ordinal);
    }

    [Fact]
    public void ReturnBeforeCall_IsRejectedAsMalformed()
    {
        Assert.Throws<ArgumentException>(() => KvLinearizability.Check(new[] { Get(1, "x", null, 10, 5) }));
    }

    [Fact]
    public void SearchBudget_YieldsUnknownInsteadOfHanging()
    {
        var history = Enumerable.Range(0, 40)
            .Select(i => Put(i, "x", i.ToString(System.Globalization.CultureInfo.InvariantCulture), 0, 1000))
            .Append(Get(99, "x", "nope", 2000, 2001))
            .ToArray();

        var result = KvLinearizability.Check(history, new CheckOptions { MaxSteps = 1000 });

        Assert.Equal(CheckOutcome.Unknown, result.Outcome);
    }
}
