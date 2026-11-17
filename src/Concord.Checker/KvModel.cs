using System.Globalization;

namespace Concord.Checker;

public enum KvOpKind
{
    Get,
    Put,
    Delete,
    CompareAndSet,
}

/// <summary>
/// What a client observed. Get: found + value. Put: succeeded. Delete: whether the key existed.
/// Compare-and-set: whether it swapped + the value after it.
/// </summary>
public sealed record KvOutcome(bool Succeeded, string? Value);

/// <summary>
/// One client operation on one key. A null <see cref="Outcome"/> means the client never learned the
/// result (timeout): any outcome is acceptable, and the operation may not have happened at all.
/// </summary>
public sealed record KvOperation(int Client, KvOpKind Kind, string Key, string? Value, string? Expected, KvOutcome? Outcome)
{
    public override string ToString()
    {
        var call = Kind switch
        {
            KvOpKind.Get => $"get({Key})",
            KvOpKind.Put => $"put({Key}, {Quote(Value)})",
            KvOpKind.Delete => $"delete({Key})",
            _ => $"cas({Key}, {Quote(Expected)} -> {Quote(Value)})",
        };
        var result = Outcome is null ? "?" : $"{(Outcome.Succeeded ? "ok" : "fail")} {Quote(Outcome.Value)}";
        return string.Create(CultureInfo.InvariantCulture, $"c{Client} {call} => {result}");
    }

    private static string Quote(string? value) => value is null ? "nil" : $"\"{value}\"";
}

/// <summary>The state of one key: its value, or absent.</summary>
public readonly record struct RegisterState(string? Value);

/// <summary>Sequential specification of a single key, used per partition of a KV history.</summary>
public sealed class KvRegisterModel : ISequentialModel<RegisterState, KvOperation>
{
    public static KvRegisterModel Instance { get; } = new();

    public RegisterState Initial => new(null);

    public bool TryApply(RegisterState state, KvOperation operation, out RegisterState after)
    {
        ArgumentNullException.ThrowIfNull(operation);
        var current = state.Value;
        var outcome = operation.Outcome;
        switch (operation.Kind)
        {
            case KvOpKind.Get:
                after = state;
                return outcome is null || (outcome.Succeeded == (current is not null) && outcome.Value == current);
            case KvOpKind.Put:
                after = new RegisterState(operation.Value);
                return outcome is null || outcome.Succeeded;
            case KvOpKind.Delete:
                after = new RegisterState(null);
                return outcome is null || outcome.Succeeded == (current is not null);
            case KvOpKind.CompareAndSet when current == operation.Expected:
                after = new RegisterState(operation.Value);
                return outcome is null || (outcome.Succeeded && outcome.Value == operation.Value);
            case KvOpKind.CompareAndSet:
                after = state;
                return outcome is null || (!outcome.Succeeded && outcome.Value == current);
            default:
                throw new ArgumentOutOfRangeException(nameof(operation), operation.Kind, "Unknown operation kind.");
        }
    }

    public string Describe(KvOperation operation) => operation?.ToString() ?? string.Empty;
}

/// <summary>
/// Linearizability of a whole KV history. Keys are independent objects, and linearizability is
/// compositional (Herlihy and Wing), so each key's sub-history is checked on its own: far cheaper
/// than one search over every operation.
/// </summary>
public static class KvLinearizability
{
    public static CheckResult Check(IReadOnlyList<Timed<KvOperation>> history, CheckOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(history);
        CheckResult? unknown = null;
        foreach (var partition in history.GroupBy(op => op.Operation.Key, StringComparer.Ordinal).OrderBy(g => g.Key, StringComparer.Ordinal))
        {
            var result = LinearizabilityChecker.Check(KvRegisterModel.Instance, partition.ToArray(), options);
            if (result.Outcome == CheckOutcome.Illegal)
            {
                return result with { Partition = partition.Key, Operations = history.Count };
            }

            if (result.Outcome == CheckOutcome.Unknown)
            {
                unknown ??= result with { Partition = partition.Key, Operations = history.Count };
            }
        }

        return unknown ?? CheckResult.Ok(history.Count);
    }
}
