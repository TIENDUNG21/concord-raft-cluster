using System.Globalization;

namespace Concord.Sim;

internal sealed class UsageException(string message) : Exception(message);

/// <summary>Minimal "command --name value --flag" parser with strict validation.</summary>
internal sealed class CommandLine
{
    private readonly Dictionary<string, string?> _options;

    private CommandLine(string command, Dictionary<string, string?> options)
    {
        Command = command;
        _options = options;
    }

    public string Command { get; }

    public static CommandLine Parse(IReadOnlyList<string> args, IReadOnlySet<string> flags)
    {
        if (args.Count == 0 || args[0].StartsWith('-'))
        {
            return new CommandLine("help", new Dictionary<string, string?>(StringComparer.Ordinal));
        }

        var options = new Dictionary<string, string?>(StringComparer.Ordinal);
        for (var i = 1; i < args.Count; i++)
        {
            var arg = args[i];
            if (!arg.StartsWith("--", StringComparison.Ordinal) || arg.Length == 2)
            {
                throw new UsageException($"unexpected argument '{arg}'");
            }

            var name = arg[2..];
            if (options.ContainsKey(name))
            {
                throw new UsageException($"option --{name} given twice");
            }

            if (flags.Contains(name))
            {
                options[name] = null;
            }
            else if (i + 1 < args.Count && !args[i + 1].StartsWith("--", StringComparison.Ordinal))
            {
                options[name] = args[++i];
            }
            else
            {
                throw new UsageException($"option --{name} needs a value");
            }
        }

        return new CommandLine(args[0], options);
    }

    public void AllowOnly(params string[] names)
    {
        foreach (var name in _options.Keys)
        {
            if (!names.Contains(name, StringComparer.Ordinal))
            {
                throw new UsageException($"unknown option --{name} for '{Command}'");
            }
        }
    }

    public bool Has(string flag) => _options.ContainsKey(flag);

    public string? String(string name) => _options.GetValueOrDefault(name);

    public long Number(string name, long fallback, long min, long max)
    {
        if (!_options.TryGetValue(name, out var text) || text is null)
        {
            return fallback;
        }

        if (!long.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var value) || value < min || value > max)
        {
            throw new UsageException($"--{name} must be a whole number between {min} and {max}, got '{text}'");
        }

        return value;
    }
}
