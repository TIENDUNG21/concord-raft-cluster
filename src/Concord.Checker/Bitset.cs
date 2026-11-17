namespace Concord.Checker;

/// <summary>Immutable set of operation ids with value equality; the memoization key of the search.</summary>
internal readonly struct Bitset : IEquatable<Bitset>
{
    private readonly ulong[] _words;
    private readonly int _hash;

    public Bitset(int size)
        : this(new ulong[(size + 63) / 64])
    {
    }

    private Bitset(ulong[] words)
    {
        _words = words;
        var hash = new HashCode();
        foreach (var word in words)
        {
            hash.Add(word);
        }

        _hash = hash.ToHashCode();
    }

    public bool Contains(int id) => (_words[id >> 6] & (1UL << (id & 63))) != 0;

    public Bitset With(int id)
    {
        var words = (ulong[])_words.Clone();
        words[id >> 6] |= 1UL << (id & 63);
        return new Bitset(words);
    }

    public Bitset Without(int id)
    {
        var words = (ulong[])_words.Clone();
        words[id >> 6] &= ~(1UL << (id & 63));
        return new Bitset(words);
    }

    public bool Equals(Bitset other) => _hash == other._hash && _words.AsSpan().SequenceEqual(other._words);

    public override bool Equals(object? obj) => obj is Bitset other && Equals(other);

    public override int GetHashCode() => _hash;

    public static bool operator ==(Bitset left, Bitset right) => left.Equals(right);

    public static bool operator !=(Bitset left, Bitset right) => !left.Equals(right);
}
