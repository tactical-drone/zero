using System.Runtime.CompilerServices;
using System.Threading;

namespace zero.core.misc;

/// <summary>
///     int class
/// </summary>
public class IoInt32
{
    private int _value;

    public IoInt32(int value)
    {
        _value = value;
        Interlocked.MemoryBarrier();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static implicit operator IoInt32(int value)
    {
        return new IoInt32(value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static implicit operator int(IoInt32 integer)
    {
        return Volatile.Read(ref integer._value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int operator +(IoInt32 one, IoInt32 two)
    {
        return one._value + two._value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IoInt32 operator +(int one, IoInt32 two)
    {
        return new IoInt32(one + (int)two);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int operator -(IoInt32 one, IoInt32 two)
    {
        return one._value - two._value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IoInt32 operator -(int one, IoInt32 two)
    {
        return new IoInt32(one - (int)two);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool operator ==(int one, IoInt32 two)
    {
        if (two == null)
            return false;

        return one == two._value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool operator !=(int one, IoInt32 two)
    {
        if (two == null)
            return false;

        return one != two._value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool operator ==(IoInt32 one, IoInt32 two)
    {
        if ((object)one == null || (object)two == null)
            return one == (object)two;

        if (one == (object)two)
            return true;

        return one._value == two._value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool operator !=(IoInt32 one, IoInt32 two)
    {
        if (one == null || two == null)
            return false;

        return one._value != two._value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AtomicAdd(int value)
    {
        Interlocked.Add(ref _value, value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int AtomicCas(int value, int cmp)
    {
        return Interlocked.CompareExchange(ref _value, value, cmp);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool Equals(object obj)
    {
        if (obj == null)
            return false;

        return ((IoInt32)obj)._value == _value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int GetHashCode()
    {
        return _value.GetHashCode();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override string ToString()
    {
        return _value.ToString();
    }
}