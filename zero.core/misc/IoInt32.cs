using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace zero.core.misc
{
	/// <summary>
	/// int class
	/// </summary>
	public class IoInt32
	{
        int _value;
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
			Interlocked.MemoryBarrier();
			return integer._value;
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
            if (one == null || two == null)
                return one == two;

            if (one == two)
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
        public int AtomicCas(int value, int cmp) => Interlocked.CompareExchange(ref _value, value, cmp);

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
}
