using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace zero.core.misc
{
	/// <summary>
	/// long class
	/// </summary>
	public class IoInt64
	{
        readonly long _value;
        public IoInt64(long value)
		{
			_value = value;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static implicit operator IoInt64(long value)
		{
			return new IoInt64(value);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static implicit operator long(IoInt64 integer)
		{
			return integer._value;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static long operator +(IoInt64 one, IoInt64 two)
		{
			return one._value + two._value;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static IoInt64 operator +(long one, IoInt64 two)
		{
			return new IoInt64(one + (long)two);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static long operator -(IoInt64 one, IoInt64 two)
		{
			return one._value - two._value;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static IoInt64 operator -(long one, IoInt64 two)
		{
			return new IoInt64(one - (long)two);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static bool operator ==(long one, IoInt64 two)
		{
            if (two == null)
                return false;

			return one == two._value;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static bool operator !=(long one, IoInt64 two)
        {
            if (two == null)
                return false;

			return one != two._value;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static bool operator ==(IoInt64 one, IoInt64 two)
		{
            if (one == null || two == null)
                return one == two;

            if (one == two)
                return true;

			return one._value == two._value;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static bool operator !=(IoInt64 one, IoInt64 two)
        {
            if (one == null || two == null)
                return false;
            
            return one._value != two._value;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override bool Equals(object obj)
        {
            if (obj == null)
                return false;

			return ((IoInt64)obj)._value == _value;
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
