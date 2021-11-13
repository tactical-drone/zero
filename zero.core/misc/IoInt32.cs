using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace zero.core.misc
{
	/// <summary>
	/// int class
	/// </summary>
	public class IoInt32
	{
		int value = 0;

		public IoInt32(int value)
		{
			this.value = value;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static implicit operator IoInt32(int value)
		{
			return new IoInt32(value);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static implicit operator int(IoInt32 integer)
		{
			return integer.value;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static int operator +(IoInt32 one, IoInt32 two)
		{
			return one.value + two.value;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static IoInt32 operator +(int one, IoInt32 two)
		{
			return new IoInt32(one + two);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static int operator -(IoInt32 one, IoInt32 two)
		{
			return one.value - two.value;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static IoInt32 operator -(int one, IoInt32 two)
		{
			return new IoInt32(one - two);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static bool operator ==(int one, IoInt32 two)
		{
			return one == two.value;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static bool operator != (int one, IoInt32 two)
		{
			return one != two.value;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static bool operator ==(IoInt32 one, IoInt32 two)
		{
			return one.value == two.value;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static bool operator !=(IoInt32 one, IoInt32 two)
		{
			return one.value != two.value;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override bool Equals(object obj)
        {
			return ((IoInt32)obj).value == value;
        }

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override int GetHashCode()
        {
            return value.GetHashCode();
        }

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override string ToString()
        {
            return value.ToString();
        }
    }
}
