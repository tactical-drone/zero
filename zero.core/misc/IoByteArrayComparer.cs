using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace zero.core.misc
{
    /// <summary>
    /// Compares two hashed byte arrays quickly
    /// </summary>
    public class IoByteArrayComparer : EqualityComparer<byte[]>
    {
        public override bool Equals(byte[] first, byte[] second)
        {
            return first.SequenceEqual(second);
            //if (first == null || second == null)
            //{
            //    // null == null returns true.
            //    // non-null == null returns false.
            //    return first == second;
            //}
            //if (ReferenceEquals(first, second))
            //{
            //    return true;
            //}
            //if (first.Length != second.Length)
            //{
            //    return false;
            //}
            // Linq extension method is based on IEnumerable, must evaluate every item.
        }
        
        public override int GetHashCode(byte[] array)
        {
            return BitConverter.ToInt32(array, 0);
            //if (array == null)
            //{
            //    throw new ArgumentNullException("array");
            //}
            //if (array.Length >= 4)
            //{
            //    return BitConverter.ToInt32(array, 0);
            //}
            //// Length occupies at most 2 bits. Might as well store them in the high order byte
            //int value = array.Length;
            //foreach (var b in array)
            //{
            //    value <<= 8;
            //    value += b;
            //}
            //return value;
        }
    }

}
