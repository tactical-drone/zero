using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace zero.core.misc
{
    /// <summary>
    /// Compares two hashed byte arrays quickly
    /// </summary>
    public class IoByteArrayComparer : IEqualityComparer<byte[]>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(byte[] first, byte[] second)
        {
            return first.ArrayEqual(second);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(byte[] array)
        {
            return StructuralComparisons.StructuralEqualityComparer.GetHashCode(array);
        }
    }

}
