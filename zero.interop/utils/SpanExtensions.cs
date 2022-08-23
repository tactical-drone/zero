using System;
using System.Runtime.InteropServices;

namespace zero.interop.utils
{
    public static class SpanExtensions
    {
        public static T[] AsArray<T>(this ReadOnlyMemory<T> memory)
        {            
            return MemoryMarshal.TryGetArray(memory, out var arraySegment) ? arraySegment.Array : null;            
        }

        public static T[] AsArray<T>(this ReadOnlyMemory<T> memory, int start, int offset)
        {
            return MemoryMarshal.TryGetArray(memory, out var arraySegment) ? arraySegment.Slice(start,offset).Array : null;
        }
    }
}
