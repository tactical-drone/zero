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
    }
}
