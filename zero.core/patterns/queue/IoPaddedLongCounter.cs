using System.Runtime.InteropServices;

namespace zero.core.patterns.queue;

// Split hot data across multiple cache lines for long values
[StructLayout(LayoutKind.Explicit, Size = 128)] // Cache line padding
public struct IoPaddedLongCounter
{
    [FieldOffset(64)] public long Index;
}