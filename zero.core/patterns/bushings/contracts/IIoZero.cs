using System.Runtime.CompilerServices;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushings.contracts;

public interface IIoZero : IIoNanite
{
    IIoSource IoSource { get; }

    bool IsArbitrating { get; }

    bool ZeroRecoveryEnabled { get; }

    long EventCount { get; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void IncEventCounter();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ZeroEventCounter();
}