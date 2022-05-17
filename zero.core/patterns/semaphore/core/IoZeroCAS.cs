using System.Runtime.CompilerServices;
using System.Threading;

namespace zero.core.patterns.semaphore.core
{
    public static class IoZeroCAS
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ZeroNext(this ref long val, in long cap)
        {
            long inc;
            long latch;
            while ((inc = (latch = val) + 1) >= cap || Interlocked.CompareExchange(ref val, inc, latch) != latch)
            {
                if (inc >= cap)
                    return cap;
            }

            return latch;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ZeroNextBounded(this ref long val, in long cap)
        {
            long inc;
            long latch;
            while ((inc = (latch = val) + 1) > cap || Interlocked.CompareExchange(ref val, inc, latch) != latch)
            {
                if (inc > cap)
                    return cap;
            }

            return latch;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ZeroInc(this ref long val, in long cap) => ZeroNextBounded(ref val, in cap) + 1;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ZeroDecCap(this ref long val, long cap)
        {
            long slot;
            long latch;
            while ((slot = (latch = val) - 1) < cap || Interlocked.CompareExchange(ref val, slot, latch) != latch)
            {
                if (slot < cap)
                    return cap;
            }

            return slot;
        }
    }
}
