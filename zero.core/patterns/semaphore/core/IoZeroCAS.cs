using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace zero.core.patterns.semaphore.core;

public static class IoZeroCAS
{
    private const int Redundancy = 3;
    private static int _cheapMonitor = Redundancy;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ZeroNextBounded(this ref long val, long cap)
    {
        if (val == cap)
            return -1;

        long inc;
        long latch;
        var sw = new SpinWait();
        while ((inc = (latch = val) + 1) >= cap || Interlocked.CompareExchange(ref val, inc, latch) != latch)
        {
            if (inc >= cap)
                return -1;
            sw.SpinOnce();
        }

        return latch;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ZeroNextHard(this ref long val, long cap)
    {
        if (val == cap)
            return -1;

        //lock (_syncroot)
        {
#if DEBUG
            var ts = Environment.TickCount;
#endif
            try
            {
                retry:
                var curLevel = Redundancy;
                while (curLevel > 0)
                {
                    while (Interlocked.CompareExchange(ref _cheapMonitor, curLevel - 1, curLevel) != curLevel)
                    {
                        Debug.Assert(false);
                        Interlocked.MemoryBarrierProcessWide();
                        goto retry;
                    }

                    curLevel--;
                }

                Interlocked.MemoryBarrier();
                var latch = val + 1;
                return latch > cap ? cap : Interlocked.Exchange(ref val, latch);
            }
            finally
            {
                Interlocked.Exchange(ref _cheapMonitor, Redundancy);
            }

            //Interlocked.MemoryBarrier();
            //while ((latch = val) + 1 > cap || Interlocked.CompareExchange(ref val, latch + 1, latch) != latch)
            //{
            //    if (latch + 1 > cap)
            //        return cap;
            //    //Interlocked.MemoryBarrierProcessWide();
            //    //Interlocked.MemoryBarrier();
            //}
            //Debug.Assert(latch < cap);
            //return latch;
        }
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ZeroNext(this ref long val, long cap)
    {
        if (val >= cap)
            return -1;

        var sw = new SpinWait();
        long inc;
        long latch;
        while ((inc = (latch = val) + 1) > cap || Interlocked.CompareExchange(ref val, inc, latch) != latch)
        {
            if (inc > cap)
                return -1;
            sw.SpinOnce();
        }

        Debug.Assert(latch < cap);
        return latch;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ZeroPrev(this ref long val, long cap)
    {
        if (val <= cap)
            return -1;

        long latch;
        var sw = new SpinWait();
        while ((latch = val) - 1 < cap || Interlocked.CompareExchange(ref val, latch - 1, latch) != latch)
        {
            if (latch - 1 < cap)
                return -1;
            sw.SpinOnce();
        }

        return latch;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ZeroNext(this ref int val, int cap)
    {
        if (val >= cap)
            return -1;

        int inc;
        int latch;
        var sw = new SpinWait();
        while ((inc = (latch = val) + 1) > cap || Interlocked.CompareExchange(ref val, inc, latch) != latch)
        {
            if (inc > cap)
                return -1;
            sw.SpinOnce();
        }

        return latch;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ZeroPrev(this ref int val, int cap)
    {
        if (val <= cap)
            return -1;

        int dec;
        int latch;
        var sw = new SpinWait();
        while ((dec = (latch = val) - 1) < cap || Interlocked.CompareExchange(ref val, dec, latch) != latch)
        {
            if (dec < cap)
                return -1;
            sw.SpinOnce();
        }

        return latch;
    }
}