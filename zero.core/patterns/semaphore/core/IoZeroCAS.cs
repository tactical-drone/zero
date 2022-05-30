using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using NLog;
using zero.core.misc;

namespace zero.core.patterns.semaphore.core
{
    public static class IoZeroCAS
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ZeroNextBounded(this ref long val, long cap)
        {
            if (val + 1 >= cap)
                return cap;

            long inc;
            long latch;
            while ((inc = (latch = val) + 1) >= cap || Interlocked.CompareExchange(ref val, inc, latch) != latch)
            {
                if (inc >= cap)
                    return cap;
            }

            return latch;
        }
        static object _syncroot = new object();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ZeroNext(this ref long val, long cap)
        {
            if (val + 1 > cap)
                return cap;


            long latch;
            //lock (_syncroot)
            {
#if DEBUG
                var ts = Environment.TickCount;
#endif
                try
                {
                    while ((latch = val) + 1 > cap || Interlocked.CompareExchange(ref val, val + 1, val) != latch)
                    {
                        if (val + 1 > cap)
                            return cap;
                    }
                }
                finally
                {
#if DEBUG
                    if (ts.ElapsedMs() > 16)
                    {
                        LogManager.GetCurrentClassLogger().Fatal($"{nameof(ZeroNext)}: CAS took => {ts.ElapsedMs()} ms");
                    }
#endif
                }
                Debug.Assert(latch < cap);
                return latch>cap?cap:latch;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ZeroPrev(this ref long val, long cap)
        {
            if (val - 1 < cap)
                return cap;

            long latch;
            while ((latch = val) - 1 < cap || Interlocked.CompareExchange(ref val, latch - 1, latch) != latch)
            {
                if (latch - 1 < cap)
                    return cap;
            }

            return latch;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ZeroNext(this ref int val, int cap)
        {
            if (val + 1 > cap)
                return cap;

            int inc;
            int latch;
            while ((inc = (latch = val) + 1) > cap || Interlocked.CompareExchange(ref val, inc, latch) != latch)
            {
                if (inc > cap)
                    return cap;
            }

            return latch;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ZeroPrev(this ref int val, int cap)
        {
            int dec;
            int latch;
            while ((dec = (latch = val) - 1) < cap || Interlocked.CompareExchange(ref val, dec, latch) != latch)
            {
                if (dec < cap)
                    return cap;
            }

            return latch;
        }

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public static void EnsureRelease<T>(this IIoZeroSemaphoreBase<T> s, T val, bool forceAsync = false, int cmp = 1)
        //{
        //    var c = 300;
        //    while (s.Release(val, forceAsync) != cmp && s.ReadyCount != cmp && !s.Zeroed())
        //    {
        //        if (c-- < 0)
        //        {
        //            LogManager.GetCurrentClassLogger().Fatal($"{nameof(EnsureRelease)}: Ensuring {cmp} release(s) [FAILED], ready = {s.ReadyCount}, {s.Description}");
        //            s.ZeroSem();
        //            break;
        //        }
        //        Thread.Yield();
        //    }
        //}
    }
}
