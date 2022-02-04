using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace zero.gauge.core.misc
{
    public class InterlockedGauge
    {
        private int _nonVolatile;
        private volatile int _volatile;

        private long _nonVolatile64;

        [Benchmark]
        void SingleThreadedNonVolatileInt32()
        {
            _nonVolatile++;
        }

        [Benchmark]
        void SingleThreadedVolatileInt32_interlocked()
        {
            Interlocked.Increment(ref _volatile);
        }

        [Benchmark]
        void SingleThreadedNonVolatileInt32_interlocked()
        {
            Interlocked.Increment(ref _nonVolatile);
        }

        [Benchmark]
        void SingleThreadedNonVolatileInt64()
        {
            _nonVolatile64++;
        }

        [Benchmark]
        void SingleThreadedVolatileInt64_interlocked()
        {
            Interlocked.Increment(ref _nonVolatile64);
        }

        [Benchmark]
        void SingleThreadedNonVolatileInt64_interlocked()
        {
            Interlocked.Increment(ref _nonVolatile64);
        }

        void StNvCompareInt32()
        {
            var c = 100000000;
            while(c--> 0)
                Interlocked.CompareExchange(ref _nonVolatile, 1, 0);
        }

        void MtNvCompareInt32()
        {
            var c = 100000000;

            new Thread(() =>
            {
                var C = 100000000;
                while (C-- > 0)
                    Interlocked.CompareExchange(ref _nonVolatile, 1, 0);
            }).Start();

            while (c-- > 0)
                Interlocked.CompareExchange(ref _nonVolatile, 0, 1);
        }

        void StNvCompareInt64()
        {
            var c = 100000000;
            while (c-- > 0)
            {
                if(c%2 ==0)
                    Interlocked.CompareExchange(ref _nonVolatile64, 1, 0);
                else
                    Interlocked.CompareExchange(ref _nonVolatile64, 0, 1);
            }
                
        }

        void MtNvCompareInt64()
        {
            var c = 100000000;

            new Thread(() =>
            {
                var C = 100000000;
                while (C-- > 0)
                    Interlocked.CompareExchange(ref _nonVolatile64, 1, 0);
            }).Start();

            while (c-- > 0)
                Interlocked.CompareExchange(ref _nonVolatile64, 0, 1);
        }
    }
}
