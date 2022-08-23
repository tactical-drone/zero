using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace zero.gauge.core.misc
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("CodeQuality", "IDE0051:Remove unused private members", Justification = "<Pending>")]
    public class InterlockedGauge
    {
        private int _nonVolatile;
        private volatile int _volatile;

        private long _nonVolatile64;

        [Benchmark]
        private void SingleThreadedNonVolatileInt32()
        {
            _nonVolatile++;
        }

        [Benchmark]
        private void SingleThreadedVolatileInt32_interlocked()
        {
            Interlocked.Increment(ref _volatile);
        }

        [Benchmark]
        private void SingleThreadedNonVolatileInt32_interlocked()
        {
            Interlocked.Increment(ref _nonVolatile);
        }

        [Benchmark]
        private void SingleThreadedNonVolatileInt64()
        {
            _nonVolatile64++;
        }

        [Benchmark]
        private void SingleThreadedVolatileInt64_interlocked()
        {
            Interlocked.Increment(ref _nonVolatile64);
        }

        [Benchmark]
        private void SingleThreadedNonVolatileInt64_interlocked()
        {
            Interlocked.Increment(ref _nonVolatile64);
        }

        private void StNvCompareInt32()
        {
            var c = 100000000;
            while(c--> 0)
                Interlocked.CompareExchange(ref _nonVolatile, 1, 0);
        }

        private void MtNvCompareInt32()
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

        private void StNvCompareInt64()
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

        private void MtNvCompareInt64()
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
