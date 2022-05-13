using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using zero.core.misc;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore.core;

namespace zero.test.core.patterns.semaphore
{
    public class IoZeroSemCoreTest
    {
        private const int ERR_T = 16 << 1;
        private readonly ITestOutputHelper _output;

        public IoZeroSemCoreTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        async Task SmokeReadyTestAsync()
        {
            var threads = 10;
            var delayTime = 50;
            IIoZeroSemaphoreBase<int> m = new IoZeroSemCore<int>("test", threads, threads);
            m.ZeroRef(ref m, _ => Environment.TickCount);

            await Task.Factory.StartNew(async () =>
            {
                for (int i = 0; i < threads * 2; i++)
                {
                    await Task.Delay(delayTime);
                    var ts = Environment.TickCount;
                    m.Release(Environment.TickCount);
                    Assert.InRange(ts.ElapsedMs(), 0, delayTime + ERR_T);
                    _output.WriteLine($"R -> {i} {ts.ElapsedMs()}ms");
                }
            });

            int ts;
            for (int i = 0; i < threads; i++)
            {
                ts = Environment.TickCount;
                var qt = await m.WaitAsync().FastPath();
                Assert.InRange(ts.ElapsedMs(), 0, delayTime + ERR_T);
                Assert.InRange(qt.ElapsedMs(), 0, ERR_T);
                _output.WriteLine($"P -> {i} {ts.ElapsedMs()}ms");
            }
            
            _output.WriteLine("pre-load done...");

            for (int i = 0; i < threads; i++)
            {
                ts = Environment.TickCount;
                var qt = await m.WaitAsync().FastPath();
                Assert.InRange(ts.ElapsedMs(), delayTime - ERR_T, delayTime + ERR_T);
                Assert.InRange(qt.ElapsedMs(), 0, ERR_T);
                _output.WriteLine($"DQ => {ts.ElapsedMs()}ms");
            }
        }

        [Fact]
        async Task SmokeSpamTestAsync()
        {
            var threads = 25;
            var spamFactor = 1;
            var delayTime = 0;
            IIoZeroSemaphoreBase<int> m = new IoZeroSemCore<int>("test", threads, threads, false);
            m.ZeroRef(ref m, _ => Environment.TickCount);

            _ = Task.Factory.StartNew(async () =>
            {
                for (int i = 0; i < threads * spamFactor + threads; i++)
                {
                    await Task.Delay(delayTime);

                    var ts = Environment.TickCount;
                    if (m.Release(Environment.TickCount) <= 0)
                    {
                        i--;
                        await Task.Delay(1);
                        _output.WriteLine($"D -> {i}, {ts.ElapsedMs()}ms");
                    }

                    _output.WriteLine($"R -> {i}, {ts.ElapsedMs()}ms");
                }
                _output.WriteLine($"Done signalling");
            });

            int ts;
            for (int i = 0; i < threads; i++)
            {
                ts = Environment.TickCount;
                var qt = await m.WaitAsync().FastPath();
                Assert.InRange(ts.ElapsedMs(), 0, delayTime + ERR_T);
                Assert.InRange(qt.ElapsedMs(), 0, ERR_T);
                _output.WriteLine($"P -> {i}, {ts.ElapsedMs()}ms");
            }

            _output.WriteLine("pre-load done...");

            for (int i = 0; i < threads * spamFactor; i++)
            {
                ts = Environment.TickCount;
                var qt = await m.WaitAsync().FastPath();
                Assert.InRange(ts.ElapsedMs(), delayTime - ERR_T, delayTime + ERR_T);
                Assert.InRange(qt.ElapsedMs(), 0, ERR_T);
                _output.WriteLine($"DQ -> {i}, {ts.ElapsedMs()}ms");
            }
        }

        [Fact]
        async Task ReadyTestAsync()
        {
            var batchLog = 50;
            var threads = 100;
            var delayTime = 1;
            IIoZeroSemaphoreBase<int> m = new IoZeroSemCore<int>("test", threads, threads);
            m.ZeroRef(ref m, _ => Environment.TickCount);

            await Task.Factory.StartNew(async () =>
            {
                var t = Environment.TickCount;
                for (int i = 0; i < threads * 2; i++)
                {
                    await Task.Delay(delayTime);
                    var ts = Environment.TickCount;
                    m.Release(Environment.TickCount);
                    Assert.InRange(ts.ElapsedMs(), 0, delayTime + ERR_T);
                    if(i % batchLog == 0)
                        _output.WriteLine($"R -> {i} {ts.ElapsedMs()}ms - {(double)i/t.ElapsedMsToSec():0.0} r/s");
                }
            });

            int ts;
            var t = Environment.TickCount;
            for (int i = 0; i < threads; i++)
            {
                ts = Environment.TickCount;
                var qt = await m.WaitAsync().FastPath();
                Assert.InRange(ts.ElapsedMs(), 0, delayTime + ERR_T);
                Assert.InRange(qt.ElapsedMs(), 0, ERR_T);
                if (i % batchLog == 0)
                    _output.WriteLine($"P -> {i} {ts.ElapsedMs()}ms - {(double)i / t.ElapsedMsToSec():0.0} r/s");
            }

            _output.WriteLine("pre-load done...");
            t  = Environment.TickCount;
            for (int i = 0; i < threads; i++)
            {
                ts = Environment.TickCount;
                var qt = await m.WaitAsync().FastPath();
                Assert.InRange(ts.ElapsedMs(), delayTime - ERR_T, delayTime + ERR_T);
                Assert.InRange(qt.ElapsedMs(), 0, ERR_T);
                if (i % batchLog == 0)
                    _output.WriteLine($"D -> {i} {ts.ElapsedMs()}ms - {(double)i / t.ElapsedMsToSec():0.0} r/s");
            }
        }

        [Fact]
        async Task SpamTestAsync()
        {
            var batchLog = 10000000;
            var threads = short.MaxValue>>1;
            var spamFactor = 2000;
            //var batchLog = 1;
            //var threads = 10;
            //var spamFactor = 1;
            var delayTime = 0;
            IIoZeroSemaphoreBase<int> m = new IoZeroSemCore<int>("test", threads, threads, false);
            m.ZeroRef(ref m, _ => Environment.TickCount);

            _ = Task.Factory.StartNew(async () =>
            {
                var t = Environment.TickCount;
                for (int i = 0; i < threads * spamFactor; i++)
                {
                    await Task.Delay(delayTime);

                    var ts = Environment.TickCount;
                    if (m.Release(Environment.TickCount) <= 0)
                    {
                        i--;
                        await Task.Delay(1);
                        _output.WriteLine($"D -> {i}, {ts.ElapsedMs()}ms");
                    }

                    if (i % batchLog == 0)
                        _output.WriteLine($"R -> {i} {ts.ElapsedMs()}ms - {(double)i / t.ElapsedMsToSec():0.0} r/s");
                }
                _output.WriteLine($"Done signalling");
            });

            int ts;
            var t = Environment.TickCount;
            for (int i = 0; i < threads; i++)
            {
                ts = Environment.TickCount;
                var qt = await m.WaitAsync().FastPath();
                Assert.InRange(ts.ElapsedMs(), 0, delayTime + ERR_T);
                Assert.InRange(qt.ElapsedMs(), 0, ERR_T);
                if (i % batchLog == 0)
                    _output.WriteLine($"P -> {i} {ts.ElapsedMs()}ms - {(double)i / t.ElapsedMsToSec():0.0} r/s");
            }

            _output.WriteLine($"pre-load done... ");
            t = Environment.TickCount;
            for (int i = 0; i < threads * spamFactor; i++)
            {
                ts = Environment.TickCount;
                var qt = await m.WaitAsync().FastPath();
                Assert.InRange(ts.ElapsedMs(), delayTime - ERR_T, delayTime + ERR_T);
                Assert.InRange(qt.ElapsedMs(), 0, ERR_T);
                if (i % batchLog == 0)
                    _output.WriteLine($"D -> {i} {ts.ElapsedMs()}ms - {(double)i / t.ElapsedMsToSec():0.0} r/s");
            }
        }
    }
}
