﻿using System;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using zero.core.misc;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;
using zero.core.runtime.scheduler;

namespace zero.test.core.patterns.bushings
{
    public class IoConduitTest
    {
        public IoConduitTest(ITestOutputHelper output)
        {
            _output = output;
            var prime = IoZeroScheduler.ZeroDefault;
            if (prime.Id > 1)
                Console.WriteLine("using IoZeroScheduler");
        }
        private readonly ITestOutputHelper _output;

        [Fact]
        public async Task IoConduitSmokeAsync()
        {
            var concurrencyLevel = 1;
            var count = 20;
            var s1 = new IoZeroSource("zero source 1", false, concurrencyLevel + 1, concurrencyLevel, false, disableZero:true);
            var c1 = new IoConduit<IoZeroProduct>("conduit smoke test 1", null, s1, static (ioZero, _) => new IoZeroProduct("test product 1", ((IoConduit<IoZeroProduct>)ioZero).Source, 100));

            var z1 = Task.Factory.StartNew(async () => await c1.BlockOnReplicateAsync(), CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap();

            var ts = Environment.TickCount;
            var totalTime = count * 100;
            while (!z1.IsCompleted)
            {
                try
                {
                    if (c1.EventCount > count || ts.ElapsedMs() > totalTime * 3)
                    {
                        await c1.DisposeAsync(null, "test done");
                    }
                    _output.WriteLine($"{c1.EventCount}/{count}");
                    await Task.Delay(500);
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.Message);
                    throw;
                }
            }
            await z1.WaitAsync(TimeSpan.FromMilliseconds(totalTime * 4));

            Assert.InRange(ts.ElapsedMs(), totalTime/2, totalTime*2);
            _output.WriteLine($"{ts.ElapsedMs()}ms ~ {totalTime}ms");

            await Task.Delay(100);
            Assert.InRange(c1.EventCount, count, count*2);
            _output.WriteLine($"#event = {c1.EventCount} ~ {count}");
        }

        [Fact]
        public async Task IoConduitConcurrencySmokeAsync()
        {
            
            _output.WriteLine($"{IoZeroScheduler.Zero?.Id}");
            var concurrencyLevel = 10;
            var count = 1000;
            
            var s1 = new IoZeroSource("zero source 1", false, concurrencyLevel << 1, concurrencyLevel);
            var c1 = new IoConduit<IoZeroProduct>("conduit smoke test 1", null, s1, static (ioZero, _) 
                => new IoZeroProduct("test product 1", ((IoConduit<IoZeroProduct>)ioZero)?.Source, 16*3));

            var z1 = Task.Factory.StartNew(async () =>
            {
                await c1.BlockOnReplicateAsync();
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap();

            var ts = Environment.TickCount;

            var targetTime = count * 16 * 3 / concurrencyLevel;

            while (!z1.IsCompleted && !c1.Zeroed())
            {
                if (c1.EventCount > count || ts.ElapsedMs() > targetTime * 3)
                {
                    _output.WriteLine($"test done!!!!!!!!!!!");
                    await c1.DisposeAsync(null, "test done").FastPath();
                    break;
                }
            }

            await z1.WaitAsync(TimeSpan.FromMilliseconds(targetTime * 5));

            _output.WriteLine($"{ts.ElapsedMs()}ms ~ {targetTime}");
            Assert.InRange(ts.ElapsedMs(), targetTime/2, targetTime * 3);

            await Task.Delay(100);
            Assert.InRange(c1.EventCount, count, count*2);
            _output.WriteLine($"#event = {c1.EventCount} ~ {count}");
        }

        [Fact]
        public async Task IoConduitSpamAsync()
        {
#if DEBUG
            var count = 500000;
            var concurrencyLevel = 4;
#else
            var count = 3000000;
            var concurrencyLevel = Environment.ProcessorCount;
#endif
            var s1 = new IoZeroSource("zero source 1", false, concurrencyLevel + concurrencyLevel/2, concurrencyLevel);
            var c1 = new IoConduit<IoZeroProduct>("conduit spam test", null, s1, static (ioZero, _) 
                => new IoZeroProduct("test product 1", ((IoConduit<IoZeroProduct>)ioZero).Source, 0));

            var z1 = Task.Factory.StartNew(async () =>
            {
                await c1.BlockOnReplicateAsync();
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap();
            var ts = Environment.TickCount;
            
            var targetTime = count / concurrencyLevel;
            while (!z1.IsCompleted)
            {
                if (c1.EventCount > count || ts.ElapsedMs() > targetTime)
                {
                    _output.WriteLine(c1.DumpStats());
                    await c1.DisposeAsync(null, "test done");
                }
                _output.WriteLine($"{c1.EventCount}/{count}");
                await Task.Delay(2000);
            }
            await z1.WaitAsync(TimeSpan.FromMilliseconds(targetTime));

            var fpses = c1.EventCount / (double)ts.ElapsedMs();

#if DEBUG
            Assert.InRange(fpses, 0, int.MaxValue);
#else
            Assert.InRange(fpses, 5, int.MaxValue);
#endif
            _output.WriteLine($"FPSes = {fpses:0.0} kub/s, {ts.ElapsedMs()}ms ~ {targetTime}ms");

            await Task.Delay(100);
            Assert.InRange(c1.EventCount, count, int.MaxValue);
            _output.WriteLine($"#event = {c1.EventCount} ~ {count}");
        }

        //TODO
        [Fact]
        public async Task IoConduitHorizontalScaleSmokeAsync()
        {
            var count = 200;
            var totalTimeMs = count * 100;
            var concurrencyLevel = 10;
            var s1 = new IoZeroSource("zero source 1", false, concurrencyLevel * 2, concurrencyLevel, false, true);
            var c1 = new IoConduit<IoZeroProduct>("conduit smoke test 1", null, s1, static (ioZero, _) => new IoZeroProduct("test product 1", ((IoConduit<IoZeroProduct>)ioZero).Source, 100));

            var z1 = Task.Factory.StartNew(async () => await c1.BlockOnReplicateAsync(), CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap();

            var ts = Environment.TickCount;
            long last = -1;
            while (!z1.IsCompleted)
            {
                if (c1.EventCount > count || ts.ElapsedMs() > totalTimeMs / concurrencyLevel * 3)
                {
                    await c1.DisposeAsync(null, "test done");
                    break;
                }

                //if (last == c1.EventCount && last > 0 && last < count)
                //    Assert.Fail($"Producer stalled at {c1.EventCount}");
                
                _output.WriteLine((last = c1.EventCount).ToString());
                await Task.Delay(500);
            }

            await z1.WaitAsync(TimeSpan.FromMilliseconds(totalTimeMs / (double)concurrencyLevel) * 4);

            _output.WriteLine($"{ts.ElapsedMs()}ms ~ {totalTimeMs / concurrencyLevel}ms");
            Assert.InRange(ts.ElapsedMs(), totalTimeMs / concurrencyLevel / 2, totalTimeMs / concurrencyLevel * 2);
            

            await Task.Delay(100);
            Assert.InRange(c1.EventCount, count, count * 3);
            _output.WriteLine($"#event = {c1.EventCount} ~ {count}");
        }
    }
}
