﻿using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using zero.core.misc;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore;
using zero.core.runtime.scheduler;

namespace zero.test.core.patterns.semaphore
{
    public class IoZeroSemaphoreTest
    {
        private const int ERR_T = 16 * 30;
        public IoZeroSemaphoreTest(ITestOutputHelper output)
        {
            _output = output;
            var prime = IoZeroScheduler.ZeroDefault;
            if (prime.Id > 1)
                Console.WriteLine("using IoZeroScheduler");
        }
        private readonly ITestOutputHelper _output;
        private volatile bool _running;

        [Fact]
        async Task TestMutexModeAsync()
        {
            _running = true;
#if DEBUG
            int loopCount = 10;
            int targetSleep = 100;
#else
            int loopCount = 20;
            int targetSleep = 50;
#endif
            var m = new IoZeroSemaphoreSlim(new CancellationTokenSource(), "test mutex", maxBlockers: 1, initialCount: 1);

            Assert.True((await m.WaitAsync().FastPath()).ElapsedMs() < 0x7ffffff);

            var t = Task.Factory.StartNew(static async state =>
            {
                var (@this, m, targetSleep) = (ValueTuple<IoZeroSemaphoreTest, IoZeroSemaphoreSlim, int>)state!;
                while(@this._running)
                {
                    await Task.Delay(targetSleep);
                    m.Release(Environment.TickCount, true);
                }
            },(this,m,targetSleep), CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault);

            var c = 0;
            long ave = 0;
            while (c++ < loopCount)
            {
                var s = Environment.TickCount;
                Assert.True((await m.WaitAsync().FastPath()).ElapsedMs() < 0x7ffffff);
                var delta = Environment.TickCount - s;
                ave += delta;
                _output.WriteLine($"d = {delta}");
                if (delta < targetSleep * targetSleep || c > 1)//gitlab glitches on c == 0
                    Assert.InRange(delta, targetSleep/2, targetSleep * targetSleep);
            }

            _running = false;

            Assert.InRange(ave/loopCount, targetSleep/2, targetSleep * targetSleep);
        }


        private volatile int _releaseCount;
        [Fact]
        async Task PrefetchRushAsync()
        {
            await Task.Factory.StartNew(async state =>
            {
                var threads = 4;
                var preloadCount = short.MaxValue/3;
                var m = new IoZeroSemaphoreSlim(new CancellationTokenSource(), "test mutex", maxBlockers: preloadCount, initialCount: preloadCount, zeroAsyncMode:false);

                var c = 0;
                while (true)
                {
                    if (Interlocked.Increment(ref _releaseCount) < preloadCount)
                        await m.WaitAsync().FastPath();
                    else
                        break;

                    if (++c % 10000 == 0)
                        _output.WriteLine($"-> {c}");
                }

                Assert.Equal(_releaseCount, preloadCount);

                for (int i = 0; i < threads; i++)
                {
                    _ = Task.Factory.StartNew(async () =>
                    {
                        while (!m.Zeroed() && _releaseCount > 0)
                        {
                            if (Interlocked.Decrement(ref _releaseCount) >= 0)
                            {
                                if (!m.Release(Environment.TickCount, true))
                                    await Task.Delay(100);
                            }
                        }

                        return Task.CompletedTask;
                    }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault);
                }

                while (true)
                {
                    await m.WaitAsync().FastPath();

                    if (_releaseCount > preloadCount - 5)
                        _output.WriteLine($"<- {_releaseCount}");

                    if (_releaseCount < 5)
                        _output.WriteLine($"<- {_releaseCount}");
                    if (_releaseCount <= 0)
                    {
                        await m.DisposeAsync(null, "test done");
                        break;
                    }
                }

                Assert.InRange(_releaseCount, -2, 0);
            },this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap();
        }

        [Fact]
        async Task PrefetchAsync()
        {
            var m = new IoZeroSemaphoreSlim(new CancellationTokenSource(), "test mutex", maxBlockers: 3, initialCount: 3, zeroAsyncMode:false);

            await Task.Factory.StartNew(async () =>
            {
                await Task.Delay(500);
                m.Release(Environment.TickCount, true);
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault);

            var ts = Environment.TickCount;
            Assert.Equal(3, m.ReadyCount);
            await m.WaitAsync().FastPath();
            Assert.Equal(2, m.ReadyCount);
            Assert.Equal(0, m.WaitCount);
            await m.WaitAsync().FastPath();
            Assert.Equal(1, m.ReadyCount);
            Assert.Equal(0, m.WaitCount);
            await m.WaitAsync().FastPath();
            Assert.Equal(0, m.ReadyCount);
            Assert.Equal(0, m.WaitCount);
            Assert.InRange(ts.ElapsedMs(), 0, ERR_T);
            await m.WaitAsync().FastPath();
            Assert.InRange(ts.ElapsedMs(),400, 2000);
            Assert.Equal(0, m.WaitCount);
        }


        [Fact]
        async Task ReleaseAsync()
        {
            var m = new IoZeroSemaphoreSlim(new CancellationTokenSource(), "test mutex", maxBlockers: 10, initialCount: 3, zeroAsyncMode: false);
            
            await Task.Factory.StartNew(async () =>
            {
                m.Release(Environment.TickCount, 2);
                await Task.Delay(500);
                m.Release(Environment.TickCount);
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault);

            var ts = Environment.TickCount;

            await m.WaitAsync();
            await m.WaitAsync();
            await m.WaitAsync();
            await m.WaitAsync();
            await m.WaitAsync();

            Assert.InRange(ts.ElapsedMs(), 0, ERR_T);
            await m.WaitAsync();
            Assert.InRange(ts.ElapsedMs(), 400, 2000);
        }

        [Fact]
        async Task MutexSpamAsync()
        {
            var m = new IoZeroSemaphoreSlim(new CancellationTokenSource(), "test mutex", maxBlockers: 2, initialCount: 1, zeroAsyncMode:false);
            var running = true;
            var count = 1000000;
            var waits = 0;
            var scheduler = IoZeroScheduler.ZeroDefault;

            var t2 = Task.Factory.StartNew(async () =>
            {
                while (running)
                {
                    var ts = Environment.TickCount;
                    var r = 0;
                    try
                    {
                        r = await m.WaitAsync().FastPath();
                    }
                    catch
                    {
                        break;
                    }

                    if (ts.ElapsedMs() > 15 * 2 || r.ElapsedMs() > 500)
                    {
                        _output.WriteLine($"DQ {waits} took {ts.ElapsedMs()}, r = {r.ElapsedMs()} ms!!!");
                    }
                    
                    waits++;

                    if (waits % 1000000 == 0)
                    {
                        _output.WriteLine($"-> {waits}");
                    }
                }

                _output.WriteLine($"Wait done {waits / 1000000}M");
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, scheduler).Unwrap();

            await Task.Delay(20);

            var t1 = Task.Factory.StartNew(() =>
            {
                var ts = Environment.TickCount;
                
                while (count --> 0)
                {
                    try
                    {
                        Assert.True(m.Release(Environment.TickCount, true));
                    }
                    catch (Exception e)
                    {
                        _output.WriteLine($"FAIL! -> {ts.ElapsedMs()} ms ({e.Message})");
                    }
                }

                running = false;
                m.ZeroSem();

                _output.WriteLine("Release done");
                return Task.CompletedTask;
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, scheduler).Unwrap();

            var ts = Environment.TickCount;
            await Task.WhenAll(t1, t2).WaitAsync(TimeSpan.FromSeconds(60));

            _output.WriteLine($"Test done... {ts.ElapsedMs()}ms - {waits/(double)(ts.ElapsedMs()/1000+1)} dq/ps");
            Assert.Equal(0, m.WaitCount);
            Assert.InRange(waits, count, int.MaxValue);
        }

        [Fact]
        async Task TestIoZeroSemaphoreSlimAsync()
        {
            var count = 50;
            var minDelay = 16 * 10;
            var v = new IoZeroSemaphoreSlim(new CancellationTokenSource(), string.Empty, 1, 0);
            //v.Release(true);
            var t = Task.Factory.StartNew(async () =>
            {
                for (int i = 0; i < count; i++)
                {
                    await Task.Delay(minDelay);
                    //_output.WriteLine($"R");
                    try
                    {
                        Assert.Equal(0, v.ReadyCount);
                    }
                    catch (Exception e)
                    {
                        _output.WriteLine($"{e.Message}: RELEASE FAILED!");
                    }
                    v.Release(Environment.TickCount, true);
                }
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap();

            var ts = Environment.TickCount;
            //Assert.True(await v.WaitAsync().FastPath());
            //Assert.Equal(0,v.ReadyCount);
            //Assert.InRange(ts.ElapsedMs(), 0, minDelay * 2);

            for (var i = 0; i < count; i++)
            {
                ts = Environment.TickCount;
                Assert.Equal(0, v.ReadyCount);
                if ((await v.WaitAsync().FastPath()).ElapsedMs() > 0x7ffffff)
                {
                    Assert.Equal(0, v.ReadyCount);
                    _output.WriteLine($"FAIL[{Environment.CurrentManagedThreadId}] -> {i} -> {v.EgressCount}, r = {v.ReadyCount}");
                    Assert.Fail("Expected true");
                }
                else
                    //if (i % 10 == 0)
                {
                    Assert.Equal(0, v.ReadyCount);
                    //_output.WriteLine($"DQ[{Environment.CurrentManagedThreadId}] -> {i} -> {v.EgressCount}, r = {v.ReadyCount}");
                }
                Assert.InRange(ts.ElapsedMs(), minDelay / 2, minDelay * 3);
            }

            await t;
            if(!t.IsCompletedSuccessfully)
                Assert.Fail($"Enqueue failed {t.Exception}");
        }

        [Fact]
        async Task TestIoZeroSemaphoreSlimSpamAsync()
        {
#if DEBUG
            long count = 1000000;
#else
            long count = 10000000;
#endif

            var v = new IoZeroSemaphoreSlim(new CancellationTokenSource(), string.Empty, 2, 0);

            var totalTime = Environment.TickCount;

            

            var t2 = Task.Factory.StartNew(async () =>
            {
                long ave = 0;
                int waiters = 1;
                try
                {
                    var i = 0;
                    for (i = 0; i < count; i++)
                    {
                        var ts = Environment.TickCount;
                        Interlocked.Decrement(ref waiters);
                        await v.WaitAsync().FastPath();
                        Interlocked.Increment(ref waiters);
                        ave += ts.ElapsedMs();
                        if (i % 100000 == 0)
                            _output.WriteLine($"DQ - {i} ({count}); w = {waiters}");
                    }
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                }

                Assert.InRange(ave / count, 0, 16);
                v.ZeroSem();
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap();

            await Task.Delay(200);

            var t = Task.Factory.StartNew(() =>
            {
                int i = 0;
                while (!v.Zeroed())
                {
                    if (!v.Release(Environment.TickCount, true))
                    {
                        //_output.WriteLine($"{i} - Jammed!");
                        //await Task.Delay(0);
                    }
                    else
                    {
                        if (i++ > count)
                        {
                            _output.WriteLine($"Done inserting {count}");
                            break;
                        }
                        if( i % 10000000 == 0)
                            _output.WriteLine($"EQ - {i}");
                    }
                        
                }

                return Task.CompletedTask;
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap();

            await Task.WhenAll(t,t2).WaitAsync(TimeSpan.FromSeconds(30));
            
            var maps = count * 1000 / (totalTime.ElapsedMs() + 1) / 1000;
            _output.WriteLine($"MAPS = {maps} K/s, t = {totalTime.ElapsedMs()}ms");
            Assert.InRange(maps, 1, int.MaxValue);
        }
    }
}
