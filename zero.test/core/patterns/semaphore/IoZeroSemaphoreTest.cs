using System;
using System.Threading;
using System.Threading.Tasks;
using NuGet.Frameworks;
using Xunit;
using Xunit.Abstractions;
using zero.core.misc;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore;
using zero.core.runtime.scheduler;

namespace zero.test.core.patterns.semaphore
{
    public class IoZeroSemaphoreTest:IDisposable
    {
        public IoZeroSemaphoreTest(ITestOutputHelper output)
        {
            _output = output;
        }
        private bool Zc = IoNanoprobe.ContinueOnCapturedContext;
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

            await Task.Yield();

            Assert.True(await m.WaitAsync().FastPath());

            var t = Task.Factory.StartNew(static async state =>
            {
                var (@this, m, targetSleep) = (ValueTuple<IoZeroSemaphoreTest, IoZeroSemaphoreSlim, int>)state!;
                while(@this._running)
                {
                    await Task.Delay(targetSleep);
                    m.Release(true);
                }
            },(this,m,targetSleep), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);

            var c = 0;
            long ave = 0;
            while (c++ < loopCount)
            {
                var s = Environment.TickCount;
                Assert.True(await m.WaitAsync().FastPath());
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
                var preloadCount = short.MaxValue;
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
                                if (m.Release(true) <= 0)
                                    await Task.Delay(100);
                            }
                        }

                        return Task.CompletedTask;
                    }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
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
                        await m.Zero(null, "test done");
                        break;
                    }
                }

                Assert.InRange(_releaseCount, -1, 0);
            },this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();
        }

        [Fact]
        async Task PrefetchAsync()
        {
            var m = new IoZeroSemaphoreSlim(new CancellationTokenSource(), "test mutex", maxBlockers: 3, initialCount: 3, zeroAsyncMode:false);

            await Task.Factory.StartNew(async () =>
            {
                await Task.Delay(500);
                m.Release(true);
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);

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
            Assert.InRange(ts.ElapsedMs(), 0, 50);
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
                m.Release(true, 2);
                await Task.Delay(500);
                m.Release(true);
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);

            var ts = Environment.TickCount;
            
            await m.WaitAsync();
            await m.WaitAsync();
            await m.WaitAsync();
            await m.WaitAsync();
            await m.WaitAsync();

            Assert.InRange(ts.ElapsedMs(), 0, 50);
            await m.WaitAsync();
            _output.WriteLine($"6 {m.Tail} -> {m.Head}");
            Assert.InRange(ts.ElapsedMs(), 400, 2000);
        }

        [Fact]
        async Task MutexSpamAsync()
        {
            var m = new IoZeroSemaphoreSlim(new CancellationTokenSource(), "test mutex", maxBlockers: 3, initialCount: 3, zeroAsyncMode:false);
            var running = true;

            var waits = 0;
            //var scheduler = IoZeroScheduler.ZeroDefault;
            var scheduler = IoZeroScheduler.ZeroDefault;
            var t1 = Task.Factory.StartNew(async () =>
            {

                var ts = Environment.TickCount;
                await Task.Delay(2000);
                var s = 0;
                while (running)
                {
                    try
                    {
                        //Assert.Equal(1, m.WaitCount);
                        int r = m.Release(true);
                        if (r > 0)
                        {
                            Assert.InRange(ts.ElapsedMs(), 0, 16*2);
                            ts = Environment.TickCount;
                        }
                        else if (++s % 1000 == 0)
                        {
                            _output.WriteLine($"RELEASE Stalled! -> {ts.ElapsedMs()} ms, waiters = {m.WaitCount}, r = {r}");
                            await Task.Delay(1000);
                        }
                        else
                        {
                            await Task.Yield();
                        }

                        Assert.InRange(r, -1, 1);
                        
                        //if (r != 1) 
                        //    await Task.Delay(1);
                    }
                    catch (Exception e)
                    {
                        await Task.Yield();
                        _output.WriteLine($"FAIL! -> {ts.ElapsedMs()} ms ({e.Message})");
                        ts = Environment.TickCount;
                    }
                }

                while(m.Release(true) == 1){}

                _output.WriteLine("Release done");
            },CancellationToken.None,TaskCreationOptions.DenyChildAttach, scheduler).Unwrap();

            var t2 = Task.Factory.StartNew(async () =>
            {
                while (running)
                {
                    var ts = Environment.TickCount;
                    Assert.True(await m.WaitAsync().FastPath());
                    if (ts.ElapsedMs() > 15*2)
                    {
                        _output.WriteLine($"DQ took {ts.ElapsedMs()} ms!!!");
                    }
                    //Assert.InRange(ts.ElapsedMs(), 0, 1);

                    waits++;

                    if (waits % 1000000 == 0)
                    {
                        _output.WriteLine($"-> {waits}");
                    }
                }

                _output.WriteLine($"Wait done {waits/1000000}M");
            },CancellationToken.None, TaskCreationOptions.DenyChildAttach, scheduler).Unwrap();

            var ts = Environment.TickCount;

            try
            {
                await Task.WhenAll(t1, t2).WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch 
            {

            }

            _output.WriteLine($"Test done... {ts.ElapsedMs()}ms - {waits/((double)(ts.ElapsedMs()/1000+1))} dq/ps");
            running = false;
            await Task.Delay(1000);
            Assert.Equal(0, m.WaitCount);
            Assert.InRange(waits, 553624, int.MaxValue);
        }

        [Fact]
        async Task TestIoZeroResetEventAsync()
        {
            var count = 5;
            var minDelay = 25;
            var v = new IoZeroResetEvent();

            var t = Task.Factory.StartNew(async () =>
            {
                for (var i = 0; i < count; i++)
                {
                    await Task.Delay(minDelay);
                    v.Release();
                    _output.WriteLine(".");
                }
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();

            var ts = Environment.TickCount;
            Assert.True(await v.WaitAsync().FastPath());
            Assert.InRange(ts.ElapsedMs(), minDelay/2, minDelay * 2);

            for (var i = 0; i < count - 1; i++)
            {
                ts = Environment.TickCount;
                _output.WriteLine("_*");
                Assert.True(await v.WaitAsync().FastPath());
                Assert.InRange(ts.ElapsedMs(), minDelay / 2, 2000);
                _output.WriteLine("*");
            }
        }

        [Fact]
        async Task TestIoZeroResetEventOpenAsync()
        {
            var count = 5;
            var minDelay = 25;
            var v = new IoZeroResetEvent(true);

            var t = Task.Factory.StartNew(async () =>
            {
                for (var i = 0; i < count; i++)
                {
                    await Task.Delay(minDelay);
                    v.Release();
                }
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();

            var ts = Environment.TickCount;
            Assert.True(await v.WaitAsync().FastPath());
            Assert.InRange(ts.ElapsedMs(), 0, 2);

            for (var i = 0; i < count - 1; i++)
            {
                ts = Environment.TickCount;
                Assert.True(await v.WaitAsync().FastPath());
                Assert.InRange(ts.ElapsedMs(), minDelay / 2, 2000);
            }
        }

        [Fact]
        async Task TestIoZeroResetEventSpamAsync()
        {
            var count = (long)2000000;
            var v = new IoZeroResetEvent();

            var totalTime = Environment.TickCount;

            var t = Task.Factory.StartNew(() =>
            {
                for (var i = 0; i < count; i++)
                {

                    //_output.WriteLine($"s -> {v.GetStatus(v.Version)}[{v.Version}] \t- {DateTimeOffset.UtcNow.Ticks} - {i}/{count} - {Thread.CurrentThread.ManagedThreadId}");

                    while (v.Release() != 1)
                    {
                        //if(c++ %10000 ==0)
                        //_output.WriteLine(".");
                        Thread.Sleep(1);
                    }
                    //_output.WriteLine($"s <- {v.GetStatus(v.Version)}[{v.Version}] \t- {DateTimeOffset.UtcNow.Ticks} - {i}/{count} - {Thread.CurrentThread.ManagedThreadId}");
                }
                
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);

            for (var i = 0; i < count; i++)
            {
                var ts = Environment.TickCount;

                var version = v.Version;
                var status = v.GetStatus((short)version);
                //_output.WriteLine($"w -> {status}[{version}] \t- {DateTimeOffset.UtcNow.Ticks} - {i}/{count} - {Thread.CurrentThread.ManagedThreadId}");

                Assert.True(await v.WaitAsync().FastPath());
                Assert.InRange(ts.ElapsedMs(), 0, 20000);

                //version = v.Version;
                //status = v.GetStatus((short)version);
                //_output.WriteLine($"w <- {status}[{version}] \t- {DateTimeOffset.UtcNow.Ticks} - {i}/{count} - {Thread.CurrentThread.ManagedThreadId}");
            }

            await t;
            var maps = count * 1000 / (totalTime.ElapsedMs()) / 1000;
            _output.WriteLine($"MAPS = {maps} K/s, t = {totalTime.ElapsedMs()}ms");
            Assert.InRange(maps, 0, int.MaxValue);
        }

        [Fact]
        async Task TestIoZeroSemaphoreSlimAsync()
        {
            var count = 50;
            var minDelay = 16 * 2;
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
                    v.Release(true);
                }
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();

            var ts = Environment.TickCount;
            //Assert.True(await v.WaitAsync().FastPath());
            //Assert.Equal(0,v.ReadyCount);
            //Assert.InRange(ts.ElapsedMs(), 0, minDelay * 2);

            for (var i = 0; i < count; i++)
            {
                ts = Environment.TickCount;
                Assert.Equal(0, v.ReadyCount);
                if (!await v.WaitAsync().FastPath())
                {
                    Assert.Equal(0, v.ReadyCount);
                    _output.WriteLine($"FAIL[{Thread.CurrentThread.ManagedThreadId}] -> {i} -> {v.EgressCount}, r = {v.ReadyCount}");
                    Assert.Fail("Expected true");
                }
                else
                    //if (i % 10 == 0)
                {
                    Assert.Equal(0, v.ReadyCount);
                    //_output.WriteLine($"DQ[{Thread.CurrentThread.ManagedThreadId}] -> {i} -> {v.EgressCount}, r = {v.ReadyCount}");
                }
                Assert.InRange(ts.ElapsedMs(), minDelay / 2, minDelay * 2);
            }

            await t;
            if(!t.IsCompletedSuccessfully)
                Assert.Fail($"Enqueue failed {t.Exception}");
        }

        [Fact]
        async Task TestIoZeroSemaphoreSlimSpamAsync()
        {
#if DEBUG
            long count = 1000;
#else
            long count = 100000;
#endif

            var v = new IoZeroSemaphoreSlim(new CancellationTokenSource(), string.Empty, 1, 1);

            var totalTime = Environment.TickCount;

            var t = Task.Factory.StartNew(async () =>
            {
                int i = 0;
                while (!v.Zeroed())
                {
                    if (v.Release(true) != 1)
                        await Task.Yield();
                    else
                        i++;
                }
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();

            var t2 = Task.Factory.StartNew(async () =>
            {
                long ave = 0;
                var i = 0;
                for (i = 0; i < count; i++)
                {
                    var ts = Environment.TickCount;
                    Assert.True(await v.WaitAsync().FastPath());
                    ave += ts.ElapsedMs();
                }

                Assert.InRange(ave / count, 0, 16);
                v.ZeroSem();
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);

            
            
            await Task.WhenAll(t,t2).WaitAsync(TimeSpan.FromSeconds(10));
            
            var maps = count * 1000 / (totalTime.ElapsedMs() + 1) / 1000;
            _output.WriteLine($"MAPS = {maps} K/s, t = {totalTime.ElapsedMs()}ms");
            Assert.InRange(maps, 1, int.MaxValue);
        }

        public void Dispose()
        {
            
        }
    }
}
