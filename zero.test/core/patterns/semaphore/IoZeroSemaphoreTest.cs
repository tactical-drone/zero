using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using zero.core.misc;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore;

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
                    m.Release();
                }
            },(this,m,targetSleep), TaskCreationOptions.DenyChildAttach);

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


        [Fact]
        async Task PrefetchAsync()
        {
            var m = new IoZeroSemaphoreSlim(new CancellationTokenSource(), "test mutex", maxBlockers: 1, initialCount: 3);

            await Task.Factory.StartNew(async () =>
            {
                await Task.Delay(500);
                m.Release();
            });

            var ts = Environment.TickCount;
            Assert.Equal(3, m.ReadyCount);
            await m.WaitAsync().FastPath();
            Assert.Equal(0, m.CurNrOfBlockers);
            await m.WaitAsync().FastPath();
            Assert.Equal(0, m.CurNrOfBlockers);
            await m.WaitAsync().FastPath();
            Assert.Equal(0, m.CurNrOfBlockers);
            Assert.InRange(ts.ElapsedMs(), 0, 50);
            await m.WaitAsync().FastPath();
            Assert.InRange(ts.ElapsedMs(),400, 2000);
            Assert.Equal(0, m.CurNrOfBlockers);
        }


        [Fact]
        async Task ReleaseAsync()
        {
            var m = new IoZeroSemaphoreSlim(new CancellationTokenSource(), "test mutex", maxBlockers: 10, initialCount: 3, maxAsyncWork: 0);
            
            await Task.Factory.StartNew(async () =>
            {
                m.Release(2);
                await Task.Delay(500);
                m.Release();
            });

            var ts = Environment.TickCount;
            
            await m.WaitAsync().FastPath();
            await m.WaitAsync().FastPath();
            await m.WaitAsync().FastPath();
            await m.WaitAsync().FastPath();
            await m.WaitAsync().FastPath();

            Assert.InRange(ts.ElapsedMs(), 0, 50);
            await m.WaitAsync().FastPath();
            _output.WriteLine($"6 {m.Tail} -> {m.Head}");
            Assert.InRange(ts.ElapsedMs(), 400, 2000);
        }

        [Fact]
        async Task MutexSpamAsync()
        {
            var m = new IoZeroSemaphoreSlim(new CancellationTokenSource(), "test mutex", maxBlockers: 1, initialCount: 3);
            var running = true;
            var done = false;
            var waits = 0;
            var t1 = Task.Factory.StartNew(async () =>
            {
                while (running)
                {
                    try
                    {
                        int r = m.Release();
                        Assert.InRange(r, 0, 1);

                        if (r != 1) 
                            await Task.Delay(1);
                    }
                    catch
                    {
                        await Task.Delay(1);
                    }
                }

                while(m.Release() == 1){}

                done = true;
                _output.WriteLine("Release done");
            },TaskCreationOptions.DenyChildAttach);

            var t2 = await Task.Factory.StartNew(async () =>
            {
                while (running)
                {
                    Assert.True(await m.WaitAsync().FastPath());
                    waits++;
                }

                _output.WriteLine($"Wait done {waits/1000000}M");
            });

            var ts = Environment.TickCount;

            try
            {
                await Task.WhenAll(t1, t2).WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch 
            {

            }

            _output.WriteLine($"Test done... {ts.ElapsedMs()}ms");
            running = false;

            Assert.Equal(0, m.CurNrOfBlockers);
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
            }).Unwrap();

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
            }).Unwrap();

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
                
            }, TaskCreationOptions.DenyChildAttach);

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
            var v = new IoZeroSemaphoreSlim(new CancellationTokenSource(), string.Empty, 1, 1);

            var t = Task.Factory.StartNew(async () =>
            {
                for (int i = 0; i < count; i++)
                {
                    await Task.Delay(minDelay);
                    v.Release();
                }
            }).Unwrap();

            var ts = Environment.TickCount;
            Assert.True(await v.WaitAsync().FastPath());
            Assert.InRange(ts.ElapsedMs(), 0, 16 * 2);

            for (var i = 0; i < count; i++)
            {
                ts = Environment.TickCount;
                Assert.True(await v.WaitAsync().FastPath());
                Assert.InRange(ts.ElapsedMs(), minDelay / 2, minDelay * 2);
            }
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
                    if (v.Release() != 1)
                        await Task.Delay(1);
                    else
                        i++;
                }
            }).Unwrap();

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
            });

            
            await v.ZeroManagedAsync();
            await Task.WhenAll(t,t2).WaitAsync(TimeSpan.FromSeconds(15));
            
            var maps = count * 1000 / (totalTime.ElapsedMs() + 1) / 1000;
            _output.WriteLine($"MAPS = {maps} K/s, t = {totalTime.ElapsedMs()}ms");
            Assert.InRange(maps, 1, int.MaxValue);
        }

        public void Dispose()
        {
            
        }
    }
}
