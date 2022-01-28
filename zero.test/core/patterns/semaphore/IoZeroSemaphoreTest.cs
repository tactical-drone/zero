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

            Assert.True(await m.WaitAsync().FastPath().ConfigureAwait(Zc));

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
                var s = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                Assert.True(await m.WaitAsync().FastPath().ConfigureAwait(Zc));
                var delta = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - s;
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

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            Assert.Equal(3, m.ReadyCount);
            await m.WaitAsync().FastPath().ConfigureAwait(Zc);
            Assert.Equal(0, m.CurNrOfBlockers);
            await m.WaitAsync().FastPath().ConfigureAwait(Zc);
            Assert.Equal(0, m.CurNrOfBlockers);
            await m.WaitAsync().FastPath().ConfigureAwait(Zc);
            Assert.Equal(0, m.CurNrOfBlockers);
            Assert.InRange(ts.ElapsedMs(), 0, 50);
            await m.WaitAsync().FastPath().ConfigureAwait(Zc);
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

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            
            await m.WaitAsync().FastPath().ConfigureAwait(Zc);
            await m.WaitAsync().FastPath().ConfigureAwait(Zc);
            await m.WaitAsync().FastPath().ConfigureAwait(Zc);
            await m.WaitAsync().FastPath().ConfigureAwait(Zc);
            await m.WaitAsync().FastPath().ConfigureAwait(Zc);

            Assert.InRange(ts.ElapsedMs(), 0, 50);
            await m.WaitAsync().FastPath().ConfigureAwait(Zc);
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
            var t = Task.Factory.StartNew(async () =>
            {
                while (running)
                {
                    try
                    {
                        Assert.InRange(m.Release(), 0, 1);
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

            await Task.Factory.StartNew(async () =>
            {
                while (running)
                {
                    Assert.True(await m.WaitAsync().FastPath().ConfigureAwait(Zc));
                    waits++;
                }

                _output.WriteLine($"Wait done {waits/1000000}M");
            });

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            while (ts.ElapsedMs() < 1000)
            {
                await Task.Delay(100);
            }

            _output.WriteLine($"Test done... {ts.ElapsedMs()}ms");
            running = false;

            while (!done)
            {
                await Task.Delay(100);
            }

            Assert.Equal(0, m.CurNrOfBlockers);
            Assert.Equal(1, m.ReadyCount);
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

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            Assert.True(await v.WaitAsync().FastPath().ConfigureAwait(Zc));
            Assert.InRange(ts.ElapsedMs(), minDelay/2, minDelay * 2);

            for (var i = 0; i < count - 1; i++)
            {
                ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                _output.WriteLine("_*");
                Assert.True(await v.WaitAsync().FastPath().ConfigureAwait(Zc));
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

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            Assert.True(await v.WaitAsync().FastPath().ConfigureAwait(Zc));
            Assert.InRange(ts.ElapsedMs(), 0, 2);

            for (var i = 0; i < count - 1; i++)
            {
                ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                Assert.True(await v.WaitAsync().FastPath().ConfigureAwait(Zc));
                Assert.InRange(ts.ElapsedMs(), minDelay / 2, 2000);
            }
        }

        [Fact]
        async Task TestIoZeroResetEventSpamAsync()
        {
            var count = (long)2000000;
            var v = new IoZeroResetEvent();

            var totalTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            var t = Task.Factory.StartNew(() =>
            {
                for (var i = 0; i < count; i++)
                {

                    //_output.WriteLine($"s -> {v.GetStatus(v.Version)}[{v.Version}] \t- {DateTimeOffset.UtcNow.Ticks} - {i}/{count} - {Thread.CurrentThread.ManagedThreadId}");

                    while (v.Release(bestEffort: true) != 1)
                    {
                        //if(c++ %10000 ==0)
                        //_output.WriteLine(".");
                        //Thread.Sleep(0);
                    }
                    //_output.WriteLine($"s <- {v.GetStatus(v.Version)}[{v.Version}] \t- {DateTimeOffset.UtcNow.Ticks} - {i}/{count} - {Thread.CurrentThread.ManagedThreadId}");
                }
                
            }, TaskCreationOptions.DenyChildAttach);

            for (var i = 0; i < count; i++)
            {
                var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                var version = v.Version;
                var status = v.GetStatus((short)version);
                //_output.WriteLine($"w -> {status}[{version}] \t- {DateTimeOffset.UtcNow.Ticks} - {i}/{count} - {Thread.CurrentThread.ManagedThreadId}");

                Assert.True(await v.WaitAsync().FastPath().ConfigureAwait(Zc));
                Assert.InRange(ts.ElapsedMs(), 0, 20000);

                version = v.Version;
                status = v.GetStatus((short)version);
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
            var minDelay = 25;
            var v = new IoZeroSemaphoreSlim(new CancellationTokenSource(), string.Empty, 1, 1);

            var t = Task.Factory.StartNew(async () =>
            {
                for (int i = 0; i < count; i++)
                {
                    await Task.Delay(minDelay);
                    v.Release();
                }
            }).Unwrap();

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            Assert.True(await v.WaitAsync().FastPath().ConfigureAwait(Zc));
            Assert.InRange(ts.ElapsedMs(), 0, 2);

            for (var i = 0; i < count; i++)
            {
                ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                Assert.True(await v.WaitAsync().FastPath().ConfigureAwait(Zc));
                Assert.InRange(ts.ElapsedMs(), minDelay / 2, 2000);
            }
        }

        [Fact]
        async Task TestIoZeroSemaphoreSlimSpamAsync()
        {
#if DEBUG
            long count = 100000;
#else
            long count = 100000;
#endif

            var v = new IoZeroSemaphoreSlim(new CancellationTokenSource(), string.Empty, 1, 1);

            var totalTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            var t = Task.Factory.StartNew(() =>
            {
                for (var i = 0; i < count - 1; i++)
                {
                    while (v.Release(bestEffort: true) != 1) {}
                }
            });

            long ave = 0;
            int i = 0;
            for (i = 0; i < count; i++)
            {
                var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                Assert.True(await v.WaitAsync().FastPath().ConfigureAwait(Zc));
                ave += ts.ElapsedMs();
            }

            Assert.InRange(ave/count, 0, 15 * 4);

            try
            {
                await t.WaitAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
            }
            catch (Exception)
            {
                _output.WriteLine($"completed {i}/{count}");
            }

            var maps = count * 1000 / totalTime.ElapsedMs() / 1000;
            _output.WriteLine($"MAPS = {maps} K/s, t = {totalTime.ElapsedMs()}ms");
            Assert.InRange(maps, 1, int.MaxValue);
        }

        public void Dispose()
        {
            
        }
    }
}
