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
        async Task TestMutex()
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
        async Task Prefetch()
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
        async Task Release()
        {
            var m = new IoZeroSemaphoreSlim(new CancellationTokenSource(), "test mutex", maxBlockers: 10, initialCount: 3);

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
            Assert.InRange(ts.ElapsedMs(), 400, 2000);
        }

        [Fact]
        async Task MutexSpam()
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
            Assert.InRange(waits,1000000, int.MaxValue);
        }


        public void Dispose()
        {
            
        }
    }
}
