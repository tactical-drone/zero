using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
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

        public void Dispose()
        {

        }
    }
}
