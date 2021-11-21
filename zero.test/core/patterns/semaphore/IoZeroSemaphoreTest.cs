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

        [Fact]
        async Task TestMutex()
        {
            var running = true;
#if DEBUG
            int loopCount = 10;
            int targetSleep = 100;
#else
            int loopCount = 200;
            int targetSleep = 50;
#endif
            var m = new IoZeroSemaphoreSlim(new CancellationTokenSource(), "test mutex", maxBlockers: 1, initialCount: 1);

            var s = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            var t = Task.Factory.StartNew(async () =>
            {
                // ReSharper disable once AccessToModifiedClosure
                while(running)
                {
                    await Task.Delay(targetSleep);
                    m.Release();
                }
            }, TaskCreationOptions.DenyChildAttach | TaskCreationOptions.LongRunning);

            Assert.True(await m.WaitAsync().FastPath().ConfigureAwait(Zc));

            var c = 0;
            long ave = 0;
            while (c++ < loopCount)
            {
                Assert.True(await m.WaitAsync().FastPath().ConfigureAwait(Zc));
                var delta = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - s;
                ave += delta;
                _output.WriteLine($"d = {delta}");
                if (delta < targetSleep * targetSleep || c > 1)//gitlab glitches on c == 0
                    Assert.InRange(delta, targetSleep/2, targetSleep * targetSleep);
                s = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            }

            running = false;

            Assert.InRange(ave/loopCount, targetSleep/2, targetSleep/2 + targetSleep);
        }

        public void Dispose()
        {

        }
    }
}
