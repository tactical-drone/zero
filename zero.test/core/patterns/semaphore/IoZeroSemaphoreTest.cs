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
        private bool Zc = true;
        private readonly ITestOutputHelper _output;

        [Fact]
        async Task TestMutex()
        {
            int loopCount = 10;
            var m = new IoZeroSemaphoreSlim(new CancellationTokenSource(), "test mutex", maxBlockers: 1, initialCount: 1);

            var s = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            var t = Task.Factory.StartNew(async () =>
            {
                for (int i = 0; i < loopCount; i++)
                {
                    await Task.Delay(50);
                    var r = m.Release();
                    if (r != 1)
                    {
                        i--;
                        continue;
                    }
                }
            }, TaskCreationOptions.DenyChildAttach);

            Assert.True(await m.WaitAsync().FastPath().ConfigureAwait(Zc));

            var c = 0;
            while (c++ < loopCount)
            {
                Assert.True(await m.WaitAsync().FastPath().ConfigureAwait(Zc));
                var delta = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - s;
                _output.WriteLine($"d = {delta}");
                Assert.InRange(delta, 50, 3000);
                s = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            }

            Assert.Equal(loopCount + 1, c);
        }

        public void Dispose()
        {

        }
    }
}
