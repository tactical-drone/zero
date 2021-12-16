using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using zero.core.misc;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;

namespace zero.test.core.patterns.bushings
{
    public class IoConduitTest
    {
        public IoConduitTest(ITestOutputHelper output)
        {
            _output = output;
        }
        private bool Zc = IoNanoprobe.ContinueOnCapturedContext;
        private readonly ITestOutputHelper _output;

        [Fact]
        public async Task IoConduitSmokeAsync()
        {
            var concurrencyLevel = 1;
            var s1 = new IoZeroSource("zero source 1", false, concurrencyLevel * 2, concurrencyLevel, 0);
            var c1 = new IoConduit<IoZeroProduct>("conduit smoke test 1", s1, static (ioZero, _) => new IoZeroProduct("test product 1", ((IoConduit<IoZeroProduct>)ioZero).Source, 100) );

            var z1 = Task.Factory.StartNew(async () => await c1.BlockOnReplicateAsync()).Unwrap();

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            while (!z1.IsCompletedSuccessfully && c1.EventCount < 10)
            {
                await Task.Delay(0);
            }
            c1.Zero(null,"test done");
            Thread.MemoryBarrier();
            Assert.InRange(ts.ElapsedMs(), 950, 1300);
            _output.WriteLine($"{ts.ElapsedMs()}ms ~ 1000ms");

            await Task.Delay(100);
            Assert.InRange(c1.EventCount, 10,15);
            _output.WriteLine($"#event = {c1.EventCount} ~ 10");
        }
    }
}
