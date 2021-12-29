using System;
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
            var s1 = new IoZeroSource("zero source 1", false, concurrencyLevel, concurrencyLevel, 0);
            var c1 = new IoConduit<IoZeroProduct>("conduit smoke test 1", s1, static (ioZero, _) => new IoZeroProduct("test product 1", ((IoConduit<IoZeroProduct>)ioZero).Source, 100) );

            var z1 = Task.Factory.StartNew(async () => await c1.BlockOnReplicateAsync(), TaskCreationOptions.DenyChildAttach).Unwrap();

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            while (!z1.IsCompleted && c1.EventCount < 10)
            {
                await Task.Delay(15).ConfigureAwait(false);
            }
            await c1.Zero(null,"test done").FastPath().ConfigureAwait(Zc);
            await z1.ConfigureAwait(false);

            Assert.InRange(ts.ElapsedMs(), 1000, 2000);
            _output.WriteLine($"{ts.ElapsedMs()}ms ~ 1000");

            await Task.Delay(100).ConfigureAwait(false);
            Assert.InRange(c1.EventCount, 10,15);
            _output.WriteLine($"#event = {c1.EventCount} ~ 10");
        }

        [Fact]
        public async Task IoConduitConcurrencySmokeAsync()
        {
            var concurrencyLevel = 2;
            var s1 = new IoZeroSource("zero source 1", false, concurrencyLevel * 2, concurrencyLevel, 0);
            var c1 = new IoConduit<IoZeroProduct>("conduit smoke test 1", s1, static (ioZero, _) => new IoZeroProduct("test product 1", ((IoConduit<IoZeroProduct>)ioZero).Source, 200));

            var z1 = Task.Factory.StartNew(async () => await c1.BlockOnReplicateAsync(), TaskCreationOptions.DenyChildAttach).Unwrap();

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            while (!z1.IsCompleted && c1.EventCount < 20)
            {
                await Task.Delay(15).ConfigureAwait(false);
            }
            await c1.Zero(null, "test done").FastPath().ConfigureAwait(Zc);
            await z1.ConfigureAwait(false);

            Assert.InRange(ts.ElapsedMs(), 1000, 4000 * 1.2);
            _output.WriteLine($"{ts.ElapsedMs()}ms ~ 2000");

            await Task.Delay(100).ConfigureAwait(false);
            Assert.InRange(c1.EventCount, 20, 25);
            _output.WriteLine($"#event = {c1.EventCount} ~ 20");
        }

        [Fact]
        public async Task IoConduitSpamAsync()
        {
            var count = 200000;
            var concurrencyLevel = 10;
            var s1 = new IoZeroSource("zero source 1", false, concurrencyLevel * 2, concurrencyLevel, concurrencyLevel/2, true);
            var c1 = new IoConduit<IoZeroProduct>("conduit smoke test 1", s1, static (ioZero, _) => new IoZeroProduct("test product 1", ((IoConduit<IoZeroProduct>)ioZero).Source, 0));

            var z1 = Task.Factory.StartNew(async () => await c1.BlockOnReplicateAsync(), TaskCreationOptions.DenyChildAttach).Unwrap();

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            while (!z1.IsCompleted && c1.EventCount < count)
            {
                await Task.Delay(30).ConfigureAwait(false);
            }
            await c1.Zero(null, "test done").FastPath().ConfigureAwait(Zc);
            await z1.ConfigureAwait(false);
            var fpses = count * 1000 / ts.ElapsedMs() / 1000;

            Assert.InRange(fpses, 10, int.MaxValue);
            _output.WriteLine($"FPSes = {fpses} kub/s, {ts.ElapsedMs()}ms ~ 1000ms");

            await Task.Delay(100);
            Assert.InRange(c1.EventCount, count, count * 1.25);
            _output.WriteLine($"#event = {c1.EventCount} ~ {count}");
        }

        //TODO
        [Fact]
        public async Task IoConduitHorizontalScaleSmokeAsync()
        {
            var count = 200;
            var totalTimeMs = count * 100;
            var concurrencyLevel = 2;
            var s1 = new IoZeroSource("zero source 1", false, concurrencyLevel, concurrencyLevel, concurrencyLevel, true);
            var c1 = new IoConduit<IoZeroProduct>("conduit smoke test 1", s1, static (ioZero, _) => new IoZeroProduct("test product 1", ((IoConduit<IoZeroProduct>)ioZero).Source, 100), concurrencyLevel);

            var z1 = Task.Factory.StartNew(async () => await c1.BlockOnReplicateAsync(), TaskCreationOptions.DenyChildAttach).Unwrap();

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            while (!z1.IsCompleted && c1.EventCount < count)
            {
                _output.WriteLine(c1.EventCount.ToString());
                await Task.Delay(1000).ConfigureAwait(false);
            }

            await c1.Zero(null, "test done").FastPath().ConfigureAwait(Zc);
            await z1.ConfigureAwait(false);

            Assert.InRange(ts.ElapsedMs(), totalTimeMs / concurrencyLevel / 2, totalTimeMs / concurrencyLevel * 1.25);
            _output.WriteLine($"{ts.ElapsedMs()}ms ~ {totalTimeMs / concurrencyLevel}ms");

            await Task.Delay(100);
            Assert.InRange(c1.EventCount, count, count * 1.25);
            _output.WriteLine($"#event = {c1.EventCount} ~ {count}");
        }
    }
}
