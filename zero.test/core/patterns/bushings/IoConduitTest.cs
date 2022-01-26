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
            var count = 20;
            var s1 = new IoZeroSource("zero source 1", false, 3, concurrencyLevel, 0, disableZero:true);
            var c1 = new IoConduit<IoZeroProduct>("conduit smoke test 1", s1, static (ioZero, _) => new IoZeroProduct("test product 1", ((IoConduit<IoZeroProduct>)ioZero).Source, 100) );

            var z1 = Task.Factory.StartNew(async () => await c1.BlockOnReplicateAsync(), TaskCreationOptions.DenyChildAttach).Unwrap();

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            while (!z1.IsCompleted)
            {
                try
                {
                    if (c1.EventCount > count || ts.ElapsedMs() > 10000)
                    {
                        await c1.Zero(null, "test done").FastPath().ConfigureAwait(Zc);
                    }
                    _output.WriteLine($"{c1.EventCount}/{count}");
                    await Task.Delay(500).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.Message);
                    throw;
                }
            }
            await z1.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);

            Assert.InRange(ts.ElapsedMs(), 1400, 2000);
            _output.WriteLine($"{ts.ElapsedMs()}ms ~ 1500");

            await Task.Delay(100).ConfigureAwait(false);
            Assert.InRange(c1.EventCount, 10,200);
            _output.WriteLine($"#event = {c1.EventCount} ~ 10");
        }

        [Fact]
        public async Task IoConduitConcurrencySmokeAsync()
        {
            var concurrencyLevel = 2;
            var count = 50;
            
            var s1 = new IoZeroSource("zero source 1", false, 3, concurrencyLevel, 0, disableZero:true);
            var c1 = new IoConduit<IoZeroProduct>("conduit smoke test 1", s1, static (ioZero, _) => new IoZeroProduct("test product 1", ((IoConduit<IoZeroProduct>)ioZero).Source, 100));

            var z1 = Task.Factory.StartNew(async () =>
            {
                await c1.BlockOnReplicateAsync();
            }, TaskCreationOptions.DenyChildAttach).Unwrap();

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            while (!z1.IsCompleted)
            {
                if (c1.EventCount > count || ts.ElapsedMs() > 10000)
                {
                    await c1.Zero(null, "test done").FastPath().ConfigureAwait(Zc);
                }
                _output.WriteLine($"{c1.EventCount}/{count}");
                await Task.Delay(500).ConfigureAwait(true);
            }
            await z1.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);

            var targetTime = count * 100 / concurrencyLevel;

            Assert.InRange(ts.ElapsedMs(), targetTime, targetTime * 2);
            _output.WriteLine($"{ts.ElapsedMs()}ms ~ {targetTime}");

            await Task.Delay(100).ConfigureAwait(false);
            Assert.InRange(c1.EventCount, count, count + concurrencyLevel * 2);
            _output.WriteLine($"#event = {c1.EventCount} ~ 20");
        }

        [Fact]
        public async Task IoConduitSpamAsync()
        {
            var count = 200000;
            var concurrencyLevel = 10;
            var s1 = new IoZeroSource("zero source 1", false, 3, concurrencyLevel, 0, true);
            var c1 = new IoConduit<IoZeroProduct>("conduit smoke test 1", s1, static (ioZero, _) => new IoZeroProduct("test product 1", ((IoConduit<IoZeroProduct>)ioZero).Source, 0));

            var z1 = Task.Factory.StartNew(async () => await c1.BlockOnReplicateAsync(), TaskCreationOptions.DenyChildAttach).Unwrap();

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            while (!z1.IsCompleted)
            {
                if (c1.EventCount > count || ts.ElapsedMs() > 10000)
                {
                    await c1.Zero(null, "test done").FastPath().ConfigureAwait(Zc);
                }
                _output.WriteLine($"{c1.EventCount}/{count}");
                await Task.Delay(500).ConfigureAwait(false);
            }
            await z1.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);

            var fpses = count * 1000 / ts.ElapsedMs() / 1000;

            Assert.InRange(fpses, 10, int.MaxValue);
            _output.WriteLine($"FPSes = {fpses} kub/s, {ts.ElapsedMs()}ms ~ 1000ms");

            await Task.Delay(100);
            Assert.InRange(c1.EventCount, count, count * 2);
            _output.WriteLine($"#event = {c1.EventCount} < {count * 2}");
        }

        //TODO
        [Fact]
        public async Task IoConduitHorizontalScaleSmokeAsync()
        {
            var count = 200;
            var totalTimeMs = count * 100;
            var concurrencyLevel = 8;
            var s1 = new IoZeroSource("zero source 1", false, concurrencyLevel, concurrencyLevel, 0, true);
            var c1 = new IoConduit<IoZeroProduct>("conduit smoke test 1", s1, static (ioZero, _) => new IoZeroProduct("test product 1", ((IoConduit<IoZeroProduct>)ioZero).Source, 100), concurrencyLevel);

            var z1 = Task.Factory.StartNew(async () => await c1.BlockOnReplicateAsync(), TaskCreationOptions.DenyChildAttach).Unwrap();

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            while (!z1.IsCompleted)
            {
                if (c1.EventCount > count || ts.ElapsedMs() > 10000)
                {
                    await c1.Zero(null, "test done").FastPath().ConfigureAwait(Zc);
                }
                _output.WriteLine(c1.EventCount.ToString());
                await Task.Delay(500).ConfigureAwait(false);
            }

            await z1.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);

            Assert.InRange(ts.ElapsedMs(), totalTimeMs / concurrencyLevel / 2, totalTimeMs / concurrencyLevel * 1.5);
            _output.WriteLine($"{ts.ElapsedMs()}ms ~ {totalTimeMs / concurrencyLevel}ms");

            await Task.Delay(100);
            Assert.InRange(c1.EventCount, count, count * 1.25);
            _output.WriteLine($"#event = {c1.EventCount} ~ {count}");
        }
    }
}
