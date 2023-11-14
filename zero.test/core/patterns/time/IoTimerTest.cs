using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using zero.core.feat.patterns.time;
using zero.core.misc;
using zero.core.patterns.misc;

namespace zero.test.core.patterns.time
{
    public class IoTimerTest
    {
        private readonly ITestOutputHelper _output;
        public IoTimerTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task SmokeAsync()
        {
            var t = new IoTimer(TimeSpan.FromSeconds(1));

            var c = 3;
            while (c-- > 0)
            {
                var ts = Environment.TickCount;
                var d = await t.TickAsync().FastPath();
                Assert.InRange(d.ElapsedMs(), -32, 100);
                Assert.InRange(ts.ElapsedMs(), 1000 - 64, 2000 + 64);
                _output.WriteLine($"-> q = {d.ElapsedMs()}ms - {ts.ElapsedMs()}ms");
            }
        }
    }
}
