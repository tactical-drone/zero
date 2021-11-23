using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using zero.core.patterns.misc;
using zero.core.patterns.queue;

namespace zero.test.core.patterns.queue
{
    public class IoHashCodesTest
    {
        private bool Zc => IoNanoprobe.ContinueOnCapturedContext;

        [Fact]
        void InsertTest()
        {
            var bag = new IoHashCodes("test", 11, true);

            for (int i = 0; i < bag.Capacity-1; i++)
            {
                bag.Add(i);
            }

            Assert.True(bag.Contains((int)bag.Capacity / 2));

            var sb = new StringBuilder();
            foreach (var i in bag)
            {
                sb.Append($"{i}");
                if (i == 7)
                    bag.Add(11);

                if (i == 11)
                    break;
            }

            foreach (var i in bag)
            {
                bag.TryTake(out var r);
                sb.Append($"{i}");
            }

            Assert.Equal("112345678911112345678911", sb.ToString());
        }


        [Fact]
        public async Task Iterator()
        {
            var threads = 100;

            var bag = new IoHashCodes("test", threads, true);

            var c = 0;
            foreach (var ioInt32 in bag)
                c++;

            Assert.Equal(0, c);

            var idx = 0;
            var insert = new List<Task>();
            for (var i = 0; i < threads; i++)
            {
                insert.Add(Task.Factory.StartNew(static state =>
                {
                    var (@this, bag, idx) = (ValueTuple<IoHashCodesTest, IoHashCodes, int>)state!;
                    bag.Add(Interlocked.Increment(ref idx));
                }, (this, bag, idx), TaskCreationOptions.DenyChildAttach));
            }

            await Task.WhenAll(insert).ConfigureAwait(Zc);

            Assert.Equal(threads, bag.Count);

            bag.TryTake(out _);
            bag.TryTake(out _);
            bag.TryTake(out _);

            Assert.Equal(threads - 3, bag.Count);

            c = 0;
            foreach (var ioInt32 in bag)
            {
                c++;
            }

            Assert.Equal(threads - 3, c);

            while (bag.Count > 0)
                bag.TryTake(out _);

            Assert.Equal(0, bag.Count);

            c = 0;
            foreach (var ioInt32 in bag)
                c++;

            Assert.Equal(0, c);
        }
    }
}
