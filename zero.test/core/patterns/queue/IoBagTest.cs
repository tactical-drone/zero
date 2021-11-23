using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using zero.core.misc;
using zero.core.patterns.misc;
using zero.core.patterns.queue;

namespace zero.test.core.patterns.queue
{
    public class IoBagTest
    {
        private readonly ITestOutputHelper _output;

        public IoBagTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        void InsertTest()
        {

            var _bag = new IoBag<IoInt32>("test", 11, true);

            for (int i = 0; i < _bag.Capacity - 1; i++)
            {
                _bag.Add(i);
            }

            Assert.True(_bag.Contains(_bag.Capacity / 2));

            var sb = new StringBuilder();
            foreach (var i in _bag)
            {
                sb.Append($"{i}");
                if (i == 7)
                    _bag.Add(11);

                if (i == 11)
                    break;
            }

            foreach (var i in _bag)
            {
                _bag.TryTake(out var r);
                sb.Append($"{i}");
            }

            Assert.Equal("012345678911012345678911", sb.ToString());
        }

        [Fact]
        public async Task Iterator()
        {
            var threads = 100;
            var bag = new IoBag<IoInt32>("test", 100, true);

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
                    var (@this,_bag, idx) = (ValueTuple<IoBagTest, IoBag<IoInt32>, int>)state!;
                    _bag.Add(Interlocked.Increment(ref idx));
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
        public bool Zc => IoNanoprobe.ContinueOnCapturedContext;
    }
}
