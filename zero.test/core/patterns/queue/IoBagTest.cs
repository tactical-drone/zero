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

            var bag = new IoZeroQ<IoInt32>("test", 16, true);

            for (int i = 0; i < bag.Capacity - 1; i++)
            {
                bag.TryEnqueue(i);
            }

            Assert.True(bag.Contains(bag.Capacity / 2));

            var sb = new StringBuilder();

            foreach (var i in bag)
            {
                sb.Append($"{i}");
            }

            Assert.Equal("012345678910111213", sb.ToString());

            foreach (var i in bag)
            {
                sb.Append($"{i}");
                if (i == 7)
                    bag.TryEnqueue(11);

                if (i == 11)
                    break;
            }

            Assert.Equal("01234567891011121301234567891011", sb.ToString());

            foreach (var i in bag)
            {
                bag.TryDequeue(out var r);
                sb.Append($"{i}");
            }

            Assert.Equal("0123456789101112130123456789101101234567891011121311", sb.ToString());
        }

        [Fact]
        public async Task IteratorAsync()
        {
            var threads = 100;
            var bag = new IoZeroQ<IoInt32>("test", 128, true);
            await Task.Yield();
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
                    var (@this,_bag, idx) = (ValueTuple<IoBagTest, IoZeroQ<IoInt32>, int>)state!;
                    _bag.TryEnqueue(Interlocked.Increment(ref idx));
                }, (this, bag, idx), TaskCreationOptions.DenyChildAttach));
            }

            await Task.WhenAll(insert).WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);

            Assert.Equal(threads, bag.Count);

            bag.TryDequeue(out _);
            bag.TryDequeue(out _);
            bag.TryDequeue(out _);

            Assert.Equal(threads - 3, bag.Count);

            c = 0;
            foreach (var ioInt32 in bag)
            {
                c++;
            }

            Assert.Equal(threads - 3, c);

            while (bag.Count > 0)
                bag.TryDequeue(out _);

            Assert.Equal(0, bag.Count);

            c = 0;
            foreach (var ioInt32 in bag)
                c++;

            Assert.Equal(0, c);
        }

        [Fact]
        void AutoScale()
        {
            var bag = new IoZeroQ<IoInt32>("test", 2, true);

            bag.TryEnqueue(0);
            bag.TryEnqueue(1);
            bag.TryEnqueue(2);
            bag.TryEnqueue(3);
            bag.TryEnqueue(4);

            Assert.Equal(7, bag.Capacity);
            Assert.Equal(5, bag.Count);

            bag.TryEnqueue(5);
            bag.TryEnqueue(6);
            bag.TryEnqueue(7);
            bag.TryEnqueue(8);
            bag.TryEnqueue(9);

            Assert.Equal(15, bag.Capacity);

            var p = -1;
            var c = bag.Count;
            for (int i = 0; i < c; i++)
            {
                if (bag.TryDequeue(out var t))
                {
                    Assert.True(t > p);
                    p = t;
                }
            }

            Assert.Equal(15, bag.Capacity);
            Assert.Equal(0, bag.Count);
        }

        [Fact]
        void ZeroSupport()
        {
            var bag = new IoZeroQ<IoInt32>("test", 2, true);

            bag.TryEnqueue(0);
            bag.TryEnqueue(1);
            var idx = bag.TryEnqueue(2);
            bag.TryEnqueue(3);
            bag.TryEnqueue(4);

            //bag[idx] = default;

            Assert.Equal(5, bag.Count);

            var sb = new StringBuilder();
            foreach (var i in bag)
            {
                sb.Append($"{i}");
            }
            foreach (var i in bag)
            {
                sb.Append($"{i}");
            }

            Assert.Equal(5, bag.Count);
            Assert.Equal("0123401234", sb.ToString());

            IoInt32 prev = -1;
            var size = bag.Count;
            for (int i = 0; i < size; i++)
            {
                if (bag.TryDequeue(out var i32))
                {
                    Assert.True(i32 > prev);
                    prev = i32;
                }
            }

            Assert.Equal(0, bag.Count);
            Assert.True(bag.Tail>=bag.Head);

            for (var i = 0; i < bag.Capacity; i++)
                Assert.True(bag[i] == null);
        }

        public bool Zc => IoNanoprobe.ContinueOnCapturedContext;
    }
}
