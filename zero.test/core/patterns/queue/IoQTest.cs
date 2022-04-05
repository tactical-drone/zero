using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
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
    public class IoQTest
    {
        private readonly ITestOutputHelper _output;

        public IoQTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        void InsertTest()
        {

            var bag = new IoZeroQ<IoInt32>("test", 16,true);

            for (int i = 0; i < bag.Capacity - 1; i++)
            {
                bag.TryEnqueue(i);
            }

            Assert.True(bag.Contains((IoInt32)(bag.Capacity / 2)));

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
            var threads = 32;
            
            var bag = new IoZeroQ<IoInt32>("test", 128,true);
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
                    var (@this,bag, idx) = (ValueTuple<IoQTest, IoZeroQ<IoInt32>, int>)state!;
                    var c = 0;
                    while (c < InsertsPerThread)
                    {
                        if (bag.TryEnqueue(Interlocked.Increment(ref idx)) != -1)
                        {
                            c++;
                        }
                    }
                        
                    
                }, (this, bag, idx), TaskCreationOptions.DenyChildAttach));
            }

            await Task.WhenAll(insert).WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(threads * InsertsPerThread, bag.Count);

            bag.TryDequeue(out _);
            bag.TryDequeue(out _);
            bag.TryDequeue(out _);

            Assert.Equal(threads * InsertsPerThread - 3, bag.Count);

            c = 0;
            foreach (var ioInt32 in bag)
            {
                c++;
            }

            Assert.Equal(threads * InsertsPerThread - 3, c);

            while (bag.Count > 0)
                bag.TryDequeue(out _);

            Assert.Equal(0, bag.Count);

            c = 0;
            foreach (var ioInt32 in bag)
                c++;

            Assert.Equal(0, c);
        }

        private volatile bool SpamTestAsyncDone = false;
        private volatile int SpamTestAsyncThreadCount = 4;
        private volatile int SpamTestAsyncThreadId = 0;
        private volatile int SpamTestAsyncThreadsDone = 0;
        private const int InsertsPerThread = 10000;
        [Fact]
        public async Task SpamTestAsync()
        {
            SpamTestAsyncThreadCount = Environment.ProcessorCount * 1;

            var initialSize = 16384 << 2;
            //var initialSize = 64;
            var bag = new IoZeroQ<IoInt32>("test", initialSize, false); //TODO: fix this test with scaling
            
            var c = 0;
            foreach (var ioInt32 in bag)
                c++;

            Assert.Equal(0, c);

            var idx = 0;
            var done = 0;
            var spam = new List<Task>();
            Task t = null;
            for (var i = 0; i < SpamTestAsyncThreadCount; i++)
            {
                spam.Add(Task.Factory.StartNew(static state =>
                {
                    var (@this, bag) = (ValueTuple<IoQTest, IoZeroQ<IoInt32>>)state!;
                    var maxIdx = -1;
                    var success = 0;
                    while (success < InsertsPerThread)
                    {
                        if (bag.TryEnqueue(Interlocked.Increment(ref @this.SpamTestAsyncThreadId)) != -1)
                        {
                            success++;
                        }
                        
                        maxIdx = Math.Max(maxIdx, bag.Count);
                    }

                    Interlocked.Increment(ref @this.SpamTestAsyncThreadsDone);
                    @this._output.WriteLine($"Max q size was = {maxIdx}, success ={success}");
                }, (this, bag), TaskCreationOptions.DenyChildAttach));


                spam.Add(Task.Factory.StartNew(static state =>
                {
                    var (@this, bag) = (ValueTuple<IoQTest, IoZeroQ<IoInt32>>)state!;

                    var tries = 0;
                    var success = 0;
                    while (!@this.SpamTestAsyncDone || bag.Count > 0)
                    {
                        tries++;
                        if (bag.TryDequeue(out var item))
                        {
                            success++;
                        }
                        else
                        {
                            Thread.Sleep(1);
                        }
                    }
                    @this._output.WriteLine($"left = {success}/{tries} ({success / (tries + 1.0) * 100:0.0}%)");
                }, (this, bag), TaskCreationOptions.DenyChildAttach));
            }

            spam.Add(Task.Factory.StartNew(static state =>
            {
                var (@this, bag) = (ValueTuple<IoQTest, IoZeroQ<IoInt32>>)state!;
                while (@this.SpamTestAsyncThreadsDone != @this.SpamTestAsyncThreadCount)
                {
                    Thread.Sleep(100);
                }
                @this.SpamTestAsyncDone = true;
            }, (this, bag), TaskCreationOptions.DenyChildAttach));

            try
            {
                await Task.WhenAll(spam).WaitAsync(TimeSpan.FromSeconds(10));
            }
            catch (Exception e)
            {
                _output.WriteLine(e.Message);
            }

            Assert.Equal(0, bag.Count);
            Assert.Equal(bag.Head, bag.Tail);
            //Assert.True(bag.IsAutoScaling);
            Assert.True(!bag.Zeroed);

            for (int i = 0; i < bag.Capacity; i++)
            {
                Assert.True(bag[i] == null);
            }

            bag.TryEnqueue(23);
            await bag.ZeroManagedAsync<object>();
            for (int i = 0; i < bag.Capacity; i++)
            {
                Assert.True(bag[i] == null);
            }

            Assert.Equal(0, bag.Count);
            Assert.Equal(bag.Head + 1, bag.Tail);

            _output.WriteLine($"Eventual size = {bag.Capacity}");
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
            var bag = new IoZeroQ<IoInt32>("test", 2,true);

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
