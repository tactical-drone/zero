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

            var cap = 16;
            var bag = new IoZeroQ<IoInt32>("test", 16,true);

            for (int i = 0; i < cap - 1; i++)
            {
                bag.TryEnqueue(i);
            }

            Assert.True(bag.Contains((IoInt32)(cap / 2)));

            var sb = new StringBuilder();

            foreach (var i in bag)
            {
                sb.Append($"{i}");
            }

            Assert.Equal("01234567891011121314", sb.ToString());

            foreach (var i in bag)
            {
                sb.Append($"{i}");
                if (i == 7)
                    bag.TryEnqueue(11);

                if (i == 11)
                    break;
            }

            Assert.Equal("0123456789101112131401234567891011", sb.ToString());

            foreach (var i in bag)
            {
                bag.TryDequeue(out var r);
                sb.Append($"{i}");
            }

            Assert.Equal("01234567891011121314012345678910110123456789101112131411", sb.ToString());
        }


        private volatile bool _smokeTestDone;
        [Fact]
        public async Task SmokeTestAsync()
        {
            var threads = 2;
            //var threads = 1;

            var bag = new IoZeroQ<IoInt32>("test", 8192,true);
            
            var c = 0;
            foreach (var ioInt32 in bag)
                c++;
            
            Assert.Equal(0, c);

            var insert = new List<Task>();
            var remove = new List<Task>();
            for (var i = 0; i < threads; i++)
            {
                if(i < threads>>1)
                    remove.Add(Task.Factory.StartNew(static state =>
                    {
                        var (@this, bag, threads) = (ValueTuple<IoQTest, IoZeroQ<IoInt32>, int>)state!;
                        var c = 0;
                        while (!@this._smokeTestDone || bag.Count > 0)
                        {
                            if (bag.TryDequeue(out var _))
                            {
                                if (Interlocked.Increment(ref @this.SpamTestAsyncThreadsDone) >=
                                    threads * InsertsPerThread)
                                {
                                    break;
                                }
                                c++;
                            }
                        }
                        @this._output.WriteLine($"{c} dq done");
                    }, (this, bag, threads), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default));

                insert.Add(Task.Factory.StartNew(static state =>
                {
                    var (@this,bag) = (ValueTuple<IoQTest, IoZeroQ<IoInt32>>)state!;
                    var c = 0;
                    while (c < InsertsPerThread)
                    {
                        if (bag.TryEnqueue(Interlocked.Increment(ref @this.SpamTestAsyncThreadId)) != -1)
                        {
                            c++;
                        }
                    }
                    @this._output.WriteLine($"{c} eq done");
                }, (this, bag), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default));
            }

            await Task.WhenAll(insert).WaitAsync(TimeSpan.FromSeconds(15));
            _smokeTestDone = true;
            await Task.WhenAll(remove).WaitAsync(TimeSpan.FromSeconds(15));

            Assert.Equal(0, bag.Count);

            c = 0;
            foreach (var ioInt32 in bag)
            {
                c++;
            }

            Assert.Equal(0, bag.Count);
        }

        private volatile bool SpamTestAsyncDone = false;
        private volatile int SpamTestAsyncThreadCount = 4;
        private volatile int SpamTestAsyncThreadId = 0;
        private volatile int SpamTestAsyncThreadsDone = 0;
        private const int InsertsPerThread = 10000;
        [Fact]
        public async Task SpamTestAsync()
        {
            SpamTestAsyncThreadCount = Environment.ProcessorCount * 2;

            var initialSize = 16384 <<4;
            //var initialSize = 64;
            var bag = new IoZeroQ<IoInt32>("test", initialSize, true); //TODO: fix this test with scaling
            
            var c = 0;
            foreach (var ioInt32 in bag)
                c++;

            Assert.Equal(0, c);

            var spam = new List<Task>();
            
            for (var i = 0; i < SpamTestAsyncThreadCount; i++)
            {
                if (i < SpamTestAsyncThreadCount/2)
                    spam.Add(Task.Factory.StartNew(static state =>
                    {
                        var (@this, bag) = (ValueTuple<IoQTest, IoZeroQ<IoInt32>>)state!;

                        var tries = 0;
                        var success = 0;
                        var ts = Environment.TickCount;
                        while (!@this.SpamTestAsyncDone || bag.Count > 0)
                        {
                            tries++;
                            if (bag.TryDequeue(out var item))
                            {
                                success++;
                            }
                            else
                            {
                                Thread.Sleep(20);
                            }
                        }
                        @this._output.WriteLine($"left = {success}/{tries} ({success / (tries + 1.0) * 100:0.0}%), {success * 1000 / (ts+1).ElapsedMs()} dq/s, t = {ts.ElapsedMs()}ms");
                    }, (this, bag), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default));

                spam.Add(Task.Factory.StartNew(static state =>
                {
                    var (@this, bag) = (ValueTuple<IoQTest, IoZeroQ<IoInt32>>)state!;
                    var maxIdx = -1;
                    var success = 0;
                    var ts = Environment.TickCount;
                    while (success < InsertsPerThread)
                    {
                        if (bag.TryEnqueue(Interlocked.Increment(ref @this.SpamTestAsyncThreadId)) != -1)
                        {
                            success++;
                        }
                        
                        maxIdx = (int)Math.Max(maxIdx, bag.Count);
                    }

                    Interlocked.Increment(ref @this.SpamTestAsyncThreadsDone);
                    @this._output.WriteLine($"Max q size was = {maxIdx}, success ={success}, {success*1000/(ts+1).ElapsedMs()} q/s, t = {ts.ElapsedMs()}ms");
                }, (this, bag), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default));
            }

            
            spam.Add(Task.Factory.StartNew(static state =>
            {
                var (@this, bag) = (ValueTuple<IoQTest, IoZeroQ<IoInt32>>)state!;
                while (@this.SpamTestAsyncThreadsDone != @this.SpamTestAsyncThreadCount)
                {
                    Thread.Sleep(100);
                }
                @this.SpamTestAsyncDone = true;
            }, (this, bag), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default));

            try
            {
                await Task.WhenAll(spam).WaitAsync(TimeSpan.FromSeconds(15));
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
            //Assert.Equal(bag.Head + 1, bag.Tail);

            _output.WriteLine($"Eventual size = {bag.Capacity}");
        }

        [Fact]
        void AutoScale()
        {
            var bag = new IoZeroQ<IoInt32>("test", 2, true);
            var count = 64;

            for (int i = 0; i < count; i++)
            {
                bag.TryEnqueue(i);
            }
            
            Assert.Equal(255, bag.Capacity);
            Assert.Equal(64, bag.Count);

            for (int i = count; i < count + 5; i++)
            {
                bag.TryEnqueue(i);
            }

            Assert.Equal(255, bag.Capacity);

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

            Assert.Equal(255, bag.Capacity);
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

        private volatile bool _insertsDone = false;
        private volatile bool _dequeueDone = false;
        private volatile int _haltCount = 0;
        [Fact]
        public async Task BlockingCollectionTestAsync()
        {
            var threads = 4;
            
            var cs = new CancellationTokenSource();
            var bag = new IoZeroQ<IoInt32>("test", 16384, true, cs, threads);
            
            var c = 0;
            foreach (var ioInt32 in bag)
                c++;

            Assert.Equal(0, c);

            bag.TryEnqueue(0);
            bag.TryEnqueue(1);
            bag.TryEnqueue(2);
            var preload = 0;

            await foreach (var item in bag.BlockOnConsumeAsync(0))
            {
                //_output.WriteLine(item.ToString());
                if(++preload == 3)
                    break;
            }

            var insert = new List<Task>();
            var remove = new List<Task>();
            for (var i = 0; i < threads; i++)
            {
                remove.Add(Task.Factory.StartNew(static async state =>
                {
                    var (@this, bag, threads,i) = (ValueTuple<IoQTest, IoZeroQ<IoInt32>, int,int>)state!;

                    var c = 0;

                    try
                    {
                        while (!@this._dequeueDone)
                        {
                            try
                            {
                                await foreach (var item in bag.BlockOnConsumeAsync(i))
                                {
                                    if (@this._dequeueDone || Interlocked.Increment(ref @this.SpamTestAsyncThreadsDone) >=
                                        (threads) * InsertsPerThread)
                                    {
                                        @this._output.WriteLine($"[{Environment.CurrentManagedThreadId}] [SUCCESS] {c} _dequeueDone!!!!!!!!!!! {@this.SpamTestAsyncThreadsDone} > {(threads) * InsertsPerThread}");
                                        @this._dequeueDone = true;
                                        break;
                                    }

                                    if (++c % 5000 == 0)
                                    {
                                        @this._output.WriteLine($"[{Environment.CurrentManagedThreadId}] {c}/{@this.SpamTestAsyncThreadsDone} > {(threads) * InsertsPerThread} -->");
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                @this._output.WriteLine(e.Message);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        @this._output.WriteLine(e.Message);
                    }
                    @this._output.WriteLine($"[{Environment.CurrentManagedThreadId}] [SUCCESS] DQ DONE!!!! {@this.SpamTestAsyncThreadsDone} > {(threads>>1) * InsertsPerThread}");
                    Interlocked.Increment(ref @this._haltCount);
                }, (this, bag, threads,i), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap());

                insert.Add(Task.Factory.StartNew(static state =>
                {
                    var (@this, bag,threads) = (ValueTuple<IoQTest, IoZeroQ<IoInt32>,int>)state!;
                    var c = 0;
                    while (@this._haltCount < threads)
                    {
                        if (bag.TryEnqueue(Interlocked.Increment(ref @this.SpamTestAsyncThreadId)) != 1)
                        {
                            c++;
                        }
                        else
                        {
                            Thread.Yield();
                            Interlocked.Decrement(ref @this.SpamTestAsyncThreadId);
                        }
                    }

                    try
                    {
                        @this._output.WriteLine($"[{Environment.CurrentManagedThreadId}] [SUCCESS] {c} eq done <--");
                    }
                    catch
                    {
                        // ignored
                    }
                }, (this, bag,threads), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default));
            }

            
            await Task.WhenAll(insert).WaitAsync(TimeSpan.FromSeconds(15));
            _output.WriteLine($"Inserts tasks {insert.Count}");

            _smokeTestDone = true;
            //bag.TryEnqueue(-1);
            await Task.WhenAll(remove).WaitAsync(TimeSpan.FromSeconds(15));
            _output.WriteLine("remove done");

            //Assert.Equal(threads * InsertsPerThread + 4, bag.Count);
            Assert.InRange(SpamTestAsyncThreadId, threads & InsertsPerThread, int.MaxValue);


            c = 0;
            foreach (var ioInt32 in bag)
            {
                c++;
            }

            Assert.Equal(c, bag.Count);
        }

        [Fact]
        public async Task BalanceOnConsumeTestAsync()
        {
            var threads = 4;
            
            var cs = new CancellationTokenSource();
            var bag = new IoZeroQ<IoInt32>("test", 8192, true, cs, threads, zeroAsyncMode:false);
            
            var c = 0;
            foreach (var ioInt32 in bag)
                c++;

            Assert.Equal(0, c);

            bag.TryEnqueue(0);
            bag.TryEnqueue(1);
            bag.TryEnqueue(2);
            var preload = 0;
            await foreach (var item in bag.BalanceOnConsumeAsync(0))
            {
                //_output.WriteLine(item.ToString());
                if (++preload == 3)
                    break;
            }

            var insert = new List<Task>();
            var remove = new List<Task>();
            for (var i = 0; i < threads; i++)
            {
                remove.Add(Task.Factory.StartNew(static async state =>
                {
                    var (@this, bag, threads,i) = (ValueTuple<IoQTest, IoZeroQ<IoInt32>, int,int>)state!;

                    var c = 0;

                    try
                    {
                        await foreach (var item in bag.BalanceOnConsumeAsync(i))
                        {
                            if ((@this._smokeTestDone && bag.Count == 0) || Interlocked.Increment(ref @this.SpamTestAsyncThreadsDone) >=
                                threads * InsertsPerThread)
                            {
                                @this._output.WriteLine($"[{Environment.CurrentManagedThreadId}] done DQ {c}, t  = {@this.SpamTestAsyncThreadsDone}...");
                                break;
                            }
                            //if (c % 10000 == 0 && c > 0)
                            //    @this._output.WriteLine($"dq {c}");
                            c++;
                        }
                    }
                    catch (Exception e)
                    {
                        @this._output.WriteLine($"{c} dq error - {e.Message}");
                    }
                    @this._output.WriteLine($"{c} dq done");
                }, (this, bag, threads, i), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap());

                insert.Add(Task.Factory.StartNew(static state =>
                {
                    var (@this, bag) = (ValueTuple<IoQTest, IoZeroQ<IoInt32>>)state!;
                    var c = 0;
                    while (c < InsertsPerThread)
                    {
                        if (bag.TryEnqueue(Interlocked.Increment(ref @this.SpamTestAsyncThreadId)) != -1)
                        {
                            //if (c % 10000 == 0 && c > 0)
                            //    @this._output.WriteLine($"eq {c}");
                            c++;
                        }
                        else
                        {
                            Interlocked.Decrement(ref @this.SpamTestAsyncThreadId);
                        }
                    }
                    @this._output.WriteLine($"{c} eq done");
                }, (this, bag), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default));
            }

            _output.WriteLine($"Inserts tasks {insert.Count}");
            await Task.WhenAll(insert).WaitAsync(TimeSpan.FromSeconds(15));
            _output.WriteLine("Inserts done");
            _smokeTestDone = true;
            await bag.ZeroManagedAsync<object>(zero:true).FastPath();
            await Task.WhenAll(remove).WaitAsync(TimeSpan.FromSeconds(15));

            Assert.Equal(0, bag.Count);
            Assert.Equal(threads * InsertsPerThread, SpamTestAsyncThreadId);

            c = 0;
            foreach (var ioInt32 in bag)
            {
                c++;
            }

            Assert.Equal(c, bag.Count);
        }

        [Fact]
        public async Task PumpOnConsumeTestAsync()
        {
            var threads = 1;
            //var threads = 1;

            var cs = new CancellationTokenSource();
            var bag = new IoZeroQ<IoInt32>("test", 8192, false, cs, threads, zeroAsyncMode:false);
            
            var c = 0;
            foreach (var ioInt32 in bag)
                c++;

            Assert.Equal(0, c);

            bag.TryEnqueue(0);
            bag.TryEnqueue(1);
            bag.TryEnqueue(2);
            var preload = 0;
            await foreach (var item in bag.PumpOnConsumeAsync(0))
            {
                _output.WriteLine(item.ToString());
                if (++preload == 3)
                    break;
            }

            var insert = new List<Task>();
            var remove = new List<Task>();
            for (var i = 0; i < threads; i++)
            {
                if (i < threads)
                    remove.Add(Task.Factory.StartNew(static async state =>
                    {
                        var (@this, bag, threads, i) = (ValueTuple<IoQTest, IoZeroQ<IoInt32>, int, int>)state!;

                        var c = 0;
                        @this._output.WriteLine($" DQ at -> thread id {Environment.CurrentManagedThreadId}");
                        //while(!@this._smokeTestDone)
                            await foreach (var item in bag.PumpOnConsumeAsync(i))
                            {
                                if (Interlocked.Increment(ref @this.SpamTestAsyncThreadId) > threads * InsertsPerThread)
                                {
                                    @this._insertsDone = true;
                                    @this._output.WriteLine($"{c} dq break! count = {@this.SpamTestAsyncThreadId} test done = {@this._smokeTestDone}");
                                    break;
                                }
                                if (c % 2500 == 0)
                                    @this._output.WriteLine($"[{Environment.CurrentManagedThreadId}]dq {c}");
                                c++;
                            }
                        @this._output.WriteLine($"{c} dq done");
                        //while(bag.TryDequeue(out var _)){} ;
                        //@this._output.WriteLine($"{c} dq DRAIN! done");
                    }, (this, bag, threads, i), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap());

                insert.Add(Task.Factory.StartNew(static state =>
                {
                    var (@this, bag) = (ValueTuple<IoQTest, IoZeroQ<IoInt32>>)state!;
                    var c = 0;
                    while (!@this._insertsDone)
                    {
                        if (bag.TryEnqueue(@this.SpamTestAsyncThreadId) > 0)
                        {
                            Interlocked.Increment(ref @this.SpamTestAsyncThreadId);
                            //if (c % 2000 == 0)
                            //    @this._output.WriteLine($"eq {c}");
                            c++;
                        }
                    }
                    @this._output.WriteLine($"{c} eq done");
                    while (bag.TryDequeue(out var _)) { };
                    @this._output.WriteLine($"{c} eq DRAIN! done");
                }, (this, bag), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default));
            }

            _output.WriteLine($"Inserts tasks {insert.Count}");
            await Task.WhenAll(remove).WaitAsync(TimeSpan.FromSeconds(15));
            _output.WriteLine("Inserts done");
            _smokeTestDone = true;
            await Task.WhenAll(insert).WaitAsync(TimeSpan.FromSeconds(15));

            bag.TryEnqueue(-1);
            
            Assert.InRange(SpamTestAsyncThreadId, threads * InsertsPerThread, int.MaxValue);
            Assert.Equal(1, bag.Count);


            c = 0;
            foreach (var ioInt32 in bag)
            {
                c++;
            }

            Assert.Equal(c, bag.Count);
        }
    }
}
