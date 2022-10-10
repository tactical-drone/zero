using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.Threading;
using Xunit;
using Xunit.Abstractions;
using zero.core.misc;
using zero.core.patterns.misc;
using zero.core.patterns.queue;


namespace zero.test.core.patterns.queue{

    public class IoQueueTest : IDisposable
    {
        private const int ERR_T = 16 * 30;
        public IoQueueTest(ITestOutputHelper output)
        {
            _output = output;
            context = new Context();
        }
        private readonly Context context;
        private readonly ITestOutputHelper _output;

        [Fact]
        void InitQueueTest()
        {
            var sb = new StringBuilder();
            foreach (var ioZNode in context.Q)
            {
                sb.Append(ioZNode.Value);
            }

            Assert.Equal("123456789", sb.ToString());
        }

        [Fact]
        void RemoveHead()
        {
            context.Q.RemoveAsync(context.Head, context.Head.Qid).FastPath().GetAwaiter();

            var sb = new StringBuilder();
            foreach (var ioZNode in context.Q)
            {
                sb.Append(ioZNode.Value);
            }

            Assert.Equal("23456789", sb.ToString());

            sb.Clear();

            var c = context.Q.Tail;
            while (c != null)
            {
                sb.Append(c.Value);
                c = c.Prev;
            }
            Assert.Equal("98765432", sb.ToString());
        }

        [Fact]
        void RemoveTail()
        {
            context.Q.RemoveAsync(context.Tail, context.Tail.Qid).FastPath().GetAwaiter();

            var sb = new StringBuilder();
            foreach (var ioZNode in context.Q)
            {
                sb.Append(ioZNode.Value);
            }

            Assert.Equal("12345678", sb.ToString());

            sb.Clear();

            var c = context.Q.Tail;
            while (c != null)
            {
                sb.Append(c.Value);
                c = c.Prev;
            }
            Assert.Equal("87654321", sb.ToString());
        }

        [Fact]
        void RemoveMid()
        {
            context.Q.RemoveAsync(context.Middle, context.Middle.Qid).FastPath().GetAwaiter();

            var sb = new StringBuilder();
            foreach (var ioZNode in context.Q)
            {
                sb.Append(ioZNode.Value);
            }

            Assert.Equal("12346789", sb.ToString());

            sb.Clear();

            var c = context.Q.Tail;
            while (c != null)
            {
                sb.Append(c.Value);
                c = c.Prev;
            }
            Assert.Equal("98764321", sb.ToString());
        }

        [Fact]
        void DequeueSecondLastPrime()
        {
            context.Q.DequeueAsync().FastPath().GetAwaiter();
            context.Q.RemoveAsync(context.Q.Head, context.Q.Head.Qid).FastPath().GetAwaiter();
            context.Q.RemoveAsync(context.Q.Head, context.Q.Head.Qid).FastPath().GetAwaiter();
            context.Q.DequeueAsync().FastPath().GetAwaiter();

            context.Q.RemoveAsync(context.Q.Tail, context.Q.Tail.Qid).FastPath().GetAwaiter();
            context.Q.DequeueAsync().FastPath().GetAwaiter();
            

            var sb = new StringBuilder();
            foreach (var ioZNode in context.Q)
            {
                sb.Append(ioZNode.Value);
            }

            Assert.Equal("678", sb.ToString());

            sb.Clear();

            var c = context.Q.Tail;
            while (c != null)
            {
                sb.Append(c.Value);
                c = c.Prev;
            }
            Assert.Equal("876", sb.ToString());
        }

        [Fact]
        public void DequeueSecondLast()
        {
            context.Q.DequeueAsync().FastPath().GetAwaiter();
            context.Q.RemoveAsync(context.Q.Head, context.Q.Head.Qid).FastPath().GetAwaiter();
            context.Q.RemoveAsync(context.Q.Head, context.Q.Head.Qid).FastPath().GetAwaiter();
            context.Q.DequeueAsync().FastPath().GetAwaiter();

            context.Q.RemoveAsync(context.Q.Tail, context.Q.Tail.Qid).FastPath().GetAwaiter();
            context.Q.DequeueAsync().FastPath().GetAwaiter();
            context.Q.RemoveAsync(context.Q.Tail, context.Q.Tail.Qid).FastPath().GetAwaiter();
            context.Q.DequeueAsync().FastPath().GetAwaiter();
            context.Q.RemoveAsync(context.Q.Head, context.Q.Tail.Qid).FastPath().GetAwaiter();

            var sb = new StringBuilder();
            foreach (var ioZNode in context.Q)
            {
                sb.Append(ioZNode.Value);
            }

            Assert.Equal("", sb.ToString());

            sb.Clear();

            var c = context.Q.Tail;
            while (c != null)
            {
                sb.Append(c.Value);
                c = c.Prev;
            }
            Assert.Equal("", sb.ToString());
        }

        [Fact]
#pragma warning disable IDE0051 // Remove unused private members
        private async Task SpamTestAsync()
#pragma warning restore IDE0051 // Remove unused private members
        {
            var concurrentTasks = new List<Task>();
#if DEBUG
            var rounds = 4;
            var mult = 1000;
#else
            var rounds = 16;
            var mult = 10000;
#endif
            
            var start = Environment.TickCount;

            for (var i = 0; i < rounds; i++)
            {
                concurrentTasks.Add(Task.Factory.StartNew(static async state =>
                {
#if DEBUG
                    var mult = 1000;
#else
                    var mult = 10000;
#endif

                    var @this = (IoQueueTest)state!;
                    for (int j = 0; j < mult; j++)
                    {
                        try
                        {
                            await @this.context.Q.PushBackAsync(0).FastPath();
                            await @this.context.Q.EnqueueAsync(1).FastPath();
                            await @this.context.Q.PushBackAsync(2).FastPath();
                            await @this.context.Q.EnqueueAsync(3).FastPath();
                            await @this.context.Q.PushBackAsync(4).FastPath();

                            await @this.context.Q.DequeueAsync().FastPath();
                            await @this.context.Q.DequeueAsync().FastPath();
                            await @this.context.Q.DequeueAsync().FastPath();
                            await @this.context.Q.DequeueAsync().FastPath();
                            await @this.context.Q.DequeueAsync().FastPath();
                        }
                        catch (Exception e)
                        {
                            @this._output.WriteLine(e.ToString());
                        }
                    }
                    //@this._output.WriteLine($"({@this.context.Q.Count})");
                },this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap());
            }
            await Task.WhenAll(concurrentTasks).WaitAsync(TimeSpan.FromSeconds(60));

            _output.WriteLine($"count = {context.Q.Count}, Head = {context.Q?.Tail?.Value}, tail = {context.Q?.Head?.Value}, time = {Environment.TickCount - start}ms, {rounds * mult * 6 / (Environment.TickCount - start + 1)} kOPS");

            Assert.Equal(9, context.Q!.Count);
            Assert.NotNull(context.Q.Head);
            Assert.NotNull(context.Q.Tail);

            var kops = rounds * mult * 6 / (Environment.TickCount - start + 1);
            Assert.InRange(kops, 100, int.MaxValue);

            _output.WriteLine($"kops = {kops}");

            while(context.Q.Count > 0)
            {
                await context.Q.DequeueAsync().FastPath();
            }

            Assert.Equal(0, context.Q.Count);
            Assert.Null(context.Q.Head);
            Assert.Null(context.Q.Tail);
        }

        private volatile int _inserted;
        [Fact]
        public async Task IteratorAsync()
        {
#if DEBUG
            var threads = 3;
            var itemsPerThread = 1000;
#else
            var threads = 10;
            var itemsPerThread = 10000;
#endif

            var capacity = threads * itemsPerThread;

            var q = new IoQueue<int>("test Q", capacity, threads);

            var c = 0;
            foreach (var ioInt32 in q)
                c++;

            Assert.Equal(0, c);

            var insert = new List<Task>();

            
            for (var i = 0; i < threads; i++)
            {
                insert.Add(Task.Factory.StartNew(static async state =>
                {
                    var (@this, q, idx, itemsPerThread, output) = (ValueTuple<IoQueueTest,IoQueue<int>, int, int, ITestOutputHelper>)state!;
                    for (var i = 0; i < itemsPerThread; i++)
                    {
                        if(i%2 == 0)
                            await q.EnqueueAsync(i).FastPath();
                        else
                            await q.PushBackAsync(i).FastPath();
                        Interlocked.Increment(ref @this._inserted);
                        if(i% itemsPerThread/2 == 0)
                            output.WriteLine($"thread[{idx}] = done... {@this._inserted}");
                    }
                        
                    output.WriteLine($"thread[{idx}] = done... {@this._inserted}");
                }, (this, q, i, itemsPerThread, _output), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap());
            }

            await Task.WhenAll(insert).WaitAsync(TimeSpan.FromSeconds(60));

            Assert.Equal(capacity, _inserted);

            await q.DequeueAsync().FastPath();
            await q.DequeueAsync().FastPath();
            await q.DequeueAsync().FastPath();

            Assert.Equal(capacity - 3, q.Count);

            c = 0;
            foreach (var ioInt32 in q)
            {
                c++;
            }

            Assert.Equal(capacity - 3, c);

            await q.ZeroManagedAsync<object>(zero:true).FastPath();

            Assert.Equal(0, q.Count);
            Assert.Null(q.Head);
            Assert.Null(q.Tail);

            c = 0;
            foreach (var ioInt32 in q)
                c++;

            Assert.Equal(0, c);
        }

        //[Fact]
        //async Task AutoScaleAsync()
        //{
        //    await Task.Yield();
        //    var q = new IoQueue<int>("test Q", 2, 1, IoQueue<int>.Mode.DynamicSize);
        //    await q.EnqueueAsync(0);
        //    await q.EnqueueAsync(1);
        //    await q.EnqueueAsync(2);
        //    await q.EnqueueAsync(3);
        //    await q.EnqueueAsync(4);

        //    await q.DequeueAsync();
        //    await q.DequeueAsync();
        //    await q.DequeueAsync();
        //    await q.DequeueAsync();
        //    await q.DequeueAsync();

        //    Assert.Equal(15, q.Capacity);
        //}

        private IoQueue<IoInt32> _queuePressure = null!;
        private Task _queueNoBlockingTask = null!;
        private CancellationTokenSource _blockCancellationSignal = null!;

        [Fact]
#pragma warning disable IDE0051 // Remove unused private members
        private async Task NoQueuePressureAsync()
#pragma warning restore IDE0051 // Remove unused private members
        {
            
            _blockCancellationSignal = new CancellationTokenSource();
            _queuePressure = new IoQueue<IoInt32>("test Q", 1, 2);

            await _queuePressure.EnqueueAsync(0);

            var item = await _queuePressure.DequeueAsync().FastPath();

            Assert.NotNull(item);

            _queueNoBlockingTask = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoQueueTest)state!;

                var item = await @this._queuePressure.DequeueAsync().FastPath();
                Assert.Null(item);
            }, this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();

            var enqueueTask = _queueNoBlockingTask.ContinueWith((_,@this) => ((IoQueueTest)@this!)._blockCancellationSignal.Cancel(), this, CancellationToken.None, TaskContinuationOptions.DenyChildAttach, TaskScheduler.Default);

            
            var dequeue = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoQueueTest)state!;

                try
                {
                    //Wait for up to 2 seconds for results
                    await Task.Delay(2000, @this._blockCancellationSignal.Token);
                }
                catch
                {
                    // ignored
                }

                Assert.True(@this._queueNoBlockingTask.IsCompletedSuccessfully);
            }, this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();

            await dequeue;
            await enqueueTask;
            Assert.True(dequeue.IsCompletedSuccessfully);
            Assert.True(enqueueTask.IsCompletedSuccessfully);
        }

        [Fact]
#pragma warning disable IDE0051 // Remove unused private members
        private async Task QueuePressureAsync()
#pragma warning restore IDE0051 // Remove unused private members
        {
            _blockCancellationSignal = new CancellationTokenSource();
            _queuePressure = new IoQueue<IoInt32>("test Q", 10, 2, IoQueue<IoInt32>.Mode.Pressure);

            await _queuePressure.EnqueueAsync(0);

            var item = await _queuePressure.DequeueAsync().FastPath();

            Assert.NotNull(item);

            _queueNoBlockingTask = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoQueueTest)state!;
                var s = Environment.TickCount;
                var item = await @this._queuePressure.DequeueAsync().FastPath();
                Assert.InRange(s.ElapsedMs(), 100, 100 + ERR_T);
                Assert.NotNull(item);
            }, this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();
                
            var dequeTask = _queueNoBlockingTask.ContinueWith((_, @this) =>
            {
                ((IoQueueTest)@this!)._blockCancellationSignal.Cancel();
                if (_.Exception != null)
                {
                    throw _.Exception;
                }
                //Assert.True(_.IsCompletedSuccessfully);
            }, this, CancellationToken.None, TaskContinuationOptions.DenyChildAttach, TaskScheduler.Default);


            var insertTask = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoQueueTest)state!;

                try
                {
                    await Task.Delay(100, @this._blockCancellationSignal.Token);
                    await @this._queuePressure.EnqueueAsync(1).FastPath();
                    //Wait for up to 2 seconds for results
                    await Task.Delay(2000, @this._blockCancellationSignal.Token);
                }
                catch
                {
                    // ignored
                }
            }, this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();

            await Task.WhenAll(new[]{insertTask, dequeTask}).WaitAsync(TimeSpan.FromSeconds(60));
        }

        [Fact]
#pragma warning disable IDE0051 // Remove unused private members
        private async Task QueueBackPressureAsync()
#pragma warning restore IDE0051 // Remove unused private members
        {
            _blockCancellationSignal = new CancellationTokenSource();
            _queuePressure = new IoQueue<IoInt32>("test Q", 10, 1, IoQueue<IoInt32>.Mode.BackPressure | IoQueue<IoInt32>.Mode.Pressure);

            await _queuePressure.EnqueueAsync(0).FastPath();

            var item = await _queuePressure.DequeueAsync().FastPath();
            Assert.NotNull(item);

            await _queuePressure.EnqueueAsync(0).FastPath();

            item = await _queuePressure.DequeueAsync().FastPath();
            Assert.NotNull(item);

            _queueNoBlockingTask = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoQueueTest)state!;
                var s = Environment.TickCount;
                int item;
                //var item = await @this._queuePressure.DequeueAsync().FastPath();
                //Assert.InRange(s.ElapsedMs(), 100 - 16, 2000);
                //Assert.NotNull(item);

                s = Environment.TickCount;
                item = await @this._queuePressure.DequeueAsync().FastPath();
                Assert.InRange(s.ElapsedMs(), 100 - ERR_T, 2000);

                await Task.Delay(100);

                s = Environment.TickCount;
                item = await @this._queuePressure.DequeueAsync().FastPath();
                Assert.InRange(s.ElapsedMs(), 0, ERR_T);
                Assert.Equal(2, item);

            }, this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();

            var dequeTask = _queueNoBlockingTask.ContinueWith((task, @this) =>
            {
                ((IoQueueTest)@this!)._blockCancellationSignal.Cancel();
                if (task.Exception != null)
                {
                    throw task.Exception;
                }
                //Assert.True(_.IsCompletedSuccessfully);
            }, this, CancellationToken.None, TaskContinuationOptions.DenyChildAttach, TaskScheduler.Default);


            var insertTask = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoQueueTest)state!;
                var s = Environment.TickCount;
                await Task.Delay(100, @this._blockCancellationSignal.Token);
                await @this._queuePressure.EnqueueAsync(1).FastPath();
                Assert.InRange(s.ElapsedMs(),  100 - ERR_T, 10000);
                //blocking
                //_ = @this._queuePressure.EnqueueAsync(1).FastPath();

                s = Environment.TickCount;
                await @this._queuePressure.EnqueueAsync(2).FastPath();
                Assert.InRange(s.ElapsedMs(), 0, 10000);

                await @this._queuePressure.EnqueueAsync(3).FastPath();
                Assert.InRange(s.ElapsedMs(), 100 - ERR_T, 10000);
                //Wait for up to 2 seconds for results
                await Task.Delay(2000, @this._blockCancellationSignal.Token);
            }, this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap().ContinueWith(task =>
            {
                if (task.Exception != null)
                {
                    //throw task.Exception.InnerExceptions.First();
                    _output.WriteLine(task.Exception.InnerExceptions.First().Message);
                    _output.WriteLine(task.Exception.InnerExceptions.First().StackTrace);
                }
            }, CancellationToken.None, TaskContinuationOptions.DenyChildAttach, TaskScheduler.Default);

            await Task.WhenAll(insertTask, dequeTask).WaitAsync(TimeSpan.FromSeconds(60));
        }

        
        const int BlockDelay = 100;
        const int Concurrency = 10;
        const int NrOfItems = Concurrency * 10;
        [Fact]
#pragma warning disable IDE0051 // Remove unused private members
        private async Task QueueBackPressureBlockingAsync()
#pragma warning restore IDE0051 // Remove unused private members
        {
            
            _blockCancellationSignal = new CancellationTokenSource();
            _queuePressure = new IoQueue<IoInt32>("test Q", Concurrency, Concurrency, IoQueue<IoInt32>.Mode.BackPressure | IoQueue<IoInt32>.Mode.Pressure);
            
            for (var i = 0; i < Concurrency; i++)
                await _queuePressure.EnqueueAsync(i);

            var dq = Task.Factory.StartNew(async o =>
            {
                var output = (ITestOutputHelper)o;
                var count = 0;
                var dqList = new List<Task<IoInt32>>(Concurrency);
                while (!_queuePressure.Zeroed)
                {
                    await Task.Delay(BlockDelay);

                    for (int j = 0; j < Concurrency; j++)
                    {
                        dqList.Add(_queuePressure.DequeueAsync().AsTask());
                        count++;
                    }
                    await Task.WhenAll(dqList);

                    foreach (var t in dqList)
                    {
#pragma warning disable VSTHRD103 // Call async methods when in an async method
                        Assert.NotNull(t.Result);
                        Assert.True(t.Result < count + Concurrency);
#pragma warning restore VSTHRD103 // Call async methods when in an async method
                    }

                    dqList.Clear();

                    output.WriteLine($"processed count = {count}");
                    if (++count >= NrOfItems)
                    {
                        output.WriteLine($"DQ DONE! count = {count}");
                        await _queuePressure.ZeroManagedAsync<object>(zero: true);
                    }
                }

                Assert.InRange(count, NrOfItems, NrOfItems + Concurrency);

            }, _output, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();

            _output.WriteLine($"Processing {NrOfItems}");
            var q = Task.Factory.StartNew(async o =>
            {
                var output = (ITestOutputHelper)o;
                var eqList = new List<Task<IoQueue<IoInt32>.IoZNode>>(Concurrency);
                long ave = 0;
                for (var i = 0; i < NrOfItems/Concurrency && !_queuePressure.Zeroed; i++)
                {
                    var ts = Environment.TickCount;
                    
                    for (int j = 0; j < Concurrency; j++)
                    {
                        eqList.Add(_queuePressure.EnqueueAsync(i + j).AsTask());
                    }

                    await Task.WhenAll(eqList);

                    if (_queuePressure.Zeroed)
                    {
                        break;
                    }
                    
                    Assert.InRange(ts.ElapsedMs(), BlockDelay - ERR_T, BlockDelay * 4);

                    foreach (var t in eqList)
                    {
                        var item = await t;
                        Assert.NotNull(item);
                        Assert.True(item.Value < (i + 1) * Concurrency);
                    }

                    eqList.Clear();

                    output.WriteLine($"[{i}/{NrOfItems / Concurrency}] Inserted count = { (i + 1) * Concurrency}, t = {ts.ElapsedMs()}ms ~ {BlockDelay}");
                    ave += ts.ElapsedMs();
                }

                _output.WriteLine("Q DONE");
                Assert.InRange(ave / (NrOfItems / Concurrency), BlockDelay - 16, BlockDelay * 2);
            },_output, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();

            try
            {
                await Task.WhenAll(q, dq).WithTimeout(TimeSpan.FromSeconds(30));
            }
            catch(TaskCanceledException){}
        }

        public class Context : IDisposable
        {
            public Context()
            {
                Q = new IoQueue<int>("test Q", 2000, 1000);
#pragma warning disable VSTHRD002 // Avoid problematic synchronous waits
                Q.EnqueueAsync(2).FastPath().GetAwaiter().GetResult();
                Q.EnqueueAsync(3).FastPath().GetAwaiter();
                Head = Q.PushBackAsync(1).FastPath().GetAwaiter().GetResult();
                Q.EnqueueAsync(4).FastPath().GetAwaiter();
                Middle = Q.EnqueueAsync(5).FastPath().GetAwaiter().GetResult();
                Q.EnqueueAsync(6).FastPath().GetAwaiter();
                Q.EnqueueAsync(7).FastPath().GetAwaiter();
                Q.EnqueueAsync(8).FastPath().GetAwaiter();
                Tail = Q.EnqueueAsync(9).FastPath().GetAwaiter().GetResult();
#pragma warning restore VSTHRD002 // Avoid problematic synchronous waits
            }

            public IoQueue<int> Q;
            public IoQueue<int>.IoZNode Head;
            public IoQueue<int>.IoZNode Middle;
            public IoQueue<int>.IoZNode Tail;

            public void Dispose()
            {
                Q.ZeroManagedAsync<object>(zero:true).GetAwaiter();
                Q = default!;
            }
        }

        public void Dispose()
        {
            context.Dispose();
        }
    }
}
