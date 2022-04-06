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
        public IoQueueTest(ITestOutputHelper output)
        {
            _output = output;
            context = new Context();
        }
        private readonly bool _zc = IoNanoprobe.ContinueOnCapturedContext;
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
            context.Q.RemoveAsync(context.Head).FastPath().ConfigureAwait(_zc).GetAwaiter();

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
            context.Q.RemoveAsync(context.Tail).FastPath().ConfigureAwait(_zc).GetAwaiter();

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
            context.Q.RemoveAsync(context.Middle).FastPath().ConfigureAwait(_zc).GetAwaiter();

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
            context.Q.DequeueAsync().FastPath().ConfigureAwait(_zc).GetAwaiter();
            context.Q.RemoveAsync(context.Q.Head).FastPath().ConfigureAwait(_zc).GetAwaiter();
            context.Q.RemoveAsync(context.Q.Head).FastPath().ConfigureAwait(_zc).GetAwaiter();
            context.Q.DequeueAsync().FastPath().ConfigureAwait(_zc).GetAwaiter();

            context.Q.RemoveAsync(context.Q.Tail).FastPath().ConfigureAwait(_zc).GetAwaiter();
            context.Q.DequeueAsync().FastPath().ConfigureAwait(_zc).GetAwaiter();
            

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
            context.Q.DequeueAsync().FastPath().ConfigureAwait(_zc).GetAwaiter();
            context.Q.RemoveAsync(context.Q.Head).FastPath().ConfigureAwait(_zc).GetAwaiter();
            context.Q.RemoveAsync(context.Q.Head).FastPath().ConfigureAwait(_zc).GetAwaiter();
            context.Q.DequeueAsync().FastPath().ConfigureAwait(_zc).GetAwaiter();

            context.Q.RemoveAsync(context.Q.Tail).FastPath().ConfigureAwait(_zc).GetAwaiter();
            context.Q.DequeueAsync().FastPath().ConfigureAwait(_zc).GetAwaiter();
            context.Q.RemoveAsync(context.Q.Tail).FastPath().ConfigureAwait(_zc).GetAwaiter();
            context.Q.DequeueAsync().FastPath().ConfigureAwait(_zc).GetAwaiter();
            context.Q.RemoveAsync(context.Q.Head).FastPath().ConfigureAwait(_zc).GetAwaiter();

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
        async Task SpamTestAsync()
        {
            var concurrentTasks = new List<Task>();
#if DEBUG
            var rounds = 2;
            var mult = 10000;
#else
            var rounds = 16;
            var mult = 10000;
#endif
            
            var start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

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
                            var eq1 = @this.context.Q.PushBackAsync(0);
                            var eq2 = @this.context.Q.EnqueueAsync(1);
                            var i1 = @this.context.Q.PushBackAsync(2);
                            var i2 = @this.context.Q.EnqueueAsync(3);
                            var i4 = @this.context.Q.PushBackAsync(4);

                            await eq2.ConfigureAwait(@this._zc);
                            await eq1.ConfigureAwait(@this._zc);
                            await i4.ConfigureAwait(@this._zc);
                            await i2.ConfigureAwait(@this._zc);
                            await i1.ConfigureAwait(@this._zc);

                            
                            var d2 = @this.context.Q.DequeueAsync();
                            var d4 = @this.context.Q.DequeueAsync();
                            var d5 = @this.context.Q.DequeueAsync();
                            await @this.context.Q.DequeueAsync().FastPath().ConfigureAwait(@this._zc);
                            await @this.context.Q.DequeueAsync().FastPath().ConfigureAwait(@this._zc);
                            await d5.ConfigureAwait(@this._zc);
                            await d4.ConfigureAwait(@this._zc);
                            await d2.ConfigureAwait(@this._zc);
                        }
                        catch (Exception e)
                        {
                            @this._output.WriteLine(e.ToString());
                        }
                    }
                    //@this._output.WriteLine($"({@this.context.Q.Count})");
                },this, TaskCreationOptions.DenyChildAttach).Unwrap());
            }
            await Task.WhenAll(concurrentTasks).WaitAsync(TimeSpan.FromSeconds(10));

            _output.WriteLine($"count = {context.Q.Count}, Head = {context.Q?.Tail?.Value}, tail = {context.Q?.Head?.Value}, time = {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start}ms, {rounds * mult * 6 / (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start)} kOPS");

            Assert.Equal(9, context.Q!.Count);
            Assert.NotNull(context.Q.Head);
            Assert.NotNull(context.Q.Tail);

            var kops = rounds * mult * 6 / (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start);

#if DEBUG
            Assert.InRange(kops, 20, int.MaxValue);
#else
            Assert.InRange(kops, 700, int.MaxValue);
#endif

            _output.WriteLine($"kops = {kops}");

            while(context.Q.Count > 0)
            {
                await context.Q.DequeueAsync().FastPath().ConfigureAwait(_zc);
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
            var threads = 2;
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

            var idx = 0;
            var insert = new List<Task>();

            
            for (var i = 0; i < threads; i++)
            {
                insert.Add(Task.Factory.StartNew(static async state =>
                {
                    var (@this, q, idx, itemsPerThread) = (ValueTuple<IoQueueTest,IoQueue<int>, int, int>)state!;
                    for (var i = 0; i < itemsPerThread; i++)
                    {
                        if(i%2 == 0)
                            await q.EnqueueAsync(Interlocked.Increment(ref idx));
                        else
                            await q.PushBackAsync(Interlocked.Increment(ref idx));
                        Interlocked.Increment(ref @this._inserted);
                    }
                        
                }, (this, q, idx, itemsPerThread), TaskCreationOptions.DenyChildAttach).Unwrap());
            }

            await Task.WhenAll(insert).WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(capacity, _inserted);

            await q.DequeueAsync();
            await q.DequeueAsync();
            await q.DequeueAsync();

            Assert.Equal(capacity - 3, q.Count);

            c = 0;
            foreach (var ioInt32 in q)
            {
                c++;
            }

            Assert.Equal(capacity - 3, c);

            await q.ZeroManagedAsync<object>(zero:true);

            Assert.Equal(0, q.Count);
            Assert.Null(q.Head);
            Assert.Null(q.Tail);

            c = 0;
            foreach (var ioInt32 in q)
                c++;

            Assert.Equal(0, c);
        }

        [Fact]
        async Task AutoScaleAsync()
        {
            await Task.Yield();
            var q = new IoQueue<int>("test Q", 2, 1, autoScale:true);
            await q.EnqueueAsync(0);
            await q.EnqueueAsync(1);
            await q.EnqueueAsync(2);
            await q.EnqueueAsync(3);
            await q.EnqueueAsync(4);

            await q.DequeueAsync();
            await q.DequeueAsync();
            await q.DequeueAsync();
            await q.DequeueAsync();
            await q.DequeueAsync();

            Assert.Equal(15, q.Capacity);
        }

        private IoQueue<IoInt32> _queuePressure = null!;
        private Task _queueNoBlockingTask = null!;
        private CancellationTokenSource _blockCancellationSignal = null!;

        [Fact]
        async Task NoQueuePressureAsync()
        {
            
            _blockCancellationSignal = new CancellationTokenSource();
            _queuePressure = new IoQueue<IoInt32>("test Q", 1, 2);

            await Task.Yield();

            await _queuePressure.EnqueueAsync(0);

            var item = await _queuePressure.DequeueAsync().FastPath().ConfigureAwait(_zc);

            Assert.NotNull(item);

            _queueNoBlockingTask = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoQueueTest)state!;

                var item = await @this._queuePressure.DequeueAsync().FastPath().ConfigureAwait(@this._zc);
                Assert.Null(item);
            }, this, TaskCreationOptions.DenyChildAttach).Unwrap();

            var enqueueTask = _queueNoBlockingTask.ContinueWith((_,@this) => ((IoQueueTest)@this!)._blockCancellationSignal.Cancel(), this);

            
            var dequeue = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoQueueTest)state!;

                try
                {
                    //Wait for up to 2 seconds for results
                    await Task.Delay(2000, @this._blockCancellationSignal.Token).ConfigureAwait(@this._zc);
                }
                catch
                {
                    // ignored
                }

                Assert.True(@this._queueNoBlockingTask.IsCompletedSuccessfully);
            }, this, TaskCreationOptions.DenyChildAttach).Unwrap();

            await dequeue.ConfigureAwait(_zc);
            await enqueueTask.ConfigureAwait(_zc);
            Assert.True(dequeue.IsCompletedSuccessfully);
            Assert.True(enqueueTask.IsCompletedSuccessfully);
        }

        [Fact]
        async Task QueuePressureAsync()
        {
            _blockCancellationSignal = new CancellationTokenSource();
            _queuePressure = new IoQueue<IoInt32>("test Q", 1, 2, disablePressure:false);

            await Task.Yield();

            await _queuePressure.EnqueueAsync(0);

            var item = await _queuePressure.DequeueAsync().FastPath().ConfigureAwait(_zc);

            Assert.NotNull(item);

            _queueNoBlockingTask = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoQueueTest)state!;
                var s = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                var item = await @this._queuePressure.DequeueAsync().FastPath().ConfigureAwait(@this._zc);
                Assert.InRange(s.ElapsedMs(), (100/15)*15, 100 + 15 * 2);
                Assert.NotNull(item);

            }, this, TaskCreationOptions.DenyChildAttach).Unwrap();
                
            var dequeTask = _queueNoBlockingTask.ContinueWith((_, @this) =>
            {
                ((IoQueueTest)@this!)._blockCancellationSignal.Cancel();
                if (_.Exception != null)
                {
                    throw _.Exception;
                }
                //Assert.True(_.IsCompletedSuccessfully);
            }, this);


            var insertTask = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoQueueTest)state!;

                try
                {
                    await Task.Delay(100, @this._blockCancellationSignal.Token).ConfigureAwait(@this._zc);
                    await @this._queuePressure.EnqueueAsync(1).FastPath().ConfigureAwait(@this._zc);
                    //Wait for up to 2 seconds for results
                    await Task.Delay(2000, @this._blockCancellationSignal.Token).ConfigureAwait(@this._zc);
                }
                catch
                {
                    // ignored
                }
            }, this, TaskCreationOptions.DenyChildAttach).Unwrap();

            await insertTask.ConfigureAwait(_zc);
            Assert.True(insertTask.IsCompletedSuccessfully);
            await dequeTask.ConfigureAwait(_zc);
            Assert.True(dequeTask.IsCompletedSuccessfully);
        }

        [Fact]
        async Task QueueBackPressureAsync()
        {
            _blockCancellationSignal = new CancellationTokenSource();
            _queuePressure = new IoQueue<IoInt32>("test Q", 4, 2, disablePressure: false, enableBackPressure:true);

            await Task.Yield();

            await _queuePressure.EnqueueAsync(0);

            var item = await _queuePressure.DequeueAsync().FastPath().ConfigureAwait(_zc);
            Assert.NotNull(item);

            await _queuePressure.EnqueueAsync(0);

            item = await _queuePressure.DequeueAsync().FastPath().ConfigureAwait(_zc);
            Assert.NotNull(item);

            _queueNoBlockingTask = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoQueueTest)state!;
                var s = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                var item = await @this._queuePressure.DequeueAsync().FastPath().ConfigureAwait(@this._zc);
                Assert.InRange(s.ElapsedMs(), 100, 2000);
                Assert.NotNull(item);

                await Task.Delay(100).ConfigureAwait(@this._zc);

                s = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                item = await @this._queuePressure.DequeueAsync().FastPath().ConfigureAwait(@this._zc);
                Assert.InRange(s.ElapsedMs(), 0, 500);
                Assert.NotNull(item);

            }, this, TaskCreationOptions.DenyChildAttach).Unwrap();

            var dequeTask = _queueNoBlockingTask.ContinueWith((task, @this) =>
            {
                ((IoQueueTest)@this!)._blockCancellationSignal.Cancel();
                if (task.Exception != null)
                {
                    throw task.Exception;
                }
                //Assert.True(_.IsCompletedSuccessfully);
            }, this);


            var insertTask = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoQueueTest)state!;
                await Task.Delay(100, @this._blockCancellationSignal.Token).ConfigureAwait(@this._zc);
                _ = @this._queuePressure.EnqueueAsync(1).FastPath().ConfigureAwait(@this._zc);
                _ = @this._queuePressure.EnqueueAsync(1).FastPath().ConfigureAwait(@this._zc);
                var s = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                //blocking
                await @this._queuePressure.EnqueueAsync(1).FastPath().ConfigureAwait(@this._zc);
                await @this._queuePressure.EnqueueAsync(1).FastPath().ConfigureAwait(@this._zc);
                Assert.InRange(s.ElapsedMs(), 100, 10000);
                //Wait for up to 2 seconds for results
                await Task.Delay(2000, @this._blockCancellationSignal.Token).ConfigureAwait(@this._zc);
            }, this, TaskCreationOptions.DenyChildAttach).Unwrap().ContinueWith(task =>
            {
                if (task.Exception != null)
                {
                    throw task.Exception;
                }
            });

            await insertTask.ConfigureAwait(_zc);
            Assert.True(insertTask.IsCompletedSuccessfully);
            await dequeTask.ConfigureAwait(_zc);
            Assert.True(dequeTask.IsCompletedSuccessfully);
        }

        
        const int BlockDelay = 100;
        const int Concurrency = 10;
        const int NrOfItems = Concurrency * 10;
        [Fact]
        async Task QueueBackPressureBlockingAsync()
        {
            
            _blockCancellationSignal = new CancellationTokenSource();
            _queuePressure = new IoQueue<IoInt32>("test Q", Concurrency, Concurrency, disablePressure: false, enableBackPressure: true);
            
            //await Task.Yield();

            for (var i = 0; i < Concurrency; i++)
                await _queuePressure.EnqueueAsync(i);

            _output.WriteLine($"Processing {NrOfItems}");
            var q = Task.Factory.StartNew(async o =>
            {
                var output = (ITestOutputHelper)o;
                var eqList = new List<Task<IoQueue<IoInt32>.IoZNode>>(Concurrency);
                long ave = 0;
                for (var i = 0; i < NrOfItems/Concurrency && !_queuePressure.Zeroed; i++)
                {
                    var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    
                    for (int j = 0; j < Concurrency; j++)
                    {
                        eqList.Add(_queuePressure.EnqueueAsync(i + j).AsTask());
                    }

                    await Task.WhenAll(eqList);

                    if (_queuePressure.Zeroed)
                        break;

                    Assert.InRange(ts.ElapsedMs(), BlockDelay, BlockDelay * 4);

                    foreach (var t in eqList)
                    {
                        Assert.NotNull(t.Result);
                        Assert.True(t.Result.Value < (i + 1) * Concurrency);
                    }

                    eqList.Clear();

                    output.WriteLine($"Inserted count = { (i + 1) * Concurrency}, t = {ts.ElapsedMs()}ms ~ {BlockDelay}");
                    ave += ts.ElapsedMs();
                }
                Assert.InRange(ave / (NrOfItems / Concurrency), BlockDelay, BlockDelay * 2);
            },_output).Unwrap();

            var dq = Task.Factory.StartNew(async o =>
            {
                var output = (ITestOutputHelper)o;
                var count = 0;
                var dqList = new List<Task<IoInt32>>(Concurrency);
                while (!_queuePressure.Zeroed)
                {
                    await Task.Delay(BlockDelay);
                    
                    for (int j = 0; j < Concurrency - 1; j++)
                    {
                        dqList.Add(_queuePressure.DequeueAsync().AsTask());
                        //output.WriteLine(".");
                        count++;
                    }

                    await Task.WhenAll(dqList);

                    foreach (var t in dqList)
                    {
                        Assert.NotNull(t.Result);
                        Assert.True(t.Result < count + Concurrency);
                    }

                    dqList.Clear();
                    
                    output.WriteLine($"processed count = {count}");
                    if (++count >= NrOfItems)
                    {
                        output.WriteLine($"DONE! count = {count}");
                        await _queuePressure.ZeroManagedAsync<object>(zero:true);
                    }
                }
                Assert.Equal(NrOfItems, count);
                
            }, _output).Unwrap();

            await Task.WhenAll(q,dq).WithTimeout(TimeSpan.FromSeconds(BlockDelay * NrOfItems / Concurrency * 2));

        }

        public class Context : IDisposable
        {
            public Context()
            {
                Q = new IoQueue<int>("test Q", 2000, 1000);
                Q.EnqueueAsync(2).FastPath().GetAwaiter().GetResult();
                Q.EnqueueAsync(3).FastPath().GetAwaiter();
                Head = Q.PushBackAsync(1).FastPath().GetAwaiter().GetResult();
                Q.EnqueueAsync(4).FastPath().GetAwaiter();
                Middle = Q.EnqueueAsync(5).FastPath().GetAwaiter().GetResult();
                Q.EnqueueAsync(6).FastPath().GetAwaiter();
                Q.EnqueueAsync(7).FastPath().GetAwaiter();
                Q.EnqueueAsync(8).FastPath().GetAwaiter();
                Tail = Q.EnqueueAsync(9).FastPath().GetAwaiter().GetResult();
            }

            private readonly bool Zc = IoNanoprobe.ContinueOnCapturedContext;
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
