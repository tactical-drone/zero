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
            var rounds = 10;
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
                    @this._output.WriteLine($"({@this.context.Q.Count})");
                },this, TaskCreationOptions.DenyChildAttach).Unwrap());
            }
            await Task.WhenAll(concurrentTasks).WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);

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

            await Task.WhenAll(insert).WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);

            Assert.Equal(capacity, _inserted);

            await q.DequeueAsync().FastPath().ConfigureAwait(false);
            await q.DequeueAsync().FastPath().ConfigureAwait(false);
            await q.DequeueAsync().FastPath().ConfigureAwait(false);

            Assert.Equal(capacity - 3, q.Count);

            c = 0;
            foreach (var ioInt32 in q)
            {
                c++;
            }

            Assert.Equal(capacity - 3, c);

            //while (q.Count > 0)
            //    await q.DequeueAsync().FastPath().ConfigureAwait(Zc);
            await q.ZeroManagedAsync<object>(zero:true).FastPath().ConfigureAwait(false);

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
            var q = new IoQueue<int>("test Q", 1, 1, autoScale:true);

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

            Assert.Equal(8, q.Capacity);
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
            _queuePressure = new IoQueue<IoInt32>("test Q", 2, 2, disablePressure: false, enableBackPressure:true);

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
                await @this._queuePressure.EnqueueAsync(1).FastPath().ConfigureAwait(@this._zc);
                await @this._queuePressure.EnqueueAsync(1).FastPath().ConfigureAwait(@this._zc);
                var s = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                //blocking
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

        [Fact]
        async Task QueueBackPressureBlockingAsync()
        {
            _blockCancellationSignal = new CancellationTokenSource();
            _queuePressure = new IoQueue<IoInt32>("test Q", 2, 2, disablePressure: false, enableBackPressure: true);

            await Task.Yield();

            await _queuePressure.EnqueueAsync(0);

            var item = await _queuePressure.DequeueAsync().FastPath().ConfigureAwait(_zc);
            Assert.NotNull(item);


            var t = Task.Factory.StartNew(async () =>
            {
                await Task.Delay(500);
                await _queuePressure.ZeroManagedAsync<object>(zero: true).FastPath().ConfigureAwait(_zc);
            });

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var dqBlocking = await _queuePressure.DequeueAsync().FastPath().ConfigureAwait(_zc);
            Assert.Null(dqBlocking);
            Assert.InRange(ts.ElapsedMs(), 400, 2000);
        }

        public class Context : IDisposable
        {
            public Context()
            {
                Q = new IoQueue<int>("test Q", 2000, 1000);
                Q.EnqueueAsync(2).FastPath().ConfigureAwait(Zc).GetAwaiter().GetResult();
                Q.EnqueueAsync(3).FastPath().ConfigureAwait(Zc).GetAwaiter();
                Head = Q.PushBackAsync(1).FastPath().ConfigureAwait(Zc).GetAwaiter().GetResult();
                Q.EnqueueAsync(4).FastPath().ConfigureAwait(Zc).GetAwaiter();
                Middle = Q.EnqueueAsync(5).FastPath().ConfigureAwait(Zc).GetAwaiter().GetResult();
                Q.EnqueueAsync(6).FastPath().ConfigureAwait(Zc).GetAwaiter();
                Q.EnqueueAsync(7).FastPath().ConfigureAwait(Zc).GetAwaiter();
                Q.EnqueueAsync(8).FastPath().ConfigureAwait(Zc).GetAwaiter();
                Tail = Q.EnqueueAsync(9).FastPath().ConfigureAwait(Zc).GetAwaiter().GetResult();
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
