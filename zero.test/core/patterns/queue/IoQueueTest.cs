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
            _context = new Context();
        }
        private bool Zc = IoNanoprobe.ContinueOnCapturedContext;
        private readonly Context _context;
        private readonly ITestOutputHelper _output;

        [Fact]
        void InitQueueTest()
        {
            var sb = new StringBuilder();
            foreach (var ioZNode in _context.Q)
            {
                sb.Append(ioZNode.Value);
            }

            Assert.Equal("123456789", sb.ToString());
        }

        [Fact]
        void RemoveHead()
        {
            _context.Q.RemoveAsync(_context.Head).FastPath().ConfigureAwait(Zc).GetAwaiter();

            var sb = new StringBuilder();
            foreach (var ioZNode in _context.Q)
            {
                sb.Append(ioZNode.Value);
            }

            Assert.Equal("23456789", sb.ToString());

            sb.Clear();

            var c = _context.Q.Tail;
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
            _context.Q.RemoveAsync(_context.Tail).FastPath().ConfigureAwait(Zc).GetAwaiter();

            var sb = new StringBuilder();
            foreach (var ioZNode in _context.Q)
            {
                sb.Append(ioZNode.Value);
            }

            Assert.Equal("12345678", sb.ToString());

            sb.Clear();

            var c = _context.Q.Tail;
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
            _context.Q.RemoveAsync(_context.Middle).FastPath().ConfigureAwait(Zc).GetAwaiter();

            var sb = new StringBuilder();
            foreach (var ioZNode in _context.Q)
            {
                sb.Append(ioZNode.Value);
            }

            Assert.Equal("12346789", sb.ToString());

            sb.Clear();

            var c = _context.Q.Tail;
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
            _context.Q.DequeueAsync().FastPath().ConfigureAwait(Zc).GetAwaiter();
            _context.Q.RemoveAsync(_context.Q.Head).FastPath().ConfigureAwait(Zc).GetAwaiter();
            _context.Q.RemoveAsync(_context.Q.Head).FastPath().ConfigureAwait(Zc).GetAwaiter();
            _context.Q.DequeueAsync().FastPath().ConfigureAwait(Zc).GetAwaiter();

            _context.Q.RemoveAsync(_context.Q.Tail).FastPath().ConfigureAwait(Zc).GetAwaiter();
            _context.Q.DequeueAsync().FastPath().ConfigureAwait(Zc).GetAwaiter();
            

            var sb = new StringBuilder();
            foreach (var ioZNode in _context.Q)
            {
                sb.Append(ioZNode.Value);
            }

            Assert.Equal("678", sb.ToString());

            sb.Clear();

            var c = _context.Q.Tail;
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
            _context.Q.DequeueAsync().FastPath().ConfigureAwait(Zc).GetAwaiter();
            _context.Q.RemoveAsync(_context.Q.Head).FastPath().ConfigureAwait(Zc).GetAwaiter();
            _context.Q.RemoveAsync(_context.Q.Head).FastPath().ConfigureAwait(Zc).GetAwaiter();
            _context.Q.DequeueAsync().FastPath().ConfigureAwait(Zc).GetAwaiter();

            _context.Q.RemoveAsync(_context.Q.Tail).FastPath().ConfigureAwait(Zc).GetAwaiter();
            _context.Q.DequeueAsync().FastPath().ConfigureAwait(Zc).GetAwaiter();
            _context.Q.RemoveAsync(_context.Q.Tail).FastPath().ConfigureAwait(Zc).GetAwaiter();
            _context.Q.DequeueAsync().FastPath().ConfigureAwait(Zc).GetAwaiter();
            _context.Q.RemoveAsync(_context.Q.Head).FastPath().ConfigureAwait(Zc).GetAwaiter();

            var sb = new StringBuilder();
            foreach (var ioZNode in _context.Q)
            {
                sb.Append(ioZNode.Value);
            }

            Assert.Equal("", sb.ToString());

            sb.Clear();

            var c = _context.Q.Tail;
            while (c != null)
            {
                sb.Append(c.Value);
                c = c.Prev;
            }
            Assert.Equal("", sb.ToString());
        }

        [Fact]
        async Task SpamTest()
        {
            var concurrentTasks = new List<Task>();
#if DEBUG
            var rounds = 10;
            var mult = 1000;
#else
            var rounds = 25;
            var mult = 40000;
#endif

            var start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            for (var i = 0; i < rounds; i++)
            {
                concurrentTasks.Add(Task.Factory.StartNew(static async state =>
                {
#if DEBUG
                    var mult = 1000;
#else
                    var mult = 40000;
#endif

                    var @this = (IoQueueTest)state!;
                    for (int j = 0; j < mult; j++)
                    {
                        try
                        {
                            var eq1 = @this._context.Q.PushBackAsync(0);
                            var eq2 = @this._context.Q.EnqueueAsync(1);
                            var i1 = @this._context.Q.PushBackAsync(2);
                            var i2 = @this._context.Q.EnqueueAsync(3);
                            var i4 = @this._context.Q.PushBackAsync(4);

                            await eq2.ConfigureAwait(@this.Zc);
                            await eq1.ConfigureAwait(@this.Zc);
                            await i4.ConfigureAwait(@this.Zc);
                            await i2.ConfigureAwait(@this.Zc);
                            await i1.ConfigureAwait(@this.Zc);

                            
                            var d2 = @this._context.Q.DequeueAsync();
                            var d4 = @this._context.Q.DequeueAsync();
                            var d5 = @this._context.Q.DequeueAsync();
                            await @this._context.Q.DequeueAsync().FastPath().ConfigureAwait(@this.Zc);
                            await @this._context.Q.DequeueAsync().FastPath().ConfigureAwait(@this.Zc);
                            await d5.ConfigureAwait(@this.Zc);
                            await d4.ConfigureAwait(@this.Zc);
                            await d2.ConfigureAwait(@this.Zc);
                        }
                        catch (Exception e)
                        {
                            @this._output.WriteLine(e.ToString());
                        }
                    }
                    @this._output.WriteLine($"({@this._context.Q.Count})");
                },this, TaskCreationOptions.DenyChildAttach).Unwrap());
            }
            await Task.WhenAll(concurrentTasks);

            _output.WriteLine($"count = {_context.Q.Count}, Head = {_context.Q?.Tail?.Value}, tail = {_context.Q?.Head?.Value}, time = {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start}ms, {rounds * mult * 6 / (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start)} kOPS");

            Assert.Equal(9, _context.Q!.Count);
            Assert.NotNull(_context.Q.Head);
            Assert.NotNull(_context.Q.Tail);

            var kops = rounds * mult * 6 / (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start);

#if DEBUG
            Assert.InRange(kops, 20, int.MaxValue);
#else
            Assert.InRange(kops, 1000, int.MaxValue);
#endif

            _output.WriteLine($"kops = {kops}");

            while(_context.Q.Count > 0)
            {
                await _context.Q.DequeueAsync().FastPath().ConfigureAwait(Zc);
            }

            Assert.Equal(0, _context.Q.Count);
            Assert.Null(_context.Q.Head);
            Assert.Null(_context.Q.Tail);
        }

        private volatile int _inserted;
        [Fact]
        public async Task Iterator()
        {

            var threads = 20;
            var itemsPerThread = 100000;
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

            await Task.WhenAll(insert).ConfigureAwait(Zc);

            Assert.Equal(capacity, _inserted);

            await q.DequeueAsync().FastPath().ConfigureAwait(Zc);
            await q.DequeueAsync().FastPath().ConfigureAwait(Zc);
            await q.DequeueAsync().FastPath().ConfigureAwait(Zc);

            Assert.Equal(capacity - 3, q.Count);

            c = 0;
            foreach (var ioInt32 in q)
            {
                c++;
            }

            Assert.Equal(capacity - 3, c);

            while (q.Count > 0)
                await q.DequeueAsync().FastPath().ConfigureAwait(Zc);

            Assert.Equal(0, q.Count);

            c = 0;
            foreach (var ioInt32 in q)
                c++;

            Assert.Equal(0, c);
        }

        [Fact]
        async Task AutoScale()
        {
            var q = new IoQueue<int>("test Q", 1, 1, autoScale:true);

            await q.EnqueueAsync(0);
            await q.EnqueueAsync(1);
            await q.EnqueueAsync(2);
            await q.EnqueueAsync(3);
            await q.EnqueueAsync(4);

            Assert.Equal(8, q.Capacity);
        }

        private IoQueue<IoInt32> _queuePressure = null!;
        private Task _queueNoBlockingTask = null!;
        private CancellationTokenSource _blockCancellationSignal = null!;

        [Fact]
        async Task NoQueuePressure()
        {
            _blockCancellationSignal = new CancellationTokenSource();
            _queuePressure = new IoQueue<IoInt32>("test Q", 1, 2);

            await _queuePressure.EnqueueAsync(0);

            var item = await _queuePressure.DequeueAsync().FastPath().ConfigureAwait(Zc);

            Assert.NotNull(item);

            _queueNoBlockingTask = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoQueueTest)state!;

                var item = await @this._queuePressure.DequeueAsync().FastPath().ConfigureAwait(@this.Zc);
                Assert.Null(item);
            }, this, TaskCreationOptions.DenyChildAttach).Unwrap();

            var enqueueTask = _queueNoBlockingTask.ContinueWith((_,@this) => ((IoQueueTest)@this!)._blockCancellationSignal.Cancel(), this);

            
            var dequeue = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoQueueTest)state!;

                try
                {
                    //Wait for up to 2 seconds for results
                    await Task.Delay(2000, @this._blockCancellationSignal.Token).ConfigureAwait(@this.Zc);
                }
                catch
                {
                    // ignored
                }

                Assert.True(@this._queueNoBlockingTask.IsCompletedSuccessfully);
            }, this, TaskCreationOptions.DenyChildAttach).Unwrap();

            await dequeue.ConfigureAwait(Zc);
            await enqueueTask.ConfigureAwait(Zc);
            Assert.True(dequeue.IsCompletedSuccessfully);
            Assert.True(enqueueTask.IsCompletedSuccessfully);
        }

        [Fact]
        async Task QueuePressure()
        {
            _blockCancellationSignal = new CancellationTokenSource();
            _queuePressure = new IoQueue<IoInt32>("test Q", 1, 2, disablePressure:false);

            await _queuePressure.EnqueueAsync(0);

            var item = await _queuePressure.DequeueAsync().FastPath().ConfigureAwait(Zc);

            Assert.NotNull(item);

            _queueNoBlockingTask = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoQueueTest)state!;
                var s = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                var item = await @this._queuePressure.DequeueAsync().FastPath().ConfigureAwait(@this.Zc);
                Assert.InRange(s.ElapsedMs(), 100, 2000);
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
                    await Task.Delay(100, @this._blockCancellationSignal.Token).ConfigureAwait(@this.Zc);
                    await @this._queuePressure.EnqueueAsync(1).FastPath().ConfigureAwait(@this.Zc);
                    //Wait for up to 2 seconds for results
                    await Task.Delay(2000, @this._blockCancellationSignal.Token).ConfigureAwait(@this.Zc);
                }
                catch
                {
                    // ignored
                }
            }, this, TaskCreationOptions.DenyChildAttach).Unwrap();

            await insertTask.ConfigureAwait(Zc);
            Assert.True(insertTask.IsCompletedSuccessfully);
            await dequeTask.ConfigureAwait(Zc);
            Assert.True(dequeTask.IsCompletedSuccessfully);
        }

        [Fact]
        async Task QueueBackPressure()
        {
            _blockCancellationSignal = new CancellationTokenSource();
            _queuePressure = new IoQueue<IoInt32>("test Q", 2, 2, disablePressure: false, enableBackPressure:true);

            await _queuePressure.EnqueueAsync(0);

            var item = await _queuePressure.DequeueAsync().FastPath().ConfigureAwait(Zc);

            Assert.NotNull(item);

            _queueNoBlockingTask = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoQueueTest)state!;
                var s = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                var item = await @this._queuePressure.DequeueAsync().FastPath().ConfigureAwait(@this.Zc);
                Assert.InRange(s.ElapsedMs(), 100, 2000);
                Assert.NotNull(item);

                await Task.Delay(100).ConfigureAwait(@this.Zc);

                s = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                item = await @this._queuePressure.DequeueAsync().FastPath().ConfigureAwait(@this.Zc);
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
                await Task.Delay(100, @this._blockCancellationSignal.Token).ConfigureAwait(@this.Zc);
                await @this._queuePressure.EnqueueAsync(1).FastPath().ConfigureAwait(@this.Zc);
                await @this._queuePressure.EnqueueAsync(1).FastPath().ConfigureAwait(@this.Zc);
                var s = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                //blocking
                await @this._queuePressure.EnqueueAsync(1).FastPath().ConfigureAwait(@this.Zc);
                Assert.InRange(s.ElapsedMs(), 100, 10000);
                //Wait for up to 2 seconds for results
                await Task.Delay(2000, @this._blockCancellationSignal.Token).ConfigureAwait(@this.Zc);
            }, this, TaskCreationOptions.DenyChildAttach).Unwrap().ContinueWith(task =>
            {
                if (task.Exception != null)
                {
                    throw task.Exception;
                }
            });

            await insertTask.ConfigureAwait(Zc);
            Assert.True(insertTask.IsCompletedSuccessfully);
            await dequeTask.ConfigureAwait(Zc);
            Assert.True(dequeTask.IsCompletedSuccessfully);
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

            private bool Zc = IoNanoprobe.ContinueOnCapturedContext;
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
            _context.Dispose();
        }
    }
}
