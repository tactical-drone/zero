using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.test.xunit;

namespace zero.test.core.patterns.queue{

    [TestCaseOrderer("zero.test.xunit.PriorityOrderer", "zero.test")]
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

        [Fact, Priority(1)]
        void InitQueueTest()
        {
            var sb = new StringBuilder();
            foreach (var ioZNode in _context.Q)
            {
                sb.Append(ioZNode.Value);
            }

            Assert.Equal("123456789", sb.ToString());
        }

        [Fact, Priority(2)]
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

        [Fact, Priority(3)]
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

        [Fact, Priority(4)]
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

        [Fact, Priority(5)]
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

        [Fact, Priority(6)]
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

        [Fact, Priority(7)]
        void SpamTest()
        {
            var _concurrentTasks = new List<Task>();

            //_context.Q.ClearAsync().AsTask().GetAwaiter().GetResult();
            var start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
#if DEBUG
            var rounds = 10;
            var mult = 10000;
#else
            var rounds = 100;
            var mult = 100000;
#endif
            for (var i = 0; i < rounds; i++)
            {
                var i3 = i;
                _concurrentTasks.Add(Task.Factory.StartNew(async () =>
                {
                    for (int j = 0; j < mult; j++)
                    {
                        try
                        {
                            var eq1 = _context.Q.PushBackAsync(i3);
                            var eq2 = _context.Q.EnqueueAsync(i3 + 1);
                            var i1 = _context.Q.PushBackAsync(i3 + 2);
                            var i2 = _context.Q.EnqueueAsync(i3 + 3);
                            var i4 = _context.Q.PushBackAsync(i3 + 4);

                            await eq2;
                            await eq1;
                            await i4;
                            await i2;
                            await i1;

                            //await q.RemoveAsync(i1.Result);

                            //await q.RemoveAsync(i2.Result);
                            var d2 = _context.Q.DequeueAsync();
                            var d4 = _context.Q.DequeueAsync();
                            var d5 = _context.Q.DequeueAsync();
                            await _context.Q.DequeueAsync();
                            await _context.Q.DequeueAsync();
                            await d5;
                            await d4;
                            await d2;
                        }
                        catch (Exception e)
                        {
                            _output.WriteLine(e.ToString());
                        }
                    }
                    _output.WriteLine($"({i3}-{_context.Q.Count})");
                }, new CancellationToken(), TaskCreationOptions.DenyChildAttach, TaskScheduler.Current).Unwrap());
            }
            Task.WhenAll(_concurrentTasks).GetAwaiter().GetResult();

            _output.WriteLine($"count = {_context.Q.Count}, Head = {_context.Q?.Tail?.Value}, tail = {_context.Q?.Head?.Value}, time = {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start}ms, {rounds * mult * 6 / (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start)} kOPS");

            Assert.Equal(9, _context.Q!.Count);
            Assert.NotNull(_context.Q.Head);
            Assert.NotNull(_context.Q.Tail);

            var kops = rounds * mult * 6 / (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start);

#if DEBUG
            Assert.InRange(kops, 225, int.MaxValue);
#else
            Assert.InRange(kops, 4500, int.MaxValue);
#endif

            Debug.WriteLine($"kops = {kops}");

            while(_context.Q.Count > 0)
            {
                _context.Q.DequeueAsync().AsTask().GetAwaiter().GetResult();
            }

            Assert.Equal(0, _context.Q.Count);
            Assert.Null(_context.Q.Head);
            Assert.Null(_context.Q.Tail);
        }
        public class Context : IDisposable
        {
            public Context()
            {
                Q = new IoQueue<int>("test Q", 2000000000, 1000);
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
                Q.Dispose();
            }
        }

        public void Dispose()
        {
            _context.Dispose();
        }
    }
}
