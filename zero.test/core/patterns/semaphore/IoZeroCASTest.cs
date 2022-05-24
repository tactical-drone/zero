using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using zero.core.patterns.semaphore.core;

namespace zero.test.core.patterns.semaphore
{
    public class IoZeroCASTest
    {
        private long _reg;
        private int _count = 1000000;
        private readonly ITestOutputHelper _output;

        public IoZeroCASTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        async Task NextAsync()
        {
            var threads = 100;

            var tasks = new List<Task>();
            for (var t = 0; t < threads; t++)
            {
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    for (int i = 0; i < _count; i++)
                    {
                        var l = _reg;
                        Assert.InRange(_reg.ZeroNext(_count), l, _count);
                    }
                }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default));
            }

            await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(15));

            Assert.Equal(_count, _reg);
        }

        [Fact]
        async Task NextOneAsync()
        {
            var threads = 100;

            var tasks = new List<Task>();
            for (var t = 0; t < threads; t++)
            {
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    for (int i = 0; i < _count; i++)
                    {
                        var l = _reg;
                        Assert.InRange(_reg.ZeroNextBounded(_count), l, _count);
                    }
                }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default));
            }

            await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(15));

            Assert.Equal(_count - 1, _reg);
        }

        [Fact]
        void Huh()
        {
            var cap = 10L;
            var idx1 = 9L;
            var idx2 = 9L;
            long prev;
            _output.WriteLine($"idx = {idx1}, cap = {cap}");
            if ((prev = idx1.ZeroNext(cap)) != cap)
            {
                _output.WriteLine($"[PROCESS] ZeroNext prev = {prev}, next =  {idx1} -> ");
            }
            else
            {
                _output.WriteLine($"[SKIP] ZeroNext prev = {prev}, next =  {idx1} -> ");
            }

            _output.WriteLine($"idx = {idx2}, cap = {cap}");
            if (idx2.ZeroNextBounded(cap) != cap)
            {
                _output.WriteLine($"[PROCESS] ZeroNext prev = {prev}, next = {idx2} -> ");
            }
            else
            {
                _output.WriteLine($"[SKIP] ZeroNext prev = {prev}, next = {idx2} -> ");
            }


            idx1 = 3;
            long v;
            prev = -1L;
            while ((v = idx1.ZeroNext(10)) <= 10)
            {
                _output.WriteLine($"ZeroNext(10) -> {v}");
                if (prev == v)
                    break;
                prev = v;
            }

            idx1 = 3;
            prev = -1L;
            while ((v = idx1.ZeroPrev(0)) >= 0)
            {
                _output.WriteLine($"Decrement -> {v}");
                if (prev == v)
                    break;
                prev = v;
            }
        }
    }
}
