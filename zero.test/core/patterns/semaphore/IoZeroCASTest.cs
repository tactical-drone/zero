using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
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
        private int _count = 10000000;
        private readonly ITestOutputHelper _output;

        public IoZeroCASTest(ITestOutputHelper output)
        {
            _output = output;
        }

        List<long> _selection = new List<long>(short.MaxValue);
        private int _accepted;
        private int _rejected;
        [Fact]
        async Task NextAsync()
        {
            var threads = 128;

            var tasks = new List<Task>();
            for (var t = 0; t < threads; t++)
            {
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    Thread.Sleep(Random.Shared.Next(0,50));
                    //for (int i = 0; i < _count; i++)
                    while(true)
                    {
                        var l = _reg;
                        var cap = _reg + threads / 4;

                        if (cap > _count)
                            cap = _count;

                        var r = _reg.ZeroNext(cap);
                        if (r != cap && r < _count)
                        {
                            Interlocked.Increment(ref _accepted);
                            _selection.Add(r);
                        }
                        else if(r == cap)
                            Interlocked.Increment(ref _rejected);

                        if(r >= _count)
                            return;

                        Assert.InRange(r, l, cap + 1);
                    }
                }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default));
            }

            await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(25));

            //Assert.Equal(_count, _reg);
            var sorted = _selection.OrderBy(i => i);
            long prev = -1;

            var bad = 0;
            var duplicates = 0;
            foreach (var next in sorted)
            {
                //_output.WriteLine($"next = {next}");
                if (next != prev + 1)
                    bad++;

                if (next == prev)
                    duplicates++;

                //Assert.True(next > prev);
                //Assert.True(next == prev + 1);
                prev = next;
            }
            _output.WriteLine($"Bad = {bad}/{_count}, {(double)bad/(_count) * 100:0.0}%, rejected = {_rejected}/{_accepted} = {(double)_rejected/_accepted * 100:0.0}%, duplicates = {duplicates}");
            Assert.InRange(bad, 0,0);
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
        void Smoke()
        {
            var cap = 10L;
            var idx1 = 9L;
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

            //_output.WriteLine($"idx = {idx2}, cap = {cap}");
            //if (idx2.ZeroNextBounded(cap) != cap)
            //{
            //    _output.WriteLine($"[PROCESS] ZeroNext prev = {prev}, next = {idx2} -> ");
            //}
            //else
            //{
            //    _output.WriteLine($"[SKIP] ZeroNext prev = {prev}, next = {idx2} -> ");
            //}


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
