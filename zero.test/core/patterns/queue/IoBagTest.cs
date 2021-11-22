﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using zero.core.misc;
using zero.core.patterns.queue;

namespace zero.test.core.patterns.queue
{
    public class IoBagTest :IDisposable
    {
        private IoBag<IoInt32> _bag;

        public IoBagTest()
        {
            _bag = new IoBag<IoInt32>("test", 11, true);
        }

        [Fact]
        void InsertTest()
        {
            for (int i = 1; i < _bag.Capacity; i++)
            {
                _bag.Add(i);
            }

            Assert.True(_bag.Contains((int)_bag.Capacity / 2));

            var sb = new StringBuilder();
            foreach (var i in _bag)
            {
                sb.Append($"{i}");
                if (i == 7)
                    _bag.Add(11);

                if (i == 11)
                    break;
            }

            foreach (var i in _bag)
            {
                _bag.TryTake(out var r);
                sb.Append($"{i}");
            }

            Assert.Equal("12345678910111234567891011", sb.ToString());
        }

        [Fact]
        public void Iterator()
        {
            var capacity = 100;
            
            var c = 0;
            foreach (var ioInt32 in _bag)
                c++;
            
            Assert.Equal(0, c);

            var idx = 0;
            var insert = new List<Task>();

            for (var i = 0; i < capacity; i++)
            {
                insert.Add(Task.Factory.StartNew(() => _bag.Add(Interlocked.Increment(ref idx))));
            }

            Task.WhenAll(insert).GetAwaiter().GetResult();

            _bag.TryTake(out _);
            _bag.TryTake(out _);
            _bag.TryTake(out _);

            Assert.Equal(capacity - 3, _bag.Count);

            c = 0;
            foreach (var ioInt32 in _bag)
            {
                c++;
            }

            Assert.Equal(capacity - 3, c);

            while (_bag.Count > 0)
                _bag.TryTake(out _);

            Assert.Equal(0, _bag.Count);

            c = 0;
            foreach (var ioInt32 in _bag)
                c++;

            Assert.Equal(0, c);
        }

        public void Dispose()
        {
            _bag.ZeroManagedAsync<object>().AsTask().GetAwaiter().GetResult();
            _bag = default!;
        }
    }
}
