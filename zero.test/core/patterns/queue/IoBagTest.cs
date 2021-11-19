﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
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

        public void Dispose()
        {
            _bag.ZeroManagedAsync<object>().AsTask().GetAwaiter().GetResult();
            _bag = null;
        }
    }
}
