using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using zero.core.misc;
using zero.core.patterns.queue;

namespace zero.test.core.patterns.queue
{
    public class IoBagTest :IDisposable
    {
        private IoBag<IoInt32> _bag;
        private readonly ITestOutputHelper _output;

        public IoBagTest(ITestOutputHelper output)
        {
            _bag = new IoBag<IoInt32>("test", 11, true);
            _output = output;
        }

        [Fact]
        void InsertTest()
        {
            for (int i = 0; i < _bag.Capacity; i++)
            {
                _bag.Add(i);
            }

            Assert.True(_bag.Contains(_bag.Capacity / 2));

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

            Assert.Equal("01234567891001234567891011", sb.ToString());
        }

        [Fact]
        public async Task Iterator()
        {
            var threads = 100;

            var c = 0;
            foreach (var ioInt32 in _bag)
                c++;
            
            Assert.Equal(0, c);

            var idx = 0;
            var insert = new List<Task>();
            for (var i = 0; i < threads; i++)
            {
                insert.Add(Task.Factory.StartNew(static state =>
                {
                    var (@this, idx) = (ValueTuple<IoBagTest, int>)state!;
                    @this._bag.Add(Interlocked.Increment(ref idx));
                }, (this, idx), TaskCreationOptions.DenyChildAttach));
            }
            
            //await Task.WhenAll(insert).ConfigureAwait(Zc);

            //Assert.Equal(capacity, _bag.Count);

            //_bag.TryTake(out _);
            //_bag.TryTake(out _);
            //_bag.TryTake(out _);

            //Assert.Equal(capacity - 3, _bag.Count);

            //c = 0;
            //foreach (var ioInt32 in _bag)
            //{
            //    c++;
            //}

            //Assert.Equal(capacity - 3, c);

            //while (_bag.Count > 0)
            //    _bag.TryTake(out _);

            //Assert.Equal(0, _bag.Count);

            //c = 0;
            //foreach (var ioInt32 in _bag)
            //    c++;

            //Assert.Equal(0, c);
        }
        public bool Zc => true;

        public void Dispose()
        {
            _bag.ZeroManagedAsync<object>().AsTask().GetAwaiter().GetResult();
            _bag = default!;
        }
    }
}
