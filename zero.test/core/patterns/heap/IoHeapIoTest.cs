using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.test.core.patterns.heap
{
    public class IoHeapIoTest
    {
        [Fact]
        void SpamTest()
        {
            var h = new IoHeapIo<TestHeapItem, IoHeapIoTest>("test heap", _capacity * _capacity, context:this)
            {
                Malloc = (o, test) => new TestHeapItem(_localVar, (int)o), 
                PopAction = (item, o) =>
                {
                    item.TestVar = (int)o;
                }
            };

            var spamTasks = new List<Task>();
            for (var i = 0; i < _capacity; i++)
            {
                spamTasks.Add(Task.Factory.StartNew(static state =>
                {
                    var (@this,h) = (ValueTuple<IoHeapIoTest, IoHeapIo<TestHeapItem, IoHeapIoTest>>)state!;
                    for (var j = 0; j < @this._capacity; j++)
                    {
                        var item = h.Take(j);
                        Assert.NotNull(item);
                        Assert.Equal(j, item.TestVar);
                        Assert.Equal(2, item.TestVar2);
                        Assert.InRange(item.TestVar3, 0, @this._capacity);
                        h.Return(item);
                    }
                }, (this, h), TaskCreationOptions.DenyChildAttach));
            }

            Task.WhenAll(spamTasks).WaitAsync(TimeSpan.FromSeconds(10)).GetAwaiter().GetResult();

            Assert.Equal(0, h.ReferenceCount);
            Assert.InRange(h.Count, 0,_capacity);
            Assert.Equal(_capacity * _capacity, h.Capacity);
        }

        [Fact]
        async Task ConstructionTestAsync()
        {
            var h = new IoHeapIo<TestHeapItem, IoHeapIoTest>("test heap", _capacity, true, this)
            {
                Malloc = (o, test) => new TestHeapItem(_localVar, (int)o),
                Constructor = (newHeapItem, context) =>
                {
                    newHeapItem.TestVar6 = (int)context;
                },
                PopAction = (poppedItem, context) =>
                {
                    poppedItem.TestVar7 = (int)context;
                }
            };

            var item = await h.TakeAsync((newItem, context) =>
            {
                newItem.TestVar5 = context;
                return new ValueTask<TestHeapItem>(newItem);
            }, userData:3).FastPath().ConfigureAwait(Zc);
            Assert.Equal(3, item.TestVar4);
            Assert.Equal(3, item.TestVar5);
            Assert.Equal(3, item.TestVar6);
            Assert.Equal(3, item.TestVar7);

            Assert.Equal(1, h.ReferenceCount);
            Assert.Equal(0, h.Count);


            Assert.Equal(3, item.TestVar);
            Assert.Equal(2, item.TestVar2);
            Assert.Equal(3, item.TestVar3);

            h.Return(item);
            Assert.Equal(1, h.Count);
            Assert.Equal(0, h.ReferenceCount);
        }

        [Fact]
        async Task DestructionTestAsync()
        {
            var capacity = 10;
            var h = new IoHeapIo<TestHeapItem, IoHeapIoTest>("test heap", capacity, true, this)
            {
                Malloc = (o, test) => new TestHeapItem(_localVar, (int)o)
            };
            await Task.Yield();
            var i1 = h.Take(0);
            var i2 = h.Take(0);
            var i3 = h.Take(0);
            h.Return(i3, true);
            h.Return(i2, true);
            h.Return(i1, true);
            
            Assert.Equal(0, h.ReferenceCount);
            Assert.Equal(0, h.Count);

            h.Return(h.Take(5));
            h.Return(h.Take(5));
            h.Return(h.Take(5));

            await h.ZeroManagedAsync((i, o) =>
            {
                Assert.Equal(5, i.TestVar3);
                Assert.Equal(2, o._localVar);
                return ValueTask.CompletedTask;
            }, this).FastPath().ConfigureAwait(Zc);

            Assert.Equal(0, h.ReferenceCount);
            Assert.Equal(0, h.Count);
        }

        public IoHeapIoTest()
        {
            
        }


        private int _localVar = 2;
        private bool Zc = IoNanoprobe.ContinueOnCapturedContext;
       

#if true
        private int _capacity = 10;
#else
        private int _capacity = 100;
#endif


        
    }

    public class TestHeapItem : IoNanoprobe, IIoHeapItem
    {
        public TestHeapItem(int testVar2, int testVar3):base()
        {
            TestVar2 = testVar2;
            TestVar3 = testVar3;
        }

        public int TestVar;
        public readonly int TestVar2;
        public readonly int TestVar3;
        public int TestVar4;
        public int TestVar5;
        public int TestVar6;
        public int TestVar7;
        public ValueTask<IIoHeapItem> HeapPopAsync(object context)
        {
            TestVar = (int)context;
            return ValueTask.FromResult((IIoHeapItem)this);
        }

        public IIoHeapItem HeapConstructAsync(object context)
        {
            TestVar4 = (int)context;
            return this;
        }
    }

}
