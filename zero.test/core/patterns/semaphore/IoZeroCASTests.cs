using System.Threading;
using System.Threading.Tasks;
using Xunit;
using zero.core.patterns.semaphore.core;

namespace zero.test.core.patterns.semaphore
{
    public class IoZeroCASTests
    {
        [Fact]
        public void ZeroNext_Int_WithinCapacity_ReturnsCurrentValue()
        {
            int val = 0;
            int cap = 10;
            
            var result = val.ZeroNext(cap);
            
            Assert.Equal(0, result);
            Assert.Equal(1, val);
        }

        [Fact]
        public void ZeroNext_Int_AtCapacity_ReturnsMinus1()
        {
            int val = 10;
            int cap = 10;
            
            var result = val.ZeroNext(cap);
            
            Assert.Equal(-1, result);
            Assert.Equal(10, val);
        }

        [Fact]
        public void ZeroNext_Int_ExceedsCapacity_ReturnsMinus1()
        {
            int val = 15;
            int cap = 10;
            
            var result = val.ZeroNext(cap);
            
            Assert.Equal(-1, result);
            Assert.Equal(15, val);
        }

        [Fact]
        public void ZeroNext_Int_MultipleIncrements_IncrementsCorrectly()
        {
            int val = 0;
            int cap = 5;
            
            Assert.Equal(0, val.ZeroNext(cap));
            Assert.Equal(1, val.ZeroNext(cap));
            Assert.Equal(2, val.ZeroNext(cap));
            Assert.Equal(3, val.ZeroNext(cap));
            Assert.Equal(4, val.ZeroNext(cap));
            Assert.Equal(-1, val.ZeroNext(cap));
        }

        [Fact]
        public void ZeroPrev_Int_WithinCapacity_ReturnsCurrentValue()
        {
            int val = 5;
            int cap = 0;
            
            var result = val.ZeroPrev(cap);
            
            Assert.Equal(5, result);
            Assert.Equal(4, val);
        }

        [Fact]
        public void ZeroPrev_Int_AtCapacity_ReturnsMinus1()
        {
            int val = 0;
            int cap = 0;
            
            var result = val.ZeroPrev(cap);
            
            Assert.Equal(-1, result);
            Assert.Equal(0, val);
        }

        [Fact]
        public void ZeroPrev_Int_BelowCapacity_ReturnsMinus1()
        {
            int val = -5;
            int cap = 0;
            
            var result = val.ZeroPrev(cap);
            
            Assert.Equal(-1, result);
            Assert.Equal(-5, val);
        }

        [Fact]
        public void ZeroNext_Long_WithinCapacity_ReturnsCurrentValue()
        {
            long val = 0;
            long cap = 100;
            
            var result = val.ZeroNext(cap);
            
            Assert.Equal(0, result);
            Assert.Equal(1, val);
        }

        [Fact]
        public void ZeroNext_Long_AtCapacity_ReturnsMinus1()
        {
            long val = 100;
            long cap = 100;
            
            var result = val.ZeroNext(cap);
            
            Assert.Equal(-1, result);
            Assert.Equal(100, val);
        }

        [Fact]
        public void ZeroPrev_Long_WithinCapacity_ReturnsCurrentValue()
        {
            long val = 50;
            long cap = 0;
            
            var result = val.ZeroPrev(cap);
            
            Assert.Equal(50, result);
            Assert.Equal(49, val);
        }

        [Fact]
        public void ZeroPrev_Long_AtCapacity_ReturnsMinus1()
        {
            long val = 0;
            long cap = 0;
            
            var result = val.ZeroPrev(cap);
            
            Assert.Equal(-1, result);
            Assert.Equal(0, val);
        }

        [Fact]
        public void ZeroNext_Int_ConcurrentAccess_AllThreadsSucceed()
        {
            int val = 0;
            int cap = 100;
            int successCount = 0;
            
            var tasks = new Task[10];
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(() =>
                {
                    for (int j = 0; j < 10; j++)
                    {
                        var result = val.ZeroNext(cap);
                        if (result >= 0)
                            Interlocked.Increment(ref successCount);
                    }
                });
            }
            
            Task.WaitAll(tasks);
            
            Assert.Equal(100, val);
            Assert.Equal(100, successCount);
        }

        [Fact]
        public void ZeroPrev_Int_ConcurrentAccess_AllThreadsSucceed()
        {
            int val = 100;
            int cap = 0;
            int successCount = 0;
            
            var tasks = new Task[10];
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(() =>
                {
                    for (int j = 0; j < 10; j++)
                    {
                        var result = val.ZeroPrev(cap);
                        if (result >= 0)
                            Interlocked.Increment(ref successCount);
                    }
                });
            }
            
            Task.WaitAll(tasks);
            
            Assert.Equal(0, val);
            Assert.Equal(100, successCount);
        }

        [Fact]
        public void ZeroNextBounded_WithinCapacity_ReturnsCurrentValue()
        {
            long val = 0;
            long cap = 50;
            
            var result = val.ZeroNextBounded(cap);
            
            Assert.Equal(0, result);
            Assert.Equal(1, val);
        }

        [Fact]
        public void ZeroNextBounded_AtCapacity_ReturnsMinus1()
        {
            long val = 50;
            long cap = 50;
            
            var result = val.ZeroNextBounded(cap);
            
            Assert.Equal(-1, result);
            Assert.Equal(50, val);
        }

        [Fact]
        public void ZeroNext_Int_ConsistentOrdering_NoSkippedValues()
        {
            int val = 0;
            int cap = 20;
            var results = new int[20];
            
            for (int i = 0; i < 20; i++)
            {
                results[i] = val.ZeroNext(cap);
            }
            
            for (int i = 0; i < 20; i++)
            {
                Assert.Equal(i, results[i]);
            }
        }

        [Fact]
        public void ZeroPrev_Int_ConsistentOrdering_NoSkippedValues()
        {
            int val = 20;
            int cap = 0;
            var results = new int[20];
            
            for (int i = 0; i < 20; i++)
            {
                results[i] = val.ZeroPrev(cap);
            }
            
            for (int i = 0; i < 20; i++)
            {
                Assert.Equal(20 - i, results[i]);
            }
        }
    }
}