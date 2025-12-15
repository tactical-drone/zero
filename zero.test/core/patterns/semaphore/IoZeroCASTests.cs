using System.Threading;
using System.Threading.Tasks;
using Xunit;
using zero.core.patterns.semaphore.core;

#pragma warning disable xUnit1051

namespace zero.test.core.patterns.semaphore;

public class IoZeroCASTests
{
    [Fact]
    public void ZeroNext_Int_WithinCapacity_ReturnsCurrentValue()
    {
        var val = 0;
        var cap = 10;

        var result = val.ZeroNext(cap);

        Assert.Equal(0, result);
        Assert.Equal(1, val);
    }

    [Fact]
    public void ZeroNext_Int_AtCapacity_ReturnsMinus1()
    {
        var val = 10;
        var cap = 10;

        var result = val.ZeroNext(cap);

        Assert.Equal(-1, result);
        Assert.Equal(10, val);
    }

    [Fact]
    public void ZeroNext_Int_ExceedsCapacity_ReturnsMinus1()
    {
        var val = 15;
        var cap = 10;

        var result = val.ZeroNext(cap);

        Assert.Equal(-1, result);
        Assert.Equal(15, val);
    }

    [Fact]
    public void ZeroNext_Int_MultipleIncrements_IncrementsCorrectly()
    {
        var val = 0;
        var cap = 5;

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
        var val = 5;
        var cap = 0;

        var result = val.ZeroPrev(cap);

        Assert.Equal(5, result);
        Assert.Equal(4, val);
    }

    [Fact]
    public void ZeroPrev_Int_AtCapacity_ReturnsMinus1()
    {
        var val = 0;
        var cap = 0;

        var result = val.ZeroPrev(cap);

        Assert.Equal(-1, result);
        Assert.Equal(0, val);
    }

    [Fact]
    public void ZeroPrev_Int_BelowCapacity_ReturnsMinus1()
    {
        var val = -5;
        var cap = 0;

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
    public async Task ZeroNext_Int_ConcurrentAccess_AllThreadsSucceed()
    {
        var val = 0;
        var cap = 100;
        var successCount = 0;

        var tasks = new Task[10];
        for (var i = 0; i < tasks.Length; i++)
            tasks[i] = Task.Run(() =>
            {
                for (var j = 0; j < 10; j++)
                {
                    var result = val.ZeroNext(cap);
                    if (result >= 0)
                        Interlocked.Increment(ref successCount);
                }
            });

        await Task.WhenAll(tasks);

        Assert.Equal(100, val);
        Assert.Equal(100, successCount);
    }

    [Fact]
    public async Task ZeroPrev_Int_ConcurrentAccess_AllThreadsSucceed()
    {
        var val = 100;
        var cap = 0;
        var successCount = 0;

        var tasks = new Task[10];
        for (var i = 0; i < tasks.Length; i++)
            tasks[i] = Task.Run(() =>
            {
                for (var j = 0; j < 10; j++)
                {
                    var result = val.ZeroPrev(cap);
                    if (result >= 0)
                        Interlocked.Increment(ref successCount);
                }
            });

        await Task.WhenAll(tasks);

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
        var val = 0;
        var cap = 20;
        var results = new int[20];

        for (var i = 0; i < 20; i++) results[i] = val.ZeroNext(cap);

        for (var i = 0; i < 20; i++) Assert.Equal(i, results[i]);
    }

    [Fact]
    public void ZeroPrev_Int_ConsistentOrdering_NoSkippedValues()
    {
        var val = 20;
        var cap = 0;
        var results = new int[20];

        for (var i = 0; i < 20; i++) results[i] = val.ZeroPrev(cap);

        for (var i = 0; i < 20; i++) Assert.Equal(20 - i, results[i]);
    }
}