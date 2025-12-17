using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using zero.core.misc;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore.core;
using zero.core.runtime.scheduler;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace zero.test.core.patterns.semaphore;

public class IoZeroSemCoreTest
{
    private const int ERR_T = 16 * 120;
    private readonly ITestOutputHelper _output;


    private volatile int _exclusiveCheck;
    private volatile int _exclusiveCount;

    public IoZeroSemCoreTest(ITestOutputHelper output)
    {
        _output = output;
        var prime = IoZeroScheduler.ZeroDefault;
        if (prime.Id > 1)
            Console.WriteLine("using IoZeroScheduler");
    }

    [Fact]
    private async Task SmokeReadyTestAsync()
    {
        var threads = 10;
        var delayTime = 50;
        IIoZeroSemaphoreBase<int> m = new IoZeroCore<int>("test", threads, new CancellationTokenSource(), threads);
        m.ZeroRef(ref m, _ => Environment.TickCount);

        await Task.Factory.StartNew(async () =>
        {
            for (var i = 0; i < threads * 2; i++)
            {
                await Task.Delay(delayTime);
                var ts = Environment.TickCount;
                m.Release(Environment.TickCount, true);
                Assert.InRange(ts.ElapsedMs(), 0, delayTime + ERR_T);
                _output.WriteLine($"R -> {i} {ts.ElapsedMs()}ms");
            }
        }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault);

        int ts;
        for (var i = 0; i < threads; i++)
        {
            ts = Environment.TickCount;
            var qt = await m.WaitAsync().FastPath();
            Assert.InRange(ts.ElapsedMs(), 0, delayTime + ERR_T);
            Assert.InRange(qt.ElapsedMs(), 0, ERR_T);
            _output.WriteLine($"P -> {i} {ts.ElapsedMs()}ms");
        }

        _output.WriteLine("pre-load done");

        for (var i = 0; i < threads; i++)
        {
            ts = Environment.TickCount;
            var qt = await m.WaitAsync().FastPath();
            Assert.InRange(ts.ElapsedMs(), delayTime - ERR_T, delayTime + ERR_T);
            Assert.InRange(qt.ElapsedMs(), 0, ERR_T);
            _output.WriteLine($"DQ => {ts.ElapsedMs()}ms");
        }
    }

    [Fact]
    private async Task SmokeSpamTestAsync()
    {
        var threads = 25;
        var spamFactor = 1;
        var delayTime = 0;
        IIoZeroSemaphoreBase<int> m = new IoZeroCore<int>("test", threads, new CancellationTokenSource(), threads);
        m.ZeroRef(ref m, _ => Environment.TickCount);

        _ = Task.Factory.StartNew(async () =>
        {
            for (var i = 0; i < threads * spamFactor + threads; i++)
            {
                await Task.Delay(delayTime);

                var ts = Environment.TickCount;
                if (!m.Release(Environment.TickCount, true))
                {
                    i--;
                    await Task.Delay(1);
                    _output.WriteLine($"D -> {i}, {ts.ElapsedMs()}ms");
                }

                _output.WriteLine($"R -> {i}, {ts.ElapsedMs()}ms");
            }

            _output.WriteLine("Done signalling");
        }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault);

        int ts;
        for (var i = 0; i < threads; i++)
        {
            ts = Environment.TickCount;
            var qt = await m.WaitAsync().FastPath();
            Assert.InRange(ts.ElapsedMs(), 0, delayTime + ERR_T);
            Assert.InRange(qt.ElapsedMs(), 0, ERR_T);
            _output.WriteLine($"P -> {i}, {ts.ElapsedMs()}ms");
        }

        _output.WriteLine("pre-load done");

        for (var i = 0; i < threads * spamFactor; i++)
        {
            ts = Environment.TickCount;
            var qt = await m.WaitAsync().FastPath();
            Assert.InRange(ts.ElapsedMs(), delayTime - ERR_T, delayTime + ERR_T);
            Assert.InRange(qt.ElapsedMs(), 0, ERR_T);
            _output.WriteLine($"DQ -> {i}, {ts.ElapsedMs()}ms");
        }
    }

    [Fact]
    private async Task ReadyTestAsync()
    {
        var batchLog = 50;
        var threads = Environment.ProcessorCount * 2;
        var delayTime = 1;
        IIoZeroSemaphoreBase<int> m = new IoZeroCore<int>("test", threads, new CancellationTokenSource(), threads);
        m.ZeroRef(ref m, _ => Environment.TickCount);

        await Task.Factory.StartNew(async () =>
        {
            var t = Environment.TickCount;
            for (var i = 0; i < threads * 2; i++)
            {
                await Task.Delay(delayTime);
                var ts = Environment.TickCount;
                m.Release(Environment.TickCount, true);
                Assert.InRange(ts.ElapsedMs(), 0, delayTime + ERR_T);
                if (i % batchLog == 0)
                    _output.WriteLine($"R -> {i} {ts.ElapsedMs()}ms - {(double)i / t.ElapsedMsToSec():0.0} r/s");
            }
        }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault);

        int ts;
        var t = Environment.TickCount;
        for (var i = 0; i < threads; i++)
        {
            ts = Environment.TickCount;
            var qt = await m.WaitAsync().FastPath();
            Assert.InRange(ts.ElapsedMs(), 0, delayTime + ERR_T);
            Assert.InRange(qt.ElapsedMs(), 0, ERR_T);
            if (i % batchLog == 0)
                _output.WriteLine($"P -> {i} {ts.ElapsedMs()}ms - {(double)i / t.ElapsedMsToSec():0.0} r/s");
        }

        _output.WriteLine("pre-load done");
        t = Environment.TickCount;
        for (var i = 0; i < threads; i++)
        {
            ts = Environment.TickCount;
            var qt = await m.WaitAsync().FastPath();
            Assert.InRange(ts.ElapsedMs(), delayTime - ERR_T, delayTime + ERR_T);
            Assert.InRange(qt.ElapsedMs(), 0, ERR_T);
            if (i % batchLog == 0)
                _output.WriteLine($"D -> {i} {ts.ElapsedMs()}ms - {(double)i / t.ElapsedMsToSec():0.0} r/s");
        }
    }

    [Fact]
    private async Task SpamTestAsync()
    {
        var total = 50000;
        var batchLog = 1000;
        var threads = Environment.ProcessorCount * 2;
        var threadsRemaining = threads;
        var spamFactor = 0.5;
        var done = false;
        //var batchLog = 1;
        //var threads = 10;
        //var spamFactor = 1;
        var delayTime = 0;
        IIoZeroSemaphoreBase<int> m = new IoZeroCore<int>("test", threads, new CancellationTokenSource(), threads);
        m.ZeroRef(ref m, _ => Environment.TickCount);

        for (var j = 0; j < threads; j++)
            _ = Task.Factory.StartNew(async () =>
            {
                var t = Environment.TickCount;
                
                for (var i = 0; i < total/threads; i++)
                {
                    //await Task.Delay(delayTime);
                    if (!m.Release(Environment.TickCount, true))
                    {
                        i--;
                        await Task.Delay(1);
                        //_output.WriteLine($"D -> {i}, {ts.ElapsedMs()}ms");
                    }

                    //if (i != 0 && i % batchLog/2 == 0)
                    //    _output.WriteLine($"R -> {i} {ts.ElapsedMs()}ms - {(double)i / t.ElapsedMs()*1000:0.0} r/s");
                }
                done = --threadsRemaining == 0;
                if(done)
                    _output.WriteLine("Done signalling");
                m.Release(Environment.TickCount, true);
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault);

        int ts;
        var t = Environment.TickCount;
        for (var i = 0; i < threads; i++)
        {
            ts = Environment.TickCount;
            var qt = await m.WaitAsync().FastPath();
            Assert.InRange(ts.ElapsedMs(), 0, delayTime + ERR_T);
            Assert.InRange(qt.ElapsedMs(), 0, ERR_T);
            if (i != 0 && i % batchLog == 0)
                _output.WriteLine($"P -> {i} {ts.ElapsedMs()}ms - {(double)i / t.ElapsedMs() * 1000:0.0} r/s");
        }

        var k = 0;
        _output.WriteLine("pre-load done... ");
        t = Environment.TickCount;
        while(!done)
        {
            ts = Environment.TickCount;
            var qt = await m.WaitAsync().FastPath();
            Assert.InRange(ts.ElapsedMs(), delayTime - ERR_T, delayTime + ERR_T);
            Assert.InRange(qt.ElapsedMs(), 0, ERR_T);
            if (k++ != 0 && k % batchLog == 0 || k < 5)
                _output.WriteLine($"D -> {k} {ts.ElapsedMs()}ms - {(double)k / t.ElapsedMs() * 1000:0.0} r/s");
        }

        _output.WriteLine("Test done!!!");
    }

    [Fact]
    public async Task ExclusiveZoneAsync()
    {
        var realThreads = Environment.ProcessorCount * 2;
        var spamFactor = 10000;
        var delayTime = 0;

        IIoZeroSemaphoreBase<int> m = new IoZeroCore<int>("test", realThreads, new CancellationTokenSource(), 1);
        m.ZeroRef(ref m, _ => Environment.TickCount);

        var ts = Environment.TickCount;
        var tests = new List<Task>();

        for (var i = 0; i < realThreads; i++)
            tests.Add(Task.Factory.StartNew(async () =>
            {
                await Task.Delay(Random.Shared.Next(realThreads));
                //_output.WriteLine($"Staring thread [{Environment.CurrentManagedThreadId}]");
                var t = Environment.TickCount;
                long x = 0;
                for (var i = 0; i < spamFactor; i++)
                    try
                    {
                        var qt = await m.WaitAsync().FastPath();

                        if (i % (spamFactor / 2) == 0 && i > 0)
                            _output.WriteLine(
                                $"[{Environment.CurrentManagedThreadId}] R => {i} , {qt.ElapsedMs()}ms, {(double)_exclusiveCount / t.ElapsedMsToSec():0.0} r/s");
                        Assert.InRange(qt.ElapsedMs(), -ERR_T, delayTime + ERR_T);
                        var Q = qt.ElapsedMs();
                        if (Q < -ERR_T || Q > delayTime + ERR_T)
                            _output.WriteLine($"FAILED RANGE ({-ERR_T}, {Q} , {delayTime + ERR_T})");
                        Assert.Equal(1, Interlocked.Increment(ref _exclusiveCheck));

                        Interlocked.Increment(ref _exclusiveCount);
                        for (var j = 0; j < spamFactor; j++)
                            x++;
                    }
                    catch (Exception ex)
                    {
                        _output.WriteLine($"{ex.Message}\n{ex.StackTrace}");
                    }
                    finally
                    {
                        Assert.Equal(0, Interlocked.Decrement(ref _exclusiveCheck));
                        await Task.Delay(delayTime);
                        Assert.Equal(0, m.ReadyCount);
                        m.Release(Environment.TickCount, true);
                    }
                //_output.WriteLine($"Done signalling count = {_exclusiveCheck}, {(double)_exclusiveCheck / t.ElapsedMsToSec():0.0} r/s");
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap());

        await Task.WhenAll(tests).WaitAsync(TimeSpan.FromSeconds(1000), CancellationToken.None);

        Assert.Equal(spamFactor * realThreads, _exclusiveCount);
        _output.WriteLine(
            $"Done signalling count = {_exclusiveCount}, {(double)_exclusiveCount / ts.ElapsedMsToSec():0.0} r/s");
    }
}