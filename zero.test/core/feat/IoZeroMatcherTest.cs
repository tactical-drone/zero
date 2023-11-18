using System;
using System.Buffers;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Xunit;
using Xunit.Abstractions;
using zero.core.feat.misc;
using zero.core.misc;
using zero.core.patterns.misc;
using zero.core.runtime.scheduler;
using zero.test.core.patterns.queue;

namespace zero.test.core.feat
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("CodeQuality", "IDE0051:Remove unused private members", Justification = "<Pending>")]
    public class IoZeroMatcherTest
    {
        [ThreadStatic]
        private static SHA256 _sha256;
        public static SHA256 Sha256 => _sha256 ??= SHA256.Create();


        public IoZeroMatcherTest(ITestOutputHelper output)
        { 
            var prime = IoZeroScheduler.ZeroDefault;
            if (prime.Id > 1)
                Console.WriteLine("using IoZeroScheduler");
        }


        [Fact]
        async Task SmokeAsync()
        {
            var threads = 2;
            var count = 100;
            var capacity = threads * count;
            var m = new IoZeroMatcher("Test matcher", threads, 256, 10000, autoscale: false);

            var oneShotTasks = new List<Task>();
            for (var i = 0; i < threads; i++)
            {
                oneShotTasks.Add(await Task.Factory.StartNew(async payload =>
                {
                    var array = payload as byte[];
                    var key = ((ReadOnlyMemory<byte>)array).HashSig();
                    var c = await m.ChallengeAsync(key, array).FastPath();

                    var reqHash = RandomNumberGenerator.GetBytes(32);

                    Sha256.TryComputeHash(array, reqHash, out var written);

                    await Task.Factory.StartNew(static async state =>
                    {
                        var (k, hash, matcher) = (ValueTuple<string, byte[], IoZeroMatcher>)state;
                        
                        var dud = new byte[hash.Length];
                        hash.CopyTo(dud, 0);
                        
                        Volatile.Write(ref dud[0]  , dud[1]);
                        Volatile.Write(ref dud[^1] , dud[^2]);
                        Volatile.Write(ref dud[^2] , dud[^3]);
                        Volatile.Write(ref dud[^3] , dud[^4]);
                        Volatile.Write(ref dud[dud.Length>>1] , dud[(dud.Length >> 1) - 1]);

                        Assert.False(await matcher.ResponseAsync(k, UnsafeByteOperations.UnsafeWrap(dud)).FastPath());
                        Assert.True(await matcher.ResponseAsync(k, UnsafeByteOperations.UnsafeWrap(hash)).FastPath());
                    }, (key, reqHash, m), CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap();

                }, BitConverter.GetBytes(i), CancellationToken.None,TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault));
            }

            await Task.WhenAll(oneShotTasks).WaitAsync(TimeSpan.FromSeconds(30));
            Assert.Equal(0,m.Count);
        }

        [Fact]
        async Task SpamTestAsync()
        {
            var threads = 20;
            var count = 10000;
            var capacity = threads * count;
            var m = new IoZeroMatcher("Test matcher", threads, capacity, autoscale: false);

            var oneShotTasks = new List<Task>();
            for (int i = 0; i < threads; i++)
            {
                oneShotTasks.Add(await Task.Factory.StartNew(async payload =>
                {
                    var array = payload as byte[];
                    var key = ((ReadOnlyMemory<byte>)array).HashSig();
                    var c = await m.ChallengeAsync(key, array).FastPath();

                    var reqHash = ArrayPool<byte>.Shared.Rent(32);

                    Sha256.TryComputeHash(array, reqHash, out var written);

                    await Task.Factory.StartNew(static async state =>
                    {
                        var (k, hash, matcher) = (ValueTuple<string, byte[], IoZeroMatcher>)state;
                        var dud = new byte[hash.Length];
                        hash.CopyTo(dud, 0);
                        dud[0] = 0;

                        Assert.False(await matcher.ResponseAsync(k, UnsafeByteOperations.UnsafeWrap(dud)));
                        Assert.True(await matcher.ResponseAsync(k, UnsafeByteOperations.UnsafeWrap(hash)));
                    }, (key, reqHash, m), CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap();

                }, BitConverter.GetBytes(i), CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault));
            }

            await Task.WhenAll(oneShotTasks).WaitAsync(TimeSpan.FromSeconds(60));
            Assert.Equal(0, m.Count);

        }

        readonly int _delayTime = 15 * 30;

        [Fact]
        async Task TimeoutAsync()
        {
            var threads = 1;
            var count = 100;
            var capacity = threads * count;
            var m = new IoZeroMatcher("Test matcher", threads, _delayTime, capacity, autoscale: false);

            var oneShotTasks = new List<Task>();
            for (int i = 0; i < threads; i++)
            {
                oneShotTasks.Add(await Task.Factory.StartNew(async payload =>
                {
                    var array = payload as byte[];
                    var key = ((ReadOnlyMemory<byte>)array).HashSig();
                    var c = await m.ChallengeAsync(key, array).FastPath();

                    var reqHash = ArrayPool<byte>.Shared.Rent(32);

                    Sha256.TryComputeHash(array, reqHash, out var written);

                    await Task.Factory.StartNew(static async state =>
                    {
                        //Delay time
                        var (k, hash, matcher, delay) = (ValueTuple<string, byte[], IoZeroMatcher, int>)state;
                        var dud = new byte[hash.Length];
                        hash.CopyTo(dud, 0);
                        dud[0] = 0;

                        Assert.False(await matcher.ResponseAsync(k, UnsafeByteOperations.UnsafeWrap(dud)));
                        await Task.Delay(delay);
                        Assert.False(await matcher.ResponseAsync(k, UnsafeByteOperations.UnsafeWrap(hash)));
                    }, (key, reqHash, m, _delayTime * 2), CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap();

                }, BitConverter.GetBytes(i), CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault));
            }

            await Task.WhenAll(oneShotTasks).WaitAsync(TimeSpan.FromSeconds(60));
            Assert.Equal(0, m.Count);
        }
    }
}
