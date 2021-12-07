using System;
using System.Buffers;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Google.Protobuf;
using Xunit;
using zero.core.feat.misc;
using zero.core.misc;

namespace zero.test.core.feat
{
    public class IoZeroMatcherTest
    {
        [ThreadStatic]
        private static SHA256 _sha256;
        public static SHA256 Sha256 => _sha256 ??= SHA256.Create();


        [Fact]
        async Task OneShot()
        {
            var threads = 2;
            var count = 100;
            var capacity = threads * count;
            var m = new IoZeroMatcher("Test matcher", threads, capacity, autoscale: false);

            var oneShotTasks = new List<Task>();
            for (int i = 0; i < threads; i++)
            {
                oneShotTasks.Add(await Task.Factory.StartNew(async payload =>
                {
                    var array = payload as byte[];
                    var key = ((ReadOnlyMemory<byte>)array).HashSig();
                    var c = await m.ChallengeAsync(key, array);

                    var reqHash = ArrayPool<byte>.Shared.Rent(32);

                    Sha256.TryComputeHash(array, reqHash, out var written);

                    await Task.Factory.StartNew(static async state =>
                    {
                        var (k, hash, matcher) = (ValueTuple<string, byte[], IoZeroMatcher>)state;
                        var dud = new byte[hash.Length];
                        hash.CopyTo(dud, 0);
                        dud[dud.Length / 2] = (byte)~dud[dud.Length / 2];

                        Assert.False(await matcher.ResponseAsync(k, UnsafeByteOperations.UnsafeWrap(dud)));
                        Assert.True(await matcher.ResponseAsync(k, UnsafeByteOperations.UnsafeWrap(hash)));
                    }, (key, reqHash, m), TaskCreationOptions.DenyChildAttach).Unwrap();

                }, BitConverter.GetBytes(i), TaskCreationOptions.DenyChildAttach));
            }

            await Task.WhenAll(oneShotTasks);
            Assert.Equal(0,m.Count);

        }

        [Fact]
        async Task SpamTest()
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
                    var c = await m.ChallengeAsync(key, array);

                    var reqHash = ArrayPool<byte>.Shared.Rent(32);

                    Sha256.TryComputeHash(array, reqHash, out var written);

                    await Task.Factory.StartNew(static async state =>
                    {
                        var (k, hash, matcher) = (ValueTuple<string, byte[], IoZeroMatcher>)state;
                        var dud = new byte[hash.Length];
                        hash.CopyTo(dud, 0);
                        dud[dud.Length / 2] = (byte)~dud[dud.Length / 2];

                        Assert.False(await matcher.ResponseAsync(k, UnsafeByteOperations.UnsafeWrap(dud)));
                        Assert.True(await matcher.ResponseAsync(k, UnsafeByteOperations.UnsafeWrap(hash)));
                    }, (key, reqHash, m), TaskCreationOptions.DenyChildAttach).Unwrap();

                }, BitConverter.GetBytes(i), TaskCreationOptions.DenyChildAttach));
            }

            await Task.WhenAll(oneShotTasks);
            Assert.Equal(0, m.Count);

        }
    }
}
