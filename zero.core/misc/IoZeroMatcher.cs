using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;

namespace zero.core.misc
{
    /// <summary>
    /// Matches challenge requests with a true response
    /// </summary>
    /// <typeparam name="T">The payload matched</typeparam>
    public class IoZeroMatcher<T> : IoNanoprobe
    where T:IEnumerable<byte>, IEquatable<ByteString>
    {
        public IoZeroMatcher(string description, int concurrencyLevel, long ttlMs = 2000, uint capacity = 10) : base($"{nameof(IoZeroMatcher<T>)}", concurrencyLevel)
        {
            _capacity = capacity * 2;
            _description = description??$"{GetType()}";
            _ttlMs = ttlMs;

            _lut = new IoQueue<IoChallenge>($"Matcher: {description}", (uint)Math.Max(concurrencyLevel*2, capacity), concurrencyLevel * 2);

            _valHeap = new IoHeap<IoChallenge>(_capacity)
            {
                Make = static (o,s) => new IoChallenge()
            };
        }

        /// <summary>
        /// Description
        /// </summary>
        private string _description;


        /// <summary>
        /// Holds requests
        /// </summary>
        //private readonly ConcurrentDictionary<string, System.Collections.Generic.List<IoChallenge>> _challenges = new ConcurrentDictionary<string, System.Collections.Generic.List<IoChallenge>>();
        private IoQueue<IoChallenge> _lut;

        /// <summary>
        /// Time to live
        /// </summary>
        private readonly long _ttlMs;


        /// <summary>
        /// The heap
        /// </summary>
        private IoHeap<IoChallenge> _valHeap;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
            _valHeap.ZeroUnmanaged();

#if SAFE_RELEASE
            _lut = null;
            _valHeap = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        /// <returns></returns>
        public override async ValueTask ZeroManagedAsync()
        {
            await _lut.ZeroManagedAsync<object>().FastPath().ConfigureAwait(Zc);

            await _valHeap.ZeroManagedAsync<object>().FastPath().ConfigureAwait(Zc);

            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);
        }

        /// <summary>
        /// Present a challenge
        /// </summary>
        /// <param name="key">The key</param>
        /// <param name="body">The payload</param>
        /// <param name="bump">bump the current challenge</param>
        /// <returns>True if successful</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<IoQueue<IoChallenge>.IoZNode> ChallengeAsync(string key, T body, bool bump = true)
        {
            IoChallenge challenge = null;
            IoQueue<IoChallenge>.IoZNode node = null;
            try
            {
                if ((challenge = await _valHeap.TakeAsync().FastPath().ConfigureAwait(Zc)) == null)
                {
                    await ResponseAsync("", ByteString.Empty).FastPath().ConfigureAwait(Zc);

                    challenge = await _valHeap.TakeAsync().FastPath().ConfigureAwait(Zc);
                    if (challenge == null)
                        throw new OutOfMemoryException(
                        $"{Description}: {nameof(_valHeap)} - heapSize = {_valHeap.Count}, ref = {_valHeap.ReferenceCount}");
                }
                
                challenge.Payload = body;
                challenge.Key = key;
                challenge.TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                challenge.Hash = 0;

                node = await _lut.EnqueueAsync(challenge).FastPath().ConfigureAwait(Zc);
                return node;
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                LogManager.GetCurrentClassLogger().Fatal(e);
                // ignored
            }
            finally
            {
                if (challenge!= null && node == null && _valHeap != null)
                    await _valHeap.ReturnAsync(challenge)
                        .FastPath().ConfigureAwait(Zc);
            }

            return null;
        }
        
        [ThreadStatic]
        private static SHA256 _sha256;
        public static SHA256 Sha256 => _sha256 ??= SHA256.Create();
        
        /// <summary>
        /// The bucket capacity this matcher targets
        /// </summary>
        private readonly uint _capacity;

        /// <summary>
        /// Present a response
        /// </summary>
        /// <param name="key">The response key</param>
        /// <param name="reqHash"></param>
        /// <returns>The response payload</returns>
        public async ValueTask<bool> ResponseAsync(string key, ByteString reqHash)
        {
            IoChallenge potential = default;
            var cmp = reqHash.Memory;
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            try
            {
                var cur = _lut.Tail;
                while(cur != null)
                {
                    //restart on collisions
                    if (_lut.Modified)
                    {
                        cur = _lut.Tail;
                        Thread.Yield();
                        continue;
                    }
                    if (cur.Value.TimestampMs <= timestamp && cur.Value.Key == key)
                    {
                        potential = cur.Value;
                        if (potential.Hash == 0)
                        {
                            var h = ArrayPool<byte>.Shared.Rent(32);
                            if (!Sha256.TryComputeHash((potential.Payload as ByteString)!.Memory.Span,
                                h, out var written))
                            {
                                LogManager.GetCurrentClassLogger().Fatal($"{_description}: Unable to compute hash");
                            }

                            potential.Payload = default;
                            potential.Hash = MemoryMarshal.Read<long>(h);
                            ArrayPool<byte>.Shared.Return(h);
                        }

                        if (potential.Hash != 0 && potential.Hash == MemoryMarshal.Read<long>(cmp.Span))
                        {
                            await _lut.RemoveAsync(cur).FastPath().ConfigureAwait(Zc);
                            await _valHeap.ReturnAsync(potential).FastPath().ConfigureAwait(Zc);
                            return  potential.TimestampMs.ElapsedMs() < _ttlMs;
                        }
                    }
                    
                    //drop old ones while we are at it
                    if (cur.Value.TimestampMs.ElapsedMs() > _ttlMs)
                    {
                        await _lut.RemoveAsync(cur).FastPath().ConfigureAwait(Zc);
                        await _valHeap.ReturnAsync(cur.Value).FastPath().ConfigureAwait(Zc);
                    }

                    cur = cur.Prev;
                }
            }
            finally
            {
                try
                {
                    if (_lut.Count > _capacity *4/5)
                    {
                        for (var n = _lut.Tail; n != null; n = n.Prev)
                        {
                            if (n.Value.TimestampMs.ElapsedMs() > _ttlMs)
                            {
                                await _lut.RemoveAsync(n).ConfigureAwait(Zc);
                                await _valHeap.ReturnAsync(n.Value).FastPath().ConfigureAwait(Zc); ;
                            }
                            else if (potential?.Key != null && potential.Key == n.Value.Key &&
                                     n.Value.TimestampMs < potential.TimestampMs)
                            {
                                await _lut.RemoveAsync(n).ConfigureAwait(Zc);
                                await _valHeap.ReturnAsync(n.Value).FastPath().ConfigureAwait(Zc); ;
                            }
                        }
                    }
                }
                catch(Exception e)
                {
                    LogManager.GetCurrentClassLogger().Error(e, $"{Description}");
                    // ignored
                }
            }
            return false;
        }

        /// <summary>
        /// Dump challenges into target matcher
        /// </summary>
        /// <param name="target"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask DumpAsync(IoZeroMatcher<T> target)
        {
            if(target == null)
                return;
            try
            {
                var cur = _lut.Head;
                while (cur != null)
                {
                    await target._lut.EnqueueAsync(cur.Value).FastPath().ConfigureAwait(Zc);
                    cur = cur.Next;
                }
            }
            catch 
            {
                //
            }
        }


        /// <summary>
        /// Remove a challenge from the matcher
        /// </summary>
        /// <param name="node">The challenge to remove</param>
        /// <returns>true on success, false otherwise</returns>
        public async ValueTask<bool> RemoveAsync(IoQueue<IoChallenge>.IoZNode node)
        {
            await _lut.RemoveAsync(node).FastPath().ConfigureAwait(Zc);
            await _valHeap.ReturnAsync(node.Value).FastPath().ConfigureAwait(Zc); ;

            return true;
        }

        /// <summary>
        /// Challenges held
        /// </summary>
        public uint Count => (uint)_lut.Count;

#if DEBUG
        /// <summary>
        /// used internally for debug
        /// </summary>
        //private static readonly SHA256 Sha256 = new SHA256CryptoServiceProvider();

        //public override string ToString()
        //{
        //    try
        //    {
        //        return $"{Count}:  {string.Join(", ",_challenges.Select(kv=>$"{kv.Key}::{Sha256.ComputeHash((kv.Payload as ByteString)?.ToByteArray()).HashSig()}, "))}";
        //    }
        //    catch (Exception e)
        //    {
        //        return e.Message;
        //    }
        //}
#endif
        /// <summary>
        /// Meta payload to be matched
        /// </summary>
        public class IoChallenge
        {
            /// <summary>
            /// The key
            /// </summary>
            public string Key;
            /// <summary>
            /// When the payload was challenged
            /// </summary>
            public long TimestampMs;

            /// <summary>
            /// The payload
            /// </summary>
            public T Payload;
            
            /// <summary>
            /// The computed hash
            /// </summary>
            public long Hash;

        }
    }
}
