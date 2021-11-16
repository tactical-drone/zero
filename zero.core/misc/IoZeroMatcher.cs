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
        public IoZeroMatcher(string description, int concurrencyLevel, long ttlMs = 2000, int capacity = 10) : base($"{nameof(IoZeroMatcher<T>)}", concurrencyLevel)
        {
            _capacity = capacity * 2;
            _description = description??$"{GetType()}";
            _ttlMs = ttlMs;

            _lut = new IoQueue<IoChallenge>($"Matcher: {description}", Math.Max(concurrencyLevel*2, _capacity), concurrencyLevel * 2);

            _valHeap = new IoHeap<IoChallenge>($"{nameof(_valHeap)}: {description}", _capacity)
            {
                Make = static (o,s) => new IoChallenge()
            };

            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// Description
        /// </summary>
        private readonly string _description;

        /// <summary>
        /// Logger.
        /// </summary>
        private Logger _logger;

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
            _logger = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        /// <returns></returns>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);

            await _lut.ZeroManagedAsync<object>().FastPath().ConfigureAwait(Zc);

            await _valHeap.ZeroManagedAsync<object>().FastPath().ConfigureAwait(Zc);
        }


        internal class ChallengeAsyncResponse
        {
            public IoZeroMatcher<T> @this;
            public string key;
            public T body;
            public IoQueue<IoChallenge>.IoZNode node;
        }


        /// <summary>
        /// Present a challenge
        /// </summary>
        /// <param name="key">The key</param>
        /// <param name="body">The payload</param>
        /// <param name="bump">bump the current challenge</param>
        /// <returns>True if successful</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IoQueue<IoChallenge>.IoZNode> ChallengeAsync(string key, T body, bool bump = true)
        {
            IoQueue<IoChallenge>.IoZNode node = new IoQueue<IoChallenge>.IoZNode();
            var state = new ChallengeAsyncResponse {@this = this, key = key, body = body, node = node};
            ZeroAtomic(static async (_, state, __) =>
            {
                IoChallenge challenge = null;
                try
                {
                    if ((challenge = await state.@this._valHeap.TakeAsync().FastPath().ConfigureAwait(state.@this.Zc)) == null)
                    {
                        try
                        {
                            await state.@this.PurgeAsync().FastPath().ConfigureAwait(state.@this.Zc);
                        }
                        catch (Exception e)
                        {
                            state.@this._logger.Error(e, $" Purge failed: {state.@this.Description}");
                            // ignored
                        }

                        challenge = await state.@this._valHeap.TakeAsync().FastPath().ConfigureAwait(state.@this.Zc);
                        if (challenge == null)
                        {
                            var c = state.@this._lut.Tail;
                            long ave = 0;
                            var aveCounter = 0;
                            while(c != null)
                            {
                                ave += c.Value.TimestampMs.ElapsedMs();
                                aveCounter++;
                                c = c.Next;
                            }

                            if (aveCounter == 0)
                                aveCounter = 1;

                            throw new OutOfMemoryException($"{state.@this.Description}: {nameof(_valHeap)} - heapSize = {state.@this._valHeap.Count}, ref = {state.@this._valHeap.ReferenceCount}, ave Ttl = {ave/aveCounter}ms / {state.@this._ttlMs}ms, (c = {aveCounter})");
                        }
                            
                    }

                    challenge.Payload = state.body;
                    challenge.Key = state.key;
                    challenge.TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    challenge.Hash = 0;
                    state.node = await state.@this._lut.EnqueueAsync(challenge).FastPath().ConfigureAwait(state.@this.Zc);
                }
                catch when (state.@this.Zeroed()) { }
                catch (Exception e) when (!state.@this.Zeroed())
                {
                    state.@this._logger.Fatal(e);
                    // ignored
                }
                finally
                {
                    if (challenge != null && state.node == null && state.@this._valHeap != null)
                        await state.@this._valHeap.ReturnAsync(challenge).FastPath().ConfigureAwait(state.@this.Zc);
                }

                return true;
            }, state);

            return new ValueTask<IoQueue<IoChallenge>.IoZNode>(state.node);
        }
        
        [ThreadStatic]
        private static SHA256 _sha256;
        public static SHA256 Sha256 => _sha256 ??= SHA256.Create();
        
        /// <summary>
        /// The bucket capacity this matcher targets
        /// </summary>
        private readonly int _capacity;

        /// <summary>
        /// Present a response
        /// </summary>
        /// <param name="key">The response key</param>
        /// <param name="reqHash"></param>
        /// <returns>The response payload</returns>
        public ValueTask<bool> ResponseAsync(string key, ByteString reqHash)
        {
            return new ValueTask<bool>(ZeroAtomic(static async (_, state, __) =>
            {
                var (@this, key, reqHash) = state;
                var cmp = reqHash.Memory;
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                var cur = @this._lut.Tail;
                while (cur != null)
                {
                    //restart on collisions
                    if (@this._lut.Modified)
                    {
                        cur = @this._lut.Tail;
                        Thread.Yield();
                        continue;
                    }

                    if (cur.Value.TimestampMs <= timestamp && cur.Value.Key == key)
                    {
                        var potential = cur.Value;
                        if (potential.Hash == 0)
                        {
                            var h = ArrayPool<byte>.Shared.Rent(32);
                            if (!Sha256.TryComputeHash((potential.Payload as ByteString)!.Memory.Span,
                                    h, out var written))
                            {
                                LogManager.GetCurrentClassLogger()
                                    .Fatal($"{@this._description}: Unable to compute hash");
                            }

                            potential.Payload = default;
                            potential.Hash = MemoryMarshal.Read<long>(h);
                            ArrayPool<byte>.Shared.Return(h, true);
                        }

                        if (potential.Hash != 0 && potential.Hash == MemoryMarshal.Read<long>(cmp.Span))
                        {
                            await @this._lut.RemoveAsync(cur).FastPath().ConfigureAwait(@this.Zc);
                            await @this._valHeap.ReturnAsync(potential).FastPath().ConfigureAwait(@this.Zc);
                            return potential.TimestampMs.ElapsedMs() < @this._ttlMs;
                        }
                    }

                    //drop old ones while we are at it
                    if (cur.Value.TimestampMs.ElapsedMs() > @this._ttlMs)
                    {
                        await @this._lut.RemoveAsync(cur).FastPath().ConfigureAwait(@this.Zc);
                        await @this._valHeap.ReturnAsync(cur.Value).FastPath().ConfigureAwait(@this.Zc);
                    }

                    cur = cur.Next;
                }

                return false;
            }, (this, key, reqHash)));
        }

        /// <summary>
        /// Purge the lut from old challenges
        /// </summary>
        /// <returns>A value task</returns>
        private async ValueTask PurgeAsync()
        {
            if (_lut.Count > _capacity * 2 / 3)
            {
                var n = _lut.Tail;
                while (n != null)
                {
                    var t = n.Next;
                    if (n.Value.TimestampMs.ElapsedMs() > _ttlMs)
                    {
                        await _lut.RemoveAsync(n).FastPath().ConfigureAwait(Zc);
                        await _valHeap.ReturnAsync(n.Value).FastPath().ConfigureAwait(Zc);
                    }
                    n = t;
                }
            }
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
