using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using zero.core.misc;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;

namespace zero.core.feat.misc
{
    /// <summary>
    /// Matches challenge requests with a true response
    /// </summary>
    public class IoZeroMatcher : IoNanoprobe
    {
        public IoZeroMatcher(string description, int concurrencyLevel, long ttlMs = 2000, int capacity = 10, bool autoscale = true) : base($"{nameof(IoZeroMatcher)}", concurrencyLevel)
        {
            _capacity = capacity * 2;
            _description = description??$"{GetType()}";
            _ttlMs = ttlMs;

            _lut = new IoQueue<IoChallenge>($"Matcher: {description}", Math.Max(concurrencyLevel*2, _capacity), concurrencyLevel * 2, autoScale: autoscale);

            _valHeap = new IoHeap<IoChallenge>($"{nameof(_valHeap)}: {description}", Math.Max(concurrencyLevel * 2, _capacity), autoScale: autoscale)
            {
                Malloc = static (_,_) => new IoChallenge()
            };

            _carHeap = new IoHeap<ChallengeAsyncResponse>($"{nameof(_valHeap)}: {description}", _capacity, autoScale: autoscale)
            {
                Malloc = static (_, _) => new ChallengeAsyncResponse()
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
        /// The challenges heap
        /// </summary>
        private IoHeap<ChallengeAsyncResponse> _carHeap;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
            _valHeap.ZeroUnmanaged();
            _carHeap.ZeroUnmanaged();

#if SAFE_RELEASE
            _lut = null;
            _valHeap = null;
            _logger = null;
            _carHeap = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        /// <returns></returns>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);

            await _lut.ZeroManagedAsync<object>(zero: true).FastPath().ConfigureAwait(Zc);

            await _valHeap.ZeroManagedAsync<object>().FastPath().ConfigureAwait(Zc);

            await _carHeap.ZeroManagedAsync<object>().FastPath().ConfigureAwait(Zc);
        }


        internal class ChallengeAsyncResponse
        {
            //public IoZeroMatcher This;
            public string Key;
            public byte[] Body;
            public IoQueue<IoChallenge>.IoZNode Node;
        }


        /// <summary>
        /// Present a challenge
        /// </summary>
        /// <param name="key">The key</param>
        /// <param name="body">The payload</param>
        /// <returns>True if successful</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<IoQueue<IoChallenge>.IoZNode> ChallengeAsync(string key, byte[] body)
        {
            if (body == null)
                return default;

            ChallengeAsyncResponse response = null;
            IoQueue<IoChallenge>.IoZNode node;
            try
            {
                response = _carHeap.Take(this);
                
                if (response == null)
                    throw new OutOfMemoryException($"{nameof(_carHeap)}, {Description}");

                response.Key = key;
                response.Body = body;

                await ZeroAtomic(static async (_, state, _) =>
                {
                    var (@this, response) = state;
                    IoChallenge challenge = null;
                    try
                    {
                        if ((challenge = @this._valHeap.Take()) == null)
                        {
                            try
                            {
                                await @this.PurgeAsync().FastPath().ConfigureAwait(@this.Zc);
                            }
                            catch (Exception e)
                            {
                                @this._logger.Error(e, $" Purge failed: {@this.Description}");
                                // ignored
                            }

                            challenge = @this._valHeap.Take();
                        
                            if (challenge == null)
                            {
                                var c = @this._lut.Head;
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

                                throw new OutOfMemoryException($"{@this.Description}: {nameof(_valHeap)} - heapSize = {@this._valHeap.Count}, ref = {@this._valHeap.ReferenceCount}, ave Ttl = {ave/aveCounter}ms / {@this._ttlMs}ms, (c = {aveCounter})");
                            }
                            
                        }

                        challenge.Payload = response.Body;
                        challenge.Key = response.Key;
                        challenge.TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        challenge.Hash = 0;
                        response.Node = await @this._lut.EnqueueAsync(challenge).FastPath().ConfigureAwait(@this.Zc);
                    }
                    catch when (@this.Zeroed()) { }
                    catch (Exception e) when (!@this.Zeroed())
                    {
                        @this._logger.Fatal(e);
                        // ignored
                    }
                    finally
                    {
                        if (challenge != null && response.Node == null && @this._valHeap != null)
                            @this._valHeap.Return(challenge);
                    }

                    return true;
                }, (this,response)).FastPath().ConfigureAwait(Zc);
            }
            finally
            {
                node = response?.Node;
                _carHeap.Return(response);
            }

            return node;
        }
        
        [ThreadStatic]
        private static SHA256 _sha256;
        public static SHA256 Sha256 => _sha256 ??= SHA256.Create();
        
        /// <summary>
        /// The bucket capacity this matcher targets
        /// </summary>
        private readonly int _capacity;

        /// <summary>
        /// Matches a challenge with a response
        /// </summary>
        /// <param name="ioNanite"></param>
        /// <param name="state"></param>
        /// <param name="_"></param>
        /// <returns></returns>
        private async ValueTask<bool> MatchAsync(IIoNanite ioNanite, (IoZeroMatcher, string key, ByteString reqHash) state, bool _)
        {
            var (@this, key, reqHash) = state;
            var reqHashMemory = reqHash.Memory;
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            var cur = @this._lut.Head;
            @this._lut.Reset();

            //TODO params:
            var insane = 20;

            while (cur != null)
            {
                //restart on collisions
                if (@this._lut.Modified && insane --> 0)
                {
                    cur = @this._lut.Head;
                    @this._lut.Reset();
                    continue;
                }

                if (cur.Value.TimestampMs <= timestamp && cur.Value.Key == key)
                {
                    var potential = cur.Value;

                    if (potential.Hash == 0)
                    {
                        StacklessAsync(potential, @this);
                    }

                    [MethodImpl(MethodImplOptions.AggressiveInlining)]
                    static void StacklessAsync(IoChallenge potential, IoZeroMatcher @this)
                    {
                        Span<byte> h = stackalloc byte[32];
                        
                        if (!Sha256.TryComputeHash(potential.Payload, h, out var _))
                        {
                            LogManager.GetCurrentClassLogger()
                                .Fatal($"{@this._description}: Unable to compute hash");
                        }

                        potential.Payload = default;
                        potential.Hash = MemoryMarshal.Read<long>(h);
                    }

                    if (potential.Hash != 0 && potential.Hash == MemoryMarshal.Read<long>(reqHashMemory.Span))
                    {
                        await @this._lut.RemoveAsync(cur).FastPath().ConfigureAwait(@this.Zc);
                        @this._valHeap.Return(potential);
                        return potential.TimestampMs.ElapsedMs() < @this._ttlMs;
                    }
                }

                //drop old ones while we are at it
                if (cur.Value.TimestampMs.ElapsedMs() > @this._ttlMs)
                {
                    var value = cur.Value;
                    await @this._lut.RemoveAsync(cur).FastPath().ConfigureAwait(@this.Zc);
                    @this._valHeap.Return(value);
                }

                cur = cur.Next;
            }

            if(insane <= 0)
                _logger.Trace($"{nameof(MatchAsync)}: Failed insane check, too many collisions... [OK]");
            return false;
        }

        /// <summary>
        /// Present a response
        /// </summary>
        /// <param name="key">The response key</param>
        /// <param name="reqHash"></param>
        /// <returns>The response payload</returns>
        public async ValueTask<bool> ResponseAsync(string key, ByteString reqHash)
        {
            return reqHash.Length != 0 && await ZeroAtomic(MatchAsync, (this, key, reqHash)).FastPath().ConfigureAwait(Zc);
        }

        /// <summary>
        /// Purge the lut from old challenges
        /// </summary>
        /// <returns>A value task</returns>
        private async ValueTask PurgeAsync()
        {
            if (_lut.Count > _capacity * 2 / 3)
            {
                var c = _lut.Capacity * 2;
                var n = _lut.Head;
                while (n != null && c --> 0)
                {
                    var t = n.Next;
                    if (n.Value.TimestampMs.ElapsedMs() > _ttlMs)
                    {
                        var value = n.Value;
                        await _lut.RemoveAsync(n).FastPath().ConfigureAwait(Zc);
                        _valHeap.Return(value);
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
        public async ValueTask DumpAsync(IoZeroMatcher target)
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
        /// Dump challenges to log
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DumpToLog()
        {
            try
            {
                Span<byte> h = stackalloc byte[32];
                var cur = _lut.Head;
                while (cur != null)
                {
                    if (!Sha256.TryComputeHash(cur.Value.Payload, h, out var _))
                    {
                        LogManager.GetCurrentClassLogger().Fatal($"{_description}: Unable to compute hash");
                    }

                    cur.Value.Payload = default;

                    _logger.Error($"{h.HashSig()}[{cur.Value.Key}], t = {cur.Value.TimestampMs.ElapsedMs()}ms, z = {MemoryMarshal.Read<long>(h)}");
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
            var value = node.Value;
            await _lut.RemoveAsync(node).FastPath().ConfigureAwait(Zc);
            _valHeap.Return(value);

            return true;
        }

        /// <summary>
        /// Challenges held
        /// </summary>
        public int Count => _lut.Count;


        /// <summary>
        /// Capacity
        /// </summary>
        public int Capacity => _lut.Capacity;

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
            public byte[] Payload;

            private long _hash;
            /// <summary>
            /// The computed hash
            /// </summary>
            public long Hash {
                get => Volatile.Read(ref _hash);
                set => Interlocked.Exchange(ref _hash, value);
            }
        }
    }
}
