﻿using System;
using System.Runtime.CompilerServices;
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
        public IoZeroMatcher(string description, int concurrencyLevel, long ttlMs = 2000, int capacity = 32, bool autoscale = true) : base($"{nameof(IoZeroMatcher)}", concurrencyLevel * 2)
        {
            _capacity = capacity;
            _description = description??$"{GetType()}";
            _ttlMs = ttlMs;

            _lut = new IoQueue<IoChallenge>($"Matcher: {description}", capacity, concurrencyLevel, autoscale?IoQueue<IoChallenge>.Mode.DynamicSize : 0);

            _valHeap = new IoHeap<IoChallenge>($"{nameof(_valHeap)}: {description}", capacity, static (_, _) => new IoChallenge(), autoscale);

            _carHeap = new IoHeap<ChallengeAsyncResponse>($"{nameof(_carHeap)}: {description}", capacity, static (_, _) => new ChallengeAsyncResponse(), autoscale);

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
        private readonly IoHeap<IoChallenge> _valHeap;

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
            await base.ZeroManagedAsync().FastPath();

            await _lut.ZeroManagedAsync<object>(zero: true).FastPath();

            await _valHeap.ZeroManagedAsync<object>().FastPath();

            await _carHeap.ZeroManagedAsync<object>().FastPath();
        }

        internal class ChallengeAsyncResponse
        {
            //public IoZeroMatcher This;
            public volatile string Key;
            public byte[] Body;
            public volatile IoQueue<IoChallenge>.IoZNode Node;
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

                IoChallenge challenge = null;
                try
                {
                    if ((challenge = _valHeap.Take()) == null || _valHeap.ReferenceCount > _valHeap.Capacity)
                    {
                        try
                        {
                            await PurgeAsync().FastPath();
                        }
                        catch (Exception e)
                        {
                            _logger.Error(e, $" Purge failed: {Description}");
                            // ignored
                        }

                        challenge ??= _valHeap.Take();

                        if (challenge == null)
                        {
                            var c = _lut.Head;
                            long ave = 0;
                            var aveCounter = 0;
                            while (c != null)
                            {
                                ave += c.Value.TimestampMs.ElapsedMs();
                                aveCounter++;
                                c = c.Next;
                            }

                            if (aveCounter == 0)
                                aveCounter = 1;

                            throw new OutOfMemoryException(
                                $"{Description}: {nameof(_valHeap)} - heapSize = {_valHeap.Count}, ref = {_valHeap.ReferenceCount}, ave Ttl = {ave / aveCounter}ms / {_ttlMs}ms, (c = {aveCounter})");
                        }
                    }

                    if (!Sha256.TryComputeHash(response.Body, challenge.Hash, out var bytesWritten))
                    {
                        LogManager.GetCurrentClassLogger().Fatal($"{_description}: Unable to compute hash");
                        return default;
                    }

                    if (bytesWritten > 0)
                    {
                        challenge.Key = response.Key;
                        challenge.TimestampMs = Environment.TickCount;
                        response.Node = await _lut.EnqueueAsync(challenge).FastPath();
                        if (response.Node == null)
                        {
                            _logger.Fatal($"{nameof(ChallengeAsync)}: unable to Q challange, {_lut.Description}");
                        }
                    }
                }
                catch when (Zeroed())
                {
                }
                catch (Exception e) when (!Zeroed())
                {
                    _logger.Fatal(e);
                    // ignored
                }
                finally
                {
                    if (challenge != null && response.Node == null && _valHeap != null)
                        _valHeap.Return(challenge);
                }
            }
            catch when (Zeroed())
            {
            }
            catch (Exception e) when (!Zeroed())
            {
                _logger.Fatal(e);
                // ignored
            }
            finally
            {
                node = response?.Node;
                _carHeap?.Return(response);
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
        /// <returns>True if matched, false otherwise</returns>
        private async ValueTask<bool> MatchAsync(IIoNanite ioNanite, (IoZeroMatcher, string key, ByteString reqHash) state, bool _)
        {
            var (@this, key, reqHash) = state;

            @this._lut.Modified = false;
            var cur = @this._lut.Head;
            
            while (cur != null)
            {
                //restart on collisions
                if (@this._lut.Modified)
                {
                    @this._lut.Modified = false;
                    cur = @this._lut.Head;
                    continue;
                }

                try
                {
                    if (cur.Value.TimestampMs.ElapsedMs() <= _ttlMs && cur.Value.Key == key &&
                        cur.Value.Hash.ArrayEqual(reqHash.Span))
                    {
                        var tmp = Volatile.Read(ref cur.Value);
                        await @this._lut.RemoveAsync(cur).FastPath();
                        @this._valHeap.Return(tmp);
                        return true;
                    }

                    if (cur.Value.TimestampMs.ElapsedMs() > _ttlMs)
                    {
                        var value = Volatile.Read(ref cur.Value);
                        await @this._lut.RemoveAsync(cur).FastPath();
                        cur = @this._lut.Head;
                        @this._lut.Modified = false;
                        @this._valHeap.Return(value);
                        continue;
                    }

                    cur = cur.Next;
                }
                catch
                {
                    @this._lut.Modified = false;
                    cur = @this._lut.Head;
                }
            }

            return false;
        }

        /// <summary>
        /// Present a response
        /// </summary>
        /// <param name="key">The response key</param>
        /// <param name="reqHash"></param>
        /// <returns>The response payload</returns>
        public ValueTask<bool> ResponseAsync(string key, ByteString reqHash) => ZeroAtomicAsync(MatchAsync, (this, key, reqHash));
        
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
                        await _lut.RemoveAsync(n).FastPath();
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
                    await target._lut.EnqueueAsync(cur.Value).FastPath();
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
                var cur = _lut.Head;
                while (cur != null)
                {
                    _logger.Error($"{cur.Value.Hash.HashSig()}[{cur.Value.Key}], t = {cur.Value.TimestampMs.ElapsedMs()}ms");
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
            if (Zeroed())
                return false;

            var value = node.Value;
            await _lut.RemoveAsync(node).FastPath();
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
            public volatile string Key;

            /// <summary>
            /// When the payload was challenged
            /// </summary>
            public volatile int TimestampMs;

            /// <summary>
            /// The hash
            /// </summary>
            public byte[] Hash = new byte[32];
        }
    }
}
