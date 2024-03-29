﻿using System;
using System.Runtime.CompilerServices;
using System.Runtime.ConstrainedExecution;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using NLog.Filters;
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
        public IoZeroMatcher(string description, int concurrencyLevel, long ttlMs = 2000, int capacity = 64, bool autoscale = true) : base($"{nameof(IoZeroMatcher)}", concurrencyLevel)
        {
            _capacity = capacity;
            _description = description??$"{GetType()}";
            _ttlMs = ttlMs;

            var size = Math.Max(capacity, concurrencyLevel);

            _lut = new IoQueue<IoChallenge>($"Matcher: {description}", size, size, autoscale?IoQueue<IoChallenge>.Mode.DynamicSize : 0);

            _valHeap = new IoHeap<IoChallenge>($"{nameof(_valHeap)}: {description}", size, static (_, _) => new IoChallenge(), autoscale);

            _carHeap = new IoHeap<ChallengeAsyncResponse>($"{nameof(_carHeap)}: {description}", size, static (_, _) => new ChallengeAsyncResponse(), autoscale);

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

                Interlocked.Exchange(ref response.Key, key);
                Interlocked.Exchange(ref response.Body, body);

                IoChallenge challenge = null;
                try
                {
                    if ((challenge = _valHeap.Take()) == null || _valHeap.ReferenceCount >= _valHeap.Capacity)
                    {
                        try
                        {
                            await PurgeAsync().FastPath();
                        }
                        catch when(Zeroed()){}
                        catch (Exception e) when(Zeroed())
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
                                ave += c.Value.TimestampMs.ElapsedUtcMs();
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
                        challenge.TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        Interlocked.Exchange(ref response.Node, await _lut.EnqueueAsync(challenge).FastPath());
                        if (response.Node == null)
                            _logger.Fatal($"{nameof(ChallengeAsync)}: unable to Q challange, {_lut.Description}");
#if TRACE
                        Console.WriteLine($"[{Environment.CurrentManagedThreadId}] CHALLENGE <<{key}>> {challenge.Hash.HashSig()}, -> {_lut.Description}");
#endif
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
        /// <returns>True if matched, false otherwise</returns>
        public async ValueTask<bool> ResponseAsync(string key, ByteString reqHash)
        {
            var sw = new SpinWait();
            var cur = _lut.Head;
            
            while (cur != null)
            {
                try
                {
                    var qid = cur.Qid;
#if TRACE
                    Console.WriteLine($"[{Environment.CurrentManagedThreadId}] CHECKED <<{cur.Value.Key}>> -> {cur.Value.Hash.HashSig()}");
#endif
                    if (cur.Value.TimestampMs.ElapsedUtcMs() <= _ttlMs && cur.Value.Key == key &&
                        cur.Value.Hash.ArrayEqual(reqHash.Span))
                    {
#if TRACE
                        Console.WriteLine($"[{Environment.CurrentManagedThreadId}] RESPONSE {cur.Value.Hash.HashSig()} <<{cur.Value.Key}>> -> {_lut.Description}");               
#endif
                        var tmp = Volatile.Read(ref cur.Value);
                        await _lut.RemoveAsync(cur, qid).FastPath();
                        _valHeap.Return(tmp);
                        return true;
                    }

                    if (cur.Value.TimestampMs.ElapsedUtcMs() > _ttlMs)
                    {
                        var value = Volatile.Read(ref cur.Value);
#if TRACE
                        Console.WriteLine($"[{Environment.CurrentManagedThreadId}] BURNED  {cur.Value.Hash.HashSig()} <<{cur.Value.Key}>> -> {_lut.Description}");
#endif
                        await _lut.RemoveAsync(cur, qid).FastPath();
                        cur = _lut.Head;
                        _valHeap.Return(value);
                        continue;
                    }

                    cur = cur.Next;
                }
                catch (Exception e)
                {
                    _logger.Trace(e, $"{nameof(ResponseAsync)}: {Description}");
                    if (sw.Count < byte.MaxValue && !Zeroed())
                        cur = _lut.Head;
                    else
                        break;
                }
            }

#if TRACE
            Console.WriteLine($"[{Environment.CurrentManagedThreadId}] RESPONSE FAILED for {reqHash.Memory.HashSig()}!!! -> {_lut.Description}");
#endif

            return false;
        }

        /// <summary>
        /// Purge the lut from old challenges
        /// </summary>
        /// <returns>A value task</returns>
        private async ValueTask PurgeAsync()
        {
            var sw = new SpinWait();
            if (_lut.Count > _capacity * 2 / 3)
            {
                var c = _lut.Capacity * 2;
                var n = _lut.Head;
                while (n != null && c --> 0)
                {
                    var t = n.Next;
                    try
                    {
                        var qId = n.Qid;
                        if (n.Value.TimestampMs.ElapsedUtcMs() > _ttlMs)
                        {
                            var challenge = n.Value;
                            await _lut.RemoveAsync(n, qId).FastPath();
                            _valHeap.Return(challenge);
                        }
                    }
                    catch when (Zeroed()){}
                    catch (Exception e) when(!Zeroed())
                    {
                        _logger.Trace(e,$"{Description}");
                        if (sw.Count < short.MaxValue)
                        {
                            n = _lut.Head;
                            continue;
                        }
                        break;
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
                    _logger.Error($"{cur.Value.Hash.HashSig()}[{cur.Value.Key}], t = {cur.Value.TimestampMs.ElapsedUtcMs()}ms");
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
            await _lut.RemoveAsync(node, node.Qid).FastPath();
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
            public long TimestampMs;

            /// <summary>
            /// The hash
            /// </summary>
            public byte[] Hash = new byte[32];
        }
    }
}
