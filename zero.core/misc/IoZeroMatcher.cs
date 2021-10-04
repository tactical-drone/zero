using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using NLog.Fluent;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore.core;

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

            _lut = new IoZeroQueue<IoChallenge>($"Matcher: {description}", Math.Max(concurrencyLevel*2, capacity), concurrencyLevel);

            _valHeap = new IoHeap<IoChallenge>(_capacity)
            {
                Make = _ => new IoChallenge()
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
        private IoZeroQueue<IoChallenge> _lut;

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
            //await _heap.ZeroManaged(challenge =>
            //{
            //    challenge.Payload = default;
            //    challenge.Key = null;
            //    return ValueTask.CompletedTask;
            //}).FastPath().ConfigureAwait(false);

            await _lut.ZeroManagedAsync<object>().FastPath().ConfigureAwait(false);

            await _valHeap.ZeroManagedAsync<object>().FastPath().ConfigureAwait(false);

            await base.ZeroManagedAsync().FastPath().ConfigureAwait(false);
        }

        /// <summary>
        /// Present a challenge
        /// </summary>
        /// <param name="key">The key</param>
        /// <param name="body">The payload</param>
        /// <param name="bump">bump the current challenge</param>
        /// <returns>True if successful</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<IoZeroQueue<IoChallenge>.IoZNode> ChallengeAsync(string key, T body, bool bump = true)
        {
            IoChallenge challenge = null;
            IoZeroQueue<IoChallenge>.IoZNode node = null;
            try
            {
                _valHeap.Take(out challenge);

                if (challenge == null)
                {
                    await ResponseAsync("", ByteString.Empty).FastPath().ConfigureAwait(false);
                    _valHeap.Take(out challenge);
                    
                    if (challenge == null)
                        throw new OutOfMemoryException(
                        $"{Description}: {nameof(_valHeap)} - heapSize = {_valHeap.CurrentHeapSize}, ref = {_valHeap.ReferenceCount}");
                }
                
                challenge.Payload = body;
                challenge.Key = key;
                challenge.TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                challenge.Hash = 0;

                node = await _lut.EnqueueAsync(challenge).FastPath().ConfigureAwait(false);
                return node;
            }
            catch (Exception e)
            {
                LogManager.GetCurrentClassLogger().Fatal(e);
                // ignored
            }
            finally
            {
                if (challenge!= null && node == null && _valHeap != null)
                    await _valHeap.ReturnAsync(challenge)
                        .FastPath().ConfigureAwait(false);
            }

            return null;
        }
        
        [ThreadStatic]
        private static SHA256 _sha256;
        public static SHA256 Sha256 => _sha256 ??= new SHA256Managed();
        
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<bool> ResponseAsync(string key, ByteString reqHash)
        {
            IoChallenge potential = default;
            var cmp = reqHash.Memory;
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            try
            {
                var cur = _lut.Last;
                while(cur != null)
                {
                    if (cur.Value.TimestampMs <= timestamp && cur.Value.Key == key)
                    {
                        potential = cur.Value;
                        if (potential.Hash == 0)
                        {
                            var hash = Sha256.ComputeHash((potential.Payload as ByteString)?.Memory.AsArray() ?? Array.Empty<byte>());
                            potential.Payload = default;
                            potential.Hash = MemoryMarshal.Read<long>(hash);
                        }

                        if (potential.Hash != 0 && potential.Hash == MemoryMarshal.Read<long>(cmp.Span))
                        {
                            await _lut.RemoveAsync(cur).FastPath().ConfigureAwait(false);
                            await _valHeap.ReturnAsync(potential).FastPath().ConfigureAwait(false);
                            return  potential.TimestampMs.ElapsedMs() < _ttlMs;
                        }
                    }
                    
                    //drop old ones while we are at it
                    if (cur.Value.TimestampMs.ElapsedMs() > _ttlMs)
                    {
                        await _lut.RemoveAsync(cur).FastPath().ConfigureAwait(false);
                        await _valHeap.ReturnAsync(cur.Value).FastPath().ConfigureAwait(false);
                    }

                    cur = cur.Prev;
                }
            }
            finally
            {
                try
                {
                    if (_lut.Count > _capacity / 2)
                    {
                        for (var n = _lut.Last; n != null; n = n.Prev)
                        {
                            if (n.Value.TimestampMs.ElapsedMs() > _ttlMs)
                            {
                                await _lut.RemoveAsync(n).ConfigureAwait(false);
                                await _valHeap.ReturnAsync(n.Value).FastPath().ConfigureAwait(false); ;
                            }
                            else if (potential?.Key != null && potential.Key == n.Value.Key &&
                                     n.Value.TimestampMs < potential.TimestampMs)
                            {
                                await _lut.RemoveAsync(n).ConfigureAwait(false);
                                await _valHeap.ReturnAsync(n.Value).FastPath().ConfigureAwait(false); ;
                            }
                        }
                    }
                }
                catch(Exception e)
                {
                    Console.WriteLine(e);
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
                var cur = _lut.First;
                while (cur != null)
                {
                    await target._lut.EnqueueAsync(cur.Value).FastPath().ConfigureAwait(false);
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
        public async ValueTask<bool> RemoveAsync(IoZeroQueue<IoChallenge>.IoZNode node)
        {
            await _lut.RemoveAsync(node).FastPath().ConfigureAwait(false);
            await _valHeap.ReturnAsync(node.Value).FastPath().ConfigureAwait(false); ;

            return true;
        }

        /// <summary>
        /// Challenges held
        /// </summary>
        public int Count => _lut.Count;

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
