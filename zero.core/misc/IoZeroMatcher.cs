using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Google.Protobuf;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
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
        public IoZeroMatcher(string description, int concurrencyLevel, long ttlMs = 2000, int capacity = 10) : base($"{nameof(IoZeroMatcher<T>)}")
        {
            _capacity = capacity * 2;
            _description = description??$"{GetType()}";
            _ttlMs = ttlMs;
            _matcherMutex = new IoZeroSemaphore($"{nameof(_matcherMutex)}[{_description}]", concurrencyLevel*2, 1);
            _matcherMutex.ZeroRef(ref _matcherMutex, AsyncTasks.Token);
            _heap = new IoHeap<IoChallenge>(_capacity)
            {
                Make = _ => new IoChallenge()
            };
        }

        /// <summary>
        /// Description
        /// </summary>
        private string _description;

        /// <summary>
        /// Used to sync the list of challanges 
        /// </summary>
        private IIoZeroSemaphore _matcherMutex;

        /// <summary>
        /// Holds requests
        /// </summary>
        //private readonly ConcurrentDictionary<string, System.Collections.Generic.List<IoChallenge>> _challenges = new ConcurrentDictionary<string, System.Collections.Generic.List<IoChallenge>>();
        volatile LinkedList<IoChallenge> _challenges = new LinkedList<IoChallenge>();

        /// <summary>
        /// Time to live
        /// </summary>
        private readonly long _ttlMs;


        /// <summary>
        /// The heap
        /// </summary>
        private IoHeap<IoChallenge> _heap;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
            _heap.ZeroUnmanaged();

#if SAFE_RELEASE
            _matcherMutex = null;
            _heap = null;
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

            await _heap.ZeroManaged().FastPath().ConfigureAwait(false);

            _matcherMutex.Zero();
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
        public async ValueTask<LinkedListNode<IoChallenge>> ChallengeAsync(string key, T body, bool bump = true)
        {
            var added = false;
            IoChallenge challenge = null;
            try
            {
                _heap.Take(out challenge);

                if (challenge == null)
                    throw new OutOfMemoryException($"{Description}: {nameof(_heap)} - heapSize = {_heap.CurrentHeapSize}, ref = {_heap.ReferenceCount}");

                challenge.Payload = body;
                challenge.Key = key;
                challenge.TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                challenge.Hash = 0;

                if (await _matcherMutex.WaitAsync().FastPath().ConfigureAwait(false))
                {
                    var p = (challenge.Payload as ByteString)!.Memory;
                    added = true;
                    return _challenges.AddFirst(challenge);
                }
                else
                {
                    return null;
                }
            }
            finally
            {
                if (!added && challenge != null)
                    _heap.Return(challenge);

                _matcherMutex.Release();
            }
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

            try
            {
                if (await _matcherMutex.WaitAsync().FastPath().ConfigureAwait(false))
                {
                    var cur = _challenges.Last;
                    while(cur != null)
                    {
                        if (cur.Value.Key == key)
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

                                _challenges.Remove(potential);
                                _heap.Return(potential);
                                return true;
                            }
                        }

                        cur = cur.Previous;
                    }
                }
                else
                {
                    return false;
                }
            }
            finally
            {
                try
                {
                    if (_challenges.Count > _capacity / 2)
                    {
                        for (var n = _challenges.Last; n != null; n = n.Previous)
                        {
                            
                            if (n.Value.TimestampMs.ElapsedMs() > _ttlMs)
                            {
                                _challenges.Remove(n);
                                _heap.Return(n.Value);
                            }
                            else if (potential?.Key != null && potential.Key == n.Value.Key &&
                                     n.Value.TimestampMs < potential.TimestampMs)
                            {
                                _challenges.Remove(n);
                                _heap.Return(n.Value);
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
                finally
                {
                    _matcherMutex.Release();    
                }
            }
            return false;
        }


        /// <summary>
        /// Peeks for a challenge
        /// </summary>
        /// <param name="key">They key</param>
        /// <returns>True if the challenge exists</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Peek(string key)
        {
            return _challenges.FirstOrDefault(v => v.Key == key)?.Key != null;
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
                if (await target._matcherMutex.WaitAsync().ConfigureAwait(false))
                {
                    var cur = _challenges.First;
                    while (cur != null)
                    {
                        target._challenges.AddFirst(cur.Value);
                        cur = cur.Next;
                    }
                }
            }
            catch 
            {
                //
            }
            finally
            {
                try
                {
                    target._matcherMutex.Release();
                }
                catch
                {
                    // ignored
                }
            }
        }


        /// <summary>
        /// Remove a challenge from the matcher
        /// </summary>
        /// <param name="node">The challenge to remove</param>
        /// <returns>true on success, false otherwise</returns>
        public async ValueTask<bool> RemoveAsync(LinkedListNode<IoChallenge> node)
        {
            try
            {
                if(!await _matcherMutex.WaitAsync().FastPath().ConfigureAwait(false))
                    return false;

                _challenges.Remove(node);
                _heap.Return(node.Value);

                return true;
            }
            finally
            {
                _matcherMutex.Release();
            }
        }

        /// <summary>
        /// Challenges held
        /// </summary>
        public int Count => _challenges.Count;


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
