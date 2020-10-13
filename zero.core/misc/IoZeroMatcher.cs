using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.Arm;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.AspNetCore.Razor.TagHelpers;
using NLog;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore;
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
        public IoZeroMatcher(string description, int concurrencyLevel, long ttlMs = 2000, int capacity = 64) : base()
        {
            _capacity = capacity;
            _description = description??"";
            _ttlMs = ttlMs;
            _matcherMutex = new IoZeroSemaphore(nameof(_matcherMutex), concurrencyLevel, 1);
            _matcherMutex.ZeroRef(ref _matcherMutex, AsyncTasks.Token);
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
        //private readonly ConcurrentDictionary<string, System.Collections.Generic.List<TemporalValue>> _challenge = new ConcurrentDictionary<string, System.Collections.Generic.List<TemporalValue>>();
        List<TemporalValue> _challenge = new List<TemporalValue>();

        /// <summary>
        /// Time to live
        /// </summary>
        private readonly long _ttlMs;

        /// <summary>
        /// Present a challenge
        /// </summary>
        /// <param name="key">The key</param>
        /// <param name="body">The payload</param>
        /// <param name="bump">bump the current challenge</param>
        /// <returns>True if successful</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<bool> ChallengeAsync(string key, T body, bool bump = true)
        {
            var temp = new TemporalValue { Key = key, Payload = body, TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()};

            try
            {
                if (await _matcherMutex.WaitAsync().ZeroBoostAsync().ConfigureAwait(false))
                {
                    _challenge.Add(temp);    
                }
                else
                {
                    return false;
                }
            }
            finally
            {
                if (_challenge.Count > _capacity)
                {
                    _challenge = _challenge.Where(c => !c.Collected).ToList();
                }
                _matcherMutex.Release();
            }

            return true;
        }

        /// <summary>
        /// Used internally
        /// </summary>
        private readonly SHA256 _sha256 = SHA256.Create();
        
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
        public async ValueTask<TemporalValue> ResponseAsync(string key, ByteString reqHash)
        {
            TemporalValue response = default;
            try
            {
                var hashMatch = MemoryMarshal.Read<long>(reqHash.Memory.AsArray());
                if (await _matcherMutex.WaitAsync().ZeroBoostAsync().ConfigureAwait(false))
                {
                    response = _challenge.FirstOrDefault(v => !v.Collected && !v.Scanned && v.Key == key);
                    while (response?.Key != null)
                    {

                        if (response.Hash == 0)
                        {
                            var hash = _sha256.ComputeHash((response.Payload as ByteString)?.Memory.AsArray() ?? Array.Empty<byte>());
                            response.Payload = default;
                            response.Hash = MemoryMarshal.Read<long>(hash);
                        }

                        if (response.Hash == hashMatch)
                        {
                            response.Collected = true;
                            return response;
                        }

                        response.Scanned = true;
                        response = _challenge.FirstOrDefault(v => !v.Collected && !v.Scanned && v.Key == key);
                    }
                }
                else
                {
                    return default;
                }
            }
            finally
            {
                foreach (var temporalValue in _challenge)
                {
                    if (temporalValue.TimestampMs.ElapsedMs() > _ttlMs)
                        temporalValue.Collected = true;
                    else if (response?.Key != null && response.Key == temporalValue.Key && temporalValue.TimestampMs < response.TimestampMs)
                         temporalValue.Collected = true;
                    
                    temporalValue.Scanned = false;
                }
                
                _matcherMutex.Release();
            }
            return default;
        }


        /// <summary>
        /// Peeks for a challenge
        /// </summary>
        /// <param name="key">They key</param>
        /// <returns>True if the challenge exists</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Peek(string key)
        {
            return _challenge.FirstOrDefault(v => !v.Collected == false && v.Key == key)?.Key != null;
        }

        /// <summary>
        /// Dump challenges into target matcher
        /// </summary>
        /// <param name="target"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask DumpAsync(IoZeroMatcher<T> target)
        {
            try
            {
                if (await target._matcherMutex.WaitAsync().ZeroBoostAsync().ConfigureAwait(false))
                {
                    target._challenge.AddRange(_challenge);
                }
            }
            finally
            {
                target._matcherMutex.Release();
            }
        }

        /// <summary>
        /// Challenges held
        /// </summary>
        public int Count => _challenge.Count;


#if DEBUG
        /// <summary>
        /// used internally for debug
        /// </summary>
        //private static readonly SHA256 Sha256 = new SHA256CryptoServiceProvider();

        //public override string ToString()
        //{
        //    try
        //    {
        //        return $"{Count}:  {string.Join(", ",_challenge.Select(kv=>$"{kv.Key}::{Sha256.ComputeHash((kv.Payload as ByteString)?.ToByteArray()).HashSig()}, "))}";
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
        public class TemporalValue
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

            /// <summary>
            /// Used internally
            /// </summary>
            public bool Scanned;

            /// <summary>
            /// if this instance has been collected
            /// </summary>
            public bool Collected;
        }
    }
}
