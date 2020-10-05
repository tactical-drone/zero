using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;
using Google.Protobuf;
using NLog;
using zero.core.patterns.misc;

namespace zero.core.misc
{
    /// <summary>
    /// Matches challenge requests with a true response
    /// </summary>
    /// <typeparam name="T">The payload matched</typeparam>
    public class IoZeroMatcher<T> : IoNanoprobe
    {
        public IoZeroMatcher(string description, long ttl = 2000)
        {
            _description = description??"";
            _ttl = ttl;
        }

        /// <summary>
        /// Description
        /// </summary>
        private string _description;

        /// <summary>
        /// Holds requests
        /// </summary>
        private readonly ConcurrentDictionary<string, TemporalValue> _challenge = new ConcurrentDictionary<string, TemporalValue>();

        /// <summary>
        /// Time to live
        /// </summary>
        private readonly long _ttl;

        /// <summary>
        /// Present a challenge
        /// </summary>
        /// <param name="key">The key</param>
        /// <param name="body">The payload</param>
        /// <returns>True if successful</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Challenge(string key, T body)
        {
            var temp = new TemporalValue { Payload = body, Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()};
            if (!_challenge.TryAdd(key, temp))
            {
                if (_challenge.TryGetValue(key, out var cur))
                {
                    if (cur.Timestamp.ElapsedMs() > _ttl)
                    {
                        if (_challenge.TryRemove(key, out var dropped))
                        {
                            //LogManager.GetCurrentClassLogger().Error($"[{_description}:{_challenge.Count}] {cur.Timestamp.ElapsedMs()}ms  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                            return Challenge(key, body);
                        }
                    }
                    //LogManager.GetCurrentClassLogger().Warn($"[{_description}:{_challenge.Count}] {cur.Timestamp.ElapsedMs()}ms <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
                    return false;
                }
                else
                {
                    Thread.Sleep(0);
                    //LogManager.GetCurrentClassLogger().Fatal($"[{_description}:{_challenge.Count}] ============================================>>");
                    return Challenge(key, body);
                }
            }
            return true;
        }

        /// <summary>
        /// Present a response
        /// </summary>
        /// <param name="key">The response key</param>
        /// <returns>The response payload</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Response(string key)
        {
            if (_challenge.TryGetValue(key, out var temp))
            {
                _challenge.TryRemove(key, out _);
                return temp.Payload;
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
            return _challenge.ContainsKey(key);
        }

        /// <summary>
        /// Challenges held
        /// </summary>
        public int Count => _challenge.Count;


#if DEBUG
        /// <summary>
        /// used internally for debug
        /// </summary>
        private static readonly SHA256 Sha256 = new SHA256CryptoServiceProvider();

        public override string ToString()
        {
            try
            {
                return $"{Count}:  {string.Join(", ",_challenge.Select(kv=>$"{kv.Key}::{Sha256.ComputeHash((kv.Value.Payload as ByteString)?.ToByteArray()).HashSig()}, "))}";
            }
            catch (Exception e)
            {
                return e.Message;
            }
        }
#endif
        /// <summary>
        /// Meta payload to be matched
        /// </summary>
        public class TemporalValue
        {
            /// <summary>
            /// When the payload was challenged
            /// </summary>
            public long Timestamp;

            /// <summary>
            /// The payload
            /// </summary>
            public T Payload;
        }
    }
}
