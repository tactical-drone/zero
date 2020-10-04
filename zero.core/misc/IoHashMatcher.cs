using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using Google.Protobuf;
using zero.core.patterns.misc;

namespace zero.core.misc
{
    public class IoHashMatcher<T> : IoNanoprobe
    {
        /// <summary>
        /// Holds unrouted ping requests
        /// </summary>
        private readonly ConcurrentDictionary<string, T> _challenge = new ConcurrentDictionary<string, T>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Challenge(string key, T body)
        {
            return _challenge.TryAdd(key, body);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Response(string key)
        {
            if (_challenge.TryGetValue(key, out var value))
            {
                _challenge.TryRemove(key, out _);
                return value;
            }

            return default;
        }

        public int Count => _challenge.Count;

        private static SHA256 Sha256 = new SHA256CryptoServiceProvider();
        public override string ToString()
        {
            return $"{Count}:  {string.Join(", ",_challenge.Select(kv=>$"{kv.Key}::{Sha256.ComputeHash((kv.Value as ByteString).ToByteArray()).HashSig()}, "))}";
        }
    }
}
