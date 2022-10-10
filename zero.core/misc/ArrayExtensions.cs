using System;
using System.Collections;
using System.Collections.Generic;
#if DEBUG
using System.Security.Cryptography;
#endif
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using NLog;

namespace zero.core.misc
{
    public static class ArrayExtensions
    {

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ArraySegment<byte> AsSegment(this ReadOnlyMemory<byte> memory)
        {
            try
            {
                MemoryMarshal.TryGetArray(memory, out var array);
                return array;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Casts <see cref="Memory{T}"/> to <see cref="ArraySegment{T}"/>
        /// </summary>
        /// <param name="memory">The memory</param>
        /// <returns>The array segment</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte[] AsArray(this Memory<byte> memory)
        {
            return ((ReadOnlyMemory<byte>)memory).AsArray();
        }

        /// <summary>
        /// Casts <see cref="Memory{T}"/> to <see cref="ArraySegment{T}"/>
        /// </summary>
        /// <param name="memory">The memory</param>
        /// <returns>The array segment</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte[] AsArray(this ReadOnlyMemory<byte> memory)
        {
            try
            {
                MemoryMarshal.TryGetArray(memory, out var array);
                return array.Array;
            }
            catch(Exception e)
            {
                LogManager.GetCurrentClassLogger().Error(e);
                return null;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ArrayEqual<T>(this ReadOnlyMemory<T> array, ReadOnlyMemory<T> cmp)
            where T : IEquatable<T>
        {
            return array.Span.ArrayEqual(cmp.Span);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ArrayEqual<T>(this T[] array, ReadOnlySpan<T> cmp)
            where T : IEquatable<T>
        {
            return ((ReadOnlySpan<T>)array).ArrayEqual(cmp);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ArrayEqual<T>(this T[] array, ReadOnlyMemory<T> cmp)
            where T : IEquatable<T>
        {
            return ((ReadOnlySpan<T>)array).ArrayEqual(cmp.Span);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ArrayEqual<T>(this T[] array, T[] cmp)
            where T : IEquatable<T>
        {
            return ((ReadOnlySpan<T>)array).ArrayEqual(cmp);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ArrayEqual(this byte[] array, byte[] cmp)
        {
            return ((IStructuralEquatable)array).Equals(cmp, EqualityComparer<byte>.Default);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ArrayEqual<T>(this ReadOnlySpan<T> array, ReadOnlySpan<T> cmp)
            where T : IEquatable<T>
        {
            return array.SequenceEqual(cmp);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ArrayEqual(this ReadOnlySpan<byte> array, ReadOnlySpan<byte> cmp)
        {
            return array.SequenceEqual(cmp);
        }


        /// <summary>
        /// Create a hash from an array of bytes
        /// </summary>
        /// <param name="array"></param>
        /// <returns>A weak hash</returns>
        public static long ZeroHash(this byte[] array)
        {
            return ((ReadOnlySpan<byte>)array).ZeroHash();
        }

        /// <summary>
        /// Create a hash from an array of bytes
        /// </summary>
        /// <param name="array"></param>
        /// <returns>A weak hash</returns>
        public static long ZeroHash(this Span<byte> array)
        {
            return ((ReadOnlySpan<byte>)array).ZeroHash();
        }

        /// <summary>
        /// Create a hash from an array of bytes
        /// </summary>
        /// <param name="array"></param>
        /// <returns>A weak hash</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ZeroHash(this ReadOnlySpan<byte> array)
        {
            var strides = array.Length / sizeof(long);
            var remainder = array.Length % sizeof(long);
            var hash = 0xaaaaaaaaaaaaaaa;

            for (var i = 0; i < strides; i++)
            {
                var stride = MemoryMarshal.Read<long>(array[(i * sizeof(long))..]);
                hash ^= stride ^ ((stride >> 31) | (stride << 32)) ^ i;
            }

            if (remainder >= sizeof(int))
            {
                var start = strides * 2;
                strides = start + remainder / sizeof(int);
                remainder = array.Length % sizeof(int);
                for (var i = start; i < strides; i++)
                {
                    var stride = MemoryMarshal.Read<int>(array[(i * sizeof(int))..]);
                    hash ^= stride ^ ((stride >> 15) | (stride << 16)) ^ i;
                }
            }

            for (var i = 0; i < remainder; i++)
            {
                var stride = array[i];
                hash ^= (stride << (sizeof(int) - ((i + 2) >> 1))) ^ ((stride >> 3) | (stride << 4));
            }

            return hash;
        }

#if DEBUG //|| RELEASE //TODO remove release
        [ThreadStatic]
        private static SHA256 _sha256;
        public static SHA256 Sha256 => _sha256 ??= SHA256.Create();

        public static string PayloadSig(this byte[] payload, string T = "P")
        {
            Span<byte> hash = stackalloc byte[256];
            if (payload.Length > 0)
            {
                int read = 0;
                try
                {
                    Sha256.TryComputeHash(payload, hash, out read);
                }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Fatal(e,$"Compute hash failed: read = {read}/{payload.Length}");
                    return $"{T}(0x000)";
                }
            }
            else
            {
                return $"{T}(null)";
            }
            return $"{T}({Convert.ToBase64String(hash)[..7]})";
        }
        public static string PayloadSig(this ReadOnlyMemory<byte> memory, string tag = "P")
        {
            return memory.AsArray().PayloadSig(tag);
        }

        public static string PayloadSig(this ReadOnlySpan<byte> span, string tag = "P")
        {
            return span.ToArray().PayloadSig(tag);
        }

        public static string HashSig(this byte[] hash)
        {
            return $"H({Convert.ToBase64String(hash).Substring(0, 5)})";
        }

        public static string HashSig(this Span<byte> hash)
        {
            return $"H({Convert.ToBase64String(hash).Substring(0, 5)})";
        }

        public static string HashSig(this ReadOnlyMemory<byte> memory)
        {
            return memory.AsArray().HashSig();
        }

        public static string Print(this ReadOnlyMemory<byte> memory, string T = "F")
        {
            return $"{T}{BitConverter.ToString(memory.AsArray())}";
        }

        public static string Print(this byte[] memory, string T = "F")
        {
            return $"{T}{BitConverter.ToString(memory)}";
        }
#else
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string PayloadSig(this byte[] payload, string tag = "P")
        {
            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string PayloadSig(this ReadOnlyMemory<byte> memory, string tag = "P")
        {
            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string HashSig(this byte[] hash)
        {
            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string HashSig(this Span<byte> hash)
        {
            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string HashSig(this ReadOnlyMemory<byte> memory)
        {
            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string PayloadSig(this ReadOnlySpan<byte> span, string tag = "P")
        {
            return null;
        }

        public static string Print(this byte[] memory, string T = "F-")
        {
            return null;
        }
#endif
    }
}
