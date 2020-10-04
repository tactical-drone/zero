using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Microsoft.AspNetCore.Authentication;

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
        /// Casts <see cref="Memory{T}" to <see cref="ArraySegment{T}"/>/>
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
            catch
            {
                return null;
            }
        }

        public static SHA256 Sha256 = new SHA256CryptoServiceProvider();
        public static string PayloadSig(this byte[] payload)
        {
            return $"P({Convert.ToBase64String(Sha256.ComputeHash(payload)).Substring(0,5)})";
        }
#if DEBUG
        public static string PayloadSig(this ReadOnlyMemory<byte> memory)
        {
            return memory.AsArray().PayloadSig();
        }

        public static string HashSig(this byte[] hash)
        {
            return $"H({Convert.ToBase64String(hash).Substring(0, 5)})";
        }

        public static string HashSig(this ReadOnlyMemory<byte> memory)
        {
            return memory.AsArray().HashSig();
        }
#else
        public static string PayloadSig(this ReadOnlyMemory<byte> memory)
        {
            return "";
        }

        public static string HashSig(this byte[] hash)
        {
            return $"";
        }

        public static string HashSig(this ReadOnlyMemory<byte> memory)
        {
            return "";
        }
#endif

    }
}
