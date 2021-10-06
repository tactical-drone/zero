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
                Console.WriteLine("AsArray failed!");
                return null;
            }
        }



#if DEBUG //|| RELEASE //TODO remove release
        private static readonly SHA256 Sha256 = SHA256.Create();

        public static string PayloadSig(this byte[] payload)
        {
            Span<byte> hash = stackalloc byte[256];
            if(payload.Length > 0)
                Sha256.TryComputeHash(payload, hash, out var _);
            return $"P({Convert.ToBase64String(hash)[..5]})";
        }
        public static string PayloadSig(this ReadOnlyMemory<byte> memory)
        {
            return memory.AsArray().PayloadSig();
        }

        public static string PayloadSig(this ReadOnlySpan<byte> span)
        {
            return span.ToArray().PayloadSig();
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
        public static string PayloadSig(this byte[] payload)
        {
            return null;
        }

        public static string PayloadSig(this ReadOnlyMemory<byte> memory)
        {
            return null;
        }

        public static string HashSig(this byte[] hash)
        {
            return null;
        }

        public static string HashSig(this ReadOnlyMemory<byte> memory)
        {
            return null;
        }

         public static string PayloadSig(this ReadOnlySpan<byte> span)
        {
            return null;
        }
#endif

    }
}
