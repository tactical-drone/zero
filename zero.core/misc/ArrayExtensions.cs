using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
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
            catch(Exception e)
            {
                LogManager.GetCurrentClassLogger().Error(e);
                return null;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ArrayEqual<T>(this ReadOnlyMemory<T> array, ReadOnlyMemory<T> cmp)
        {
            for (int i = 0; i < array.Length; i++)
            {
                if (!array.Span[i].Equals(cmp.Span[i]))
                    return false;
            }

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ArrayEqual<T>(this T[] array, ReadOnlyMemory<T> cmp)
        {
            for (int i = 0; i < array.Length; i++)
            {
                if (!array[i].Equals(cmp.Span[i]))
                    return false;
            }

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ArrayEqual<T>(this T[] array, T[] cmp)
        {
            for (int i = 0; i < array.Length; i++)
            {
                if (!array[i].Equals(cmp[i]))
                    return false;
            }

            return true;
        }

#if DEBUG //|| RELEASE //TODO remove release
        [ThreadStatic]
        private static SHA256 _sha256;
        public static SHA256 Sha256 => _sha256 ??= SHA256.Create();

        public static string PayloadSig(this byte[] payload)
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
                    return $"P(0x000)";
                }
            }
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
