using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using NLog;
#if DEBUG
using System.Security.Cryptography;
#endif

namespace zero.core.misc;

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
    ///     Casts <see cref="Memory{T}" /> to <see cref="ArraySegment{T}" />
    /// </summary>
    /// <param name="memory">The memory</param>
    /// <returns>The array segment</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte[] AsArray(this Memory<byte> memory)
    {
        return ((ReadOnlyMemory<byte>)memory).AsArray();
    }

    /// <summary>
    ///     Casts <see cref="Memory{T}" /> to <see cref="ArraySegment{T}" />
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
        catch (Exception e)
        {
            LogManager.GetCurrentClassLogger().Error(e);
            return null;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ArrayEqual<T>(this ReadOnlyMemory<T> array, ReadOnlyMemory<T> cmp)
        where T : IEquatable<T>
    {
        return array.Length == cmp.Length && array.Span.ArrayEqual(cmp.Span);
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ArrayEqual<T>(this T[] array, ReadOnlySpan<T> cmp)
        where T : IEquatable<T>
    {
        return array.Length == cmp.Length && ((ReadOnlySpan<T>)array).ArrayEqual(cmp);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ArrayEqual<T>(this T[] array, ReadOnlyMemory<T> cmp)
        where T : IEquatable<T>
    {
        return array.Length == cmp.Length && ((ReadOnlySpan<T>)array).ArrayEqual(cmp.Span);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ArrayEqual<T>(this T[] array, T[] cmp)
        where T : IEquatable<T>
    {
        return array.Length == cmp.Length && ((ReadOnlySpan<T>)array).ArrayEqual(cmp);
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
        return array.Length == cmp.Length && array.SequenceEqual(cmp);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ArrayEqual(this ReadOnlySpan<byte> array, ReadOnlySpan<byte> cmp)
    {
        return array.Length == cmp.Length && array.SequenceEqual(cmp);
    }

#if DEBUG //|| RELEASE //TODO remove release
    [ThreadStatic] private static SHA256 _sha256;
    private static SHA256 Sha256 => _sha256 ??= SHA256.Create();

    public static string PayloadSig(this byte[] payload, string T = "P")
    {
        Span<byte> hash = stackalloc byte[256];
        if (payload.Length > 0)
        {
            var read = 0;
            try
            {
                Sha256.TryComputeHash(payload, hash, out read);
            }
            catch (Exception e)
            {
                LogManager.GetCurrentClassLogger().Fatal(e, $"Compute hash failed: read = {read}/{payload.Length}");
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