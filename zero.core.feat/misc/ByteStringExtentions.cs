using System;
using System.Buffers;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Serialization.Formatters.Binary;
using Google.Protobuf;
using zero.core.misc;
using zero.core.patterns.heap;

namespace zero.core.feat.misc
{
    public static class ProtoBufExtensions
    {
        internal const int IPv4AddressBytes = 4;
        internal const int IPv6AddressBytes = 16;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string AsString(this ByteString buffer)
        {
            return BitConverter.ToString(buffer.Memory.AsArray());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteString AsByteString(this IPEndPoint address)
        {
            var buf = new byte[(address.AddressFamily == AddressFamily.InterNetworkV6? IPv6AddressBytes : IPv4AddressBytes) + 2];
            address.Address.TryWriteBytes(buf, out var w);
            buf[w] = (byte)((address.Port >> 8) & 0x00FF);
            buf[w+1] = (byte)(address.Port & 0x00FF);
            return UnsafeByteOperations.UnsafeWrap(buf);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte[] AsBytes(this IPEndPoint address, byte[] buf = null)
        {
            buf ??= new byte[(address.AddressFamily == AddressFamily.InterNetworkV6 ? IPv6AddressBytes : IPv4AddressBytes) + 2];
            address.Address.TryWriteBytes(buf, out var w);
            buf[w] = (byte)((address.Port >> 8) & 0x00FF);
            buf[w + 1] = (byte)(address.Port & 0x00FF);
            return buf;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static IPEndPoint GetEndpoint(this ByteString address)
        {
            var buf = address.Memory.AsArray(); 
            return new IPEndPoint(new IPAddress(buf[..^2]), ((buf[^2] << 8) & 0xFF00) | buf[^1]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static IPEndPoint GetEndpoint(this byte[] buf)
        {
            return new IPEndPoint(new IPAddress(buf[..^2]), ((buf[^2] << 8) & 0xFF00) | buf[^1]);
        }
    }
    internal sealed class Heap
    {
        public static volatile IoHeap<BinaryFormatter> FormatterHeap;
        public static volatile IoHeap<MemoryStream> StreamHeap;
        static Heap()
        {
            FormatterHeap = new IoHeap<BinaryFormatter>($"{nameof(FormatterHeap)}", 20, autoScale: true)
            {
                Make = (o, o1) => new BinaryFormatter()
            };

            var ep = new IPEndPoint(IPAddress.Broadcast, short.MaxValue);

            var bin = new BinaryFormatter();
            var mem = new MemoryStream();
            var n = new IoSocketAddress(AddressFamily.InterNetwork, 2);
            bin.Serialize(mem, n);
            var template = mem.ToArray();

            StreamHeap = new IoHeap<MemoryStream>($"{nameof(StreamHeap)}", 20, autoScale: true)
            {
                Make = (o, o1) => new MemoryStream(new byte[template.Length]),
                Prep = (stream, o) => { stream.Seek(0, SeekOrigin.Begin); }
            };
        }
    }
}
