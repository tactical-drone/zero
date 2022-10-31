using System;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using Google.Protobuf;
using zero.core.misc;

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
        public static ByteString ToByteString(this IPEndPoint address)
        {
            var buf = new byte[(address.AddressFamily == AddressFamily.InterNetworkV6? IPv6AddressBytes : IPv4AddressBytes) + 2];
            address.Address.TryWriteBytes(buf, out var w);
            buf[w] = (byte)(address.Port & 0x00ff);
            buf[w+1] = (byte)(address.Port >> 8);
            return UnsafeByteOperations.UnsafeWrap(buf);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]  
        public static IPEndPoint GetEndpoint(this ByteString address)
        {
            if (address.Length == 0)
                return null;

            var buf = address.Memory.AsArray(); 
            return new IPEndPoint(new IPAddress(buf[..^2]), (buf[^1] << 8 | buf[^2]) & 0x0FFFF);
        }
    }
}
