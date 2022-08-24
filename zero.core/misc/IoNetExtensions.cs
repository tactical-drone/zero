using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;

namespace zero.core.misc
{
    public static class IoNetExtensions
    {
        internal const int IPv4AddressBytes = 4;
        internal const int IPv6AddressBytes = 16;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte[] AsBytes(this IPEndPoint address, byte[] buf = null)
        {
            buf ??= new byte[(address.AddressFamily == AddressFamily.InterNetworkV6 ? IPv6AddressBytes : IPv4AddressBytes) + 2];
            address.Address.TryWriteBytes(buf, out var w);
            buf[w] = (byte)(address.Port & 0x00ff);
            buf[w + 1] = (byte)(address.Port >> 8);
            return buf;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte[] AsBytes(this EndPoint address, byte[] buf = null)
        {
            return ((IPEndPoint)address).AsBytes(buf);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static IPEndPoint GetEndpoint(this byte[] buf)
        {
            return new IPEndPoint(new IPAddress(buf[..^2]), (buf[^1] << 8) | buf[^2] & 0x0FFFF);
        }
    }
}
