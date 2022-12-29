using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsLocalIp(this string ip)
        {
            var address = ip.Split(":")[0];
            //var m = Regex.Match(address, @"^(?!^0\.)(?!^10\.)(?!^100\.6[4-9]\.)(?!^100\.[7-9]\d\.)(?!^100\.1[0-1]\d\.)(?!^100\.12[0-7]\.)(?!^127\.)(?!^169\.254\.)(?!^172\.1[6-9]\.)(?!^172\.2[0-9]\.)(?!^172\.3[0-1]\.)(?!^192\.0\.0\.)(?!^192\.0\.2\.)(?!^192\.88\.99\.)(?!^192\.168\.)(?!^198\.1[8-9]\.)(?!^198\.51\.100\.)(?!^203.0\.113\.)(?!^22[4-9]\.)(?!^23[0-9]\.)(?!^24[0-9]\.)(?!^25[0-5]\.)(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))$");
            var m = Regex.Match(address, @"^(172\.(1[6-9]\.|2[0-9]\.|3[0-1]\.)|192\.168\.|10\.|127\.)");
            return m.Success;
        }
    }
}
