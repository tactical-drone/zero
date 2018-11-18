using System.Net;
using System.Net.Sockets;

namespace zero.core.network.ip
{
    public static class IoSocketExtentions
    {
        public static IPAddress LocalAddress(this Socket socket)
        {
            return ((IPEndPoint) socket.LocalEndPoint).Address;
        }

        public static int LocalPort(this Socket endpoint)
        {
            return ((IPEndPoint)endpoint.LocalEndPoint).Port;
        }

        public static IPAddress RemoteAddress(this Socket socket)
        {
            return socket.ProtocolType == ProtocolType.Tcp ? ((IPEndPoint) socket.RemoteEndPoint)?.Address : null;//TODO fix nullpointer on RemoteEndpoint
        }

        public static int RemotePort(this Socket endpoint)
        {
            if( endpoint.ProtocolType == ProtocolType.Tcp )
                return ((IPEndPoint) endpoint.RemoteEndPoint)?.Port ?? 0;
            return 0;
        }
    }
}
