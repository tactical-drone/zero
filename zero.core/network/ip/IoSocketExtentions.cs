using System.Net;
using System.Net.Sockets;

namespace zero.core.network.ip
{
    /// <summary>
    /// Some useful extensions when working with sockets
    /// </summary>
    public static class IoSocketExtentions
    {
        /// <summary>
        /// Returns the local address from a socket
        /// </summary>
        /// <param name="socket">The socket.</param>
        /// <returns>The local address</returns>
        public static IPAddress LocalAddress(this Socket socket)
        {
            return ((IPEndPoint)socket.LocalEndPoint).Address;
        }

        /// <summary>
        /// Returns the local port from a socket
        /// </summary>
        /// <param name="socket">The socket.</param>
        /// <returns></returns>
        public static int LocalPort(this Socket socket)
        {
            return ((IPEndPoint)socket.LocalEndPoint).Port;
        }

        /// <summary>
        /// Returns the remote address from a socket
        /// </summary>
        /// <param name="socket">The socket.</param>
        /// <returns>The remote address</returns>
        public static IPAddress RemoteAddress(this Socket socket)
        {
            return socket.ProtocolType == ProtocolType.Tcp ? ((IPEndPoint)socket.RemoteEndPoint)?.Address : null;//TODO fix null pointer on RemoteEndpoint
        }

        /// <summary>
        /// Returns the remote port from a socket
        /// </summary>
        /// <param name="endpoint">The endpoint.</param>
        /// <returns>The remote port</returns>
        public static int RemotePort(this Socket endpoint)
        {
            if (endpoint.ProtocolType == ProtocolType.Tcp)
                return ((IPEndPoint)endpoint.RemoteEndPoint)?.Port ?? 0;
            return 0;
        }
    }
}
