using System;
using System.Net;
using System.Net.Sockets;

namespace zero.core.network.ip
{
    /// <summary>
    /// Some useful extensions when working with sockets
    /// </summary>
    public static class IoSocketExtensions
    {
        /// <summary>
        /// Returns the local address from a dotNetSocket
        /// </summary>
        /// <param name="socket">The dotNetSocket.</param>
        /// <returns>The local address</returns>
        public static IPAddress LocalAddress(this Socket socket)
        {

            try
            {
                return ((IPEndPoint)socket?.LocalEndPoint)?.Address??IPAddress.Any;
            }
            catch
            {
                // ignored
            }

            return IPAddress.Any;
        }

        /// <summary>
        /// Returns the local port from a dotNetSocket
        /// </summary>
        /// <param name="socket">The dotNetSocket.</param>
        /// <returns></returns>
        public static int LocalPort(this Socket socket)
        {
            try
            {
                return ((IPEndPoint)socket?.LocalEndPoint)?.Port??0;
            }
            catch
            {
                // ignored
            }

            return 0;
        }

        /// <summary>
        /// Returns the remote address from a dotNetSocket
        /// </summary>
        /// <param name="socket">The dotNetSocket.</param>
        /// <returns>The remote address</returns>
        public static IPAddress RemoteAddress(this Socket socket)
        {
            return socket.ProtocolType == ProtocolType.Tcp ? ((IPEndPoint)socket.RemoteEndPoint)?.Address : null;//TODO fix null pointer on RemoteEndpoint
        }

        /// <summary>
        /// Returns the remote port from a dotNetSocket
        /// </summary>
        /// <param name="socket">The dotNetSocket.</param>
        /// <returns>The remote port</returns>
        public static int RemotePort(this Socket socket)
        {
            //if (dotNetSocket.ProtocolType == ProtocolType.Tcp)
            //return 0;
            return ((IPEndPoint)socket.RemoteEndPoint)?.Port ?? 0;
        }


        /// <summary>
        /// Returns remote endpoint as <see cref="IoNodeAddress"/>
        /// </summary>
        /// <param name="socket">The dotNetSocket</param>
        /// <returns>The <see cref="IoNodeAddress"/></returns>
        public static IoNodeAddress RemoteNodeAddress(this Socket socket)
        {
            if (socket.IsBound && socket.Connected)
            {
                return IoNodeAddress.CreateFromEndpoint($"{(socket.SocketType==SocketType.Dgram?"udp":"tcp")}", (IPEndPoint) socket.RemoteEndPoint);
            }

            return null;
        }
    }
}
