using System.Net;
using System.Net.Sockets;
using zero.core.conf;
using zero.core.patterns.bushings.contracts;

namespace zero.core.network.ip
{
    /// <summary>
    /// Marks the more generic <see cref="IoSocket"/> for use in our abstraction
    /// </summary>
    /// <seealso cref="zero.core.network.ip.IoSocket" />
    /// <seealso cref="IIoSource" />
    public abstract class IoNetSocket : IoSocket
    {
        /// <summary>
        /// TCP and UDP connections
        /// </summary>
        /// <param name="socketType"></param>
        /// <param name="protocolType"></param>
        /// <param name="concurrencyLevel"></param>
        protected IoNetSocket(SocketType socketType, ProtocolType protocolType, int concurrencyLevel) : base(socketType, protocolType, concurrencyLevel)
        {

        }

        /// <summary>
        /// Used by listeners
        /// </summary>
        /// <param name="nativeSocket">The socket to wrap</param>
        /// <param name="remoteEndPoint">Optional remote endpoint specification</param>
        /// <param name="concurrencyLevel"></param>
        protected IoNetSocket(Socket nativeSocket, EndPoint remoteEndPoint = null, int concurrencyLevel = 1) : base(nativeSocket, concurrencyLevel,remoteEndPoint)
        {

        }


        /// <summary>
        /// 
        /// </summary>
        [IoParameter]
        private bool parm_enable_tcp_keep_alive = false;        
        [IoParameter]
        private int parm_enable_tcp_keep_alive_time = 120;
        [IoParameter]
        private int parm_enable_tcp_keep_alive_retry_interval_sec = 2;
        [IoParameter]
        private int parm_enable_tcp_keep_alive_retry_count = 2;

        /// <summary>
        /// Configures the socket
        /// </summary>
        protected virtual void ConfigureSocket()
        {
            if (NativeSocket.IsBound || NativeSocket.Connected)
            {
                return;
            }

            // Don't allow another socket to bind to this port.
            NativeSocket.ExclusiveAddressUse = true;

            // The socket will linger for 10 seconds after
            // Socket.Close is called.
            NativeSocket.LingerState = new LingerOption(false, 0);

            // Disable the Nagle Algorithm for this tcp socket.
            NativeSocket.NoDelay = false;

            // Set the receive buffer size to 64k
            NativeSocket.ReceiveBufferSize = 8192 * 80;

            // Set the send buffer size to 64k.
            NativeSocket.SendBufferSize = 8192 * 80;

            // Set the timeout for synchronous receive methods to
            // 1 second (1000 milliseconds.)
            NativeSocket.ReceiveTimeout = 1000;

            // Set the timeout for synchronous send methods
            // to 1 second (1000 milliseconds.)
            NativeSocket.SendTimeout = 1000;

            // Set the Time To Live (TTL) to 64 router hops.
            NativeSocket.Ttl = 64;

            //var v = new uint[] {1, 1000, 2000}.SelectMany(BitConverter.GetBytes).ToArray();
            //var r = NativeSocket.IOControl(
            //    IOControlCode.KeepAliveValues,
            //    v,
            //    null
            //);

            if(NativeSocket.ProtocolType  == ProtocolType.Tcp && parm_enable_tcp_keep_alive)
            {
                NativeSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                NativeSocket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, parm_enable_tcp_keep_alive_retry_interval_sec);
                NativeSocket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, parm_enable_tcp_keep_alive_time);
                NativeSocket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, parm_enable_tcp_keep_alive_retry_count);
            }

            //_logger.Trace($"Tcp Socket configured: {Description}:" +
            //              $"  ExclusiveAddressUse {socket.ExclusiveAddressUse}" +
            //              $"  LingerState {socket.LingerState.Enabled}, {socket.LingerState.LingerTime}" +
            //              $"  NoDelay {socket.NoDelay}" +
            //              $"  ReceiveBufferSize {socket.ReceiveBufferSize}" +
            //              $"  ReceiveTimeout {socket.ReceiveTimeout}" +
            //              $"  SendBufferSize {socket.SendBufferSize}" +
            //              $"  SendTimeout {socket.SendTimeout}" +
            //              $"  Ttl {socket.Ttl}" +
            //              $"  IsBound {socket.IsBound}");
        }
    }
}
