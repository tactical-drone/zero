using System.Net.Sockets;
using System.Threading;
using zero.core.data.contracts;
using zero.core.patterns.bushes.contracts;

namespace zero.core.network.ip
{
    /// <summary>
    /// Marks the more generic <see cref="IoSocket"/> for use in our abstraction
    /// </summary>
    /// <seealso cref="zero.core.network.ip.IoSocket" />
    /// <seealso cref="IIoSource" />
    public abstract class IoNetSocket : IoSocket, IIoSourceBase
    {
        protected IoNetSocket(SocketType socketType, ProtocolType protocolType) : base(socketType, protocolType)
        {
            
        }

        protected IoNetSocket(Socket socket, IoNodeAddress listeningAddress) : base(socket, listeningAddress)
        {

        }

        /// <summary>
        /// Description
        /// </summary>
        private string _description;

        /// <summary>
        /// Description
        /// </summary>
        public override string Description
        {
            get
            {
                if (_description == null && (ListeningAddress != null || LocalEndPoint != null))
                {
                    if (Kind == Connection.Listener)
                        return _description = $"`listener: {ListeningAddress}'";
                    else
                        return _description = $"`socket: {Key}'";
                }
                
                return _description;
            }
        }

        /// <summary>
        /// Configures the socket
        /// </summary>
        /// <param name="Socket"></param>
        protected virtual void Configure()
        {
            if (Socket.IsBound || Socket.Connected)
            {
                return;
            }

            // Don't allow another socket to bind to this port.
            Socket.ExclusiveAddressUse = true;

            // The socket will linger for 10 seconds after
            // Socket.Close is called.
            Socket.LingerState = new LingerOption(true, 10);

            // Disable the Nagle Algorithm for this tcp socket.
            Socket.NoDelay = false;

            // Set the receive buffer size to 32k
            Socket.ReceiveBufferSize = 8192 * 4;

            // Set the timeout for synchronous receive methods to
            // 1 second (1000 milliseconds.)
            Socket.ReceiveTimeout = 10000;

            // Set the send buffer size to 8k.
            Socket.SendBufferSize = 8192 * 2;

            // Set the timeout for synchronous send methods
            // to 1 second (1000 milliseconds.)
            Socket.SendTimeout = 1000;

            // Set the Time To Live (TTL) to 42 router hops.
            Socket.Ttl = 42;

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
