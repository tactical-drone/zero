using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.misc;

namespace zero.core.network.ip
{
    /// <summary>
    /// The UDP flavor of <see cref="IoSocket"/>
    /// </summary>
    class IoUdpSocket : IoNetSocket
    {
        /// <inheritdoc />
        /// <summary>
        /// Constructs the UDP socket
        /// </summary>
        /// <param name="cancellationToken">Signals cancellation</param>
        public IoUdpSocket(CancellationToken cancellationToken) : base(SocketType.Dgram, ProtocolType.Udp, cancellationToken)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <inheritdoc />
        /// <summary>
        /// A copy constructor
        /// </summary>
        /// <param name="socket">The underlying socket</param>
        /// <param name="address">The address listened on</param>
        /// <param name="cancellationToken">Token used for cancellation</param>
        public IoUdpSocket(Socket socket, IoNodeAddress address, CancellationToken cancellationToken) : base(socket, address, cancellationToken)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Used to retrieve the sender information of a UDP packet and prevent mallocs for each call to receive
        /// </summary>
        private EndPoint _udpRemoteEndpointInfo;

        /// <inheritdoc />
        /// <summary>
        /// Listen for UDP traffic
        /// </summary>
        /// <param name="address">The address to listen on</param>
        /// <param name="callback">The handler once a connection is made, mostly used in UDPs case to look function like <see cref="T:zero.core.network.ip.IoTcpSocket" /></param>
        /// <returns></returns>
        public override async Task<bool> ListenAsync(IoNodeAddress address, Action<IoSocket> callback)
        {
            if (!await base.ListenAsync(address, callback))
                return false;

            try
            {
                //Call the new connection established handler
                callback(this);

                // Prepare UDP connection orientated things                
                _udpRemoteEndpointInfo = new IPEndPoint(IPAddress.Any, 0);
            }
            catch (Exception e)
            {
                _logger.Error(e, $"There was an error handling new connection from `udp://{RemoteAddress}' to `udp://{LocalIpAndPort}'");
                return false;
            }

            return true;
        }

        /// <inheritdoc />
        /// <summary>
        /// Send UDP packet
        /// </summary>
        /// <param name="getBytes">The buffer containing the data</param>
        /// <param name="offset">Start offset into the buffer</param>
        /// <param name="length">The length of the data</param>
        /// <returns></returns>
        public override async Task<int> SendAsync(byte[] getBytes, int offset, int length)
        {
            //TODO HACKS! Remove
            if (!IsConnectingSocket)
                return 0;
            await Socket.SendToAsync(getBytes, SocketFlags.None, Address.IpEndPoint).ContinueWith(
                t =>
                {
                    switch (t.Status)
                    {
                        case TaskStatus.Canceled:
                        case TaskStatus.Faulted:
                            _logger.Error(t.Exception, $"Sending to udp://{RemoteIpAndPort} failed");
                            Close();
                            break;
                        case TaskStatus.RanToCompletion:
                            _logger.Trace($"Sent {length} bytes to udp://{RemoteIpAndPort}");
                            break;
                    }
                }, Spinners.Token);
            return length;
        }

        /// <inheritdoc />
        /// <summary>
        /// Read UDP packet data
        /// </summary>
        /// <returns>The number of bytes read</returns>
        public override async Task<int> ReadAsync(byte[] buffer, int offset, int length)
        {
            //TODO HACKS! Remove
            if (!IsListeningSocket)
                return 0;
            try
            {
                return await Task.Factory.FromAsync(Socket.BeginReceiveFrom(buffer, offset, length, SocketFlags.None, ref _udpRemoteEndpointInfo, null, null),
                    Socket.EndReceive).HandleCancellation(Spinners.Token);
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unable to read from socket `udp://{LocalIpAndPort}':");
                return 0;
            }
        }

        /// <inheritdoc />
        /// <summary>
        /// Connection status
        /// </summary>
        /// <returns>True if the connection is up, false otherwise</returns>
        public override bool Connected()
        {
            return Address.IpEndPoint != null || _udpRemoteEndpointInfo != null;
        }
    }
}
