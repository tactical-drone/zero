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
            _udpRemoteEndpointInfo = new IPEndPoint(IPAddress.Any, LocalPort);
        }

        /// <inheritdoc />
        /// <summary>
        /// A copy constructor
        /// </summary>
        /// <param name="socket">The underlying socket</param>
        /// <param name="listenerAddress">The address listened on</param>
        /// <param name="cancellationToken">Token used for cancellation</param>
        public IoUdpSocket(Socket socket, IoNodeAddress listenerAddress, CancellationToken cancellationToken) : base(socket, listenerAddress, cancellationToken)
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

        //public override IoNodeAddress RemoteAddress { get; protected set; }

        public override string Key => RemoteAddress?.IpAndPort ?? LocalIpAndPort;

        //public override string Key
        //{
        //    get
        //    {
        //        if (RemoteAddress == null)
        //        {
        //            Console.WriteLine("still null");
        //            return LocalIpAndPort;
        //        }


        //        return RemoteAddress.IpAndPort;
        //    }
        //}

        public const int SIO_UDP_CONNRESET = -1744830452;

        /// <inheritdoc />
        /// <summary>
        /// Listen for UDP traffic
        /// </summary>
        /// <param name="address">The address to listen on</param>
        /// <param name="callback">The handler once a connection is made, mostly used in UDPs case to look function like <see cref="T:zero.core.network.ip.IoTcpSocket" /></param>
        /// <returns></returns>
        public override async Task<bool> ListenAsync(IoNodeAddress address, Action<IoSocket> callback)
        {
            
            Socket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.PacketInformation, true);
            Socket.IOControl(
                (IOControlCode)SIO_UDP_CONNRESET,
                new byte[] { 0, 0, 0, 0 },
                null
            );

            if (!await base.ListenAsync(address, callback))
                return false;

            try
            {
                //Call the new connection established handler
                callback(this);

                // Prepare UDP connection orientated things                
                _udpRemoteEndpointInfo = new IPEndPoint(IPAddress.Any, LocalPort);
            }
            catch (Exception e)
            {
                _logger.Error(e, $"There was an error handling new connection from `udp://{RemoteAddressFallback}' to `udp://{LocalIpAndPort}'");
                return false;
            }

            return true;
        }

        /// <inheritdoc />
        /// <summary>
        /// Send UDP packet
        /// </summary>
        /// <param name="buffer">The buffer containing the data</param>
        /// <param name="offset">Start offset into the buffer</param>
        /// <param name="length">The length of the data</param>
        /// <returns></returns>
        public override async Task<int> SendAsync(byte[] buffer, int offset, int length, EndPoint endPoint = null)
        {
            //TODO HACKS! Remove
            //if (!IsConnectingSocket)
            //    return 0;
            if (endPoint == null)
            {
                _logger.Fatal("No endpoint supplied!");
                return 0;
            }
            
            await Socket.SendToAsync(buffer, SocketFlags.None, (EndPoint)endPoint).ContinueWith(
                t =>
                {
                    switch (t.Status)
                    {
                        case TaskStatus.Canceled:
                        case TaskStatus.Faulted:
                            _logger.Error(t.Exception, $"Sending to udp://{endPoint} failed");
                            Close();
                            break;
                        case TaskStatus.RanToCompletion:
                            _logger.Trace($"Sent {length} bytes to udp://{endPoint} from {Socket.LocalPort()}");
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
            try
            {
                if (Socket?.IsBound??false)
                {
                    var readAsync = await Task.Factory.FromAsync(Socket.BeginReceiveFrom(buffer, offset, length, SocketFlags.None, ref _udpRemoteEndpointInfo, null, null),
                        result =>
                        {
                            try
                            {
                                if(Socket!= null && Socket.IsBound)
                                    return Socket.EndReceiveFrom(result, ref _udpRemoteEndpointInfo);
                                else
                                    return 0;
                            }
                            catch (Exception e)
                            {
                                _logger.Error(e, $"Unable to read from {Socket.LocalEndPoint}");
                                Close();
                                return 0;
                            }
                        }).HandleCancellation(Spinners.Token);

                    if (RemoteAddress == null)
                        RemoteAddress = IoNodeAddress.CreateFromEndpoint("udp", _udpRemoteEndpointInfo);
                    else
                        RemoteAddress.Update($"udp://{_udpRemoteEndpointInfo}");

                    return readAsync;
                }
                else
                {
                    await Task.Delay(1000);//TODO
                    return 0;
                }
                    
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
        public override bool IsConnected()
        {
            return ListenerAddress?.IpEndPoint != null || _udpRemoteEndpointInfo != null || RemoteAddress != null;
        }

        public override object ExtraData()
        {
            return _udpRemoteEndpointInfo;
        }
    }
}
