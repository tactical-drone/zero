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
    sealed class IoUdpSocket : IoNetSocket
    {
        /// <inheritdoc />
        /// <summary>
        /// Constructs the UDP socket
        /// </summary>
        public IoUdpSocket() : base(SocketType.Dgram, ProtocolType.Udp)
        {
            _logger = LogManager.GetCurrentClassLogger();
            _udpRemoteEndpointInfo = new IPEndPoint(IPAddress.Any, 99);
        }

        /// <inheritdoc />
        /// <summary>
        /// A copy constructor
        /// </summary>
        /// <param name="socket">The underlying socket</param>
        /// <param name="listeningAddress">The address listened on</param>
        public IoUdpSocket(Socket socket, IoNodeAddress listeningAddress) : base(socket, listeningAddress)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _udpRemoteEndpointInfo = null;
            RemoteNodeAddress = null;
#endif

        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override Task ZeroManagedAsync()
        {
            return base.ZeroManagedAsync();
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

        public override string Key => RemoteAddress?.IpPort ?? LocalIpAndPort;

        //public override string Key
        //{
        //    get
        //    {
        //        if (RemoteAddress == null)
        //        {
        //            Console.WriteLine("still null");
        //            return LocalIpAndPort;
        //        }


        //        return RemoteAddress.IpPort;
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

            //TODO sec
            Socket.IOControl(
                (IOControlCode)SIO_UDP_CONNRESET,
                new byte[] { 0, 0, 0, 0 },
                null
            );

            if (!await base.ListenAsync(address, callback).ConfigureAwait(false))
                return false;

            try
            {
                //Call the new connection established handler
                callback(this);

                // Prepare UDP connection orientated things                
                _udpRemoteEndpointInfo = new IPEndPoint(IPAddress.Any, 88);

                _logger.Debug($"Started listener at {ListeningAddress}");
                while (!Zeroed())
                {
                    await Task.Delay(5000, AsyncTasks.Token).ConfigureAwait(false);
                    if (!Socket?.IsBound ?? false)
                    {
                        _logger.Warn($"Found zombie udp socket state");

                        ZeroAsync(this);

                    }
                }
                _logger.Debug($"Stopped listening at {ListeningAddress}");
            }
            catch (NullReferenceException) {}
            catch (TaskCanceledException) {}
            catch (OperationCanceledException) {}
            catch (ObjectDisposedException) {}
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
        /// <param name="endPoint">A destination, used for UDP connections</param>
        /// <param name="timeout">Send timeout</param>
        /// <returns></returns>
        public override async ValueTask<int> SendAsync(ArraySegment<byte> buffer, int offset, int length, EndPoint endPoint = null, int timeout = 0)
        {
            if (Zeroed())
            {
                await Task.Delay(1000, AsyncTasks.Token).ConfigureAwait(false);
                return 0;
            }
            
            if (endPoint == null)
            {
                _logger.Fatal("No endpoint supplied!");
                return 0;
            }
            
            await Socket.SendToAsync(buffer, SocketFlags.None, endPoint).ContinueWith(
                t =>
                {
                    switch (t.Status)
                    {
                        case TaskStatus.Canceled:
                        case TaskStatus.Faulted:
                            _logger.Trace(t.Exception?.InnerException, $"Sending to udp://{endPoint} failed");

                            ZeroAsync(this);

                            break;
                        case TaskStatus.RanToCompletion:
                            _logger.Trace($"Sent {length} bytes to udp://{endPoint} from {Socket.LocalPort()}");
                            break;
                    }
                }, AsyncTasks.Token).ConfigureAwait(false);
            return length;
        }


        private EndPoint _dummyEndPoint = new IPEndPoint(IPAddress.Any, 0);
        /// <summary>
        /// Read from the socket
        /// </summary>
        /// <param name="buffer">Read into a buffer</param>
        /// <param name="offset">Write start pos</param>
        /// <param name="length">Bytes to read</param>
        /// <param name="timeout">Timeout after ms</param>
        /// <returns></returns>
        public override async ValueTask<int> ReadAsync(ArraySegment<byte> buffer, int offset, int length, int timeout = 0)
        {
            try
            {

                if (!(Socket?.IsBound ?? false) || Zeroed())
                {
                    await Task.Delay(1000, AsyncTasks.Token).ConfigureAwait(false);
                    return 0;
                }

                Socket.ReceiveTimeout = timeout;
                var read = 0;
                if (timeout == 0)
                {
                    
                    var readResult = await Socket.ReceiveFromAsync(buffer.Slice(offset, length), SocketFlags.None, _udpRemoteEndpointInfo);
                    _udpRemoteEndpointInfo = readResult.RemoteEndPoint;
                    read = readResult.ReceivedBytes;

                    //Set the remote address
                    if (RemoteNodeAddress == null)
                        RemoteNodeAddress = IoNodeAddress.CreateFromEndpoint("udp", _udpRemoteEndpointInfo);
                    else
                        RemoteAddress.Update((IPEndPoint)_udpRemoteEndpointInfo);

                    //read = await Task.Factory.FromAsync(
                    //    Socket.BeginReceiveFrom(buffer, offset, length, SocketFlags.None, ref _udpRemoteEndpointInfo, null, null),
                    //    result =>
                    //    {
                    //        try
                    //        {
                    //            if (Zeroed())
                    //                return 0;

                    //            if (Socket != null && Socket.IsBound && result.IsCompleted)
                    //            {
                    //                var r = Socket.EndReceiveFrom(result, ref _udpRemoteEndpointInfo);

                    //                //Set the remote address
                    //                if (RemoteNodeAddress == null)
                    //                {
                    //                    RemoteNodeAddress =
                    //                        IoNodeAddress.CreateFromEndpoint("udp", _udpRemoteEndpointInfo);
                    //                    //_udpRemoteEndpointInfo = Socket.RemoteEndPoint;
                    //                    //RemoteNodeAddress = IoNodeAddress.CreateFromEndpoint("udp", Socket.RemoteEndPoint);
                    //                }
                    //                else
                    //                    RemoteAddress.Update((IPEndPoint) _udpRemoteEndpointInfo);
                    //                //RemoteAddress.Update((IPEndPoint)Socket.RemoteEndPoint);

                    //                return r;
                    //            }


                    //            return 0;
                    //        }
                    //        catch (NullReferenceException) { return 0; }
                    //        catch (TaskCanceledException) { return 0; }
                    //        catch (OperationCanceledException) { return 0; }
                    //        catch (ObjectDisposedException) { return 0; }
                    //        catch (SocketException e)
                    //        {
                    //            _logger.Debug(e, $"Unable to read from {ListeningAddress}");
                    //            ZeroAsync(this);
                    //            return 0;
                    //        }
                    //        catch (Exception e)
                    //        {
                    //            _logger.Error(e, $"Unable to read from {ListeningAddress}");
                    //            ZeroAsync(this);
                    //            return 0;
                    //        }
                    //    }).ConfigureAwait(false);


                }
                else if (timeout > 0)
                {
                    read = Socket.ReceiveFrom(buffer.Array, offset, length, SocketFlags.None, ref _udpRemoteEndpointInfo );

                    //Set the remote address
                    if (RemoteNodeAddress == null)
                    {
                        RemoteNodeAddress = IoNodeAddress.CreateFromEndpoint("udp", _udpRemoteEndpointInfo);
                        //_udpRemoteEndpointInfo = Socket.RemoteEndPoint;
                        //RemoteNodeAddress = IoNodeAddress.CreateFromEndpoint("udp", Socket.RemoteEndPoint);
                    }
                    else
                        RemoteAddress.Update((IPEndPoint)_udpRemoteEndpointInfo);
                    //RemoteAddress.Update((IPEndPoint)Socket.RemoteEndPoint);
                }
                return read;
            }
            catch (NullReferenceException) { ZeroAsync(this); return 0; }
            catch (TaskCanceledException) { ZeroAsync(this); return 0; }
            catch (OperationCanceledException) { ZeroAsync(this); return 0; }
            catch (ObjectDisposedException) { ZeroAsync(this); return 0; }
            catch (SocketException e)
            {
                _logger.Debug(e, $"Unable to read from socket `udp://{LocalIpAndPort}':");
                return 0;
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unable to read from socket `udp://{LocalIpAndPort}':");
                ZeroAsync(this);
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
            return ListeningAddress?.IpEndPoint != null || _udpRemoteEndpointInfo != null || RemoteAddress != null;
        }

        public override object ExtraData()
        {
            return RemoteAddress?.IpEndPoint;
        }
    }
}
