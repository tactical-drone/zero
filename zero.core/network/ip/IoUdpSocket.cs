using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
using zero.core.patterns.misc;
using TaskExtensions = System.Threading.Tasks.TaskExtensions;

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
        }

        /// <inheritdoc />
        /// <summary>
        /// A copy constructor
        /// </summary>
        /// <param name="socket">The underlying socket</param>
        /// <param name="listeningAddress">The address listened on</param>
        /// <param name="fromAddress">From address</param>
        public IoUdpSocket(Socket socket, IoNodeAddress listeningAddress, IoNodeAddress fromAddress = null) :  base(socket, listeningAddress)
        {
            _logger = LogManager.GetCurrentClassLogger();
            _fromAddress = fromAddress;

            if (_fromAddress != null)
            {
                RemoteNodeAddress = _fromAddress;
                Shared = true;
            }
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            RemoteNodeAddress = null;
#endif

        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override ValueTask ZeroManagedAsync()
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
        //private EndPoint _udpRemoteEndpointInfo;

        //public override IoNodeAddress RemoteAddress { get; protected set; }

        /// <summary>
        /// from address
        /// </summary>
        public override IoNodeAddress FfAddress => _fromAddress;

        //public override string Key => RemoteAddress?.IpPort ?? LocalIpAndPort;

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
        /// <param name="bootstrapAsync">Bootstrap callback invoked when a listener has started</param>
        /// <returns>True if successful, false otherwise</returns>
        public override async Task<bool> ListenAsync(IoNodeAddress address, Func<IoSocket, Task> callback,
            Func<Task> bootstrapAsync = null)
        {
            
            Socket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.PacketInformation, true);

            //TODO sec
            Socket.IOControl(
                (IOControlCode)SIO_UDP_CONNRESET,
                new byte[] { 0, 0, 0, 0 },
                null
            );

            if (!await base.ListenAsync(address, callback, bootstrapAsync).ConfigureAwait(false))
                return false;

            try
            {
                //Call the new connection established handler
                await callback(this).ConfigureAwait(false);

                if (bootstrapAsync != null)
                    await bootstrapAsync().ConfigureAwait(false);

                _logger.Debug($"Started listener at {ListeningAddress}");
                while (!Zeroed())
                {
                    await Task.Delay(5000, AsyncToken.Token).ConfigureAwait(false);
                    if (!Socket?.IsBound ?? false)
                    {
                        _logger.Warn($"Found zombie udp socket state");

                        await ZeroAsync(this).ConfigureAwait(false);

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
        public override async ValueTask<int> SendAsync(ArraySegment<byte> buffer, int offset, int length, EndPoint endPoint, int timeout = 0)
        {
            try
            {
                //fail fast
                if (Zeroed() || Socket == null || !(Socket.IsBound) )
                {
                    _logger.Error($"Socket is ded? z = {Zeroed()}, from = {ZeroedFrom.Description} bound = {Socket?.IsBound}");
                    return 0;
                }

                if (endPoint == null)
                {
                    _logger.Fatal("No endpoint supplied!");
                    return 0;
                }

                var sendTask = Socket.SendToAsync(buffer, SocketFlags.None, endPoint);
                
                //slow path
                if (!sendTask.IsCompletedSuccessfully)
                    await sendTask.ConfigureAwait(false);

                return sendTask.Result;
            }
            //catch (NullReferenceException e) { _logger.Trace(e, Description); }
            //catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            //catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            //catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            catch (Exception e)
            {
                if (!Zeroed())
                {
                    _logger.Fatal(e, $"Sending to udp://{endPoint} failed, z = {Zeroed()}, zf = {ZeroedFrom?.Description}:");
                    await ZeroAsync(this).ConfigureAwait(false);
                }
            }

            return 0;
        }


        /// <summary>
        /// dummy endpoint
        /// </summary>
        private EndPoint _dummyEndPoint = new IPEndPoint(IPAddress.Any, 0);

        /// <summary>
        /// Only receive from this address
        /// </summary>
        private readonly IoNodeAddress _fromAddress;


        // private SocketAsyncEventArgs _args = new SocketAsyncEventArgs();
        // private TaskCompletionSource<int> _tcs;

        EndPoint _remoteEpAny = new IPEndPoint(IPAddress.Any, 99);

        /// <summary>
        /// Read from the socket
        /// </summary>
        /// <param name="buffer">Read into a buffer</param>
        /// <param name="offset">Write start pos</param>
        /// <param name="length">Bytes to read</param>
        /// <param name="remoteEp"></param>
        /// <param name="blacklist"></param>
        /// <param name="timeout">Timeout after ms</param>
        /// <returns></returns>
        public override async ValueTask<int> ReadAsync(ArraySegment<byte> buffer, int offset, int length, IPEndPoint remoteEp, byte[] blacklist = null, int timeout = 0)
        {
            try
            {
                //fail fast
                if (!(Socket.IsBound) || Zeroed())
                    return 0;
                
                var read = 0;
                if (timeout == 0)
                {
                    if (_fromAddress == null)
                    {
                        var _c = 0;
                        Task<SocketReceiveFromResult> receiveTask;
                        
                        do
                        {

                            if (_c++ > 0)
                            {
                                
                            }

                            //receiveTask = Socket.ReceiveFromAsync(buffer.Slice(offset, length), SocketFlags.None, ref remoteEp);
                            receiveTask = Socket.ReceiveFromAsync(buffer.Slice(offset, length), SocketFlags.None, _remoteEpAny);

                            //slow path
                            if (!receiveTask.IsCompletedSuccessfully)
                                await receiveTask.ConfigureAwait(false);

                            read = receiveTask.Result.ReceivedBytes;

                            remoteEp.Address = ((IPEndPoint) receiveTask.Result.RemoteEndPoint).Address;
                            remoteEp.Port = ((IPEndPoint)receiveTask.Result.RemoteEndPoint).Port;

                        } while (blacklist != null && blacklist[((IPEndPoint)receiveTask.Result.RemoteEndPoint).Port] == 1);
                    }
                    else
                    {
                        var args = new SocketAsyncEventArgs();
                        TaskCompletionSource<int> tcs = new TaskCompletionSource<int>();
                        
                        args.SetBuffer(buffer.Array, offset, length);
                        args.RemoteEndPoint = _fromAddress.IpEndPoint;
                        args.UserToken = tcs;

                        
                        args.Completed += (sender, eventArgs) =>
                        {
                            remoteEp.Address = ((IPEndPoint)eventArgs.RemoteEndPoint)!.Address;
                            remoteEp.Port = ((IPEndPoint)eventArgs.RemoteEndPoint)!.Port;
                            ((TaskCompletionSource<int>)eventArgs.UserToken)!.SetResult(eventArgs.BytesTransferred);
                        };
                        
                        if (Socket.ReceiveFromAsync(args))
                        {
                            return await new ValueTask<int>(tcs.Task);
                        }

                        return args.BytesTransferred;

                    }
                }
                else if (timeout > 0)
                {
                    Socket.ReceiveTimeout = timeout;
                    EndPoint remoteEpAny = null;
                    read = Socket.ReceiveFrom(buffer.Array!, offset, length, SocketFlags.None, ref remoteEpAny);

                    remoteEp.Address = ((IPEndPoint)remoteEpAny).Address;
                    remoteEp.Port = ((IPEndPoint)remoteEpAny).Port;

                    //Set the remote address
                    if (RemoteNodeAddress == null)
                    {
                        RemoteNodeAddress = IoNodeAddress.CreateFromEndpoint("udp", remoteEpAny);
                        //_udpRemoteEndpointInfo = Socket.RemoteEndPoint;
                        //RemoteNodeAddress = IoNodeAddress.CreateFromEndpoint("udp", Socket.RemoteEndPoint);
                    }
                    else
                        RemoteAddress.Update((IPEndPoint)remoteEpAny);
                    //RemoteAddress.Update((IPEndPoint)Socket.RemoteEndPoint);
                }
                return read;
            }
            //catch (NullReferenceException e) {_logger.Trace(e,Description);}
            //catch (TaskCanceledException e)  {_logger.Trace(e,Description);}
            //catch (OperationCanceledException e) {_logger.Trace(e,Description);}
            //catch (ObjectDisposedException e) {_logger.Trace(e,Description);}
            //catch (SocketException e)
            //{
            //    _logger.Debug(e, $"Unable to read from socket `udp://{LocalIpAndPort}':");
            //}
            catch (Exception e)
            {
                if (!Zeroed())
                {
                    _logger.Error(e, $"Unable to read from socket `udp://{LocalIpAndPort}':");
                    await ZeroAsync(this).ConfigureAwait(false);
                }
            }

            return 0;
        }

        /// <inheritdoc />
        /// <summary>
        /// Connection status
        /// </summary>
        /// <returns>True if the connection is up, false otherwise</returns>
        public override bool IsConnected()
        {
            return ListeningAddress?.IpEndPoint != null || RemoteAddress != null;
        }

        //public override object ExtraData()
        //{
        //    return RemoteAddress?.IpEndPoint;
        //}
    }
}
