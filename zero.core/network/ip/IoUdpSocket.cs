using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Microsoft.AspNetCore.SignalR;
using NLog;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore;
using zero.core.patterns.semaphore.core;
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
        public IoUdpSocket(Socket socket, IoNodeAddress listeningAddress, IoNodeAddress fromAddress = null) : base(socket, listeningAddress)
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
            _argsIoHeap.ZeroManaged();
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

            Configure();

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
            catch (NullReferenceException) { }
            catch (TaskCanceledException) { }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
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
                if (Zeroed() || Socket == null || !(Socket.IsBound))
                {
                    if(!Zeroed())
                        _logger.Error($"Socket is ded? z = {Zeroed()}, from = {ZeroedFrom.Description} bound = {Socket?.IsBound}");
                    return 0;
                }

                if (endPoint == null)
                {
                    _logger.Fatal("No endpoint supplied!");
                    return 0;
                }

                Task<int> sendTask;
                //lock(Socket)
                    sendTask = Socket.SendToAsync(buffer, SocketFlags.None, endPoint);

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
                    if(!(e is ObjectDisposedException))
                        _logger.Fatal(e, $"Sending to udp://{endPoint} failed, z = {Zeroed()}, zf = {ZeroedFrom?.Description}:");
                    if (FfAddress == null)
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

        private readonly EndPoint _remoteEpAny = new IPEndPoint(IPAddress.Any, 99);

        /// <summary>
        /// //TODO params
        /// </summary>
        private IoHeap<SocketAsyncEventArgs> _argsIoHeap = new IoHeap<SocketAsyncEventArgs>(64) { Make = o => new SocketAsyncEventArgs() };


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

                    if (false)
                    {
                        Task<SocketReceiveFromResult> receiveTask;

                        //receiveTask = Socket.ReceiveFromAsync(buffer.Slice(offset, length), SocketFlags.None, ref remoteEp);
                        receiveTask = Socket.ReceiveFromAsync(buffer.Slice(offset, length), SocketFlags.None, _remoteEpAny);

                        //slow path
                        if (!receiveTask.IsCompletedSuccessfully)
                            await receiveTask.ConfigureAwait(false);

                        read = receiveTask.Result.ReceivedBytes;

                        remoteEp.Address = ((IPEndPoint)receiveTask.Result.RemoteEndPoint).Address;
                        remoteEp.Port = ((IPEndPoint)receiveTask.Result.RemoteEndPoint).Port;
                    }
                    else
                    {
                        void Signal(object sender, SocketAsyncEventArgs eventArgs)
                        {
                            eventArgs!.Completed -= Signal;


                            var t = (ValueTuple<IIoZeroSemaphore, IPEndPoint, IoHeap<SocketAsyncEventArgs>>)eventArgs.UserToken!;

                            t.Item2!.Address = ((IPEndPoint)eventArgs.RemoteEndPoint)!.Address;
                            t.Item2!.Port = ((IPEndPoint)eventArgs.RemoteEndPoint)!.Port;

                            try
                            {
                                t.Item1!.Release();
                            }
                            catch (TaskCanceledException e)
                            {
                                _logger.Trace(e);
                            }
                            catch (Exception e)
                            {
                                _logger.Error(e, Description);
                            }

                            eventArgs.SetBuffer(null, 0, 0);
                            t.Item3.Return(eventArgs);
                        };


                        IIoZeroSemaphore tcs = new IoZeroSemaphore("tcs", 1);
                        tcs.ZeroRef(ref tcs, AsyncToken.Token);

                        try
                        {
                            var args = _argsIoHeap.Take();

                            if (args == null)
                                throw new OutOfMemoryException(nameof(_argsIoHeap));

                            args.SetBuffer(buffer.Array, offset, length);
                            args.RemoteEndPoint = _fromAddress?.IpEndPoint ?? _remoteEpAny;
                            args.UserToken = ValueTuple.Create(tcs, remoteEp, _argsIoHeap);

                            args.Completed += Signal;

                            bool result;

                            //lock (Socket)
                                result = Socket.ReceiveFromAsync(args);

                            if (result)
                            {
                                var wait = await tcs.WaitAsync().ZeroBoost(oomCheck: false).ConfigureAwait(false);
                            }
                            else
                            {
                                args!.Completed -= Signal;
                                remoteEp.Address = ((IPEndPoint)args.RemoteEndPoint)!.Address;
                                remoteEp.Port = ((IPEndPoint)args.RemoteEndPoint)!.Port;
                                _argsIoHeap.Return(args);
                            }

                            return args.BytesTransferred;
                        }
                        catch (NullReferenceException e) { _logger.Trace(e, Description); }
                        catch (OutOfMemoryException)
                        {
                            throw;
                        }
                        catch (ObjectDisposedException)
                        {
                            throw;
                        }
                        catch (Exception e)
                        {
                            _logger.Error(e, Description);
                        }
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
            catch (NullReferenceException e) {_logger.Trace(e,Description);}
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
                    if(!(e is ObjectDisposedException))
                        _logger.Error(e, $"Unable to read from socket `udp://{LocalIpAndPort}':");

                    if (FfAddress == null)
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

        /// <summary>
        /// Configures the socket
        /// </summary>
        protected override void Configure()
        {
            if (Socket.IsBound || Socket.Connected)
            {
                return;
            }

            // Don't allow another socket to bind to this port.
            //Socket.ExclusiveAddressUse = true;

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
        }
    }
}
