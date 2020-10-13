using System;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore;
using zero.core.patterns.semaphore.core;

namespace zero.core.network.ip
{
    /// <summary>
    /// The UDP flavor of <see cref="IoSocket"/>
    /// </summary>
    public sealed class IoUdpSocket : IoNetSocket
    {
        /// <inheritdoc />
        /// <summary>
        /// Constructs the UDP socket
        /// </summary>
        /// <param name="concurrencyLevel"></param>
        public IoUdpSocket(int concurrencyLevel) : base(SocketType.Dgram, ProtocolType.Udp)
        {
            _logger = LogManager.GetCurrentClassLogger();
            ;
            InitHeap(concurrencyLevel);
        }

        /// <summary>
        /// Used by pseudo listeners
        /// </summary>
        /// <param name="nativeSocket">The listening address</param>
        /// <param name="remoteEndPoint">The remote endpoint</param>
        /// <param name="concurrencyLevel">Concurrency level</param>
        public IoUdpSocket(Socket nativeSocket, IPEndPoint remoteEndPoint, int concurrencyLevel) : base(nativeSocket,
            remoteEndPoint)
        {
            _logger = LogManager.GetCurrentClassLogger();
            Proxy = true;
            InitHeap(concurrencyLevel);
        }

        /// <summary>
        /// Inits the heap
        /// </summary>
        private void InitHeap(int concurrencyLevel)
        {
            _argsIoHeap = new IoHeap<SocketAsyncEventArgs>(concurrencyLevel)
            {
                Make = o =>
                {
                    var args = new SocketAsyncEventArgs();
                    args.Completed += Signal;
                    return args;
                }
            };
            _tcsHeap = new IoHeap<IIoZeroSemaphore>(concurrencyLevel)
            {
                Make = o =>
                {
                    IIoZeroSemaphore tcs = new IoZeroSemaphore("tcs", concurrencyLevel);
                    tcs.ZeroRef(ref tcs, AsyncTasks.Token);
                    return tcs;
                }
            };
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>s
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

        public const int SIO_UDP_CONNRESET = -1744830452;

        /// <inheritdoc />
        /// <summary>
        /// Listen for UDP traffic
        /// </summary>
        /// <param name="listeningAddress">The address to listen on</param>
        /// <param name="acceptConnectionHandler">The handler once a connection is made, mostly used in UDPs case to look function like <see cref="T:zero.core.network.ip.IoTcpSocket" /></param>
        /// <param name="bootstrapAsync">Bootstrap callback invoked when a listener has started</param>
        /// <returns>True if successful, false otherwise</returns>
        public override async Task ListenAsync(IoNodeAddress listeningAddress,
            Func<IoSocket, Task> acceptConnectionHandler,
            Func<Task> bootstrapAsync = null)
        {
            //base
            await base.ListenAsync(listeningAddress, acceptConnectionHandler, bootstrapAsync).ConfigureAwait(false);


            //set some socket options
            NativeSocket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.PacketInformation, true);

            //TODO sec
            NativeSocket.IOControl(
                (IOControlCode) SIO_UDP_CONNRESET,
                new byte[] {0, 0, 0, 0},
                null
            );

            //configure the socket
            Configure();

            //Init connection tracking
            //_connTrack = new ConcurrentDictionary<string, IIoZero>();

            try
            {
                _logger.Trace($"Waiting for a new connection to {LocalNodeAddress}...");

                //Call the new connection established handler
                await acceptConnectionHandler(this).ConfigureAwait(false);

                //Bootstrap on listener start
                if (bootstrapAsync != null)
                    await bootstrapAsync().ConfigureAwait(false);

                while (!Zeroed())
                {
                    await Task.Delay(5000, AsyncTasks.Token).ConfigureAwait(false);
                }

                _logger.Trace($"Stopped listening at {LocalNodeAddress}");
            }
            catch (NullReferenceException)
            {
            }
            catch (TaskCanceledException)
            {
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Error while listening: {Description}");
            }
        }

        public override async ValueTask<bool> ConnectAsync(IoNodeAddress remoteAddress)
        {
            if (!await base.ConnectAsync(remoteAddress).ZeroBoostAsync().ConfigureAwait(false))
                return false;

            Configure();

            try
            {
                await NativeSocket.ConnectAsync(remoteAddress.IpEndPoint).ConfigureAwait(false);
                LocalNodeAddress = IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) NativeSocket.LocalEndPoint);
                RemoteNodeAddress = IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) NativeSocket.RemoteEndPoint);
            }
            catch (Exception e)
            {
                _logger.Error(e, Description);
                return false;
            }

            return true;
        }

        ///// <summary>
        ///// UDP connection tracker
        ///// </summary>
        //private ConcurrentDictionary<string, IIoZero> _connTrack;

        //private async ValueTask<bool> RouteAsync(byte[] listenerBuffer, IPEndPoint endPoint, Func<IoSocket, Task<IIoZero>> newConnectionHandler)
        //{
        //    var read = await ReadAsync(listenerBuffer, 0, listenerBuffer.Length, endPoint).ZeroBoostAsync(oomCheck: false).ConfigureAwait(false);

        //    //fail fast
        //    if (read == 0)
        //        return false;

        //    //New connection?
        //    if (!_connTrack.TryGetValue(endPoint.ToString(), out var connection))
        //    {
        //        //Atomic add new route
        //        if (await ZeroAtomicAsync(async (z, b) => _connTrack.TryAdd(endPoint.ToString(),
        //            connection = await newConnectionHandler(new IoUdpSocket()).ConfigureAwait(false))).ConfigureAwait(false))
        //        {
        //            //atomic ensure teardown
        //            return await connection.ZeroAtomicAsync((z, b) =>
        //            {
        //                var sub = connection.ZeroEvent(s =>
        //                {
        //                    //Untrack connection
        //                    if (_connTrack.TryRemove(connection.IoSource.Key, out var removed))
        //                    {
        //                        _logger.Trace($"Delete route: {removed!.Description}");
        //                    }
        //                    else
        //                    {
        //                        _logger.Trace($"Delete route, not found: {connection.IoSource.Key}");
        //                    }

        //                    return Task.CompletedTask;
        //                });

        //                return Task.FromResult(sub != null);
        //            }).ConfigureAwait(false);
        //        }
        //        else if(connection != null)//race
        //        {
        //            await connection.ZeroAsync(this).ConfigureAwait(false);
        //        }
        //        //raced out
        //        return false;
        //    }

        //    await connection.IoSource.ProduceAsync(async (b, func, arg3, arg4) =>
        //    {
        //        var source = (IoNetSocket) b;

        //        return true;
        //    }).ConfigureAwait(false);
        //    //route

        //    return await connection!.ConsumeAsync();
        //}

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
        public override async ValueTask<int> SendAsync(ArraySegment<byte> buffer, int offset, int length,
            EndPoint endPoint, int timeout = 0)
        {
            try
            {
                //fail fast
                if (Zeroed() || NativeSocket == null || !(NativeSocket.IsBound))
                {
                    if (!Zeroed())
                        _logger.Error(
                            $"Socket is ded? z = {Zeroed()}, from = {ZeroedFrom.Description} bound = {NativeSocket?.IsBound}");
                    return 0;
                }

                if (endPoint == null)
                {
                    _logger.Fatal("No endpoint supplied!");
                    return 0;
                }

                Task<int> sendTask;
                //lock(Socket)
                sendTask = NativeSocket.SendToAsync(buffer, SocketFlags.None, endPoint);

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
                    if (!(e is ObjectDisposedException))
                        _logger.Fatal(e,
                            $"Sending to udp://{endPoint} failed, z = {Zeroed()}, zf = {ZeroedFrom?.Description}:");

                    await ZeroAsync(this).ConfigureAwait(false);
                }
            }

            return 0;
        }


        private readonly EndPoint _remoteEpAny = new IPEndPoint(IPAddress.Any, 99);

        /// <summary>
        /// socket args heap
        /// </summary>
        private IoHeap<SocketAsyncEventArgs> _argsIoHeap;

        /// <summary>
        /// task completion source
        /// </summary>
        private IoHeap<IIoZeroSemaphore> _tcsHeap;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Signal(object sender, SocketAsyncEventArgs eventArgs)
        {
            ((IIoZeroSemaphore) eventArgs.UserToken)!.Release();
        }

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
        public override async ValueTask<int> ReadAsync(ArraySegment<byte> buffer, int offset, int length,
            IPEndPoint remoteEp, byte[] blacklist = null, int timeout = 0)
        {
            try
            {
                //fail fast
                if (Zeroed())
                    return 0;

                if (timeout == 0)
                {
                    var args = _argsIoHeap.Take();

                    if (args == null)
                        throw new OutOfMemoryException(nameof(_argsIoHeap));

                    var tcs = _tcsHeap.Take();
                    if (tcs == null)
                        throw new OutOfMemoryException(nameof(_tcsHeap));

                    args.SetBuffer(buffer.Array, offset, length);
                    args.RemoteEndPoint = RemoteNodeAddress.IpEndPoint;
                    args.UserToken = tcs;

                    //receive
                    if (NativeSocket.ReceiveFromAsync(args))
                    {
                        if (!await tcs.WaitAsync().ZeroBoostAsync().ConfigureAwait(false))
                            return 0;
                    }

                    remoteEp.Address = ((IPEndPoint) args.RemoteEndPoint)!.Address;
                    remoteEp.Port = ((IPEndPoint) args.RemoteEndPoint)!.Port;
                    args!.SetBuffer(null, 0, 0);
                    _argsIoHeap.Return(args);
                    _tcsHeap.Return(tcs);

                    return args.BytesTransferred;
                }

                if (timeout <= 0) return 0;

                NativeSocket.ReceiveTimeout = timeout;
                EndPoint remoteEpAny = null;
                var read = NativeSocket.ReceiveFrom(buffer.Array!, offset, length, SocketFlags.None, ref remoteEpAny);

                remoteEp.Address = ((IPEndPoint) remoteEpAny).Address;
                remoteEp.Port = ((IPEndPoint) remoteEpAny).Port;

                return read;
            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e, Description);
            }
            catch (TaskCanceledException e)  {_logger.Trace(e,Description);}
            catch (OperationCanceledException e) {_logger.Trace(e,Description);}
            catch (ObjectDisposedException e) {_logger.Trace(e,Description);}
            //catch (SocketException e)
            //{
            //    _logger.Trace(e, $"Unable to read from socket `udp://{LocalIpAndPort}':");
            //}
            catch (Exception e)
            {
                if (!Zeroed())
                {
                    if (!(e is ObjectDisposedException))
                        _logger.Error(e, $"Unable to read from socket: {Description}");

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
            try
            {
                return NativeSocket != null && NativeSocket.IsBound && NativeSocket.LocalEndPoint != null;
            }
            catch (Exception e)
            {
                _logger.Trace(e, Description);
                return false;
            }
        }

        /// <summary>
        /// Configures the socket
        /// </summary>
        protected override void Configure()
        {
            if (NativeSocket.IsBound || NativeSocket.Connected)
            {
                return;
            }

            // Don't allow another socket to bind to this port.
            NativeSocket.ExclusiveAddressUse = true;

            // Set the receive buffer size to 32k
            //NativeSocket.ReceiveBufferSize = 8192 * 4;
            NativeSocket.ReceiveBufferSize = 4096;

            // Set the timeout for synchronous receive methods to
            // 1 second (1000 milliseconds.)
            NativeSocket.ReceiveTimeout = 10000;

            // Set the send buffer size to 8k.
            //NativeSocket.SendBufferSize = 8192 * 2;
            NativeSocket.SendBufferSize = 2048;

            // Set the timeout for synchronous send methods
            // to 1 second (1000 milliseconds.)
            NativeSocket.SendTimeout = 1000;

            // Set the Time To Live (TTL) to 42 router hops.
            NativeSocket.Ttl = 42;
        }
    }
}