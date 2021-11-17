using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using zero.core.patterns.misc;
using NLog;
using OperationCanceledException = System.OperationCanceledException;

namespace zero.core.network.ip
{
    /// <summary>
    /// The TCP flavor of <see cref="IoSocket"/>
    /// </summary>
    sealed class IoTcpSocket : IoNetSocket
    {
        /// <summary>
        /// Constructs a new TCP socket from connection
        /// </summary>
        public IoTcpSocket(int concurrencyLevel) : base(SocketType.Stream, ProtocolType.Tcp, concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// A copy constructor used by the listener to spawn new TCP connections
        /// </summary>
        /// <param name="nativeSocket">The connecting socket</param>
        public IoTcpSocket(Socket nativeSocket) : base(nativeSocket)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _logger = null;
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
        private Logger _logger;

        /// <summary>
        /// Starts a TCP listener
        /// </summary>
        /// <param name="listeningAddress">The <see cref="IoNodeAddress"/> that this socket listener will initialize with</param>
        /// <param name="acceptConnectionHandler">A handler that is called once a new connection was formed</param>
        /// <param name="bootstrapAsync"></param>
        /// <returns></returns>
        public override async ValueTask BlockOnListenAsync<T>(IoNodeAddress listeningAddress,
            Func<IoSocket, T,ValueTask> acceptConnectionHandler,
            T nanite,
            Func<ValueTask> bootstrapAsync = null)
        {
            //base
            await base.BlockOnListenAsync(listeningAddress, acceptConnectionHandler, nanite, bootstrapAsync).FastPath().ConfigureAwait(Zc);
            
            //Configure the socket
            ConfigureSocket();

            //Put the socket in listen mode
            try
            {
                NativeSocket.Listen(parm_socket_listen_backlog);
            }
            catch (Exception e)
            {
                _logger.Error(e, $" listener `{LocalNodeAddress}' returned with errors:");
                return;
            }

            //Execute bootstrap
            if(bootstrapAsync!=null)
                await bootstrapAsync().ConfigureAwait(Zc);

            var description = Description;
            // Accept incoming connections
            while (!Zeroed())
            {
                _logger.Trace($"Waiting for a new connection to `{LocalNodeAddress}...'");

                try
                {
                    //ZERO control passed to connection handler
                    var newSocket = new IoTcpSocket(await NativeSocket.AcceptAsync().ConfigureAwait(Zc));
                    
                    //newSocket.ClosedEvent((sender, args) => Close());

                    //Do some pointless sanity checking
                    //if (newSocket.LocalAddress != LocalNodeAddress.Ip || newSocket.LocalPort != LocalNodeAddress.Port)
                    //{
                    //    _logger.Fatal($"New connection to `tcp://{newSocket.LocalIpAndPort}' should have been to `tcp://{LocalNodeAddress.IpPort}'! Possible hackery! Investigate immediately!");
                    //    newSocket.Close();
                    //    break;
                    //}

                    _logger.Trace($"New connection from `tcp://{newSocket.RemoteNodeAddress}' to `{LocalNodeAddress}' ({Description})");

                    try
                    {
                        //ZERO
                        await acceptConnectionHandler(newSocket, nanite).FastPath().ConfigureAwait(Zc);
                    }
                    catch (Exception e)
                    {
                        await newSocket.ZeroAsync(this).FastPath().ConfigureAwait(Zc);
                        _logger.Error(e, $"There was an error handling a new connection from {newSocket.RemoteNodeAddress} to `{newSocket.LocalNodeAddress}'");
                    }
                }
                catch (ObjectDisposedException e) { _logger.Trace(e, description);}
                catch (OperationCanceledException e) { _logger.Trace(e, description); }
                catch (Exception e)
                {
                    if (!Zeroed())
                        _logger.Error(e, $"Listener at `{LocalNodeAddress}' returned with errors");
                }
            }

            _logger?.Trace($"Listener {description} exited");
        }


        private readonly Stopwatch _sw = Stopwatch.StartNew();

        /// <summary>
        /// Connecting
        /// </summary>
#if NET6_0
        private int WSAEWOULDBLOCK = 10035;
#endif

        /// <summary>
        /// Connect to a remote endpoint
        /// </summary>
        /// <param name="remoteAddress">The address to connect to</param>
        /// <param name="timeout">Connection timeout in ms</param>
        /// <returns>True on success, false otherwise</returns>
        public override async ValueTask<bool> ConnectAsync(IoNodeAddress remoteAddress, int timeout = 0)
        {
            if (!await base.ConnectAsync(remoteAddress, timeout).FastPath().ConfigureAwait(Zc))
                return false;

            //NativeSocket.Blocking = false;

            ConfigureSocket();

            _sw.Restart();
            try
            {
                var result = NativeSocket.ConnectAsync(remoteAddress.IpEndPoint);

                if (timeout > 0)
                {
                    await ZeroAsync(static async state =>
                    {
                        var (@this, result, timeout) = state;
                        await Task.Delay(timeout + 15, @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);

                        if (!@this.IsConnected())
                            result.Dispose();

                    }, ValueTuple.Create(this, result, timeout), TaskCreationOptions.DenyChildAttach);
                }

                await result.ConfigureAwait(Zc);

                //var result = NativeSocket.BeginConnect(remoteAddress.IpEndPoint, null, null);
                //result.AsyncWaitHandle.WaitOne(timeout);

                if (!IsConnected())
                    return false;

                LocalNodeAddress = IoNodeAddress.CreateFromEndpoint("tcp", (IPEndPoint)NativeSocket.LocalEndPoint);
                RemoteNodeAddress = IoNodeAddress.CreateFromEndpoint("tcp", (IPEndPoint)NativeSocket.RemoteEndPoint);

                NativeSocket.Blocking = true;
                
                _logger.Trace($"Connected to `{RemoteNodeAddress}': ({Description})");
                return true;
            }
            catch (SocketException e)
            {
                {
                    //if (exception.ErrorCode == WSAEWOULDBLOCK)
                    //{
                    //    while (!Zeroed() && _sw.ElapsedMilliseconds < 10000 &&
                    //           !NativeSocket.Poll(100000, SelectMode.SelectError) &&
                    //           !NativeSocket.Poll(100000, SelectMode.SelectWrite))
                    //    { }

                    //    if (Zeroed() || _sw.ElapsedMilliseconds >= 10000)
                    //    {
                    //        NativeSocket.Close();
                    //        return false;
                    //    }
                    //}
                }

                //NativeSocket.Blocking = true;

                _logger.Trace(e,$"Failed connecting to `{RemoteNodeAddress}': ({Description})");
                return false;
            }
            catch when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"Connecting to `{remoteAddress}' failed: {Description}");
            }

            return false;
        }


        /// <summary>
        /// Sends data over TCP async
        /// </summary>
        /// <param name="buffer">The buffer containing the data</param>
        /// <param name="offset">The offset into the buffer to start reading from</param>
        /// <param name="length">The length of the data to be sent</param>
        /// <param name="endPoint">not used</param>
        /// <param name="timeout"></param>
        /// <returns>The amount of bytes sent</returns>
        public override async ValueTask<int> SendAsync(ReadOnlyMemory<byte> buffer, int offset, int length,
            EndPoint endPoint = null, int timeout = 0)
        {
            try
            {
                if (timeout == 0)
                {
                    return await NativeSocket
                        .SendAsync(buffer.Slice(offset, length), SocketFlags.None, AsyncTasks.Token).FastPath()
                        .ConfigureAwait(Zc);
                }

                NativeSocket.SendTimeout = timeout;
                var sent = NativeSocket.Send(buffer.Span.Slice(offset,length));                
                return sent;
            }
            catch (SocketException) when (!Zeroed())
            {
                //TODO why is this spamming An established connection was aborted?
                //_logger.Trace( $"{nameof(SendAsync)}: {Description}, {e.Message}");
                await ZeroAsync(this).FastPath().ConfigureAwait(Zc);
            }
            catch (Exception) when (Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger.Error(e, $"{Description}: {nameof(SendAsync)} failed!");
                await ZeroAsync(this).FastPath().ConfigureAwait(Zc);
            }

            return 0;
        }

        /// <inheritdoc />
        /// <summary>
        /// Reads data from a TCP socket async
        /// </summary>
        /// <param name="buffer">The buffer to read into</param>
        /// <param name="offset">The offset into the buffer</param>
        /// <param name="length">The maximum bytes to read into the buffer</param>
        /// <param name="remoteEp"></param>
        /// <param name="blacklist"></param>
        /// <param name="timeout">A timeout</param>
        /// <returns>The number of bytes read</returns>
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, int offset, int length,
            IPEndPoint remoteEp = null,
            byte[] blacklist = null, int timeout = 0) //TODO can we go back to array buffers?
        {
            try
            {
                //fast path: no timeout
                if (timeout == 0)
                {
                    return await NativeSocket
                        .ReceiveAsync(buffer.Slice(offset, length), SocketFlags.None, AsyncTasks.Token).FastPath()
                        .ConfigureAwait(Zc);
                }

                //slow path: timeout
                if (MemoryMarshal.TryGetArray((ReadOnlyMemory<byte>)buffer, out var buf))
                {
                    NativeSocket.ReceiveTimeout = timeout;
                    var read = NativeSocket.Receive(buf.Array!, offset, length, SocketFlags.None);
                    NativeSocket.ReceiveTimeout = 0;
                    return read;
                }
            }
            catch (SocketException e) when (!Zeroed())
            {
                _logger.Trace($"{nameof(ReadAsync)}: {Description}, {e.Message}");
                await ZeroAsync(this).FastPath().ConfigureAwait(Zc);
            }
            catch (Exception) when (Zeroed()){}
            catch (Exception e) when(!Zeroed()) 
            {
                _logger?.Error(e, $"{nameof(ReadAsync)}: [FAILED], {Description}, l = {length}, o = {offset}: {e.Message}");
                await ZeroAsync(this).FastPath().ConfigureAwait(Zc);
            }
            return 0;
        }

        private byte[] _sentinelBuf = Array.Empty<byte>();
        private uint _expensiveCheck = 0;
        /// <inheritdoc />
        /// <summary>
        /// Connection status
        /// </summary>
        /// <returns>True if the connection is up, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool IsConnected()
        {
            
            try
            {
                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                NativeSocket.SendTimeout = 0;
                return NativeSocket is { IsBound: true, Connected: true } && (_expensiveCheck++ % 100 == 0 && NativeSocket.Send(_sentinelBuf, SocketFlags.None) == 0  || true);
            }
            catch when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {                
                _logger.Error(e, Description);
            }
            return false;
        }

    }
}
