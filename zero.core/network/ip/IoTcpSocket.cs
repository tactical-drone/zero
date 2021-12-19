using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using zero.core.patterns.misc;
using NLog;
using zero.core.patterns.semaphore;

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
            ConfigureSocket();
        }

        /// <summary>
        /// A copy constructor used by the listener to spawn new TCP connections
        /// </summary>
        /// <param name="nativeSocket">The connecting socket</param>
        public IoTcpSocket(Socket nativeSocket) : base(nativeSocket)
        {
            _logger = LogManager.GetCurrentClassLogger();
            ConfigureSocket();
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
                //_logger.Trace($"Waiting for a new connection to `{LocalNodeAddress}...'");

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

                    _logger.Trace($"Connection Received: from = `{newSocket.RemoteNodeAddress}', ({Description})");

                    try
                    {
                        //ZERO
                        await acceptConnectionHandler(newSocket, nanite).FastPath().ConfigureAwait(Zc);
                    }
                    catch (Exception e)
                    {
                       newSocket.Zero(this, $"{nameof(acceptConnectionHandler)} returned with errors");
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

            _sw.Restart();
            try
            {
                var connected = new IoManualResetValueTaskSource<bool>();
                NativeSocket.BeginConnect(remoteAddress.IpEndPoint, static result =>
                {
                    var (socket, connected) = (ValueTuple<Socket, IoManualResetValueTaskSource<bool>>)result.AsyncState;
                    socket.EndConnect(result);
                    connected.SetResult(socket.Connected);
                }, (NativeSocket,connected));

                if (timeout > 0)
                {
                    await ZeroAsync(static async state =>
                    {
                        var (@this, timeout) = state;

                        try
                        {
                            await Task.Delay(timeout + 15, @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);
                        }
                        catch
                        {
                            // ignored
                        }

                        if (!@this.IsConnected())
                        {
                            @this.Zero(@this, $"Connecting timed out, waited {timeout}ms");
                            @this.NativeSocket.Close();
                        }
                    }, ValueTuple.Create(this, timeout), TaskCreationOptions.DenyChildAttach);
                }

                if (!await connected.WaitAsync().FastPath().ConfigureAwait(Zc))
                    return false;

                LocalNodeAddress = IoNodeAddress.CreateFromEndpoint("tcp", (IPEndPoint)NativeSocket.LocalEndPoint);
                RemoteNodeAddress = IoNodeAddress.CreateFromEndpoint("tcp", (IPEndPoint)NativeSocket.RemoteEndPoint);

                NativeSocket.Blocking = true;

                _logger.Trace($"Connected to {RemoteNodeAddress}, {Description}");
                return true;
            }
            catch (TaskCanceledException){}
            catch (SocketException e)
            {
                _logger.Trace(e, $"Failed connecting to `{RemoteNodeAddress}': ({Description})");
            }
            catch when (Zeroed()) {}
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
            if (!IsConnected())
                return 0;

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
            catch (ObjectDisposedException) { }
            catch (SocketException e) when (!Zeroed())
            {
                _logger.Debug($"{nameof(SendAsync)}: {e.Message} - {Description}");
                Zero(this, $"{nameof(SendAsync)}: {e.Message}");
            }
            catch (Exception) when (Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger.Error(e, $"{Description}: {nameof(SendAsync)} failed!");
                Zero(this, $"{nameof(SendAsync)}: {e.Message}");
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
            if (!IsConnected())
                return 0;

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
                    var sw = Stopwatch.StartNew();
                    var read = 0;
                    while ((read += NativeSocket.Receive(buf.Array!, offset, length, SocketFlags.None)) == 0 && sw.ElapsedMilliseconds < timeout)
                    {
                        await Task.Delay(timeout / 10).ConfigureAwait(Zc);
                    }
                    NativeSocket.ReceiveTimeout = 0;
                    if(sw.ElapsedMilliseconds < timeout && read == 0)
                        _logger.Fatal($"{nameof(ReadAsync)}: timeout [FAILED], slept {sw.ElapsedMilliseconds}ms, wanted = {timeout}ms");
                    return read;
                }
            }
            catch (ObjectDisposedException) { }
            catch (SocketException e) when (!Zeroed())
            {
                var errMsg = $"{nameof(ReadAsync)}: {e.Message} -  {Description}";
                _logger.Debug(errMsg);
                Zero(this, errMsg);
            }
            catch (Exception) when (Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                var errMsg = $"{nameof(ReadAsync)}: [FAILED], {Description}, l = {length}, o = {offset}: {e.Message}";
                _logger?.Error(e, errMsg);
                Zero(this, errMsg);
            }
            return 0;
        }

        //private readonly byte[] _sentinelBuf = Array.Empty<byte>();
        //private uint _expensiveCheck = 0;

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
                return !Zeroed() && NativeSocket is { IsBound: true, Connected: true };//&& (_expensiveCheck++ % 10000 == 0 && NativeSocket.Send(_sentinelBuf, SocketFlags.None) == 0  || true);

                //||
                // IoNetSocket.NativeSocket.Poll(-1, SelectMode.SelectError) ||
                // !IoNetSocket.NativeSocket.Poll(-1, SelectMode.SelectRead) ||
                // !IoNetSocket.NativeSocket.Poll(-1, SelectMode.SelectWrite)
            }
            catch (ObjectDisposedException){}
            catch when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {                
                _logger.Error(e, Description);
            }
            return false;
        }

    }
}
