using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using zero.core.patterns.misc;
using NLog;
using zero.core.patterns.semaphore;
using zero.core.patterns.semaphore.core;

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
        /// The logger
        /// </summary>
        private Logger _logger;

        /// <summary>
        /// Starts a TCP listener
        /// </summary>
        /// <param name="listeningAddress">The <see cref="IoNodeAddress"/> that this socket listener will initialize with</param>
        /// <param name="acceptConnectionHandler">A handler that is called once a new connection was formed</param>
        /// <param name="context">handler context</param>
        /// <param name="bootstrapAsync">Optional bootstrap function called after connect</param>
        /// <returns></returns>
        public override async ValueTask BlockOnListenAsync<T>(IoNodeAddress listeningAddress,
            Func<IoSocket, T,ValueTask> acceptConnectionHandler, T context,
            Func<ValueTask> bootstrapAsync = null)
        {
            //base
            await base.BlockOnListenAsync(listeningAddress, acceptConnectionHandler, context, bootstrapAsync).FastPath().ConfigureAwait(Zc);
            
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
                await bootstrapAsync().FastPath().ConfigureAwait(Zc);

            var description = Description;
            // Accept incoming connections
            while (!Zeroed())
            {
                //_logger.Trace($"Waiting for a new connection to `{LocalNodeAddress}...'");

                try
                {
                    //ZERO control passed to connection handler
                    var taskCore = new IoManualResetValueTaskSource<Socket>(true);
                    NativeSocket.BeginAccept(static result =>
                    {
                        var (socket, connected) = (ValueTuple<Socket, IoManualResetValueTaskSource<Socket>>)result.AsyncState;
                        try
                        {
                            connected.SetResult(socket.EndAccept(result));
                        }
                        catch (Exception e)
                        {
                            LogManager.GetCurrentClassLogger().Trace(e, $"{nameof(NativeSocket.BeginConnect)}");
                            connected.SetResult(null);
                        }
                    }, (NativeSocket, taskCore));


                    Socket socket;
                    IoTcpSocket newSocket;
                    var connected = new ValueTask<Socket>(taskCore, taskCore.Version);
                    if ((socket = await connected.FastPath().ConfigureAwait(Zc)) != null && socket.Connected)
                    {
                        newSocket = new IoTcpSocket(socket);
                    }
                    else
                    {
                        _logger.Error($"Incoming connection failed: {socket}, {Description}");
                        continue;
                    }


                    //var newSocket = new IoTcpSocket(await NativeSocket.AcceptAsync().ConfigureAwait(Zc));
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
                        await acceptConnectionHandler(newSocket, context).FastPath().ConfigureAwait(Zc);
                    }
                    catch (Exception e)
                    {
                       await newSocket.Zero(this, $"{nameof(acceptConnectionHandler)} returned with errors").FastPath().ConfigureAwait(Zc);
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
                var taskCore = new IoManualResetValueTaskSource<bool>(true);
                NativeSocket.BeginConnect(remoteAddress.IpEndPoint, static result =>
                {
                    var (socket, taskCore) = (ValueTuple<Socket, IoManualResetValueTaskSource<bool>>)result.AsyncState;
                    try
                    {
                        socket.EndConnect(result);
                        taskCore.SetResult(socket.Connected);
                    }
                    catch (Exception e)
                    {
                        LogManager.GetCurrentClassLogger().Trace(e, $"{nameof(NativeSocket.BeginConnect)}");
                        taskCore.SetResult(false);
                    }
                }, (NativeSocket, taskCore));

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

                        if (!await @this.IsConnected().FastPath().ConfigureAwait(@this.Zc))
                        {
                            await @this.Zero(@this, $"Connecting timed out, waited {timeout}ms").FastPath().ConfigureAwait(@this.Zc);
                            @this.NativeSocket.Close();
                        }
                    }, ValueTuple.Create(this, timeout), TaskCreationOptions.DenyChildAttach);
                }
                var connected = new ValueTask<bool>(taskCore, taskCore.Version);
                if (!await connected.FastPath().ConfigureAwait(Zc))
                    return false;

                LocalNodeAddress = IoNodeAddress.CreateFromEndpoint("tcp", (IPEndPoint)NativeSocket.LocalEndPoint);
                RemoteNodeAddress = remoteAddress;

                _logger.Trace($"Connected to {RemoteNodeAddress}, {Description}");
                return true;
            }
            catch (ObjectDisposedException){}
            catch (TaskCanceledException){}
            catch (SocketException e)
            {
                _logger.Trace(e, $"[FAILED] connecting to {RemoteNodeAddress}: ({Description})");
            }
            catch when (Zeroed()) {}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"[FAILED ] Connecting to {remoteAddress}: {Description}");
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
                if (!await IsConnected().FastPath().ConfigureAwait(Zc))
                    return 0;

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
            catch (SocketException e){
                _logger.Trace( $"{nameof(SendAsync)}: err = {e.SocketErrorCode}, {Description}");
                if (e.SocketErrorCode != SocketError.TimedOut)
                    await Zero(this, e.Message).FastPath().ConfigureAwait(Zc);
            }
            catch (ObjectDisposedException) { }
            catch (Exception) when (Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                var errMsg = $"{nameof(SendAsync)}: [FAILED], {Description}, l = {length}, o = {offset}: {e.Message}";
                _logger.Trace(e, errMsg);
                await Zero(this, errMsg).FastPath().ConfigureAwait(Zc);
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
        /// <param name="timeout">A timeout</param>
        /// <returns>The number of bytes read</returns>
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, int offset, int length,
            byte[] remoteEp = null,
            int timeout = 0) //TODO can we go back to array buffers?
        {
            try
            {
                if (!await IsConnected().FastPath().ConfigureAwait(Zc))
                    return 0;

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
                var errMsg = $"{nameof(ReadAsync)}: {e.Message} - {Description}";
                _logger.Debug(errMsg);
                await Zero(this, errMsg).FastPath().ConfigureAwait(Zc);
            }
            catch (Exception) when (Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                var errMsg = $"{nameof(ReadAsync)}: [FAILED], {Description}, l = {length}, o = {offset}: {e.Message}";
                _logger?.Error(e, errMsg);
                await Zero(this, errMsg).FastPath().ConfigureAwait(Zc);
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
        public override ValueTask<bool> IsConnected()
        {
            try
            {
                return new ValueTask<bool>(!Zeroed() && NativeSocket is { IsBound: true, Connected: true });//&& (_expensiveCheck++ % 10000 == 0 && NativeSocket.Send(_sentinelBuf, SocketFlags.None) == 0  || true);

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
            return new ValueTask<bool>(false);
        }
    }
}
