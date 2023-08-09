using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using zero.core.patterns.misc;
using NLog;
using NLog.LayoutRenderers;
using zero.core.conf;
using zero.core.misc;
using zero.core.patterns.heap;
using zero.core.patterns.semaphore.core;
using zero.core.runtime.scheduler;

namespace zero.core.network.ip
{
    /// <summary>
    /// The TCP flavor of <see cref="IoSocket"/>
    /// </summary>
    sealed class IoTcpSocket : IoNetSocket
    {
        static IoTcpSocket()
        {
            _logger = LogManager.GetCurrentClassLogger();
        }
        /// <summary>
        /// Constructs a new TCP socket from connection
        /// </summary>
        public IoTcpSocket(int concurrencyLevel) : base(SocketType.Stream, ProtocolType.Tcp, concurrencyLevel)
        {
            ConfigureSocket();
            _listenerSourceCore = new IoHeap<IIoManualResetValueTaskSourceCore<Socket>>($"{nameof(_listenerSourceCore)}: tcp", concurrencyLevel * 2, (_, _) => new IoManualResetValueTaskSourceCore<Socket> { AutoReset = true });
            _connectSourceCore= new IoHeap<IIoManualResetValueTaskSourceCore<bool>>($"{nameof(_listenerSourceCore)}: tcp", concurrencyLevel * 2, (_, _) => new IoManualResetValueTaskSourceCore<bool> { AutoReset = true });
        }

        /// <summary>
        /// A copy constructor used by the listener to spawn new TCP connections
        /// </summary>
        /// <param name="nativeSocket">The connecting socket</param>
        public IoTcpSocket(Socket nativeSocket) : base(nativeSocket, Connection.Ingress)
        {
            ConfigureSocket();
        }

        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync();

            if(_listenerSourceCore!=null)
                await _listenerSourceCore.ZeroManagedAsync<object>();
            
            if(_connectSourceCore != null)
                await _connectSourceCore.ZeroManagedAsync<object>();
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
        }

        /// <summary>
        /// The logger
        /// </summary>
        private static readonly Logger _logger;

        private readonly IoHeap<IIoManualResetValueTaskSourceCore<Socket>> _listenerSourceCore;
        private readonly IoHeap<IIoManualResetValueTaskSourceCore<bool>> _connectSourceCore;

        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_socket_poll_wait_ms = 200;

        /// <summary>
        /// Starts a TCP listener
        /// </summary>
        /// <param name="listeningAddress">The <see cref="IoNodeAddress"/> that this socket listener will initialize with</param>
        /// <param name="acceptConnectionHandler">A handler that is called once a new connection was formed</param>
        /// <param name="context">handler context</param>
        /// <param name="bootFunc">Optional bootstrap function called after connect</param>
        /// <returns></returns>
        public override async ValueTask BlockOnListenAsync<T, TContext>(IoNodeAddress listeningAddress,
            Func<IoSocket, T,ValueTask> acceptConnectionHandler, T context,
            Func<TContext,ValueTask> bootFunc = null, TContext bootData = default)
        {
            //base
            await base.BlockOnListenAsync(listeningAddress, acceptConnectionHandler, context, bootFunc, bootData).FastPath();
            
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
            if (bootFunc != null)
                await bootFunc(bootData).FastPath();

            var description = Description;
            // Accept incoming connections
            while (!Zeroed())
            {
#if DEBUG
                _logger.Trace($"Waiting for a new connection to `{LocalNodeAddress}...'");
#endif
                IIoManualResetValueTaskSourceCore<Socket> taskCore = null;
                try
                {
                    //ZERO control passed to connection handler
                    taskCore = _listenerSourceCore.Take();
                    if (taskCore == null)
                        throw new OutOfMemoryException(nameof(_listenerSourceCore));

                    NativeSocket.BeginAccept(static result =>
                    {
                        var (socket, taskCore) =
                            (ValueTuple<Socket, IIoManualResetValueTaskSourceCore<Socket>>)result.AsyncState;
                        try
                        {
                            taskCore.SetResult(socket.EndAccept(result));
                        }
                        catch (ObjectDisposedException){}
                        catch (SocketException e) when (e.SocketErrorCode == SocketError.OperationAborted) {}
                        catch (Exception e)
                        {
                            try
                            {
                                LogManager.GetCurrentClassLogger().Trace(e, $"{nameof(NativeSocket.BeginConnect)}");
                                taskCore.Reset();
                            }
                            catch
                            {
                                // ignored
                            }
                        }
                    }, (NativeSocket, taskCore));


                    Socket socket;
                    IoTcpSocket newSocket;
                    var connected = new ValueTask<Socket>(taskCore, 0);

                    if ((socket = await connected.FastPath()) != null)
                    {
                        if (socket.Connected && socket.IsBound)
                            newSocket = new IoTcpSocket(socket);
                        else
                        {
                            socket.Dispose();
                            continue;
                        }
                    }
                    else
                    {
                        continue;
                    }

                    _logger.Trace($"Connection Received: from = `{newSocket.RemoteNodeAddress}', ({Description})");

                    try
                    {
                        //ZERO
                        await acceptConnectionHandler(newSocket, context).FastPath();
                    }
                    catch (Exception e)
                    {
                        await newSocket.DisposeAsync(this, $"{nameof(acceptConnectionHandler)} returned with errors")
                            .FastPath();
                        _logger.Error(e,
                            $"There was an error handling a new connection from {newSocket.RemoteNodeAddress} to `{newSocket.LocalNodeAddress}'");
                    }
                }
                catch (ObjectDisposedException) { }
                catch when (Zeroed())
                {
                }
                catch (Exception e) when (!Zeroed())
                {
                    _logger.Error(e, $"Listener at `{LocalNodeAddress}' returned with errors");
                }
                finally
                {
                    _listenerSourceCore.Return(taskCore);
                }
            }

            _logger?.Trace($"Listener {description} exited");
        }

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
            if (!await base.ConnectAsync(remoteAddress, timeout).FastPath())
                return false;

            IIoManualResetValueTaskSourceCore<bool> taskCore = null;
            try
            {
                NativeSocket.Blocking = false;
                NativeSocket.SendTimeout = timeout;
                NativeSocket.ReceiveTimeout = timeout;
                taskCore = _connectSourceCore.Take();
                if (taskCore == null)
                    throw new OutOfMemoryException(nameof(_listenerSourceCore));

                var connectAsync = NativeSocket.BeginConnect(remoteAddress.IpEndPoint, static result =>
                {
                    var (socket, taskCore) = (ValueTuple<Socket, IIoManualResetValueTaskSourceCore<bool>>)result.AsyncState;
                    try
                    {
                        socket.EndConnect(result);
                        taskCore.SetResult(socket.Connected && socket.IsBound);
                    }
                    catch (Exception e)
                    {
                        try
                        {
                            if (taskCore.Blocking)
                                taskCore.SetResult(false);
                            LogManager.GetCurrentClassLogger().Trace(e, $"{nameof(NativeSocket.BeginConnect)}");
                        }
                        catch
                        {
                            // ignored
                        }
                    }
                }, (NativeSocket, taskCore));

                if (timeout > 0)
                {
                    IoZeroScheduler.Zero.LoadAsyncContext(static async state =>
                    {
                        var (@this, taskCore, connectAsync, timeout) = (ValueTuple<IoTcpSocket, IIoManualResetValueTaskSourceCore<bool>, IAsyncResult, int>)state;

                        try
                        {
                            await Task.Delay(timeout, @this.AsyncTasks.Token);
                        }
                        catch
                        {
                            // ignored
                        }

                        try
                        {
                            if (@this.NativeSocket != null)
                            {
                                @this.NativeSocket.EndConnect(connectAsync);
                                taskCore.SetResult(@this.NativeSocket.Connected && @this.NativeSocket.IsBound);
                            }
                        }
                        catch
                        {
                            // ignored
                        }
                    }, (this, taskCore, connectAsync, timeout));
                }

                var connected = new ValueTask<bool>(taskCore, 0);
                if (!await connected.FastPath())
                {
                    try
                    {
                        NativeSocket.Close();
                    }
                    catch
                    {
                        // ignored
                    }

                    return false;
                }


                LocalNodeAddress = IoNodeAddress.CreateFromEndpoint("tcp", (IPEndPoint)NativeSocket.LocalEndPoint);
                RemoteNodeAddress = remoteAddress;

                _logger.Trace($"Connected to {RemoteNodeAddress}, {Description}");
                return true;
            }
            catch (ObjectDisposedException) { }
            catch (TaskCanceledException) { }
            catch (OperationCanceledException) { }
            catch (SocketException e)
            {
                _logger.Trace(e, $"[FAILED] connecting to {RemoteNodeAddress}: ({Description})");
            }
            catch when (Zeroed()) { }
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"[FAILED ] Connecting to {remoteAddress}: {Description}");
            }
            finally
            {
                _connectSourceCore.Return(taskCore);
                try
                {
                    NativeSocket.Blocking = true;
                }
                catch
                {
                    // ignored
                }
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
        public async ValueTask<int> SendAsync(ReadOnlyMemory<byte> buffer, int offset, int length, EndPoint endPoint = null, int timeout = 0)
        {
            try
            {
                //if (!NativeSocket.Poll(parm_socket_poll_wait_ms, SelectMode.SelectWrite))
                //    return 0;

                return await NativeSocket.SendAsync(buffer.Slice(offset, length), SocketFlags.None,
                    timeout > 0 ? new CancellationTokenSource(timeout).Token : AsyncTasks.Token).FastPath();
            }
            catch (SocketException e)
            {
                _logger.Trace($"{nameof(SendAsync)}: err = {e.SocketErrorCode}, {Description}");
                if (e.SocketErrorCode != SocketError.TimedOut)
                    await DisposeAsync(this, e.Message).FastPath();
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
            catch (Exception) when (Zeroed())
            {
            }
            catch (Exception e) when (!Zeroed())
            {
                var errMsg = $"{nameof(SendAsync)}: [FAILED], {Description}, l = {length}, o = {offset}: {e.Message}";
                _logger.Trace(e, errMsg);
                await DisposeAsync(this, errMsg).FastPath();
            }
            finally
            {
                if (timeout > 0)
                    Interlocked.Exchange(ref _timedOp, timeout);
                else if (_timedOp > 0)
                    Interlocked.Exchange(ref _timedOp, 0);
            }

            return 0;
        }

        /// <summary>
        /// Sends data over TCP async
        /// </summary>
        /// <param name="buffer">The buffer containing the data</param>
        /// <param name="offset">The offset into the buffer to start reading from</param>
        /// <param name="length">The length of the data to be sent</param>
        /// <param name="endPoint">not used</param>
        /// <param name="crc">a crc</param>
        /// <param name="timeout"></param>
        /// <returns>The amount of bytes sent</returns>
        public override ValueTask<int> SendAsync(ReadOnlyMemory<byte> buffer, int offset, int length,
            EndPoint endPoint = null, long crc = 0,
            int timeout = 0)
        {
            if (crc == 0)
                return SendAsync(buffer, offset, length, endPoint, timeout);

            return DupChecker.TryAdd(crc, Environment.TickCount) ? SendAsync(buffer, offset, length, endPoint, timeout) : new ValueTask<int>(length);
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
        public override async ValueTask<int> ReceiveAsync(Memory<byte> buffer, int offset, int length, byte[] remoteEp = null, int timeout = 0)
        {
            try
            {
                return await NativeSocket.ReceiveAsync(buffer.Slice(offset, length), SocketFlags.None,
                    timeout > 0 ? new CancellationTokenSource(timeout).Token : AsyncTasks.Token).FastPath();
            }
            catch (ObjectDisposedException)
            {
            }
            catch (OperationCanceledException)
            {
            }
            catch (SocketException e) when (!Zeroed())
            {
                var errMsg = $"{nameof(ReceiveAsync)}: {e.Message} - {Description}";
                _logger.Debug(errMsg);
                await DisposeAsync(this, errMsg).FastPath();
            }
            catch when (Zeroed())
            {
            }
            catch (Exception e) when (!Zeroed())
            {
                var errMsg = $"{nameof(ReceiveAsync)}: [FAILED], {Description}, l = {length}, o = {offset}: {e.Message}";
                _logger.Error(e, errMsg);
                await DisposeAsync(this, errMsg).FastPath();
            }
            finally
            {
                if(timeout > 0)
                    Interlocked.Exchange(ref _timedOp, timeout);
                else if(_timedOp > 0)
                    Interlocked.Exchange(ref _timedOp, 0);
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
                return Zeroed() || NativeSocket.RemoteEndPoint != null && NativeSocket.IsBound && NativeSocket.Connected || _timedOp > 0;
                //return !Zeroed() && NativeSocket is { IsBound: true, Connected: true };//&& (_expensiveCheck++ % 10000 == 0 && NativeSocket.Send(_sentinelBuf, SocketFlags.None) == 0  || true);

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
