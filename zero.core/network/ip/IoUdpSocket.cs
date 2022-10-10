using System;
using System.ComponentModel;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.misc;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore.core;
using zero.core.runtime.scheduler;


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
        public IoUdpSocket(int concurrencyLevel) : base(SocketType.Dgram, ProtocolType.Udp, concurrencyLevel)
        {
            Init(concurrencyLevel);//TODO tuning
        }

        /// <summary>
        /// Used by pseudo listeners
        /// </summary>
        /// <param name="nativeSocket">The listening address</param>
        /// <param name="remoteEndPoint">The remote endpoint</param>
        /// <param name="concurrencyLevel"></param>
        /// <param name="clone">Operator overloaded, parm not used</param>
#pragma warning disable IDE0060 // Remove unused parameter
        public IoUdpSocket(Socket nativeSocket, IPEndPoint remoteEndPoint, int concurrencyLevel, bool clone = false) : base(nativeSocket, remoteEndPoint, concurrencyLevel)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            try
            {
                NativeSocket.Blocking = true;
            }
            catch
            {
                return;
            }

            Proxy = true;
            Init(concurrencyLevel);
        }

        /// <summary>
        /// Used by pseudo listeners
        /// </summary>
        /// <param name="nativeSocket">The listening address</param>
        /// <param name="remoteEndPoint">The remote endpoint</param>
        /// <param name="concurrencyLevel"></param>
        public IoUdpSocket(Socket nativeSocket, IPEndPoint remoteEndPoint, int concurrencyLevel) : base(nativeSocket, remoteEndPoint, concurrencyLevel)
        {
            try
            {
                NativeSocket.Blocking = true;
            }
            catch
            {
                return;
            }

            //TODO tuning:
            Init(concurrencyLevel);
        }

        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_socket_poll_wait_ms = 250;

        /// <summary>
        /// Initializes the socket
        /// </summary>
        /// <param name="concurrencyLevel"></param>
        public void Init(int concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();

            _argsHeap = new IoHeap<SocketAsyncEventArgs, IoUdpSocket>($"{nameof(_argsHeap)}: {Description}",
                concurrencyLevel << 2, static (_, @this) =>
                {
                    var args = new SocketAsyncEventArgs
                    {
                        RemoteEndPoint = new IPEndPoint(0, 0),
                        UserToken = new IoManualResetValueTaskSourceCore<bool> { AutoReset = true },
                    };
                    args.Completed += @this.ZeroCompletion;

                    return args;
                }, autoScale: true)
            {
                Context = this
            };
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>s
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _logger = null;
            _argsHeap = null;
            _argsHeap = null;
            RemoteNodeAddress = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();

            if (_argsHeap != null)
            {
                await _argsHeap.ZeroManagedAsync(static (o, @this) =>
                {
                    o.Completed -= @this.ZeroCompletion;
                    o.UserToken = null;
                    //o.RemoteEndPoint = null; Leave not null
                    try
                    {
                        o.SetBuffer(null, 0, 0);
                    }
                    catch
                    {
                        // ignored
                    }
                    ((IDisposable)o).Dispose();

                    return new ValueTask(Task.CompletedTask);
                }, this).FastPath();
            }
            
        }

        /// <summary>
        /// The logger
        /// </summary>
        private Logger _logger;

        public const int SIO_UDP_CONNRESET = -1744830452;

        /// <inheritdoc />
        /// <summary>
        /// Listen for UDP traffic
        /// </summary>
        /// <param name="listeningAddress">The address to listen on</param>
        /// <param name="acceptConnectionHandler">The handler once a connection is made, mostly used in UDPs case to look function like <see cref="T:zero.core.network.ip.IoTcpSocket" /></param>
        /// <param name="context">Context</param>
        /// <param name="bootFunc">Bootstrap callback invoked when a listener has started</param>
        /// <param name="bootData"></param>
        /// <returns>True if successful, false otherwise</returns>
        public override async ValueTask BlockOnListenAsync<T, TBoot>(
            IoNodeAddress listeningAddress,
            Func<IoSocket, T, ValueTask> acceptConnectionHandler,
            T context,
            Func<TBoot, ValueTask> bootFunc = null,
            TBoot bootData = default)
        {
            //TODO sec
            _ = NativeSocket.IOControl(
                (IOControlCode)SIO_UDP_CONNRESET,
                new byte[] {0,0,0,0},
                null
            );

            //configure the socket
            ConfigureSocket();

            //base
            await base.BlockOnListenAsync<object,object>(listeningAddress, null, null).FastPath();

            //Init connection tracking
            try
            {
                try
                {
                    //ZERO
                    await acceptConnectionHandler(this, context).FastPath();
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"There was an error handling a new connection from {this.RemoteNodeAddress} to `{this.LocalNodeAddress}'");
                }
                
                //Bootstrap on listener start
                if (bootFunc != null)
                {
                    IoZeroScheduler.Zero.LoadAsyncContext(static async state =>
                    {
                        var (bootstrapAsync, bContext) = (ValueTuple<Func<TBoot, ValueTask>, TBoot>)state;
                        await bootstrapAsync(bContext).FastPath();
                    },(bootFunc,bootData));
                }

                //block
                await AsyncTasks.BlockOnNotCanceledAsync().FastPath();

                _logger?.Trace($"Stopped listening at {LocalNodeAddress}");
            }
            catch when (Zeroed()) { }
            catch (Exception e)when (!Zeroed())
            {
                _logger?.Error(e, $"Error while listening: {Description}");
            }
        }

        /// <summary>
        /// Connect to remote address
        /// </summary>
        /// <param name="remoteAddress">The remote to connect to</param>
        /// <param name="timeout">A timeout</param>
        /// <returns>True on success, false otherwise</returns>
        public override async ValueTask<bool> ConnectAsync(IoNodeAddress remoteAddress, int timeout = 0)
        {
            if (!await base.ConnectAsync(remoteAddress, timeout).FastPath())
                return false;

            ConfigureSocket();

            try
            {                
                await NativeSocket.ConnectAsync(remoteAddress.IpEndPoint);
                LocalNodeAddress = IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) NativeSocket.LocalEndPoint);
                RemoteNodeAddress = IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) NativeSocket.RemoteEndPoint);
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
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
        //    var read = await ReceiveAsync(listenerBuffer, 0, listenerBuffer.Length, endPoint).ZeroBoostAsync(oomCheck: false).ConfigureAwait(ZC);

        //    //fail fast
        //    if (read == 0)
        //        return false;

        //    //New connection?
        //    if (!_connTrack.TryGetValue(endPoint.ToString(), out var connection))
        //    {
        //        //Atomic add new route
        //        if (await ZeroAtomicAsync(async (z, b) => _connTrack.TryAdd(endPoint.ToString(),
        //            connection = await newConnectionHandler(new IoUdpSocket()).ConfigureAwait(ZC))).ConfigureAwait(ZC))
        //        {
        //            //atomic ensure teardown
        //            return await connection.ZeroAtomicAsync((z, b) =>
        //            {
        //                var sub = connection.ZeroSubscribe(s =>
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
        //            }).ConfigureAwait(ZC);
        //        }
        //        else if(connection != null)//race
        //        {
        //            await connection.ZeroAsync(this).ConfigureAwait(ZC);
        //        }
        //        //raced out
        //        return false;
        //    }

        //    await connection.IoSource.ProduceAsync(async (b, func, arg3, arg4) =>
        //    {
        //        var source = (IoNetSocket) b;

        //        return true;
        //    }).ConfigureAwait(ZC);
        //    //route

        //    return await connection!.ConsumeAsync();
        //}

        //private IIoZeroSemaphore _sendSync;
        //private IIoZeroSemaphore _rcvSync;

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
        public override async ValueTask<int> SendAsync(ReadOnlyMemory<byte> buffer, int offset, int length,
            EndPoint endPoint, int timeout = 0)
        {

            if (!NativeSocket.Poll(parm_socket_poll_wait_ms, SelectMode.SelectWrite))
                return 0;

            SocketAsyncEventArgs args = default;
            try
            {
                args = _argsHeap.Take();
                if (args == null)
                    throw new OutOfMemoryException(nameof(_argsHeap));

                var buf = Unsafe.As<ReadOnlyMemory<byte>, Memory<byte>>(ref buffer).Slice(offset, length);
                var send = new ValueTask<bool>((IIoManualResetValueTaskSourceCore<bool>)args.UserToken, 0);

                args.SetBuffer(buf);
                args.RemoteEndPoint = endPoint;

                //send
                if (NativeSocket.SendToAsync(args) && !await send.FastPath())
                    return 0;

                return args.SocketError == SocketError.Success ? args.BytesTransferred : 0;
            }
            catch(ObjectDisposedException){}
            catch (Exception) when (Zeroed()) { }
            catch (Exception e) when (!Zeroed())
            {
                var errMsg = $"Sending to {NativeSocket.LocalAddress()} ~> udp://{endPoint} failed, z = {Zeroed()}, zf = {ZeroedFrom?.Description}:";
                _logger.Error(e, errMsg);
                await DisposeAsync(this, errMsg).FastPath();
            }
            finally
            {
                _argsHeap?.Return(args);
            }

            return 0;
        }

        /// <summary>
        /// socket args heap
        /// </summary>
        private IoHeap<SocketAsyncEventArgs, IoUdpSocket> _argsHeap;

        /// <summary>
        /// Interacts with <see cref="SocketAsyncEventArgs"/> to complete async reads
        /// </summary>
        /// <param name="sender">The socket</param>
        /// <param name="eventArgs">The socket event args</param>
        private void ZeroCompletion(object sender, SocketAsyncEventArgs eventArgs)
        {
            try
            {
                var tcs = (IIoManualResetValueTaskSourceCore<bool>)eventArgs.UserToken;
                if(eventArgs.SocketError == SocketError.Success)
                    tcs.SetResult(!Zeroed());
                else
                    tcs.SetException(new SocketException((int)eventArgs.SocketError));
            }
            catch(Exception) when(Zeroed()){}
            catch(Exception e) when (!Zeroed())
            {
                try
                {
                    _logger?.Fatal(e,$"{Description}: udp signal callback failed!");
                }
                catch
                {
                    // ignored
                }
            }
        }

#if TEST_FAIL_LISTEN
        private static int _failOne;
#endif


        /// <summary>
        /// Read from the socket
        /// </summary>
        /// <param name="buffer">Read into a buffer</param>
        /// <param name="offset">Write start pos</param>
        /// <param name="length">Bytes to read</param>
        /// <param name="remoteEp"></param>
        /// <param name="timeout">Timeout after ms</param>
        /// <returns>Number of bytes received</returns>
        public override async ValueTask<int> ReceiveAsync(Memory<byte> buffer, int offset, int length, byte[] remoteEp, int timeout = 0)
        {
            try
            {
                if (length == 0)
                    return 0;

                if (timeout == 0)
                {
                    SocketAsyncEventArgs args = null;
                    try
                    {
                        args = _argsHeap.Take();

                        if (args == null)
                            throw new OutOfMemoryException(nameof(_argsHeap));

                        args.SetBuffer(buffer.Slice(offset, length));
                        var waitCore = new ValueTask<bool>((IIoManualResetValueTaskSourceCore<bool>)args.UserToken, 0);

                        try
                        {
                            if (NativeSocket.ReceiveFromAsync(args) && !await waitCore.FastPath())
                                return 0;
                        }
                        catch (SocketException e) when (!Zeroed() && e.SocketErrorCode == SocketError.OperationAborted)
                        {
#if DEBUG
                            _logger.Trace(e, $"{nameof(NativeSocket.ReceiveFromAsync)}:");
#endif
                            await DisposeAsync(this, e.Message).FastPath();
                        }
                        catch when (Zeroed()) { }
                        catch (Exception e) when (!Zeroed())
                        {
                            _logger.Error(e, $"{nameof(NativeSocket.ReceiveFromAsync)}:");
                            await DisposeAsync(this, e.Message).FastPath();
                            return 0;
                        }

                        args.RemoteEndPoint.AsBytes(remoteEp);
#if DEBUG
                        if (args.SocketError != SocketError.Success && args.SocketError != SocketError.OperationAborted)
                            _logger.Error($"{nameof(ReceiveAsync)}: socket error = {args.SocketError}");
#endif
                        LastError = args.SocketError;

                        return LastError == SocketError.Success ? args.BytesTransferred : 0;
                    }
                    catch when (Zeroed())
                    {
                    }
                    catch (Exception e) when (!Zeroed())
                    {
                        _logger.Error(e, "Receive udp failed:");
                        await DisposeAsync(this, $"{nameof(NativeSocket.ReceiveFromAsync)}: [FAILED] {e.Message}")
                            .FastPath();
                    }
                    finally
                    {
                        _argsHeap?.Return(args);

#if TEST_FAIL_LISTEN
                        if (Interlocked.CompareExchange(ref _failOne, 1, 0) == 0)
                        {
                            await DisposeAsync(this, "test").FastPath();
                            //AsyncTasks.Cancel(false);
                        }
#endif
                    }

                    return 0;
                }

                EndPoint remoteEpAny = new IPEndPoint(0, 0);
                if (MemoryMarshal.TryGetArray((ReadOnlyMemory<byte>)buffer, out var tBuf))
                {
                    NativeSocket.ReceiveTimeout = timeout;
                    var read = NativeSocket.ReceiveFrom(tBuf.Array!, offset, length, SocketFlags.None, ref remoteEpAny);
                    NativeSocket.ReceiveTimeout = 0;

                    remoteEpAny.AsBytes(remoteEp);
                    return read;
                }
            }
            catch (Win32Exception e) when (!Zeroed())
            {
                if (e.ErrorCode != 10035)
                {
                    _logger.Error(e, $"{nameof(NativeSocket.ReceiveFromAsync)}: ");
                }

                return 0;
            }
            catch (SocketException e) when (!Zeroed() && e.SocketErrorCode != SocketError.OperationAborted)
            {
                _logger.Error(e, $"{nameof(ReceiveAsync)}:");
            }
            catch (Exception) when (Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                var errMsg = $"Unable to read from socket: {Description}";
                _logger?.Error(e, errMsg);
                await DisposeAsync(this, errMsg).FastPath();
            }
            
            return 0;
        }

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
                return !Zeroed() && NativeSocket is { IsBound: true, Connected: true};
            }
            catch (ObjectDisposedException) { }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger.Error(e, Description);
            }

            return false;
        }

        /// <summary>
        /// Configures the socket
        /// </summary>
        protected override void ConfigureSocket()
        {
            if (NativeSocket.IsBound || NativeSocket.Connected)
                throw new InvalidOperationException();

            //set some socket options
            //NativeSocket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.PacketInformation, true);
        }
    }
}