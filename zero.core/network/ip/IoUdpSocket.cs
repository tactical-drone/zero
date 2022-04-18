using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
using zero.core.misc;
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
        public IoUdpSocket(int concurrencyLevel) : base(SocketType.Dgram, ProtocolType.Udp, concurrencyLevel)
        {
            Init(concurrencyLevel, false);//TODO tuning
        }

        /// <summary>
        /// Used by pseudo listeners
        /// </summary>
        /// <param name="nativeSocket">The listening address</param>
        /// <param name="remoteEndPoint">The remote endpoint</param>
        /// <param name="concurrencyLevel"></param>
        public IoUdpSocket(Socket nativeSocket, IPEndPoint remoteEndPoint, int concurrencyLevel, bool clone) : base(nativeSocket, remoteEndPoint, concurrencyLevel)
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

        /// <summary>
        /// Initializes the socket
        /// </summary>
        /// <param name="concurrencyLevel"></param>
        /// <param name="recv"></param>
        public void Init(int concurrencyLevel, bool recv = true)
        {
            _logger = LogManager.GetCurrentClassLogger();
            //TODO tuning

            //NativeSocket.Blocking = false;
            //_sendSync = new IoZeroSemaphore("udp send lock", concurrencyLevel, 1, 0);
            //_sendSync.ZeroRef(ref _sendSync, AsyncTasks);

            //_rcvSync = new IoZeroSemaphore("udp receive lock", concurrencyLevel, 1, 0);
            //_rcvSync.ZeroRef(ref _rcvSync, AsyncTasks);

            InitHeap(concurrencyLevel, recv);
        }

        /// <summary>
        /// Initializes the heap
        /// </summary>
        private void InitHeap(int concurrencyLevel, bool recv = true)
        {
            if (recv)
            {
                _recvArgs = new IoHeap<SocketAsyncEventArgs, IoUdpSocket>($"{nameof(_recvArgs)}: {Description}", concurrencyLevel, static (_, @this) =>
                {
                    //sentinel
                    if (@this == null)
                        return new SocketAsyncEventArgs();

                    var args = new SocketAsyncEventArgs { RemoteEndPoint = new IPEndPoint(0, 0) };
                    args.Completed += @this.ZeroCompletion;
                    args.RemoteEndPoint = new IPEndPoint(IPAddress.Any, 53);
                    return args;
                })
                {
                    Context = this
                };
            }
            
            //TODO: tuning
            _sendArgs = new IoHeap<SocketAsyncEventArgs, IoUdpSocket>($"{nameof(_sendArgs)}: {Description}", concurrencyLevel * 4, static (_, @this) =>
            {
                //sentinel
                if (@this == null)
                    return new SocketAsyncEventArgs();

                var args = new SocketAsyncEventArgs();
                args.Completed += @this.ZeroCompletion;
                return args;
            })
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
            _recvArgs = null;
            _sendArgs = null;
            RemoteNodeAddress = null;
            //_sendSync = null;
            //_rcvSync = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();

            if (_recvArgs != null)
            {
                await _recvArgs.ZeroManagedAsync(static (o, @this) =>
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

                    return default;
                }, this).FastPath();
            }
            
            await _sendArgs.ZeroManagedAsync(static (o,@this) =>
            {
                o.Completed -= @this.ZeroCompletion;
                o.UserToken = null;
                //o.RemoteEndPoint = null;
                
                try
                {
                    o.SetBuffer(null,0,0);
                }
                catch
                {
                    // ignored
                }
                ((IDisposable)o).Dispose();
                return default;
            },this).FastPath();


            //_sendSync.ZeroSem();
            //_rcvSync.ZeroSem();
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
        /// <param name="bootstrapAsync">Bootstrap callback invoked when a listener has started</param>
        /// <returns>True if successful, false otherwise</returns>
        public override async ValueTask BlockOnListenAsync<T>(IoNodeAddress listeningAddress,
            Func<IoSocket,T, ValueTask> acceptConnectionHandler,
            T context,
            Func<ValueTask> bootstrapAsync = null)
        {
            //base
            await base.BlockOnListenAsync(listeningAddress, acceptConnectionHandler, context,bootstrapAsync).FastPath();

            //set some socket options
            //NativeSocket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.PacketInformation, true);

            //TODO sec
            NativeSocket.IOControl(
                (IOControlCode) SIO_UDP_CONNRESET,
                new byte[] {0, 0, 0, 0},
                null
            );

            //configure the socket
            ConfigureSocket();

            //Init connection tracking
            try
            {
                //_logger.Trace($"Waiting for a new connection to {LocalNodeAddress}...");

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
                if (bootstrapAsync != null)
                {
                    await ZeroAsync(static async bootstrapAsync =>
                    {
                        //TODO: tuning;
                        await Task.Delay(2000);
                        await bootstrapAsync().FastPath();
                    }, bootstrapAsync, TaskCreationOptions.DenyChildAttach).FastPath();
                }
                
                //block
                await AsyncTasks.Token.BlockOnNotCanceledAsync().FastPath();

                _logger.Trace($"Stopped listening at {LocalNodeAddress}");
            }
            catch when (Zeroed()) { }
            catch (Exception e)when (!Zeroed())
            {
                _logger.Error(e, $"Error while listening: {Description}");
            }
        }

        /// <summary>
        /// Connect to remote address
        /// </summary>
        /// <param name="remoteAddress">The remote to connect to</param>
        /// <param name="timeout">A timeout</param>
        /// <returns>True on success, false otherwise</returns>
        public override async ValueTask<bool> ConnectAsync(IoNodeAddress remoteAddress, int timeout)
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
        //    var read = await ReadAsync(listenerBuffer, 0, listenerBuffer.Length, endPoint).ZeroBoostAsync(oomCheck: false).ConfigureAwait(ZC);

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
            //#if DEBUG
            //                if (_sendSync.CurNrOfBlockers >= _sendArgs.Capacity / 2)
            //                    _logger.Warn(
            //                        $"{Description}: Send semaphore is running lean {_sendSync.CurNrOfBlockers}/{_sendArgs.Capacity}");

            //                Debug.Assert(endPoint != null);
            //#endif
            SocketAsyncEventArgs args = default;
            try
            {
                if (!IsConnected())
                    return 0;

                //if (!await _sendSync.WaitAsync().FastPath())
                //    return 0;

                args = _sendArgs.Take();
                if (args == null)
                    throw new OutOfMemoryException(nameof(_sendArgs));

                var buf = Unsafe.As<ReadOnlyMemory<byte>, Memory<byte>>(ref buffer).Slice(offset, length);
                var taskCore = new IoManualResetValueTaskSource<bool>();

                args.UserToken = taskCore;
                args.SetBuffer(buf);
                args.RemoteEndPoint = endPoint;

                var sent = new ValueTask<bool>(taskCore, 0);
                //receive
                if (NativeSocket.SendToAsync(args) && !await sent.FastPath())
                    return 0;

                return args.SocketError == SocketError.Success ? args.BytesTransferred : 0;
            }
            catch(ObjectDisposedException){}
            catch (Exception) when (Zeroed()) { }
            catch (Exception e) when (!Zeroed())
            {
                var errMsg = $"Sending to {NativeSocket.LocalAddress()} ~> udp://{endPoint} failed, z = {Zeroed()}, zf = {ZeroedFrom?.Description}:";
                _logger.Error(e, errMsg);
                await Zero(this, errMsg).FastPath();
            }
            finally
            {
                if (args != default)
                    _sendArgs.Return(args);

                //_sendSync.Release();
            }

            return 0;
        }

        /// <summary>
        /// socket args heap
        /// </summary>
        private IoHeap<SocketAsyncEventArgs, IoUdpSocket> _recvArgs;

        /// <summary>
        /// socket args heap
        /// </summary>
        private IoHeap<SocketAsyncEventArgs, IoUdpSocket> _sendArgs;


        /// <summary>
        /// Interacts with <see cref="SocketAsyncEventArgs"/> to complete async reads
        /// </summary>
        /// <param name="sender">The socket</param>
        /// <param name="eventArgs">The socket event args</param>
        private void ZeroCompletion(object sender, SocketAsyncEventArgs eventArgs)
        {
            try
            {
                var tcs = (IoManualResetValueTaskSource<bool>)eventArgs.UserToken;
                tcs.SetResult(!Zeroed() && eventArgs.SocketError == SocketError.Success);
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

        /// <summary>
        /// Read from the socket
        /// </summary>
        /// <param name="buffer">Read into a buffer</param>
        /// <param name="offset">Write start pos</param>
        /// <param name="length">Bytes to read</param>
        /// <param name="remoteEp"></param>
        /// <param name="timeout">Timeout after ms</param>
        /// <returns></returns>
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, int offset, int length, byte[] remoteEp,
            int timeout = 0)
        {
            try
            {
                if (!IsConnected())
                    return 0;

                //concurrency
                //if (!await _rcvSync.WaitAsync().FastPath())
                //    return 0;

                if (timeout == 0)
                {
                    SocketAsyncEventArgs args = null;
                    try
                    {
                        args = _recvArgs.Take();

                        if (args == null)
                            throw new OutOfMemoryException(nameof(_recvArgs));
                        var recvSource = new IoManualResetValueTaskSource<bool>();
                        
                        var receiveAsync = new ValueTask<bool>(recvSource, 0);
                        IValueTaskSource<bool> recvSourceRef = recvSource;

                        static void SetRef(ref IValueTaskSource<bool> s, SocketAsyncEventArgs a)
                        {
                            a.UserToken = s;
                        }
                        //args.UserToken = recvSource;
                        SetRef(ref recvSourceRef, args);
                        args.SetBuffer(buffer.Slice(offset, length));
                        
                        if (NativeSocket.ReceiveFromAsync(args) && !await receiveAsync.FastPath())
                        {
                            return 0;
                        }

                        args.RemoteEndPoint.AsBytes(remoteEp);
                        return args.SocketError == SocketError.Success ? args.BytesTransferred : 0;
                    }
                    catch when (Zeroed())
                    {
                    }
                    catch (Exception e) when (!Zeroed())
                    {
                        _logger.Error(e, "Receive udp failed:");
                        await Zero(this, $"{nameof(NativeSocket.ReceiveFromAsync)}: [FAILED] {e.Message}").FastPath();
                    }
                    finally
                    {
                        if (args != null)
                        {
                            var dispose = /*args.Disposed ||*/ args.SocketError != SocketError.Success;
                            if (dispose)
                            {
                                args.SetBuffer(null, 0, 0);
                                args.Completed -= ZeroCompletion;
                                args.Dispose();
                            }

                            _recvArgs.Return(args, dispose);
                        }
                    }
                }

                //TODO, heapify 
                EndPoint remoteEpAny = new IPEndPoint(0,0);
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
            catch (Exception) when (Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                var errMsg = $"Unable to read from socket: {Description}";
                _logger?.Error(e, errMsg);
                await Zero(this, errMsg).FastPath();
            }
            finally
            {
                //_rcvSync.Release();
            }

            return 0;
        }


        /// <summary>
        /// Receive callback
        /// </summary>
        /// <param name="ar">async result</param>
        private void RecvCallback(IAsyncResult ar)
        {
            try
            {
                var tcs = (IoZeroResetValueTaskSource<ValueTuple<int, EndPoint>>)ar.AsyncState;
                EndPoint ep = new IPEndPoint(IPAddress.Any, 0);
                tcs.SetResult((NativeSocket.EndReceiveFrom(ar, ref ep), ep));
            }
            catch (Exception) when (Zeroed()) { }
            catch (Exception e) when (!Zeroed())
            {
                try
                {
                    _logger?.Fatal(e, $"{Description}: udp signal callback failed!");
                }
                catch
                {
                    // ignored
                }
            }
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
                var connected = !Zeroed() && NativeSocket is { IsBound: true, LocalEndPoint: { } };

                return connected;
            }
            catch (ObjectDisposedException d)
            {
                _ = Zero(this, $"{nameof(IsConnected)}: {d.Message}");
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger?.Trace(e, Description);
            }

            return false;
        }

        /// <summary>
        /// Configures the socket
        /// </summary>
        protected override void ConfigureSocket()
        {
            if (NativeSocket.IsBound || NativeSocket.Connected)
            {
                return;
            }

            NativeSocket.DontFragment = true;

            // Don't allow another socket to bind to this port.
            NativeSocket.ExclusiveAddressUse = true;

            // Set the receive buffer size to 32k
            NativeSocket.ReceiveBufferSize = 8192 * 8;

            // Set the timeout for synchronous receive methods to
            // 1 second (1000 milliseconds.)
            NativeSocket.ReceiveTimeout = 1000;

            // Set the send buffer size to 32k.
            NativeSocket.SendBufferSize = 8192 * 8;

            // Set the timeout for synchronous send methods
            // to 1 second (1000 milliseconds.)
            NativeSocket.SendTimeout = 1000;

            // Set the Time To Live (TTL) to 42 router hops.
            NativeSocket.Ttl = 64;

            
        }
    }
}