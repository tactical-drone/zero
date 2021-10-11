﻿using System;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
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
            Init(concurrencyLevel);
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
            Proxy = true;
            Init(concurrencyLevel);
        }


        /// <summary>
        /// Initializes the socket
        /// </summary>
        /// <param name="concurrencyLevel"></param>
        public void Init(int concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
            //TODO tuning
            _sendSync = new IoZeroSemaphore("udp send lock", 128, 1, 0);
            _sendSync.ZeroRef(ref _sendSync, AsyncTasks.Token);

            _rcvSync = new IoZeroSemaphore("udp receive lock", 128, 1, 0);
            _rcvSync.ZeroRef(ref _rcvSync, AsyncTasks.Token);

            InitHeap(concurrencyLevel);
        }

        /// <summary>
        /// Initializes the heap
        /// </summary>
        private void InitHeap(int concurrencyLevel)
        {
            //TODO tuning
            concurrencyLevel = 128;

            _recvArgs = new IoHeap<SocketAsyncEventArgs>((uint)concurrencyLevel)
            {
                Make = o =>
                {
                    var args = new SocketAsyncEventArgs{ RemoteEndPoint = new IPEndPoint(0, 0)};
                    args.Completed += SignalAsync;
                    return args;
                }
            };

            _sendArgs = new IoHeap<SocketAsyncEventArgs>((uint)concurrencyLevel)
            {
                Make = o =>
                {
                    var args = new SocketAsyncEventArgs();
                    args.Completed += SignalAsync;
                    return args;
                }
            };

            _tcsHeap = new IoHeap<IIoZeroSemaphore>((uint)concurrencyLevel)
            {
                Make = o =>
                {
                    IIoZeroSemaphore tcs = new IoZeroSemaphore("tcs", concurrencyLevel, 0, 0);
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
            _logger = null;
            _recvArgs = null;
            _sendArgs = null;
            _tcsHeap = null;
            //RemoteNodeAddress = null;
            _sendSync = null;
            _rcvSync = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await _recvArgs.ZeroManagedAsync<object>((o, _) =>
            {
                o.Completed -= SignalAsync;
                o.UserToken = null;
                o.RemoteEndPoint = null;
                try
                {
                    o.SetBuffer(null,0,0);
                }
                catch
                {
                    // ignored
                }
                ((IDisposable) o).Dispose();

                return ValueTask.CompletedTask;
            }).FastPath().ConfigureAwait(false);

            await _sendArgs.ZeroManagedAsync<object>((o,_) =>
            {
                o.Completed -= SignalAsync;
                o.UserToken = null;
                o.RemoteEndPoint = null;
                ((IDisposable) o).Dispose();
                try
                {
                    o.SetBuffer(null,0,0);
                }
                catch
                {
                    // ignored
                }
                return ValueTask.CompletedTask;
            }).FastPath().ConfigureAwait(false);

            await _tcsHeap.ZeroManagedAsync<object>((o,_) =>
            {
                o.Zero();
                return ValueTask.CompletedTask;
            }).FastPath().ConfigureAwait(false);

            _sendSync.Zero();
            _rcvSync.Zero();

            await base.ZeroManagedAsync().FastPath().ConfigureAwait(false);
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
        /// <param name="bootstrapAsync">Bootstrap callback invoked when a listener has started</param>
        /// <returns>True if successful, false otherwise</returns>
        public override async ValueTask ListenAsync<T>(IoNodeAddress listeningAddress,
            Func<IoSocket,T, ValueTask> acceptConnectionHandler,
            T nanite,
            Func<ValueTask> bootstrapAsync = null)
        {
            //base
            await base.ListenAsync(listeningAddress, acceptConnectionHandler, nanite,bootstrapAsync).FastPath().ConfigureAwait(false);


            //set some socket options
            //NativeSocket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.PacketInformation, true);

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

                try
                {
                    //ZERO
                    await acceptConnectionHandler(this, nanite).FastPath().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"There was an error handling a new connection from {this.RemoteNodeAddress} to `{this.LocalNodeAddress}'");
                }
                
                //Call the new connection established handler
                

                //Bootstrap on listener start
                if (bootstrapAsync != null)
                    await bootstrapAsync().FastPath().ConfigureAwait(false);

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
            if (!await base.ConnectAsync(remoteAddress).FastPath().ConfigureAwait(false))
                return false;

            Configure();

            try
            {
                await NativeSocket.ConnectAsync(remoteAddress.IpEndPoint, AsyncTasks.Token).FastPath().ConfigureAwait(false);
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

        private IIoZeroSemaphore _sendSync;
        private IIoZeroSemaphore _rcvSync;

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
            try
            {
#if DEBUG
                if (_sendSync.NrOfBlockers >= _sendArgs.MaxSize / 2)
                    _logger.Warn(
                        $"{Description}: Send semaphore is running lean {_sendSync.NrOfBlockers}/{_sendArgs.MaxSize}");
#endif

                if (!await _sendSync.WaitAsync().FastPath().ConfigureAwait(false))
                    return 0;

                //fail fast
                if (Zeroed() || NativeSocket == null || !(NativeSocket.IsBound))
                {
                    if (!Zeroed())
                        _logger.Error(
                            $"Socket is ded? z = {Zeroed()}, from = {ZeroedFrom?.Description} bound = {NativeSocket?.IsBound}");
                    return 0;
                }

                if (endPoint == null)
                {
                    _logger.Fatal("No endpoint supplied!");
                    return 0;
                }


                //if (MemoryMarshal.TryGetArray(buffer, out var buf))
                {

                    //Task<int> sendTask;
                    ////lock(Socket)
                    //sendTask = NativeSocket.SendToAsync(buf, SocketFlags.None, endPoint);

                    ////slow path
                    //if (!sendTask.IsCompletedSuccessfully)
                    //    await sendTask.ConfigureAwait(false);

                    //return sendTask.Result;

                    var args = await _sendArgs.TakeAsync().FastPath().ConfigureAwait(false);

                    // var alloc = true;
                    // var args = new SocketAsyncEventArgs();
                    // args.Completed += Signal;

                    if (args == null)
                        throw new OutOfMemoryException(nameof(_sendArgs));

                    // while (/*args.Disposed ||*/
                    //        args.LastOperation != SocketAsyncOperation.None &&
                    //        args.LastOperation != SocketAsyncOperation.SendTo ||
                    //        args.SocketError != SocketError.Success)
                    // {
                    //     await _sendArgs.ReturnAsync(args, true).FastPath().ConfigureAwait(false); ;
                    //     _sendArgs.Take(out args);
                    // }

                    var tcs = await _tcsHeap.TakeAsync().FastPath().ConfigureAwait(false);
                    if (tcs == null)
                        throw new OutOfMemoryException(nameof(_tcsHeap));

                    try
                    {
                        //args.BufferList = null;
                        //args.DisconnectReuseSocket = false;
                        //args.AcceptSocket = null;
                        //args.SocketFlags = SocketFlags.None;
                        var buf = Unsafe.As<ReadOnlyMemory<byte>, Memory<byte>>(ref buffer).Slice(offset, length);
                        //_sendPin = buf.Pin();
                        args.SetBuffer(buf);
                        args.RemoteEndPoint = endPoint;
                        args.UserToken = tcs;

                        //receive
                        if (NativeSocket.SendToAsync(args) && !await tcs.WaitAsync().FastPath().ConfigureAwait(false))
                        {
                            return 0;
                        }

                        //args.SetBuffer(_tmpBuf, 0, 0);
                        return args.SocketError == SocketError.Success ? args.BytesTransferred : 0;
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e, "Send udp failed:");
                    }
                    finally
                    {
                        // if (!alloc)
                        // {
                        //     var dispose = args.SocketError != SocketError.Success;//|| args.Disposed;
                        //
                        //     if (dispose /*&& !args.Disposed*/)
                        //     {
                        //         args.SetBuffer(null, 0, 0);
                        //         args.Completed -= Signal;
                        //         args.Dispose();
                        //     }
                        //
                        //     await _sendArgs.ReturnAsync(args, dispose).FastPath().ConfigureAwait(false); ;
                        // }
                        // else
                        // {
                        //     args.Completed -= Signal;
                        // }

                        await _sendArgs.ReturnAsync(args).FastPath().ConfigureAwait(false);
                        await _tcsHeap.ReturnAsync(tcs).FastPath().ConfigureAwait(false);
                    }
                }
            }
            catch (Exception) when (Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger.Error(e,$"Sending to udp://{endPoint} failed, z = {Zeroed()}, zf = {ZeroedFrom?.Description}:");
                //await ZeroAsync(this).ConfigureAwait(false);
            }
            finally
            {
                await _sendSync.ReleaseAsync().FastPath().ConfigureAwait(false);
            }

            return 0;
        }

        /// <summary>
        /// socket args heap
        /// </summary>
        private IoHeap<SocketAsyncEventArgs> _recvArgs;

        /// <summary>
        /// socket args heap
        /// </summary>
        private IoHeap<SocketAsyncEventArgs> _sendArgs;

        /// <summary>
        /// task completion source
        /// </summary>
        private IoHeap<IIoZeroSemaphore> _tcsHeap;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async void SignalAsync(object sender, SocketAsyncEventArgs eventArgs)
        {
            try
            {
                await ((IIoZeroSemaphore)eventArgs.UserToken)!.ReleaseAsync().FastPath().ConfigureAwait(false);
            }
            catch(Exception) when(!Zeroed()){}
            catch(Exception e)
            {
                _logger.Fatal(e,$"{Description}: udp signal callback failed!");
                // ignored
            }
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
        /// <returns></returns>a
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, int offset, int length,
            IPEndPoint remoteEp, byte[] blacklist = null, int timeout = 0)
        {
            try
            {
                //concurrency
                if (!await _rcvSync.WaitAsync().FastPath().ConfigureAwait(false))
                    return 0;
                
                //fail fast
                if (Zeroed())
                    return 0;

                if (timeout == 0)
                {
                    var args = await _recvArgs.TakeAsync().FastPath().ConfigureAwait(false);
                    var tcs = await _tcsHeap.TakeAsync().FastPath().ConfigureAwait(false);
                    try
                    {
                        //var args = new SocketAsyncEventArgs();
                        //args.Completed += Signal;

                        //args = new SocketAsyncEventArgsExt();
                        //args.Completed += Signal;

                        if (args == null)
                            throw new OutOfMemoryException(nameof(_recvArgs));

                        if (tcs == null)
                            throw new OutOfMemoryException(nameof(_tcsHeap));

                        args.AcceptSocket = null;
                        args.SocketFlags = SocketFlags.None;

                        args.SetBuffer(buffer.Slice(offset, length));

                        args.UserToken = tcs;

                        if (NativeSocket.ReceiveFromAsync(args) && !await tcs.WaitAsync().FastPath().ConfigureAwait(false))
                        {
                            return 0;
                        }

                        remoteEp.Address = ((IPEndPoint)args.RemoteEndPoint)!.Address;
                        remoteEp.Port = ((IPEndPoint)args.RemoteEndPoint)!.Port;

                        return args.SocketError == SocketError.Success ? args.BytesTransferred : 0;
                        
                    }
                    catch (Exception e) when (!Zeroed())
                    {
                        _logger.Error(e, "Receive udp failed:");
                    }
                    finally
                    {

                        //if (MemoryMarshal.TryGetArray((ReadOnlyMemory<byte>) buffer, out var seg))
                        //{
                        //    StringWriter w = new StringWriter();
                        //    w.Write($"{buffer.GetHashCode()} ({args.BytesTransferred}):");
                        //    var nullc = 0;
                        //    var nulld = 0;
                        //    for (int i = 0; i < args.BytesTransferred; i++)
                        //    {
                        //        if (args.MemoryBuffer.Span[i] == 0)
                        //            nullc++;

                        //        if (buffer.Span[i + offset] == 0)
                        //            nulld++;

                        //        w.Write($" {seg[i + offset]}.");
                        //    }

                        //    if (nullc > 0 && (double) nullc / args.BytesTransferred > 0.2)
                        //    {

                        //    }

                        //    if (nulld > 0 && (double) nulld / args.BytesTransferred > 0.2)
                        //    {

                        //    }

                        //    _logger.Fatal(w);

                        //}
                        //args.UserToken = null;
                        //args.AcceptSocket = null;
                        //args.RemoteEndPoint = null;
                        
                        
                        //args.SetBuffer(null, 0, 0);

                        var dispose = /*args.Disposed ||*/ args.SocketError != SocketError.Success;
                        if (dispose)
                        {
                            args.SetBuffer(null, 0, 0);
                            args.Completed -= SignalAsync;
                            args.Dispose();
                        }

                        await _recvArgs.ReturnAsync(args, dispose).FastPath().ConfigureAwait(false);
                        await _tcsHeap.ReturnAsync(tcs).FastPath().ConfigureAwait(false);
                    }
                }

                if (timeout <= 0) return 0;


                EndPoint remoteEpAny = null;
                if (MemoryMarshal.TryGetArray((ReadOnlyMemory<byte>) buffer, out var tBuf))
                {
                    NativeSocket.ReceiveTimeout = timeout;
                    var read = NativeSocket.ReceiveFrom(tBuf.Array!, offset, length, SocketFlags.None, ref remoteEpAny); //                  
                    NativeSocket.ReceiveTimeout = 0;
                    remoteEp.Address = ((IPEndPoint) remoteEpAny).Address;
                    remoteEp.Port = ((IPEndPoint) remoteEpAny).Port;
                    return read;
                }

                return 0;
            }
            catch (Exception) when (Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger?.Error(e, $"Unable to read from socket: {Description}");
                //await ZeroAsync(this).ConfigureAwait(false); //TODO ?
            }
            finally
            {
                await _rcvSync.ReleaseAsync().FastPath().ConfigureAwait(false);
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
                return NativeSocket is {IsBound: true, LocalEndPoint: { }};
            }
            catch (ObjectDisposedException)
            {
                //ignored
            }
            catch (Exception e)
            {
                _logger.Trace(e, Description);
            }

            return false;
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

            NativeSocket.DontFragment = true;

            // Don't allow another socket to bind to this port.
            NativeSocket.ExclusiveAddressUse = true;

            // Set the receive buffer size to 32k
            //NativeSocket.ReceiveBufferSize = 8192 * 4;
            NativeSocket.ReceiveBufferSize = 8192 * 8;

            // Set the timeout for synchronous receive methods to
            // 1 second (1000 milliseconds.)
            NativeSocket.ReceiveTimeout = 100;

            // Set the send buffer size to 8k.
            //NativeSocket.SendBufferSize = 8192 * 2;
            NativeSocket.SendBufferSize = 8192 * 8;

            // Set the timeout for synchronous send methods
            // to 1 second (1000 milliseconds.)
            NativeSocket.SendTimeout = 2000;

            // Set the Time To Live (TTL) to 42 router hops.
            NativeSocket.Ttl = 64;
        }
    }
}