using System;
using System.ComponentModel;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
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
        /// <param name="kind"></param>
        /// <param name="clone">Operator overloaded, parm not used</param>
#pragma warning disable IDE0060 // Remove unused parameter
        public IoUdpSocket(Socket nativeSocket, IPEndPoint remoteEndPoint, int concurrencyLevel,
            Connection kind,
            bool clone = false) : base(nativeSocket, kind, remoteEndPoint, concurrencyLevel)
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
        public IoUdpSocket(Socket nativeSocket, IPEndPoint remoteEndPoint, int concurrencyLevel) : base(nativeSocket, Connection.Ingress, remoteEndPoint, concurrencyLevel)
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
            //TODO: tuning
            _endPointHeap = new IoHeap<IPEndPoint>($"{nameof(_endPointHeap)}: {Description}", concurrencyLevel << 4, static (_, @this) => new IPEndPoint(IPAddress.Any, 0), autoScale: true);
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>s
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _logger = null;
            _endPointHeap = null;
            RemoteNodeAddress = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();

            if (_endPointHeap != null)
                await _endPointHeap.ZeroManagedAsync<object>();
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
            //_ = NativeSocket.IOControl(
            //    (IOControlCode)SIO_UDP_CONNRESET,
            //    new byte[] {0,0,0,0},
            //    null
            //);

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
        public async ValueTask<int> SendAsync(ReadOnlyMemory<byte> buffer, int offset, int length, EndPoint endPoint, int timeout = 0)
        {
            try
            {
                var core = new IoManualResetValueStructTaskSource<bool>();
                var waitCore = new ValueTask<bool>(core, 0);

                try
                {
                    var asyncResult = NativeSocket.BeginSendTo( buffer.AsArray(), offset, length, SocketFlags.None, endPoint, 
                        static result => ((IoManualResetValueStructTaskSource<bool>)result.AsyncState).SetResult(result.IsCompleted), core);

                    if (!await waitCore.FastPath())
                        return 0;

                    return NativeSocket.EndSendTo(asyncResult);
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

            }
            catch (ObjectDisposedException)
            {
            }
            catch (Exception) when (Zeroed())
            {
            }
            catch (SocketException e) when (!Zeroed() && e.ErrorCode == (int)SocketError.AddressNotAvailable)
            {
                var errMsg = $"Sending to :{NativeSocket.LocalPort()} ~> udp://{endPoint} failed ({endPoint}), z = {Zeroed()}, zf = {ZeroedFrom?.Description}:";
                _logger.Warn(e, errMsg);
            }       
            catch (Exception e) when (!Zeroed())
            {
                var errMsg = $"Sending to :{NativeSocket.LocalPort()} ~> udp://{endPoint} failed ({endPoint}), z = {Zeroed()}, zf = {ZeroedFrom?.Description}:";
                _logger.Error(e, errMsg);
                await DisposeAsync(this, errMsg).FastPath();
            }

            return 0;
        }


        public override ValueTask<int> SendAsync(ReadOnlyMemory<byte> buffer, int offset, int length,
            EndPoint endPoint,
            long crc = 0,
            int timeout = 0)
        {
            if (crc == 0)
                return SendAsync(buffer, offset, length, endPoint, timeout);

            return DupChecker.TryAdd(crc, Environment.TickCount) ? SendAsync(buffer, offset, length, endPoint, timeout) : new ValueTask<int>(-1);
        }

        /// <summary>
        /// socket args heap
        /// </summary>
        private IoHeap<IPEndPoint> _endPointHeap;

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
                if(eventArgs.SocketError is SocketError.Success or SocketError.ConnectionReset)
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

        private static EndPoint _anyAddress = new IPEndPoint(IPAddress.Any, 0);

        /// <summary>
        /// Read from the socket
        /// </summary>
        /// <param name="buffer">Read into a buffer</param>
        /// <param name="offset">Write start pos</param>
        /// <param name="length">Bytes to read</param>
        /// <param name="remoteEp">The address we received a frame from</param>
        /// <param name="timeout">Timeout after ms</param>
        /// <returns>Number of bytes received</returns>
        public override async ValueTask<int> ReceiveAsync(Memory<byte> buffer, int offset, int length, byte[] remoteEp, int timeout = 0)
        {
            try
            {
                if (timeout == 0)
                {
                    try
                    {
                        var core = new IoManualResetValueStructTaskSource<bool>();
                        var waitCore = new ValueTask<bool>(core, 0);

                        var read = 0;
                        try
                        {
                            EndPoint ep = null;
                            
                            var asyncResult = NativeSocket.BeginReceiveFrom(buffer.AsArray(), offset, length, SocketFlags.None, ref _anyAddress,
                                static result => ((IoManualResetValueStructTaskSource<bool>)result.AsyncState).SetResult(result.IsCompleted), core);

                            if (!await waitCore.FastPath())
                                return 0;

                            try
                            {
                                ep = _endPointHeap.Take();
                                if (ep == null)
                                    return 0;
                                Interlocked.MemoryBarrier();
                                read = NativeSocket.EndReceiveFrom(asyncResult, ref ep);
                                ep.AsBytes(remoteEp);
                            }
                            catch when(Zeroed()){}
                            catch (SocketException e)when(!Zeroed())
                            {
                                _logger.Trace( $"{nameof(ReceiveAsync)}: ({e.Message}); {Description}");
                                LastError = e.SocketErrorCode;//TODO: instagib
                            }
                            catch(SocketException e){ LastError = e.SocketErrorCode; }
                            catch(Exception e) when(!Zeroed())
                            {
                                _logger.Error(e, $"{nameof(ReceiveAsync)}: Failed!; {Description}");
                            }
                            finally
                            {
                                _endPointHeap?.Return((IPEndPoint)ep);
                            }
                        }
                        catch (SocketException e) when (!Zeroed() && e.SocketErrorCode == SocketError.OperationAborted)
                        {
#if DEBUG
                            _logger.Trace(e, $"{nameof(NativeSocket.ReceiveFromAsync)}:");
#endif
                            LastError = e.SocketErrorCode;//TODO: instagib
                        }
                        catch when (Zeroed()) { }
                        catch (Exception e) when (!Zeroed())
                        {
                            _logger.Error(e, $"{nameof(NativeSocket.ReceiveFromAsync)}:");
                            return 0;
                        }

                        return read;
                    }
                    catch when (Zeroed())
                    {
                    }
                    catch (Exception e) when (!Zeroed())
                    {
                        _logger.Error(e, "Receive udp failed:");
                        await DisposeAsync(this, $"{nameof(NativeSocket.ReceiveFromAsync)}: [FAILED] {e.Message}").FastPath();
                    }
                    
                    return 0;
                }

                EndPoint remoteEpAny = null;
                try
                {
                    remoteEpAny = _endPointHeap.Take();
                    if (MemoryMarshal.TryGetArray((ReadOnlyMemory<byte>)buffer, out var tBuf))
                    {
                        NativeSocket.ReceiveTimeout = timeout;
                        var read = NativeSocket.ReceiveFrom(tBuf.Array!, offset, length, SocketFlags.None, ref remoteEpAny);
                        NativeSocket.ReceiveTimeout = 0;

                        remoteEpAny.AsBytes(remoteEp);
                        return read;
                    }
                }
                finally
                {
                    _endPointHeap?.Return((IPEndPoint)remoteEpAny);
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