using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
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
        public IoTcpSocket() : base(SocketType.Stream, ProtocolType.Tcp)
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
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().ConfigureAwait(false);
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
        public override async Task ListenAsync(IoNodeAddress listeningAddress,
            Func<IoSocket, Task> acceptConnectionHandler,
            Func<Task> bootstrapAsync = null)
        {
            //base
            await base.ListenAsync(listeningAddress, acceptConnectionHandler, bootstrapAsync).ConfigureAwait(false);
            
            //Configure the socket
            Configure();

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
                await bootstrapAsync().ConfigureAwait(false);

            // Accept incoming connections
            while (!Zeroed())
            {
                _logger.Trace($"Waiting for a new connection to `{LocalNodeAddress}...'");

                try
                {
                    //ZERO control passed to connection handler
                    var newSocket = new IoTcpSocket(await NativeSocket.AcceptAsync().ConfigureAwait(false));
                    
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
                        await acceptConnectionHandler(newSocket).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        await newSocket.ZeroAsync(this).ConfigureAwait(false);
                        _logger.Error(e, $"There was an error handling a new connection from {newSocket.RemoteNodeAddress} to `{newSocket.LocalNodeAddress}'");
                    }
                }
                catch (ObjectDisposedException e) { _logger.Trace(e, Description);}
                catch (OperationCanceledException e) { _logger.Trace(e, Description); }
                catch (Exception e)
                {
                    if (!Zeroed())
                        _logger.Error(e, $"Listener at `{LocalNodeAddress}' returned with errors");
                }
            }

            _logger.Trace($"Listener {Description} exited");
        }


        private readonly Stopwatch _sw = Stopwatch.StartNew();

        /// <summary>
        /// Connecting
        /// </summary>
        private int WSAEWOULDBLOCK = 10035;

        /// <summary>
        /// Connect to a remote endpoint
        /// </summary>
        /// <param name="remoteAddress">The address to connect to</param>
        /// <returns>True on success, false otherwise</returns>
        public override async ValueTask<bool> ConnectAsync(IoNodeAddress remoteAddress)
        {
            if (!await base.ConnectAsync(remoteAddress).ConfigureAwait(false))
                return false;

            NativeSocket.Blocking = false;

            //Configure the socket
            Configure();

            _sw.Restart();
            try
            {
                await NativeSocket.ConnectAsync(remoteAddress.IpEndPoint).ConfigureAwait(false);
                LocalNodeAddress = IoNodeAddress.CreateFromEndpoint( "tcp", (IPEndPoint) NativeSocket.LocalEndPoint);
                RemoteNodeAddress = IoNodeAddress.CreateFromEndpoint("tcp", (IPEndPoint) NativeSocket.RemoteEndPoint);

                NativeSocket.Blocking = true;

                _logger.Trace($"Connected to `{RemoteNodeAddress}': ({Description})");
                return true;
            }
            catch (SocketException exception)
            {
                {
                    if (exception.ErrorCode == WSAEWOULDBLOCK)
                    {
                        while (!Zeroed() && _sw.ElapsedMilliseconds < 10000 &&
                               !NativeSocket.Poll(100000, SelectMode.SelectError) &&
                               !NativeSocket.Poll(100000, SelectMode.SelectWrite))
                        { }

                        if (Zeroed() || _sw.ElapsedMilliseconds > 10000)
                        {
                            NativeSocket.Close();
                            return false;
                        }
                    }
                }

                NativeSocket.Blocking = true;

                _logger.Trace($"Connected to `{RemoteNodeAddress}': ({Description})");
                return true;
            }
            catch (NullReferenceException e) { _logger.Trace(e, Description); }
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (Exception e)
            {
                _logger.Error(e, $"Connected to `{remoteAddress}' failed: {Description}");
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
                    return await NativeSocket.SendAsync(buffer.Slice(offset, length), SocketFlags.None, AsyncTasks.Token);
                }

                NativeSocket.SendTimeout = timeout;
                var sent = NativeSocket.Send(buffer.ToArray(), offset, length, SocketFlags.None);
                NativeSocket.SendTimeout = 0;
                return sent; //TODO optimize copy
            }
            catch (NullReferenceException e) {_logger.Trace(e, Description);}
            catch (TaskCanceledException e) {_logger.Trace(e, Description);}
            catch (OperationCanceledException e) {_logger.Trace(e, Description);}
            catch (ObjectDisposedException e) {_logger.Trace(e, Description);}
            catch (SocketException e)
            {
                if(!Zeroed())
                    _logger.Error(e, $"Send failed: {Description}");
            }
            catch (Exception e)
            {
                if (!Zeroed())
                    _logger.Error(e, $"Send failed: {Description}");
            }

            await ZeroAsync(this).ConfigureAwait(false);
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
                    return await NativeSocket.ReceiveAsync(buffer.Slice(offset, length), SocketFlags.None, AsyncTasks.Token);
                }

                //slow path: timeout
                if (MemoryMarshal.TryGetArray((ReadOnlyMemory<byte>) buffer, out var buf))
                {
                    NativeSocket.ReceiveTimeout = timeout;
                    var read = NativeSocket.Receive(buf.Array!, offset, length, SocketFlags.None);
                    NativeSocket.ReceiveTimeout = 0;
                    return read;
                }
            }
            catch (NullReferenceException e) { _logger.Trace(e, Description);}
            catch (TaskCanceledException e) { _logger.Trace(e, Description);}
            catch (OperationCanceledException e) { _logger.Trace(e, Description);}
            catch (ObjectDisposedException e) { _logger.Trace(e, Description);}
            catch (SocketException e)
            {
#if DEBUG
                _logger.Error($"{nameof(ReadAsync)}: [FAILED], {Description}, l = {length}, o = {offset}: {e.Message}");
#endif
                _logger.Trace(e, $"[FAILED], {Description}, length = `{length}', offset = `{offset}' :");
                await ZeroAsync(new IoNanoprobe($"SocketException ({e.Message})")).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unable to read from socket {Description}, length = `{length}', offset = `{offset}' :");
                await ZeroAsync(this).ConfigureAwait(false);
            }

            return 0;
        }

        private readonly byte[] _sentinelBuffer = new byte[0];
        /// <inheritdoc />
        /// <summary>
        /// Connection status
        /// </summary>
        /// <returns>True if the connection is up, false otherwise</returns>
        public override bool IsConnected()
        {
            try
            {
                return NativeSocket != null && NativeSocket.IsBound;//&& NativeSocket.Connected;//&& NativeSocket.Send(_sentinelBuffer, SocketFlags.None) == 0;
            }
            catch (Exception e)
            {
                _logger.Trace(e, Description);
                return false;
            }
        }

    }
}
