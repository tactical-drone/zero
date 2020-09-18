using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
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
        /// Constructs a new TCP socket
        /// </summary>
        public IoTcpSocket() : base(SocketType.Stream, ProtocolType.Tcp)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            RemoteNodeAddress = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override async Task ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// A copy constructor used by the listener to spawn new TCP connection handlers
        /// </summary>
        /// <param name="socket">The connecting socket</param>
        /// <param name="listeningAddress">The address that was listened on where this socket was spawned</param>
        public IoTcpSocket(Socket socket, IoNodeAddress listeningAddress) : base(socket, listeningAddress)
        {
            _logger = LogManager.GetCurrentClassLogger();
            ListeningAddress = IoNodeAddress.CreateFromRemoteSocket(socket);
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Starts a TCP listener
        /// </summary>
        /// <param name="address">The <see cref="IoNodeAddress"/> that this socket listener will initialize with</param>
        /// <param name="connectionHandler">A handler that is called once a new connection was formed</param>
        /// <param name="bootstrapAsync"></param>
        /// <returns></returns>
        public override async Task<bool> ListenAsync(IoNodeAddress address, Func<IoSocket, Task> connectionHandler, Func<Task> bootstrapAsync = null)
        {
            if (!await base.ListenAsync(address, connectionHandler, bootstrapAsync).ConfigureAwait(false))
                return false;

            try
            {
                Socket.Listen(parm_socket_listen_backlog);
            }
            catch (Exception e)
            {
                _logger.Error(e, $" listener `{ListeningAddress}' returned with errors:");
                return false;
            }

            //Configure the socket
            ConfigureTcpSocket(Socket);

            //Execute bootstrap
            if(bootstrapAsync!=null)
                await bootstrapAsync().ConfigureAwait(false);

            // Accept incoming connections
            while (!Zeroed())
            {
                _logger.Debug($"Waiting for a new connection to `{ListeningAddress}...'");

                //var acceptTask = Socket?.AcceptAsync();
                var listenerAcceptTask = Task.Factory.FromAsync(Socket.BeginAccept(null, null), Socket.EndAccept);//.HandleCancellationAsync(AsyncTasks.Token); //TODO

                try
                {
                    //ZERO control passed to connection handler
                    var newSocket = new IoTcpSocket(await listenerAcceptTask.ConfigureAwait(false), ListeningAddress)
                    {
                        Kind = Connection.Ingress
                    };

                    //newSocket.ClosedEvent((sender, args) => Close());

                    //Do some pointless sanity checking
                    //if (newSocket.LocalAddress != ListeningAddress.Ip || newSocket.LocalPort != ListeningAddress.Port)
                    //{
                    //    _logger.Fatal($"New connection to `tcp://{newSocket.LocalIpAndPort}' should have been to `tcp://{ListeningAddress.IpPort}'! Possible hackery! Investigate immediately!");
                    //    newSocket.Close();
                    //    break;
                    //}

                    _logger.Debug($"New connection from `tcp://{newSocket.RemoteIpAndPort}' to `{ListeningAddress}' ({Description})");

                    try
                    {
                        //ZERO
                        await connectionHandler(newSocket).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e,
                            $"There was an error handling a new connection from `tcp://{newSocket.RemoteIpAndPort}' to `{newSocket.ListeningAddress}'");
                    }
                }
                catch (ObjectDisposedException) { }
                catch (OperationCanceledException) { }
                catch (Exception e)
                {
                    if (!Zeroed())
                        _logger.Error(e, $"Listener at `{ListeningAddress}' returned with errors");
                }
            }

            _logger.Debug($"Listener {Description} exited");
            return true;
        }


        private readonly Stopwatch _sw = Stopwatch.StartNew();

        /// <summary>
        /// Connecting
        /// </summary>
        private int WSAEWOULDBLOCK = 10035;

        /// <summary>
        /// Connect to a remote endpoint
        /// </summary>
        /// <param name="address">The address to connect to</param>
        /// <returns>True on success, false otherwise</returns>
        public override async Task<bool> ConnectAsync(IoNodeAddress address)
        {
            if (!await base.ConnectAsync(address).ConfigureAwait(false))
                return false;

            Socket.Blocking = false;

            //Configure the socket
            ConfigureTcpSocket(Socket);

            _sw.Restart();
            try
            {
                await Socket.ConnectAsync(address.Ip, address.Port).ConfigureAwait(false);

                Socket.Blocking = true;
                //Do some pointless sanity checking
                if (ListeningAddress.IpEndPoint.Address.ToString() != Socket.RemoteAddress().ToString() || ListeningAddress.IpEndPoint.Port != Socket.RemotePort())
                {
                    _logger.Fatal($"Connection to `tcp://{ListeningAddress.IpPort}' established, but the OS reports it as `tcp://{Socket.RemoteAddress()}:{Socket.RemotePort()}'. Possible hackery! Investigate immediately!");
                    Socket.Close();
                    return false;
                }

                _logger.Debug($"Connected to `{ListeningAddress}' ({Description})");
                return true;
            }
            catch (SocketException exception)
            {
                {
                    if (exception.ErrorCode == WSAEWOULDBLOCK)
                    {
                        while (!Zeroed() && _sw.ElapsedMilliseconds < 10000 &&
                               !Socket.Poll(100000, SelectMode.SelectError) &&
                               !Socket.Poll(100000, SelectMode.SelectWrite))
                        { }

                        if (Zeroed() || _sw.ElapsedMilliseconds > 10000)
                        {
                            Socket.Close();
                            return false;
                        }
                    }
                }

                Socket.Blocking = true;

                //Do some pointless sanity checking
                try
                {
                    if (!Zeroed() && ListeningAddress.IpEndPoint.Address.ToString() != Socket.RemoteAddress().ToString() || ListeningAddress.IpEndPoint.Port != Socket.RemotePort())
                    {
                        _logger.Fatal($"Connection to `tcp://{ListeningAddress.IpPort}' established, but the OS reports it as `tcp://{Socket.RemoteAddress()}:{Socket.RemotePort()}'. Possible hackery! Investigate immediately!");
                        Socket.Close();
                        return false;
                    }
                }
                catch (SocketException e) { _logger.Trace(e, Description); }
                catch (NullReferenceException e) { _logger.Trace(e, Description); }
                catch (Exception e)
                {
                    _logger.Error(e, $"Sanity checks failed: {Description}");
                }

                _logger.Debug($"Connected to `{ListeningAddress}' ({Description})");
                return true;
            }
            catch (NullReferenceException e) { _logger.Trace(e, Description); }
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (Exception e)
            {
                _logger.Error(e, $"Conneting {Description} failed:");
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
        /// <returns>The amount of bytes sent</returns>
        public override async ValueTask<int> SendAsync(ArraySegment<byte> buffer, int offset, int length, EndPoint endPoint = null, int timeout = 0)
        {
            try
            {
                if (timeout == 0)
                {
                    return await Socket.SendAsync(buffer.Slice(offset, length), SocketFlags.None, AsyncTasks.Token).ConfigureAwait(false);
                }

                Socket.SendTimeout = timeout;
                return Socket.Send(buffer.Array!, offset, length, SocketFlags.None);
            }
            catch (NullReferenceException) { }
            catch (TaskCanceledException) { }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
            catch (SocketException e)
            {
                _logger.Trace(e, $"Failed to send on {Key}:");
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unable to send bytes to ({(Zeroed() ? "closed" : "open")})[connected = {Socket.Connected}] socket `tcp://{RemoteIpAndPort}' :");
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
        /// <param name="timeout">A timeout</param>
        /// <returns>The number of bytes read</returns>
        public override async ValueTask<int> ReadAsync(ArraySegment<byte> buffer, int offset, int length, int timeout = 0) //TODO can we go back to array buffers?
        {
            try
            {
                if (timeout == 0)
                {
                    return await Socket.ReceiveAsync(buffer.Slice(offset, length), SocketFlags.None,
                            AsyncTasks.Token).ConfigureAwait(false);
                }

                Socket.ReceiveTimeout = timeout;
                return Socket.Receive(buffer.Array!, offset, length, SocketFlags.None);
            }
            catch (NullReferenceException e) { _logger.Trace(e, Description);}
            catch (TaskCanceledException e) { _logger.Trace(e, Description);}
            catch (OperationCanceledException e) { _logger.Trace(e, Description);}
            catch (ObjectDisposedException e) { _logger.Trace(e, Description);}
            catch (SocketException e)
            {
                _logger.Error(e, $"[FAILED], {Description}, length = `{length}', offset = `{offset}' :");
                await ZeroAsync(this).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unable to read from socket {Description}, length = `{length}', offset = `{offset}' :");
                await ZeroAsync(this).ConfigureAwait(false);
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
            return (Socket?.IsBound ?? false) || (Socket?.Connected ?? false);
        }

        public override object ExtraData()
        {
            return null;
        }

        /// <summary>
        /// Configures the socket
        /// </summary>
        /// <param name="tcpSocket"></param>
        void ConfigureTcpSocket(Socket tcpSocket)
        {
            if (tcpSocket.IsBound || tcpSocket.Connected)
            {
                return;
            }

            // Don't allow another socket to bind to this port.
            tcpSocket.ExclusiveAddressUse = true;

            // The socket will linger for 10 seconds after
            // Socket.Close is called.
            tcpSocket.LingerState = new LingerOption(true, 10);

            // Disable the Nagle Algorithm for this tcp socket.
            tcpSocket.NoDelay = true;

            // Set the receive buffer size to 8k
            tcpSocket.ReceiveBufferSize = 8192 * 2;

            // Set the timeout for synchronous receive methods to
            // 1 second (1000 milliseconds.)
            tcpSocket.ReceiveTimeout = 1000;

            // Set the send buffer size to 8k.
            tcpSocket.SendBufferSize = 8192 * 2;

            // Set the timeout for synchronous send methods
            // to 1 second (1000 milliseconds.)
            tcpSocket.SendTimeout = 1000;

            // Set the Time To Live (TTL) to 42 router hops.
            tcpSocket.Ttl = 42;

            _logger.Trace($"Tcp Socket configured: {Description}:" +
                $"  ExclusiveAddressUse {tcpSocket.ExclusiveAddressUse}" +
                $"  LingerState {tcpSocket.LingerState.Enabled}, {tcpSocket.LingerState.LingerTime}" +
                $"  NoDelay {tcpSocket.NoDelay}" +
                $"  ReceiveBufferSize {tcpSocket.ReceiveBufferSize}" +
                $"  ReceiveTimeout {tcpSocket.ReceiveTimeout}" +
                $"  SendBufferSize {tcpSocket.SendBufferSize}" +
                $"  SendTimeout {tcpSocket.SendTimeout}" +
                $"  Ttl {tcpSocket.Ttl}" +
                $"  IsBound {tcpSocket.IsBound}");
        }
    }
}
