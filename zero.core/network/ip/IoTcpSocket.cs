using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.misc;
using NLog;

namespace zero.core.network.ip
{
    /// <summary>
    /// The TCP flavor of <see cref="IoSocket"/>
    /// </summary>
    class IoTcpSocket : IoNetSocket
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
        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override void ZeroManaged()
        {
            base.ZeroManaged();

            _logger.Debug($"Zeroed {ListeningAddress}");
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
        /// <returns></returns>
        public override async Task<bool> ListenAsync(IoNodeAddress address, Action<IoSocket> connectionHandler)
        {
            if (!await base.ListenAsync(address, connectionHandler).ConfigureAwait(false))
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

            Task<Socket> acceptTask = null;

            // Accept incoming connections
            while (!Spinners.Token.IsCancellationRequested && !Zeroed())
            {
                _logger.Debug($"Waiting for a new connection to `{ListeningAddress}...'");
                acceptTask = Socket?.AcceptAsync();

                try
                {
                    if (acceptTask != null)
                        await acceptTask.ContinueWith(t =>
                        {
                            switch (t.Status)
                            {
                                case TaskStatus.Canceled:
                                case TaskStatus.Faulted:
                                    _logger.Error(t.Exception,
                                        $"Listener `{ListeningAddress}' returned with status `{t.Status}':");
                                    break;
                                case TaskStatus.RanToCompletion:

                                    //ZERO control passed to connection handler
                                    var newSocket = new IoTcpSocket(t.Result, ListeningAddress)
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

                                    _logger.Debug(
                                        $"New connection from `tcp://{newSocket.RemoteIpAndPort}' to `{ListeningAddress}'");

                                    try
                                    {
                                        //ZERO
                                        connectionHandler(newSocket);
                                    }
                                    catch (Exception e)
                                    {
                                        _logger.Error(e,
                                            $"There was an error handling a new connection from `tcp://{newSocket.RemoteIpAndPort}' to `{newSocket.ListeningAddress}'");
                                    }

                                    break;
                                default:
                                    _logger.Error(
                                        $"Listener for `{ListeningAddress}' went into unknown state `{t.Status}'");
                                    break;
                            }
                        }, Spinners.Token).ConfigureAwait(false);
                }
                catch (ObjectDisposedException){}
                catch (OperationCanceledException) {}
                catch (Exception e)
                {
                    _logger.Error(e, $"Listener at `{ListeningAddress}' returned with errors");
                }
            }

            _logger.Debug($"Listener at `{ListeningAddress}' exited");
            return true;
        }

        /// <summary>
        /// Connect to a remote endpoint
        /// </summary>
        /// <param name="address">The address to connect to</param>
        /// <returns>True on success, false otherwise</returns>
        public override async Task<bool> ConnectAsync(IoNodeAddress address)
        {
            if (!await base.ConnectAsync(address).ConfigureAwait(false))
                return false;

            return await Socket.ConnectAsync(address.Ip, address.Port).ContinueWith(r =>
            {
                switch (r.Status)
                {
                    case TaskStatus.Canceled:
                    case TaskStatus.Faulted:
                        //_logger.Error(r.Exception, $"Connecting to `{Address}' failed:");
                        Socket.Close();
                        return Task.FromResult(false);
                    case TaskStatus.RanToCompletion:
                        //Do some pointless sanity checking
                        if (ListeningAddress.IpEndPoint.Address.ToString() != Socket.RemoteAddress().ToString() || ListeningAddress.IpEndPoint.Port != Socket.RemotePort())
                        {
                            _logger.Fatal($"Connection to `tcp://{ListeningAddress.IpPort}' established, but the OS reports it as `tcp://{Socket.RemoteAddress()}:{Socket.RemotePort()}'. Possible hackery! Investigate immediately!");
                            Socket.Close();
                            return Task.FromResult(false);
                        }

                        _logger.Info($"Connected to `{ListeningAddress}'");
                        break;
                    default:
                        _logger.Error($"Connecting to `{ListeningAddress}' returned with unknown state `{r.Status}'");
                        Socket.Close();
                        return Task.FromResult(false);
                }
                return Task.FromResult(true);
            }, Spinners.Token).Unwrap().ConfigureAwait(false);
        }

        /// <summary>
        /// Sends data over TCP async
        /// </summary>
        /// <param name="buffer">The buffer containing the data</param>
        /// <param name="offset">The offset into the buffer to start reading from</param>
        /// <param name="length">The length of the data to be sent</param>
        /// <param name="endPoint">not used</param>
        /// <returns>The amount of bytes sent</returns>
        public override async Task<int> SendAsync(byte[] buffer, int offset, int length, EndPoint endPoint = null)
        {
            try
            {
                if (Zeroed())
                {
                    await Task.Delay(1000).ConfigureAwait(false);
                    return 0;
                }

                //var task = Task.Factory
                //    .FromAsync<int>(Socket.BeginSend(buffer, offset, length, SocketFlags.None, null, null)!,
                //        Socket.EndSend);//.HandleCancellation(Spinners.Token); //TODO
                Task<int> task = null;
                if (MemoryMarshal.TryGetArray<byte>(buffer, out var arraySegment))
                {
                    task = Socket.SendAsync(arraySegment.Slice(offset, length), SocketFlags.None, Spinners.Token).AsTask();
                }

                if (task != null)
                {
                    await task.ContinueWith(t =>
                    {
                        switch (t.Status)
                        {
                            case TaskStatus.Canceled:
                            case TaskStatus.Faulted:
                                _logger.Error(t.Exception, $"Sending to `tcp://{RemoteIpAndPort}' failed:");
                                Zero();
                                break;
                            case TaskStatus.RanToCompletion:
                                _logger.Trace($"TX => `{length}' bytes to `tpc://{RemoteIpAndPort}'");
                                break;
                        }
                    }, Spinners.Token).ConfigureAwait(false);

                    return task.Result;
                }

                return 0;
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unable to send bytes to socket `tcp://{RemoteIpAndPort}' :");
#pragma warning disable 4014
                Zero();
#pragma warning restore 4014
                return 0;
            }
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
        public override async Task<int> ReadAsync(byte[] buffer, int offset, int length, int timeout = 0) //TODO can we go back to array buffers?
        {
            try
            {
                if (Zeroed())
                {
                    await Task.Delay(250, Spinners.Token).ConfigureAwait(false);
                    return 0;
                }

                Socket.ReceiveTimeout = timeout;

                if (timeout == 0)
                {
                    int read = 0;
                    //TODO 
                    if (MemoryMarshal.TryGetArray<byte>(buffer, out var arraySegment)  && Socket.Available > 0)
                    {
                        var t = Socket.ReceiveAsync(arraySegment.Slice(offset, length), SocketFlags.None, Spinners.Token).AsTask();
                        read = await t.ConfigureAwait(false);
                    }
                    else
                    {
                        await Task.Delay(250, Spinners.Token).ConfigureAwait(false);
                        return 0;
                    }

                    //var read = await Task.Factory.FromAsync(
                    //        Socket?.BeginReceive(buffer, offset, length, SocketFlags.None, null, null)!,
                    //        Socket.EndReceive)
                    //    .ConfigureAwait(false); //.HandleCancellation(Spinners.Token).ConfigureAwait(false); //TODO

                    //var read = Socket?.Receive(buffer, offset, length, SocketFlags.None);

                    if (!Socket.Connected || !Socket.IsBound)
                    {
                        _logger.Debug($"{Key}: Connected = {Socket.Connected}, IsBound = {Socket.IsBound}");
#pragma warning disable 4014
                        Zero();
#pragma warning restore 4014
                    }
                    
                    return read;
                }
                else if (timeout > 0)
                {
                    return Socket.Receive(buffer, offset, length, SocketFlags.None);
                }
            }
            catch (ObjectDisposedException) { }
            catch (OperationCanceledException) { }
            catch (Exception e)
            {
                _logger.Debug(e, $"Unable to read from socket `{Key}', length = `{length}', offset = `{offset}' :");
#pragma warning disable 4014
                Zero();
#pragma warning restore 4014
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
            return (Socket?.IsBound??false) && (Socket?.Connected??false);
        }

        public override object ExtraData()
        {
            return null;
        }
    }
}
