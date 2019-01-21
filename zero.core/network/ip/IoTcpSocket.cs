using System;
using System.Net.Sockets;
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
        /// <param name="cancellationToken">Token used to send cancellation signals</param>
        public IoTcpSocket(CancellationToken cancellationToken) : base(SocketType.Stream, ProtocolType.Tcp, cancellationToken)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// A copy constructor used by the listener to spawn new TCP connection handlers
        /// </summary>
        /// <param name="socket">The connecting socket</param>
        /// <param name="address">The address that was listened on where this socket was spawned</param>
        /// <param name="cancellationToken">Token used to send cancellation signals</param>
        public IoTcpSocket(Socket socket, IoNodeAddress address, CancellationToken cancellationToken) : base(socket, address, cancellationToken)
        {
            _logger = LogManager.GetCurrentClassLogger();
            IsListeningSocket = true;
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        readonly SemaphoreSlim _readLock = new SemaphoreSlim(1);
        readonly SemaphoreSlim _writeLock = new SemaphoreSlim(1);

        /// <summary>
        /// Starts a TCP listener
        /// </summary>
        /// <param name="address">The <see cref="IoNodeAddress"/> that this socket listener will initialize with</param>
        /// <param name="connectionHandler">A handler that is called once a new connection was formed</param>
        /// <returns></returns>
        public override async Task<bool> ListenAsync(IoNodeAddress address, Action<IoSocket> connectionHandler)
        {
            if (!await base.ListenAsync(address, connectionHandler))
                return false;

            try
            {
                Socket.Listen(parm_socket_listen_backlog);
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Socket listener `{Address}' returned with errors:");
                return false;
            }

            // Accept incoming connections
            while (!Spinners.Token.IsCancellationRequested)
            {
                _logger.Debug($"Listening for a new connection at `{Address}'");
                await Socket.AcceptAsync().ContinueWith(t =>
                {
                    switch (t.Status)
                    {
                        case TaskStatus.Canceled:
                        case TaskStatus.Faulted:
                            _logger.Error(t.Exception, $"Listener `{Address}' returned with status `{t.Status}':");
                            break;
                        case TaskStatus.RanToCompletion:

                            var newSocket = new IoTcpSocket(t.Result, Address, Spinners.Token);

                            //Do some pointless sanity checking
                            if (newSocket.LocalAddress != Address.Ip || newSocket.LocalPort != Address.Port)
                            {
                                _logger.Fatal($"New connection to `tcp://{newSocket.LocalIpAndPort}' should have been to `tcp://{Address.IpAndPort}'! Possible hackery! Investigate immediately!");
                                newSocket.Close();
                                break;
                            }

                            _logger.Debug($"New connection from `tcp://{newSocket.RemoteIpAndPort}' to `{Address}'");

                            try
                            {
                                connectionHandler(newSocket);
                            }
                            catch (Exception e)
                            {
                                _logger.Error(e, $"There was an error handling a new connection from `tcp://{newSocket.RemoteIpAndPort}' to `{newSocket.Address}'");
                            }
                            break;
                        default:
                            _logger.Error($"Listener for `{Address}' went into unknown state `{t.Status}'");
                            break;
                    }
                }, Spinners.Token);
            }

            _logger.Debug($"Listener at `{Address}' exited");
            return true;
        }

        /// <summary>
        /// Connect to a remote endpoint
        /// </summary>
        /// <param name="address">The address to connect to</param>
        /// <returns>True on success, false otherwise</returns>
        public override async Task<bool> ConnectAsync(IoNodeAddress address)
        {
            if (!await base.ConnectAsync(address))
                return false;

            return await Socket.ConnectAsync(Address.Ip, Address.Port).ContinueWith(r =>
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
                        if (Address.IpEndPoint.Address.ToString() != Socket.RemoteAddress().ToString() || Address.IpEndPoint.Port != Socket.RemotePort())
                        {
                            _logger.Fatal($"Connection to `tcp://{Address.IpAndPort}' established, but the OS reports it as `tcp://{Socket.RemoteAddress()}:{Socket.RemotePort()}'. Possible hackery! Investigate immediately!");
                            Socket.Close();
                            return Task.FromResult(false);
                        }

                        _logger.Info($"Connected to `{Address}'");
                        break;
                    default:
                        _logger.Error($"Connecting to `{Address}' returned with unknown state `{r.Status}'");
                        Socket.Close();
                        return Task.FromResult(false);
                }
                return Task.FromResult(true);
            }, Spinners.Token).Unwrap();
        }

        /// <summary>
        /// Sends data over TCP async
        /// </summary>
        /// <param name="getBytes">The buffer containing the data</param>
        /// <param name="offset">The offset into the buffer to start reading from</param>
        /// <param name="length">The length of the data to be sent</param>
        /// <returns>The amount of bytes sent</returns>
        public override async Task<int> SendAsync(byte[] getBytes, int offset, int length)
        {
            try
            {
                var task = Task.Factory
                    .FromAsync<int>(Socket.BeginSend(getBytes, offset, length, SocketFlags.None, null, null),
                        Socket.EndSend).HandleCancellation(Spinners.Token);
                await _writeLock.WaitAsync();
                await task.ContinueWith(t =>
                {
                    switch (t.Status)
                    {
                        case TaskStatus.Canceled:
                        case TaskStatus.Faulted:
                            _logger.Error(t.Exception, $"Sending to `tcp://{RemoteIpAndPort}' failed:");
                            Close();
                            break;
                        case TaskStatus.RanToCompletion:
                            _logger.Trace($"TX => `{length}' bytes to `tpc://{RemoteIpAndPort}'");
                            break;
                    }
                }, Spinners.Token);

                return task.Result;
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unable to send bytes to socket `tcp://{RemoteIpAndPort}' :");
                Close();
                return 0;
            }
            finally
            {
                _writeLock.Release();
            }
        }

        /// <inheritdoc />
        /// <summary>
        /// Reads data from a TCP socket async
        /// </summary>
        /// <param name="buffer">The buffer to read into</param>
        /// <param name="offset">The offset into the buffer</param>
        /// <param name="length">The maximum bytes to read into the buffer</param>        
        /// <returns>The number of bytes read</returns>
        public override async Task<int> ReadAsync(byte[] buffer, int offset, int length) //TODO can we go back to array buffers?
        {
            try
            {
                await _readLock.WaitAsync(Spinners.Token);                
                return await Task.Factory.FromAsync(
                    Socket.BeginReceive(buffer, offset, length, SocketFlags.None, null, null),
                    Socket.EndReceive).HandleCancellation(Spinners.Token);                
            }
            catch (Exception)
            {
                //_logger.Trace(e, $"Unable to read from socket `tcp://{Address.ResolvedIpAndPort}', length = `{length}', offset = `{offset}' :");
                return 0;
            }
            finally
            {
                _readLock.Release();
            }
        }

        /// <inheritdoc />
        /// <summary>
        /// Connection status
        /// </summary>
        /// <returns>True if the connection is up, false otherwise</returns>
        public override bool Connected()
        {
            return (Socket?.IsBound??false) && (Socket?.Connected??false);
        }
    }
}
