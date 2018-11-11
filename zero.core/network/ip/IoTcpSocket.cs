using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.misc;
using NLog;
using zero.core.models;

namespace zero.core.network.ip
{
    /// <summary>
    /// The TCP flaviour of <see cref="IoSocket"/>
    /// </summary>
    class IoTcpSocket :IoSocket
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
        /// <param name="rawSocket">The connecting socket</param>
        /// <param name="address">The address that was listened on where this socket was spawned</param>
        /// <param name="port">The listening port</param>
        /// <param name="cancellationToken">Token used to send cancellation signals</param>
        public IoTcpSocket(Socket rawSocket, string address, int port, CancellationToken cancellationToken) : base(rawSocket, address, port, cancellationToken)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Starts a TCP listener
        /// </summary>
        /// <param name="address">The local address to listen on</param>
        /// <param name="port">The local port to listen on</param>
        /// <param name="callback">A handler that is called once a new connection was formed</param>
        /// <returns></returns>
        public override async Task ListenAsync(string address, int port, Action<IoSocket> callback)
        {
            await base.ListenAsync(address, port, callback);
            try
            {
                // start the listener
                RawSocket.Listen(parm_socket_listen_backlog);
            }
            catch (SocketException e)
            {
                _logger.Error(e, $"Failed to listen on {address}:{port}");
                throw;
            }

            // Accept incoming connections
            while (!Spinners.Token.IsCancellationRequested)
            {
                _logger.Debug($"Listening for new connection on `{Protocol}{address}:{port}'");
                await RawSocket.AcceptAsync().ContinueWith(t =>
                {
                    switch (t.Status)
                    {
                        case TaskStatus.Canceled:
                        case TaskStatus.Faulted:
                            _logger.Warn($"Listener `{Protocol}{address}:{port}' returned with status {t.Status}");
                            break;
                        case TaskStatus.RanToCompletion:

                            if (t.Result != null)
                            {
                                var newSocket = new IoTcpSocket(t.Result, address, port, Spinners.Token);
                                _logger.Debug($"Incoming connection from `{newSocket.Protocol}{newSocket.RemoteAddress}:{newSocket.RemotePort}' to `{newSocket.Protocol}{newSocket.LocalAddress}:{newSocket.LocalPort}'");

                                try
                                {
                                    //Call the new connection esablished handler
                                    callback(newSocket);
                                }
                                catch (Exception e)
                                {
                                    _logger.Error(e, $"There was an error handling a successful connection from `{newSocket.Protocol}{newSocket.RemoteAddress}:{newSocket.RemotePort}' to `{newSocket.Protocol}{newSocket.LocalAddress}:{newSocket.LocalPort}'");
                                }
                            }
                            else
                            {
                                _logger.Warn("Connection listener ran to completion but no connection found!");
                            }

                            break;
                        default:
                            _logger.Error($"Listener `{Protocol}{address}:{port}' went into unknown state {t.Status}");
                            break;
                    }
                }, Spinners.Token);
            }
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
                    .FromAsync<int>(RawSocket.BeginSend(getBytes, offset, length, SocketFlags.None, null, null),
                        RawSocket.EndSend).HandleCancellation(Spinners.Token);

                await task.ContinueWith(t =>
                {
                    switch (t.Status)
                    {
                        case TaskStatus.Canceled:
                        case TaskStatus.Faulted:
                            _logger.Error(t.Exception, $"Sending to {Protocol}{RemoteAddress}:{RemotePort} failed");
                            Close();
                            break;
                        case TaskStatus.RanToCompletion:
                            _logger.Trace($"Sent {length} bytes to {Protocol}{RemoteAddress}:{RemotePort}");
                            break;
                    }
                }, Spinners.Token);

                return task.Result;
            }
            catch (Exception e)
            {
                _logger.Error(e, "Unable to send bytes to socket!");
                throw;
            }
        }

        /// <summary>
        /// Reads data from a TCP socket async
        /// </summary>
        /// <param name="message">The Q object that contains the buffers to read data into</param>
        /// <returns>The number of bytes read</returns>
        public override async Task<int> ReadAsync(IoMessage<IoNetClient> message)
        {
            try
            {
                var bytesRead = Task.Factory.FromAsync(
                        RawSocket.BeginReceive(message.Buffer, message.BytesRead, message.MaxRecvBufSize - message.BytesRead, SocketFlags.None, null, null),
                        RawSocket.EndReceive)
                    .HandleCancellation(Spinners.Token);

                message.BytesRead+= await bytesRead;

                return bytesRead.Result;
            }
            catch (Exception e)
            {
                _logger.Error(e, "Error reading bytes from socket: ");
                throw;
            }
        }
    }
}
