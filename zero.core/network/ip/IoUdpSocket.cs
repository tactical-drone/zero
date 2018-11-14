using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.models;

namespace zero.core.network.ip
{
    /// <summary>
    /// The UDP flaviour of <see cref="IoSocket"/>
    /// </summary>
    class IoUdpSocket :IoSocket
    {
        /// <summary>
        /// Constructs the UDP socket
        /// </summary>
        /// <param name="cancellationToken">Signals cancellation</param>
        public IoUdpSocket(CancellationToken cancellationToken) : base(SocketType.Dgram, ProtocolType.Udp, cancellationToken)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// A copy constructor
        /// </summary>
        /// <param name="rawSocket">The underlying socket</param>
        /// <param name="address">The address listened on </param>
        /// <param name="port">The listening port</param>
        /// <param name="cancellationToken">Token used for canncellation</param>
        public IoUdpSocket(Socket rawSocket, string address, int port, CancellationToken cancellationToken) : base(rawSocket, address, port, cancellationToken)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Used to retrieve the sender information of a UDP packet and prevent mallocs for each call to receive
        /// </summary>
        private EndPoint _senderRemote;

        /// <summary>
        /// Listen for UDP traffic
        /// </summary>
        /// <param name="address">The listen address</param>
        /// <param name="port">The listen port</param>
        /// <param name="callback">The handler once a connection is made, mostly used in UDPs case to look function like <see cref="IoTcpSocket"/></param>
        /// <returns></returns>
        public override async Task ListenAsync(string address, int port, Action<IoSocket> callback)
        {
            await base.ListenAsync(address, port, callback);

            try
            {
                //Call the new connection esablished handler
                callback(this);

                // Prepare UDP connection orientated things
                IPEndPoint sender = new IPEndPoint(IPAddress.Any, 0);
                _senderRemote = (EndPoint) sender;
            }
            catch (Exception e)
            {
                _logger.Error(e,
                    $"There was an error handling new connection from `{Protocol}{RemoteAddress}:{RemotePort}' to `{Protocol}{LocalAddress}:{LocalPort}'");
            }

            //_socket.SetSocketOption(SocketOptionLevel.Udp, SocketOptionName.NoChecksum, 1);
            //_socket.SetSocketOption(SocketOptionLevel.Udp, SocketOptionName.NoChecksum, true);
            //_socket.SetSocketOption(SocketOptionLevel.Udp, SocketOptionName.ChecksumCoverage, 1);            
            //var v =_socket.GetSocketOption(SocketOptionLevel.Udp, SocketOptionName.NoChecksum);            
        }

        /// <summary>
        /// Send UDP packet
        /// </summary>
        /// <param name="getBytes">The buffer containing the data</param>
        /// <param name="offset">Start offset into the buffer</param>
        /// <param name="length">The length of the data</param>
        /// <returns></returns>
        public override async Task<int> SendAsync(byte[] getBytes, int offset, int length)
        {
            await RawSocket.SendToAsync(getBytes, SocketFlags.None, RemoteEndPoint).ContinueWith(
                t =>
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
            return length;
        }

        /// <summary>
        /// Read UDP packet data
        /// </summary>
        /// <param name="message">Used to Q packets read</param>
        /// <returns></returns>
        public override async Task<int> ReadAsync(IoMessage<IoNetClient> message)
        {
            if (RawSocket.IsBound /*&& _socket.Available */ )
            {
                //_logger.Warn($"===============WE GOT DATA {_socket.Available} THREAD id = {Thread.CurrentThread.ManagedThreadId}");

                //TODO make async
                var bytesRead = RawSocket.ReceiveFrom((byte[])(Array)message.Buffer, ref _senderRemote);
                message.BytesRead += bytesRead;

                return await Task.FromResult(bytesRead) ;
            }
            else
            {
                _logger.Warn("Unable to read from udp, socket is not bound!");
                Thread.Sleep(1000);
                return 0;
            }
        }
    }
}
