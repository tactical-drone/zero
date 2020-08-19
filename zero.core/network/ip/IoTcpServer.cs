﻿using System;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.bushes.contracts;

namespace zero.core.network.ip
{
    /// <summary>
    /// The TCP flavor of <see cref="IoNetServer{TJob}"/>
    /// </summary>
    /// <seealso cref="zero.core.network.ip.IoNetServer{TJob}" />
    public class IoTcpServer<TJob> : IoNetServer<TJob>
        where TJob : IIoJob
        
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoTcpServer{TJob}"/> class.
        /// </summary>
        /// <param name="listeningAddress">The listening address</param>
        /// <param name="cancellationToken">Cancellation hooks</param>
        /// <inheritdoc />
        public IoTcpServer(IoNodeAddress listeningAddress, CancellationToken cancellationToken, int readAheadBuffer) : base(listeningAddress, cancellationToken)
        {
            _logger = LogManager.GetCurrentClassLogger();
            parm_read_ahead = readAheadBuffer;
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Start the listener
        /// </summary>
        /// <param name="connectionReceivedAction">Action to execute when an incoming connection was made</param>
        /// <param name="readAhead">Tcp readahead</param>
        /// <returns>True on success, false otherwise</returns>
        public override async Task ListenAsync(Action<IoNetClient<TJob>> connectionReceivedAction, int readAhead)
        {
            await base.ListenAsync(connectionReceivedAction, readAhead);

            IoListenSocket = new IoTcpSocket(Spinners.Token);

            await IoListenSocket.ListenAsync(ListeningAddress, newConnectionSocket =>
            {
                try
                {
                    connectionReceivedAction?.Invoke(new IoTcpClient<TJob>(newConnectionSocket, parm_read_ahead));
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Connection received handler returned with errors:");
                    newConnectionSocket.Close();
                }
            });
        }

        /// <summary>
        /// Connects the asynchronous.
        /// </summary>
        /// <param name="address">The address.</param>
        /// <param name="_">The .</param>
        /// <returns>The tcp client object managing this socket connection</returns>
        public override async Task<IoNetClient<TJob>> ConnectAsync(IoNodeAddress address, IoNetClient<TJob> _)
        {
            if (!address.Validated)
                return null;
            var ioTcpclient = new IoTcpClient<TJob>(address, parm_read_ahead);
            return await base.ConnectAsync(address, ioTcpclient);
        }
    }
}
