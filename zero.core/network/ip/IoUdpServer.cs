using System;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.bushes.contracts;

namespace zero.core.network.ip
{
    /// <summary>
    /// The UDP flavor of <see cref="IoNetServer{TJob}"/>
    /// </summary>
    /// <seealso cref="zero.core.network.ip.IoNetServer{TJob}" />
    class IoUdpServer<TJob> : IoNetServer<TJob>
        where TJob : IIoJob
        
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoUdpServer{TJob}"/> class.
        /// </summary>
        /// <param name="listeningAddress">The listening address</param>
        /// <inheritdoc />
        public IoUdpServer(IoNodeAddress listeningAddress) : base(listeningAddress)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <inheritdoc />
        /// <summary>
        /// Start the listener
        /// </summary>
        /// <param name="connectionReceivedAction">Action to execute when an incoming connection was made</param>
        /// <param name="readAheadBufferSize"></param>
        /// <returns>
        /// True on success, false otherwise
        /// </returns>
        public override async Task ListenAsync(Action<IoNetClient<TJob>> connectionReceivedAction, int readAheadBufferSize)
        {
            await base.ListenAsync(connectionReceivedAction, readAheadBufferSize);

            IoListenSocket = new IoUdpSocket();
            //IoListenSocket.ClosedEvent((sender, args) => Close());

            await IoListenSocket.ListenAsync(ListeningAddress, ioSocket =>
            {
                try
                {
                    connectionReceivedAction?.Invoke(new IoUdpClient<TJob>(ioSocket, parm_read_ahead));
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Connection received handler returned with errors:");
                    ioSocket.Close();
                }
            });
        }

        /// <inheritdoc />
        /// <summary>
        /// Connects the asynchronous.
        /// </summary>
        /// <param name="address">The address.</param>
        /// <param name="_">The .</param>
        /// <returns>The udp client object managing this socket connection</returns>
        public override async Task<IoNetClient<TJob>> ConnectAsync(IoNodeAddress address, IoNetClient<TJob> _)
        {
            var ioUdpClient = new IoUdpClient<TJob>(address, parm_read_ahead);
            return await base.ConnectAsync(address, ioUdpClient);
        }

    }
}
