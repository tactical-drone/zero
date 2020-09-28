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
        /// <param name="prefetch">Nr of reads that can lead consumption of them</param>
        /// <param name="concurrencyLevel">The Nr of concurrent consumers</param>
        /// <inheritdoc />
        public IoUdpServer(IoNodeAddress listeningAddress, int prefetch, int concurrencyLevel) : base(listeningAddress, prefetch, concurrencyLevel)
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
        /// <param name="bootstrapAsync"></param>
        /// <returns>
        /// True on success, false otherwise
        /// </returns>
        public override async Task ListenAsync(Func<IoNetClient<TJob>, Task> connectionReceivedAction, Func<Task> bootstrapAsync = null)
        {
            await base.ListenAsync(connectionReceivedAction).ConfigureAwait(false);

            (IoListenSocket,_) = ZeroOnCascade(new IoUdpSocket());

            await IoListenSocket.ListenAsync(ListeningAddress, async ioSocket =>
            {
                try
                {
                    connectionReceivedAction?.Invoke(ZeroOnCascade(new IoUdpClient<TJob>(ioSocket, ReadAheadBufferSize, ConcurrencyLevel)).Item1);
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Connection received handler returned with errors:");

                    await ioSocket.ZeroAsync(this).ConfigureAwait(false);

                }
            }, bootstrapAsync).ConfigureAwait(false);
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
            //ZEROd later on inside net server once we know the connection succeeded
            var ioUdpClient = new IoUdpClient<TJob>(address, ReadAheadBufferSize, ConcurrencyLevel);
            return await base.ConnectAsync(address, ioUdpClient).ConfigureAwait(false);
        }

    }
}
