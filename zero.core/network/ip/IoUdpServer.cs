using System;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.bushes;
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
        /// <param name="bootstrapAsync"></param>
        /// <returns>
        /// True on success, false otherwise
        /// </returns>
        public override async Task ListenAsync(Func<IoNetClient<TJob>, Task> connectionReceivedAction,
            Func<Task> bootstrapAsync = null)
        {
            await base.ListenAsync(connectionReceivedAction, bootstrapAsync).ConfigureAwait(false);

            while (!Zeroed())
            {
                //Creates a listening socket
                IoListenSocket = ZeroOnCascade(new IoUdpSocket(ConcurrencyLevel), true).target;

                await IoListenSocket.ListenAsync(ListeningAddress, async ioSocket =>
                {
                    try
                    { //creates a new udp client
                        connectionReceivedAction?.Invoke(ZeroOnCascade(new IoUdpClient<TJob>(ioSocket, ReadAheadBufferSize, ConcurrencyLevel)).target);
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e, $"Accept udp connection failed: {Description}");

                        await ioSocket.ZeroAsync(this).ConfigureAwait(false);
                    }
                }, bootstrapAsync).ConfigureAwait(false);

                if(!Zeroed())
                    _logger.Warn($"Listener stopped, restarting: {Description}");

                await IoListenSocket.ZeroAsync(this).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        /// <summary>
        /// Opens a client.
        /// </summary>
        /// <param name="remoteAddress">The address.</param>
        /// <param name="_">The .</param>
        /// <returns>The udp client object managing this socket connection</returns>
        public override async Task<IoNetClient<TJob>> ConnectAsync(IoNodeAddress remoteAddress, IoNetClient<TJob> _)
        {
            //ZEROd later on inside net server once we know the connection succeeded
            var ioUdpClient = new IoUdpClient<TJob>(ReadAheadBufferSize, ConcurrencyLevel);
            return await base.ConnectAsync(remoteAddress, ioUdpClient).ConfigureAwait(false);
        }

    }
}
