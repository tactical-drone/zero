using System;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;

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
        public override async ValueTask ListenAsync<T>(Func<T, IoNetClient<TJob>, ValueTask> connectionReceivedAction,
            T nanite = default,
            Func<ValueTask> bootstrapAsync = null)
        {
            await base.ListenAsync(connectionReceivedAction, nanite,bootstrapAsync).FastPath().ConfigureAwait(Zc);

            while (!Zeroed())
            {
                //Creates a listening socket
                IoListenSocket = (await ZeroHiveAsync(new IoUdpSocket(ReadAheadBufferSize, ConcurrencyLevel), true)).target;

                await IoListenSocket.BlockOnListenAsync(ListeningAddress, static async (ioSocket,state) =>
                {
                    var (@this, nanite, connectionReceivedAction) = state;
                    try
                    {
                        //creates a new udp client
                        await connectionReceivedAction(nanite,
                            (await @this
                                .ZeroHiveAsync(new IoUdpClient<TJob>(
                                    $"{nameof(IoUdpClient<TJob>)} ~> {@this.Description}", ioSocket,
                                    @this.ReadAheadBufferSize, @this.ConcurrencyLevel)).FastPath()
                                .ConfigureAwait(@this.Zc)).target).FastPath().ConfigureAwait(@this.Zc);
                    }
                    catch (Exception e)
                    {
                        @this._logger.Error(e, $"Accept udp connection failed: {@this.Description}");

                        await ioSocket.ZeroAsync(@this).FastPath().ConfigureAwait(@this.Zc);
                    }
                },ValueTuple.Create(this,nanite, connectionReceivedAction), bootstrapAsync).ConfigureAwait(Zc);

                if(!Zeroed())
                    _logger.Warn($"Listener stopped, restarting: {Description}");

                await IoListenSocket.ZeroAsync(this).FastPath().ConfigureAwait(Zc);
            }
        }

        /// <inheritdoc />
        /// <summary>
        /// Opens a client.
        /// </summary>
        /// <param name="remoteAddress">The address.</param>
        /// <param name="_">The .</param>
        /// <param name="timeout"></param>
        /// <returns>The udp client object managing this socket connection</returns>
        public override ValueTask<IoNetClient<TJob>> ConnectAsync(IoNodeAddress remoteAddress, IoNetClient<TJob> _, int timeout = 0)
        {
            //ZEROd later on inside net server once we know the connection succeeded
            var ioUdpClient = new IoUdpClient<TJob>($"{nameof(IoUdpServer<TJob>)} ~> {Description}",ReadAheadBufferSize, ConcurrencyLevel);
            return base.ConnectAsync(remoteAddress, ioUdpClient, timeout);
        }

    }
}
