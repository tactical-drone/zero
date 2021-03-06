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
        /// <param name="bootFunc"></param>
        /// <returns>
        /// True on success, false otherwise
        /// </returns>
        public override async ValueTask ListenAsync<T,TContext>(Func<T, IoNetClient<TJob>, ValueTask> connectionReceivedAction,
            T context = default,
            Func<TContext,ValueTask> bootFunc = null, TContext bootData = default)
        {
            await base.ListenAsync(connectionReceivedAction, context,bootFunc,bootData).FastPath();

            while (!Zeroed())
            {
                //Creates a listening socket
                try
                {
                    IoListenSocket = (await ZeroHiveAsync(new IoUdpSocket(Prefetch), true).FastPath()).target;

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
                                        @this.Prefetch, @this.ConcurrencyLevel)).FastPath()
                                ).target).FastPath();
                        }
                        catch (Exception e)
                        {
                            @this._logger.Error(e, $"Accept udp connection failed: {@this.Description}");

                            await ioSocket.DisposeAsync(@this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();
                        }
                    },ValueTuple.Create(this,context, connectionReceivedAction), bootFunc, bootData).FastPath();

                    if(!Zeroed())
                        _logger.Warn($"Listener stopped, restarting: {Description}");

                    if(IoListenSocket != null)
                        await IoListenSocket.DisposeAsync(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();
                }
                catch when (Zeroed()){}
                catch (Exception e) when (!Zeroed())
                {
                    _logger?.Error(e,$"{nameof(ListenAsync)}: ");
                }
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
            var ioUdpClient = new IoUdpClient<TJob>($"{nameof(IoUdpServer<TJob>)} ~> {Description}",Prefetch, ConcurrencyLevel);
            return base.ConnectAsync(remoteAddress, ioUdpClient, timeout);
        }

    }
}
