using System;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;

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
        /// <param name="prefetchBuffer"></param>
        /// <param name="concurrencyLevel"></param>
        /// <inheritdoc />
        public IoTcpServer(IoNodeAddress listeningAddress, int prefetchBuffer = 2,  int concurrencyLevel = 1) : base(listeningAddress, prefetchBuffer, concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Start the listener
        /// </summary>
        /// <param name="connectionReceivedAction">Action to execute when an incoming connection was made</param>
        /// <param name="context"></param>
        /// <param name="bootFunc"></param>
        /// <returns>True on success, false otherwise</returns>
        public override async ValueTask BlockOnListenAsync<T,TContext>(Func<T, IoNetClient<TJob>, ValueTask> connectionReceivedAction, T context = default, Func<TContext,ValueTask> bootFunc = null, TContext bootData = default)
        {
            await base.BlockOnListenAsync(connectionReceivedAction, context, bootFunc, bootData).FastPath();

            IoListenSocket = (await ZeroHiveAsync(new IoTcpSocket(ZeroConcurrencyLevel()), true).FastPath()).target;

            await IoListenSocket.BlockOnListenAsync(ListeningAddress, static async (newConnectionSocket, state) =>
            {
                var (@this, nanite,connectionReceivedAction) = state;
                try
                {
                    await connectionReceivedAction(
                        nanite,
                        (
                            await @this.ZeroHiveAsync(
                                new IoTcpClient<TJob>(
                                    $"{nameof(IoTcpClient<TJob>)} ~> {@this.Description}", 
                                    (IoNetSocket) newConnectionSocket, 
                                    @this.Prefetch,
                                    @this.ConcurrencyLevel)).FastPath()
                            ).target
                        ).FastPath();
                }
                catch (Exception e)
                {
                    var errMsg = $"{@this.Description} Connection received handler returned with errors:";
                    @this._logger.Error(e, errMsg);
                    await newConnectionSocket.DisposeAsync(@this, errMsg).FastPath();
                }
            }, ValueTuple.Create(this, context, connectionReceivedAction), bootFunc, bootData).FastPath();
        }

        /// <summary>
        /// Connect to remote.
        /// </summary>
        /// <param name="remoteAddress">The address.</param>
        /// <param name="_">The .</param>
        /// <param name="timeout"></param>
        /// <returns>The tcp client object managing this dotNetSocket connection</returns>
        public override ValueTask<IoNetClient<TJob>> ConnectAsync(IoNodeAddress remoteAddress,
            IoNetClient<TJob> _, int timeout = 0)
        {
            if (!remoteAddress.Validated)
                return new ValueTask<IoNetClient<TJob>>();

            //ZEROd later on inside net server once we know the connection succeeded 
            return base.ConnectAsync(remoteAddress, new IoTcpClient<TJob>($"{nameof(IoTcpClient<TJob>)} ~> {Description}", Prefetch, ConcurrencyLevel), timeout);
        }
    }
}
