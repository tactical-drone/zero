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
        /// <param name="bootstrapAsync"></param>
        /// <returns>True on success, false otherwise</returns>
        public override async ValueTask ListenAsync<T>(Func<T, IoNetClient<TJob>, ValueTask> connectionReceivedAction, T context = default, Func<ValueTask> bootstrapAsync = null)
        {
            await base.ListenAsync(connectionReceivedAction, context, bootstrapAsync).FastPath().ConfigureAwait(Zc);

            IoListenSocket = (await ZeroHiveAsync(new IoTcpSocket(ZeroConcurrencyLevel()), true).FastPath().ConfigureAwait(Zc)).target;

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
                                    @this.ConcurrencyLevel)).FastPath().ConfigureAwait(@this.Zc)
                            ).target
                        ).FastPath().ConfigureAwait(@this.Zc);
                }
                catch (Exception e)
                {
                    var errMsg = $"{@this.Description} Connection received handler returned with errors:";
                    @this._logger.Error(e, errMsg);
                    newConnectionSocket.Zero(@this, errMsg);
                }
            }, ValueTuple.Create(this, context, connectionReceivedAction), bootstrapAsync).FastPath().ConfigureAwait(Zc);
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
