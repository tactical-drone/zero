using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
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
        /// <param name="readAheadBufferSizeBuffer"></param>
        /// <param name="concurrencyLevel"></param>
        /// <inheritdoc />
        public IoTcpServer(IoNodeAddress listeningAddress, int readAheadBufferSizeBuffer = 1,  int concurrencyLevel = 1) : base(listeningAddress, readAheadBufferSizeBuffer, concurrencyLevel)
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
        /// <param name="bootstrapAsync"></param>
        /// <returns>True on success, false otherwise</returns>
        public override async ValueTask ListenAsync<T>(Func<T, IoNetClient<TJob>, ValueTask> connectionReceivedAction,
            T nanite = default,
            Func<ValueTask> bootstrapAsync = null)
        {
            await base.ListenAsync<T>(connectionReceivedAction, nanite, bootstrapAsync).FastPath().ConfigureAwait(false);

            IoListenSocket = (await ZeroHiveAsync(new IoTcpSocket(ZeroConcurrencyLevel()), true).FastPath().ConfigureAwait(false)).target;

            await IoListenSocket.ListenAsync(ListeningAddress, static async (newConnectionSocket, state) =>
            {
                var (@this, nanite,connectionReceivedAction) = state;
                try
                {
                    await connectionReceivedAction(nanite,(await @this.ZeroHiveAsync(new IoTcpClient<TJob>($"{nameof(IoTcpClient<TJob>)} ~> {@this.Description}", (IoNetSocket) newConnectionSocket, @this.ReadAheadBufferSize, @this.ConcurrencyLevel)).FastPath().ConfigureAwait(false)).target).FastPath().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    @this._logger.Error(e, $"Connection received handler returned with errors:");

                    await newConnectionSocket.ZeroAsync(@this).FastPath().ConfigureAwait(false);

                }
            }, ValueTuple.Create(this, nanite, connectionReceivedAction), bootstrapAsync).FastPath().ConfigureAwait(false);
        }

        /// <summary>
        /// Connects the asynchronous.
        /// </summary>
        /// <param name="remoteAddress">The address.</param>
        /// <param name="_">The .</param>
        /// <returns>The tcp client object managing this dotNetSocket connection</returns>
        public override async Task<IoNetClient<TJob>> ConnectAsync(IoNodeAddress remoteAddress, IoNetClient<TJob> _)
        {
            if (!remoteAddress.Validated)
                return null;

            //ZEROd later on inside net server once we know the connection succeeded 
            return await base.ConnectAsync(remoteAddress, new IoTcpClient<TJob>($"{nameof(IoTcpClient<TJob>)} ~> {Description}", ReadAheadBufferSize, ConcurrencyLevel)).ConfigureAwait(false);
        }
    }
}
