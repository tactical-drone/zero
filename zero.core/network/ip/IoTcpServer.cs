using System;
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
        /// <param name="readAheadBufferSize"></param>
        /// <param name="bootstrapAsync"></param>
        /// <returns>True on success, false otherwise</returns>
        public override async Task ListenAsync(Func<IoNetClient<TJob>, Task> connectionReceivedAction, Func<Task> bootstrapAsync = null)
        {
            await base.ListenAsync(connectionReceivedAction, bootstrapAsync).ConfigureAwait(false);

            IoListenSocket = ZeroOnCascade(new IoTcpSocket()).target;

            await IoListenSocket.ListenAsync(ListeningAddress, async newConnectionSocket =>
            {
                try
                {
                    connectionReceivedAction?.Invoke(ZeroOnCascade(new IoTcpClient<TJob>(newConnectionSocket, ReadAheadBufferSize, ConcurrencyLevel)).target);
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Connection received handler returned with errors:");

                    await newConnectionSocket.ZeroAsync(this).ConfigureAwait(false);

                }
            }, bootstrapAsync).ConfigureAwait(false);
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
            //ZEROd later on inside net server once we know the connection succeeded 
            var ioTcpclient = new IoTcpClient<TJob>(address, ReadAheadBufferSize, ConcurrencyLevel);
            return await base.ConnectAsync(address, ioTcpclient).ConfigureAwait(false);
        }
    }
}
