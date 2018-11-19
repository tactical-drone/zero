using System;
using System.Threading;
using System.Threading.Tasks;
using NLog;

namespace zero.core.network.ip
{
    /// <summary>
    /// The TCP flavor of <see cref="IoNetServer"/>
    /// </summary>
    /// <seealso cref="zero.core.network.ip.IoNetServer" />
    public class IoTcpServer:IoNetServer
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoTcpServer"/> class.
        /// </summary>
        /// <param name="listeningAddress">The listening address</param>
        /// <param name="cancellationToken">Cancellation hooks</param>
        /// <inheritdoc />
        public IoTcpServer(IoNodeAddress listeningAddress, CancellationToken cancellationToken) : base(listeningAddress, cancellationToken)
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
        /// <returns>True on success, false otherwise</returns>
        public override async Task<bool> StartListenerAsync(Action<IoNetClient> connectionReceivedAction)
        {
            if (!await base.StartListenerAsync(connectionReceivedAction))
                return false;

            IoListenSocket = new IoTcpSocket(Spinners.Token);

            return await IoListenSocket.ListenAsync(ListeningAddress, ioSocket =>
            {                                                                    
                try
                {                    
                    connectionReceivedAction?.Invoke(new IoTcpClient(ioSocket, parm_read_ahead));
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Connection received handler returned with errors:");
                    ioSocket.Close();
                }
            });
        }

        /// <summary>
        /// Connects the asynchronous.
        /// </summary>
        /// <param name="address">The address.</param>
        /// <param name="_">The .</param>
        /// <returns>The tcp client object managing this socket connection</returns>
        public override async Task<IoNetClient> ConnectAsync(IoNodeAddress address, IoNetClient _)
        {
            var ioTcpclient = new IoTcpClient(address, parm_read_ahead);
            return await base.ConnectAsync(null, ioTcpclient);
        }
    }
}
