using System;
using System.Data;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;

namespace zero.core.network.ip
{
    /// <inheritdoc />
    /// <summary>
    /// A wrap for <see cref="T:zero.core.network.ip.IoSocket" /> to make it host a server
    /// </summary>
    public abstract class IoNetServer : IoConfigurable
    {
        /// <inheritdoc />
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="listeningAddress">The listening address</param>
        /// <param name="cancellationToken">Cancellation hooks</param>
        protected IoNetServer(IoNodeAddress listeningAddress, CancellationToken cancellationToken)
        {
            ListeningAddress = listeningAddress;

            _logger = LogManager.GetCurrentClassLogger();

            Spinners = new CancellationTokenSource();
            cancellationToken.Register(Close);
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The listening address of this server
        /// </summary>
        protected readonly IoNodeAddress ListeningAddress;        

        /// <summary>
        /// The <see cref="TcpListener"/> instance that is wrapped
        /// </summary>
        protected IoSocket IoListenSocket;

        /// <summary>
        /// Cancel all listener tasks
        /// </summary>
        protected readonly CancellationTokenSource Spinners;

        /// <summary>
        /// The cancellation registration handle
        /// </summary>
        private CancellationTokenRegistration _cancellationRegistration;

        /// <summary>
        /// The amount of socket reads the producer is allowed to lead the consumer
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_read_ahead = 10;

        /// <summary>
        /// Start the listener
        /// </summary>
        /// <param name="connectionReceivedAction">Action to execute when an incoming connection was made</param>
        /// <returns>True on success, false otherwise</returns>
        public virtual Task<bool> StartListenerAsync(Action<IoNetClient> connectionReceivedAction)
        {
            if (IoListenSocket != null)
                throw new ConstraintException($"Listener has already been started for `{ListeningAddress}'");
            return Task.FromResult(true);
        }

        /// <summary>
        /// Connect to a host async
        /// </summary>
        /// <param name="_">A stub</param>
        /// <param name="ioNetClient">The client to connect to</param>
        /// <returns>The client object managing this socket connection</returns>
        public virtual async Task<IoNetClient> ConnectAsync(IoNodeAddress _, IoNetClient ioNetClient = null)
        {
            if (await ioNetClient.ConnectAsync().ContinueWith(t =>
            {
                if (!t.Result)
                {
                    ioNetClient.Close();
                    return false;
                }

                if (ioNetClient.IsSocketConnected())
                {
                    _logger.Info($"Connection established to `{ioNetClient}'");
                    return true;
                }
                else // On connect failure
                {
                    _logger.Warn($"Unable to connect to `{ioNetClient}'");
                    ioNetClient.Close();
                    return false;
                }
            }, Spinners.Token))
            {
                return ioNetClient;
            }

            return null;
        }

        /// <summary>
        /// Closes this server
        /// </summary>
        public virtual void Close()
        {
            //This method must always be at the top or we might recurse
            _cancellationRegistration.Dispose();

            Spinners.Cancel();
            IoListenSocket.Close();
        }

        /// <summary>
        /// Figures out the correct server to use from the url, <see cref="IoTcpServer"/> or <see cref="IoUdpServer"/>
        /// </summary>
        /// <param name="address"></param>
        /// <param name="spinner"></param>
        /// <returns></returns>
        public static IoNetServer GetKindFromUrl(IoNodeAddress address, CancellationToken spinner)
        {
            if (address.Protocol()== ProtocolType.Tcp)
                return new IoTcpServer(address, spinner);

            if (address.Protocol() == ProtocolType.Udp)
                return new IoUdpServer(address, spinner);

            return null;
        }
    }
}
