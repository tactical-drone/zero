using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;

namespace zero.core.network.ip
{
    /// <inheritdoc />
    public class IoTcpServer:IoNetServer
    {
        /// <inheritdoc />
        public IoTcpServer(IoNodeAddress listeningAddress, CancellationToken cancellationToken) : base(listeningAddress, cancellationToken)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <inheritdoc />
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

        public override async Task<IoNetClient> ConnectAsync(IoNodeAddress address, IoNetClient _)
        {
            var ioTcpclient = new IoTcpClient(address, parm_read_ahead);
            return await base.ConnectAsync(null, ioTcpclient);
        }
    }
}
