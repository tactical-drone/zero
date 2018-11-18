using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;

namespace zero.core.network.ip
{
    class IoUdpServer:IoNetServer
    {
        public IoUdpServer(IoNodeAddress listeningAddress, CancellationToken cancellationToken) : base(listeningAddress, cancellationToken)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        private readonly Logger _logger;

        public override async Task<bool> StartListenerAsync(Action<IoNetClient> connectionReceivedAction)
        {
            if (!await base.StartListenerAsync(connectionReceivedAction))
                return false;

            IoListenSocket = new IoUdpSocket(Spinners.Token);

            return await IoListenSocket.ListenAsync(ListeningAddress, ioSocket =>
            {
                try
                {
                    connectionReceivedAction?.Invoke(new IoUdpClient(ioSocket,parm_read_ahead));
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
            var ioUdpClient = new IoUdpClient(address, parm_read_ahead);
            return await base.ConnectAsync(null, ioUdpClient);
        }
        
    }
}
