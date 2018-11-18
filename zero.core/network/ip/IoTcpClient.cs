using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using NLog;

namespace zero.core.network.ip
{
    public class IoTcpClient:IoNetClient
    {
        public IoTcpClient(IoSocket remote, int readAhead) : base(remote, readAhead) {}

        public IoTcpClient(IoNodeAddress address, int readAhead) : base(address, readAhead) {}

        public override async Task<bool> ConnectAsync()
        {
            IoSocket = new IoTcpSocket(Spinners.Token);
            return await base.ConnectAsync();
        }
    }
}
