using System.Threading.Tasks;

namespace zero.core.network.ip
{
    class IoUdpClient:IoNetClient
    {
        public IoUdpClient(IoSocket remote, int readAhead) : base(remote, readAhead)
        {
        }

        public IoUdpClient(IoNodeAddress address, int readAhead) : base(address, readAhead)
        {
        }

        public override async Task<bool> ConnectAsync()
        {
            IoSocket = new IoUdpSocket(Spinners.Token);
            return await base.ConnectAsync();
        }
    }
}
