using System.Threading.Tasks;
using zero.core.patterns.bushes.contracts;

namespace zero.core.network.ip
{
    /// <summary>
    /// The UDP flavor of <see cref="IoNetClient"/>
    /// </summary>
    /// <seealso cref="zero.core.network.ip.IoNetClient" />
    class IoUdpClient<TJob> :IoNetClient<TJob> 
        where TJob : IIoJob
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoUdpClient"/> class.
        /// </summary>
        /// <param name="remote">The tcpclient to be wrapped</param>
        /// <param name="readAhead">The amount of socket reads the producer is allowed to lead the consumer</param>
        public IoUdpClient(IoSocket remote, int readAhead) : base((IoNetSocket) remote, readAhead)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IoUdpClient"/> class.
        /// </summary>
        /// <param name="address">The address associated with this network client</param>
        /// <param name="readAhead">The amount of socket reads the producer is allowed to lead the consumer</param>
        public IoUdpClient(IoNodeAddress address, int readAhead) : base(address, readAhead)
        {
        }

        /// <summary>
        /// Connects to a remote listener
        /// </summary>
        /// <returns>
        /// True if succeeded, false otherwise
        /// </returns>
        public override async Task<bool> ConnectAsync()
        {
            IoSocket = new IoUdpSocket(Spinners.Token);
            return await base.ConnectAsync();
        }
    }
}
