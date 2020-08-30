using System.Threading.Tasks;
using NLog;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.core.network.ip
{
    /// <summary>
    /// The <see cref="IoNetClient{TJob}"/>'s TCP flavor
    /// </summary>
    /// <seealso cref="IoNetClient{TJob}" />
    public class IoTcpClient<TJob> : IoNetClient<TJob>
        where TJob : IIoJob
        
    {

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="IoTcpClient{TJob}"/> class.
        /// </summary>
        /// <param name="remote">The tcp client to be wrapped</param>
        /// <param name="readAheadBufferSize">The amount of socket reads the source is allowed to lead the consumer</param>
        public IoTcpClient(IoSocket remote, int readAheadBufferSize) : base((IoNetSocket) remote, readAheadBufferSize)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IoTcpClient{TJob}"/> class.
        /// </summary>
        /// <param name="localAddress">The address associated with this network client</param>
        /// <param name="readAheadBufferSize">The amount of socket reads the source is allowed to lead the consumer</param>
        public IoTcpClient(IoNodeAddress localAddress, int readAheadBufferSize) : base(localAddress,
            readAheadBufferSize)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }
        
        /// <summary>
        /// Connects to a remote listener
        /// </summary>
        /// <returns>
        /// True if succeeded, false otherwise
        /// </returns>
        public override async Task<bool> ConnectAsync()
        {
            IoSocket = ZeroOnCascade(new IoTcpSocket(), true);
            return await base.ConnectAsync();
        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            IoSocket = null;
            ListeningAddress = null;
            IoChannels = null;
#endif

        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        protected override void ZeroManaged()
        {
            base.ZeroManaged();
        }
    }
}
