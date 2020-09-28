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
        /// <param name="prefetchSize">The amount of socket reads the source is allowed to lead the consumer</param>
        /// <param name="concurrencyLevel">Concurrency level</param>
        public IoTcpClient(IoSocket remote, int prefetchSize = 1,  int concurrencyLevel = 1) : base((IoNetSocket) remote, prefetchSize,  concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IoTcpClient{TJob}"/> class.
        /// </summary>
        /// <param name="localAddress">The address associated with this network client</param>
        /// <param name="prefetchSize">The amount of socket reads the source is allowed to lead the consumer</param>
        /// <param name="concurrencyLevel">Concurrency level</param>
        public IoTcpClient(IoNodeAddress localAddress, int prefetchSize,  int concurrencyLevel) : base(localAddress,
            prefetchSize,  concurrencyLevel)
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
            (IoSocket,_) = ZeroOnCascade(new IoTcpSocket(), true);
            return await base.ConnectAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override void ZeroUnmanaged()
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
        public override ValueTask ZeroManagedAsync()
        {
            return base.ZeroManagedAsync();
        }
    }
}
