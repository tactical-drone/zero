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
        private Logger _logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="IoTcpClient{TJob}"/> class.
        /// </summary>
        /// <param name="remote">The tcp client to be wrapped</param>
        /// <param name="prefetchSize">The amount of socket reads the source is allowed to lead the consumer</param>
        /// <param name="concurrencyLevel">Concurrency level</param>
        public IoTcpClient(IoNetSocket remote, int prefetchSize = 1,  int concurrencyLevel = 1) : base(remote, prefetchSize,  concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IoTcpClient{TJob}"/> class. Used in combination with <see cref="ConnectAsync"/>
        /// </summary>
        /// <param name="prefetchSize">The amount of socket reads the source is allowed to lead the consumer</param>
        /// <param name="concurrencyLevel">Concurrency level</param>
        public IoTcpClient(int prefetchSize,  int concurrencyLevel) : base(prefetchSize,  concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }
        
        /// <summary>
        /// Connects to a remote listener
        /// </summary>
        /// <returns>
        /// True if succeeded, false otherwise
        /// </returns>
        public override async ValueTask<bool> ConnectAsync(IoNodeAddress remoteAddress)
        {
            IoNetSocket = ZeroOnCascade(new IoTcpSocket(), true).target;
            return await base.ConnectAsync(remoteAddress).ConfigureAwait(false);
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _logger = null;
            IoNetSocket = null;
            IoConduits = null;
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
