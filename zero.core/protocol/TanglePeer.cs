using NLog;
using zero.core.core;
using zero.core.models.consumables;
using zero.core.network.ip;
using zero.core.patterns.schedulers;

namespace zero.core.protocol
{
    /// <inheritdoc />
    /// <summary>
    /// The iota protocol
    /// </summary>
    public class TanglePeer<TBlob> : IoNeighbor<IoTangleMessage<TBlob>> 
    {
        /// <summary>
        /// Constructs a IOTA tangle neighbor handler
        /// </summary>
        /// <param name="ioNetClient">The network client used to communicate with this neighbor</param>
        public TanglePeer(IoNetClient<IoTangleMessage<TBlob>> ioNetClient) :
            base($"{nameof(TanglePeer<TBlob>)}",ioNetClient, (userData) => new IoTangleMessage<TBlob>(ioNetClient) { JobDescription = $"rx", WorkDescription = $"{ioNetClient.AddressString}" })
        {
            _logger = LogManager.GetCurrentClassLogger();

            JobThreadScheduler = new LimitedThreadScheduler(parm_max_consumer_threads = 4);
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;                                
    }
}
