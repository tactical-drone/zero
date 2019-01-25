using System.Collections;
using NLog;
using zero.core.core;
using zero.core.models.consumables;
using zero.core.network.ip;
using zero.interop.utils;

namespace zero.core.protocol
{
    /// <inheritdoc />
    /// <summary>
    /// The iota protocol
    /// </summary>
    public class TanglePeer : IoNeighbor<IoTangleMessage> 
    {
        /// <summary>
        /// Constructs a IOTA tangle neighbor handler
        /// </summary>
        /// <param name="ioNetClient">The network client used to communicate with this neighbor</param>
        public TanglePeer(IoNetClient<IoTangleMessage> ioNetClient) :
            base($"{nameof(TanglePeer)}",ioNetClient, (userData) => new IoTangleMessage(ioNetClient) { JobDescription = $"rx", WorkDescription = $"{ioNetClient.AddressString}" })
        {
            _logger = LogManager.GetCurrentClassLogger();

            //JobThreadScheduler = new LimitedThreadScheduler(parm_max_consumer_threads = 2);                        
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Minimum difficulty
        /// </summary>
        public const int MWM = IoPow.MWM;


        /// <summary>
        /// Tcp read ahead
        /// </summary>
        public const int TcpReadAhead = 50;
    }
}
