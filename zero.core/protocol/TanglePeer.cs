using System;
using System.Text;
using System.Threading.Tasks;
using NLog;
using Tangle.Net.Cryptography;
using Tangle.Net.Entity;
using zero.core.core;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.schedulers;

namespace zero.core.protocol
{
    /// <summary>
    /// The iota protocol
    /// </summary>
    public class TanglePeer : IoNeighbor
    {
        /// <summary>
        /// Constructs a IOTA tangle neighbor handler
        /// </summary>
        /// <param name="ioNetClient">The network client used to communicate with this neighbor</param>
        public TanglePeer(IoNetClient ioNetClient) :
            base(ioNetClient, () => new IoTangleMessage(ioNetClient) { JobDescription = $"rx", WorkDescription = $"{ioNetClient.AddressString}" })
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
