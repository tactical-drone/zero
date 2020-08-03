using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;

namespace zero.cocoon.autopeer
{
    class IoPeerDiscovery<TJob, TKey> : IoNode<TJob>
        where TJob : IIoWorker
    {
        public IoPeerDiscovery(IoNodeAddress address, Func<IoNode<TJob>, IoNetClient<TJob>, IoNeighbor<TJob>> mallocNeighbor, int tcpReadAhead) : base(address, mallocNeighbor, tcpReadAhead)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        private readonly Logger _logger;
    }
}
