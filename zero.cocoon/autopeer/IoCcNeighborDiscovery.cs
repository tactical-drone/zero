using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using NLog.LayoutRenderers.Wrappers;
using zero.cocoon.models;
using zero.cocoon.models.services;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;

namespace zero.cocoon.autopeer
{
    public class IoCcNeighborDiscovery : IoNode<IoCcPeerMessage>
    {
        public IoCcNeighborDiscovery(IoCcNode ioCcNode, IoNodeAddress address,
            Func<IoNode<IoCcPeerMessage>, IoNetClient<IoCcPeerMessage>, object, IoNeighbor<IoCcPeerMessage>> mallocNeighbor, int tcpReadAhead) : base(address, mallocNeighbor, tcpReadAhead)
        {
            _logger = LogManager.GetCurrentClassLogger();
            CcNode = ioCcNode;
            ConnectedEvent += (sender, neighbor) => CcNode.AddNeighbor((IoCcNeighbor) neighbor);
        }

        private readonly Logger _logger;
        public readonly IoCcNode CcNode;

        public IoCcService Services => CcNode.Services;

    }
}
