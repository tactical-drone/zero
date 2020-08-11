using System;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.cocoon.autopeer;
using zero.cocoon.models;
using zero.cocoon.models.services;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.cocoon
{
    public class IoCcPeer : IoNeighbor<IoCcGossipMessage>
    {
        public IoCcPeer(IoNode<IoCcGossipMessage> node, IoCcNeighbor neighbor,
            IoNetClient<IoCcGossipMessage> ioNetClient) 
            : base(node, ioNetClient, userData => new IoCcGossipMessage("gossip rx", $"{ioNetClient.AddressString}", ioNetClient))
        {
            _logger = LogManager.GetCurrentClassLogger();
            IoNetClient = ioNetClient;
            _neighbor = neighbor;

            if(neighbor!= null)
                SetNeighbor(neighbor);
        }

        private readonly Logger _logger;
        private IoCcNeighbor _neighbor;

        public IoCcNeighbor Neighbor => _neighbor;

        public override string Id => _neighbor.Id;

        protected IoNetClient<IoCcGossipMessage> IoNetClient;

        public void SetNeighbor(IoCcNeighbor neighbor)
        {
            _neighbor = neighbor ?? throw new ArgumentNullException($"{nameof(neighbor)}");
            _neighbor.Closed += (sender, args) => Close(); //TODO does this make sense?
        }
    }
}
