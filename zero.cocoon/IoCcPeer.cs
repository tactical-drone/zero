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
        /// <summary>
        /// Ctor
        /// </summary>
        /// <param name="node">The node this peer belongs to </param>
        /// <param name="neighbor">Optional neighbor association</param>
        /// <param name="ioNetClient">The peer transport carrier</param>
        public IoCcPeer(IoNode<IoCcGossipMessage> node, IoCcNeighbor neighbor,
            IoNetClient<IoCcGossipMessage> ioNetClient) 
            : base(node, ioNetClient, userData => new IoCcGossipMessage("gossip rx", $"{ioNetClient.AddressString}", ioNetClient))
        {
            _logger = LogManager.GetCurrentClassLogger();
            IoNetClient = ioNetClient;
            _neighbor = neighbor;

            if(_neighbor != null)
                AttachNeighbor(_neighbor);
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        private IoCcNeighbor _neighbor;
        /// <summary>
        /// The attached neighbor
        /// </summary>
        public IoCcNeighbor Neighbor => _neighbor;

        /// <summary>
        /// Id
        /// </summary>
        public override string Id => _neighbor?.Id??"null";

        /// <summary>
        /// Helper
        /// </summary>
        protected IoNetClient<IoCcGossipMessage> IoNetClient;

        /// <summary>
        /// Attaches a neighbor to this peer
        /// </summary>
        /// <param name="neighbor"></param>
        public void AttachNeighbor(IoCcNeighbor neighbor)
        {
            _neighbor = neighbor ?? throw new ArgumentNullException($"{nameof(neighbor)}");

            //Attach the other way
            _neighbor.AttachPeer(this);

            //peer closed
            ClosedEvent += (sender, args) =>
            {
                _neighbor.Direction = IoCcNeighbor.Kind.Undefined;
                _neighbor.DetachPeer();
            };

            //peer transport closed
            IoNetClient.ClosedEvent += (sender, args) =>
            {
                Close();
            };
        }
    }
}
