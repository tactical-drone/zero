using System;
using NLog;
using zero.cocoon.models;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.cocoon
{
    public class IoCcPeer<TKey> : IoNeighbor<IoCcGossipMessage<TKey>>
    {
        public IoCcPeer(IoNode<IoCcGossipMessage<TKey>> node, IoNetClient<IoCcGossipMessage<TKey>> ioNetClient) 
            : base(node, ioNetClient, userData => new IoCcGossipMessage<TKey>("gossip rx", $"{ioNetClient.AddressString}", ioNetClient))
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        private readonly Logger _logger;
    }
}
