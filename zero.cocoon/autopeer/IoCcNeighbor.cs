using System;
using System.Collections.Generic;
using System.Text;
using NLog;
using zero.cocoon.models;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.bushes;

namespace zero.cocoon.autopeer
{
    public class IoCcNeighbor<TKey> : IoNeighbor<IoCcPeerMessage<TKey>>
    {
        public IoCcNeighbor(IoNode<IoCcPeerMessage<TKey>> node, IoNetClient<IoCcPeerMessage<TKey>> ioNetClient) : base(node, ioNetClient, userData=> new IoCcPeerMessage<TKey>("peer rx", $"{ioNetClient.AddressString}", ioNetClient))
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        public const int TcpReadAhead = 50;

    }
}
