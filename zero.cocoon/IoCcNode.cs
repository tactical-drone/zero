using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc.ModelBinding.Binders;
using NLog;
using zero.cocoon.autopeer;
using zero.cocoon.models;
using zero.core.conf;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;

namespace zero.cocoon
{
    public class IoCcNode<TJob, TKey>:IoNode<TJob>
        where TJob : IIoWorker
    {
        public IoCcNode(IoNodeAddress address, Func<IoNode<TJob>, IoNetClient<TJob>, IoNeighbor<TJob>> mallocNeighbor, int tcpReadAhead) : base(address, mallocNeighbor, tcpReadAhead)
        {
            _logger = LogManager.GetCurrentClassLogger();
            _autoPeering = new IoPeerDiscovery<IoCcPeerMessage<byte[]>, byte[]>(IoNodeAddress.Create($"{AutoPeerListenerProto}://{address.HostStr}:{AutoPeerListenerPort}"), 
                (node, client) => new IoCcNeighbor<byte[]>((IoPeerDiscovery<IoCcPeerMessage<byte[]>, byte[]>) node, client), IoCcNeighbor<byte[]>.TcpReadAhead);
            _autoPeeringTask = _autoPeering.StartAsync();
        }

        private readonly Logger _logger;
        private readonly IoNode<IoCcPeerMessage<byte[]>> _autoPeering;
        private readonly Task _autoPeeringTask;

        [IoParameter]
        public string AutoPeerListenerPort { get; set; }= "14627";

        [IoParameter]
        public string AutoPeerListenerProto { get; set; } = "udp";

    }
}
