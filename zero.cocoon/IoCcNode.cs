using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc.ModelBinding.Binders;
using NLog;
using Tangle.Net.Repository.Responses;
using zero.cocoon.autopeer;
using zero.cocoon.identity;
using zero.cocoon.models;
using zero.cocoon.models.services;
using zero.core.conf;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;

namespace zero.cocoon
{
    public class IoCcNode:IoNode<IoCcGossipMessage>
    {
        public IoCcNode(IoNodeAddress gossipAddress, IoNodeAddress peerAddress, IoNodeAddress fpcAddress, IoNodeAddress extAddress, Func<IoNode<IoCcGossipMessage>, IoNetClient<IoCcGossipMessage>, object, IoNeighbor<IoCcGossipMessage>> mallocNeighbor, int tcpReadAhead) : base(gossipAddress, mallocNeighbor, tcpReadAhead)
        {
            _logger = LogManager.GetCurrentClassLogger();
            _gossipAddress = gossipAddress;
            _peerAddress = peerAddress;
            _fpcAddress = fpcAddress;
            ExtAddress = extAddress;
            _autoPeering = new IoCcNeighborDiscovery(this, _peerAddress, 
                (node, client, extraData) => new IoCcNeighbor((IoCcNeighborDiscovery) node, client, extraData), IoCcNeighbor.TcpReadAhead);

            Services.IoCcRecord.Endpoints.TryAdd(IoCcService.Keys.peering, _peerAddress);
            Services.IoCcRecord.Endpoints.TryAdd(IoCcService.Keys.gossip, _gossipAddress);
            Services.IoCcRecord.Endpoints.TryAdd(IoCcService.Keys.fpc, _fpcAddress);

            _autoPeeringTask = _autoPeering.StartAsync();
            
        }

        private readonly Logger _logger;
        private readonly IoNode<IoCcPeerMessage> _autoPeering;
        private readonly IoNodeAddress _gossipAddress;
        private readonly IoNodeAddress _peerAddress;
        private readonly IoNodeAddress _fpcAddress;

        public IoNodeAddress ExtAddress { get; protected set; }

        [IoParameter] public bool UdpTunnelSupport = true;

        public IoCcNeighborDiscovery DiscoveryService => (IoCcNeighborDiscovery) _autoPeering;
        public IoCcService Services { get; set; } = new IoCcService();

        private readonly Task _autoPeeringTask;

        /// <summary>
        /// Max inbound neighbors
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_inbound = 4;

        /// <summary>
        /// Max inbound neighbors
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_outbound = 4;


        protected override Task SpawnListenerAsync(Func<IoNeighbor<IoCcGossipMessage>, Task<bool>> acceptConnection = null)
        {
            return base.SpawnListenerAsync(neighbor =>
            {
                
                if (Neighbors.Count > parm_max_inbound + parm_max_outbound)
                    return Task.FromResult(false);
                var searchStr = neighbor.IoSource.Key.Split(":")[1].Replace($"//", "");
                var ccNeighbor = (IoCcNeighbor) _autoPeering.Neighbors
                    .FirstOrDefault(n => n.Value.Id.Contains(searchStr))
                    .Value;

                if (ccNeighbor == default)
                {
                    _logger.Debug($"Dropping connection {neighbor.IoSource.Key}, {searchStr} not verified! ({_autoPeering.Neighbors.Count})");
                    return Task.FromResult(false);
                }
                    
                
                //if (!_autoPeering.Neighbors.ContainsKey(neighbor.Id))
                //    return Task.FromResult(false);

                if (ccNeighbor.Direction == IoCcNeighbor.Kind.Inbound)
                {
                    _logger.Info($"Peering selected {ccNeighbor.Direction}: {ccNeighbor.Id}");
                    ((IoCcPeer)neighbor).AttachPeerNeighbor(ccNeighbor);
                    return Task.FromResult(true);
                }
                else
                {
                    _logger.Debug($"Dropping inbound connection to {ccNeighbor.Id}, Kind = {ccNeighbor.Direction}");
                    return Task.FromResult(false);
                }
            });
        }

        /// <summary>
        /// Handles a neighbor that was selected for gossip
        /// </summary>
        /// <param name="neighbor">The verified neighbor</param>
        public void AddNeighbor(IoCcNeighbor neighbor)
        {
            Task<IoNeighbor<IoCcGossipMessage>> peer;

            //if (Neighbors.Count < parm_max_outbound &&
            //    //TODO add distance calc &&
            //    neighbor.Services.IoCcRecord.Endpoints.ContainsKey(IoCcService.Keys.peering) && 
            //    (peer = new IoCcPeer(this, neighbor, new IoTcpClient<IoCcGossipMessage>(neighbor.Services.IoCcRecord.Endpoints[IoCcService.Keys.peering], 1))) != null &&
            //    Neighbors.TryAdd(peer.Id, peer))
            //{
            //    _logger.Info($"Spawning new gossip peer: `{peer.Id}'");
            //    peer.SpawnProcessingAsync(CancellationToken);
            //}

            if (neighbor.Address != null && (((IoUdpClient<IoCcPeerMessage>)neighbor.Source).Socket.IsListeningSocket || ((IoUdpClient<IoCcPeerMessage>)neighbor.Source).Socket.IsConnectingSocket) &&
                Neighbors.Count < parm_max_outbound &&
                //TODO add distance calc &&
                neighbor.Services.IoCcRecord.Endpoints.ContainsKey(IoCcService.Keys.gossip))
            {
                if (neighbor.Direction == IoCcNeighbor.Kind.OutBound)
                {
                    SpawnConnectionAsync(neighbor.Services.IoCcRecord.Endpoints[IoCcService.Keys.gossip], neighbor)
                        .ContinueWith(async (task) =>
                        {
                            switch (task.Status)
                            {
                                case TaskStatus.RanToCompletion:
                                    if (task.Result != null)
                                    {
                                        _logger.Info($"Peering selected {neighbor.Direction}: {task.Result.Id}");
                                        ((IoCcPeer) task.Result).AttachPeerNeighbor(neighbor);
                                        await task.Result.SpawnProcessingAsync(CancellationToken);
                                    }
                                    break;
                                case TaskStatus.Canceled:
                                case TaskStatus.Faulted:
                                    _logger.Error(task.Exception, $"Peer select {neighbor.Address} failed");
                                    break;
                            }
                        }).ConfigureAwait(false);
                }
            }
            else
            {
                _logger.Trace($"Handled {neighbor.Description}");
            }
            
        }
    }
}
