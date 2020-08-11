﻿using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc.ModelBinding.Binders;
using NLog;
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
        public IoCcNode(IoNodeAddress gossipAddress, IoNodeAddress peerAddress, IoNodeAddress fpcAddress, Func<IoNode<IoCcGossipMessage>, IoNetClient<IoCcGossipMessage>, object, IoNeighbor<IoCcGossipMessage>> mallocNeighbor, int tcpReadAhead) : base(gossipAddress, mallocNeighbor, tcpReadAhead)
        {
            _logger = LogManager.GetCurrentClassLogger();
            _gossipAddress = gossipAddress;
            _peerAddress = peerAddress;
            _fpcAddress = fpcAddress;
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

        /// <summary>
        /// Handles a neighbor that was selected for gossip
        /// </summary>
        /// <param name="neighbor">The verified neighbor</param>
        public void HandleVerifiedNeighbor(IoCcNeighbor neighbor)
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

            if (!((IoUdpClient<IoCcPeerMessage>)neighbor.Producer).Socket.IsListeningSocket && !((IoUdpClient<IoCcPeerMessage>)neighbor.Producer).Socket.IsConnectingSocket &&
                Neighbors.Count < parm_max_outbound &&
                //TODO add distance calc &&
                neighbor.Services.IoCcRecord.Endpoints.ContainsKey(IoCcService.Keys.peering))
            {
                SpawnConnectionAsync(neighbor.Services.IoCcRecord.Endpoints[IoCcService.Keys.peering]).ContinueWith(async (task) =>
                    {
                        switch (task.Status)
                        {
                            case TaskStatus.RanToCompletion:
                                if (task.Result != null)
                                {
                                    _logger.Info($"Spawning new gossip peer: `{task.Result.Id}'");
                                    ((IoCcPeer)task.Result).SetNeighbor(neighbor);
                                    await task.Result.SpawnProcessingAsync(CancellationToken);
                                }
                                break;
                            case TaskStatus.Canceled:
                            case TaskStatus.Faulted:
                                break;
                        }
                    }).ConfigureAwait(false);
                
            }
            
        }
    }
}
