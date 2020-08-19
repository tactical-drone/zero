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
    /// <summary>
    /// Connects to cocoon
    /// </summary>
    public class IoCcNode:IoNode<IoCcGossipMessage>
    {
        public IoCcNode(IoNodeAddress gossipAddress, IoNodeAddress peerAddress, IoNodeAddress fpcAddress, IoNodeAddress extAddress, int tcpReadAhead) 
            : base(gossipAddress, (node, ioNetClient, extraData) => new IoCcPeer((IoCcNode)node, (IoCcNeighbor) extraData, ioNetClient), tcpReadAhead)
        {
            _logger = LogManager.GetCurrentClassLogger();
            _gossipAddress = gossipAddress;
            _peerAddress = peerAddress;
            _fpcAddress = fpcAddress;
            ExtAddress = extAddress;

            Services.IoCcRecord.Endpoints.TryAdd(IoCcService.Keys.peering, _peerAddress);
            Services.IoCcRecord.Endpoints.TryAdd(IoCcService.Keys.gossip, _gossipAddress);
            Services.IoCcRecord.Endpoints.TryAdd(IoCcService.Keys.fpc, _fpcAddress);

            _autoPeering = new IoCcNeighborDiscovery(this, _peerAddress,
                (node, client, extraData) => new IoCcNeighbor((IoCcNeighborDiscovery)node, client, extraData), IoCcNeighbor.TcpReadAhead);
        }

        private readonly Logger _logger;
        private readonly IoNode<IoCcPeerMessage> _autoPeering;
        private readonly IoNodeAddress _gossipAddress;
        private readonly IoNodeAddress _peerAddress;
        private readonly IoNodeAddress _fpcAddress;

        /// <summary>
        /// Reachable from DMZ
        /// </summary>
        public IoNodeAddress ExtAddress { get; protected set; }

        /// <summary>
        /// Experimental support for detection of tunneled UDP connections (WSL)
        /// </summary>
        [IoParameter] 
        public bool UdpTunnelSupport = true;

        /// <summary>
        /// The discovery service
        /// </summary>
        public IoCcNeighborDiscovery DiscoveryService => (IoCcNeighborDiscovery) _autoPeering;

        /// <summary>
        /// The services this node supports
        /// </summary>
        public IoCcService Services { get; set; } = new IoCcService();

        /// <summary>
        /// The autopeering task handler
        /// </summary>
        private Task _autoPeeringTask;

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
        /// Maximum clients allowed
        /// </summary>
        public int MaxClients => parm_max_outbound + parm_max_inbound;

        /// <summary>
        /// Spawn the node listeners
        /// </summary>
        /// <param name="acceptConnection"></param>
        /// <returns></returns>
        protected override async Task SpawnListenerAsync(Func<IoNeighbor<IoCcGossipMessage>, Task<bool>> acceptConnection = null)
        {
            //start peering
            _autoPeeringTask = Task.Factory.StartNew(async () => await _autoPeering.StartAsync().ConfigureAwait(false), TaskCreationOptions.LongRunning );

            await base.SpawnListenerAsync(neighbor =>
            {
                
                if (Neighbors.Count > MaxClients)
                    return Task.FromResult(false);

                //var searchStr = neighbor.IoSource.Key.Split(":")[1].Replace($"//", "");
                //var connected = false;
                //_autoPeering.Neighbors
                //    .Where(n => n.Value.Id.Contains(searchStr)).ToList().ForEach(kv =>
                //    {
                //        var ccNeighbor = (IoCcNeighbor)kv.Value;

                //        if (ccNeighbor == default)
                //        {
                //            _logger.Debug($"Connection {neighbor.IoSource.Key}, {searchStr} not verified! ({_autoPeering.Neighbors.Count})");
                //            return;
                //        }

                //        if (ccNeighbor.Direction == IoCcNeighbor.Kind.Inbound)
                //        {
                //            _logger.Info($"Connection Received {ccNeighbor.Direction}: {ccNeighbor.Id}");
                //            ((IoCcPeer)neighbor).AttachNeighbor(ccNeighbor);
                //            connected = true;
                //        }
                //        else
                //        {
                //            _logger.Debug($"Dropping inbound connection from {ccNeighbor.Id}, neighbor is set as {ccNeighbor.Direction}");
                //        }
                //    });
                return Task.FromResult(true);
            });
        }

        /// <summary>
        /// Opens an <see cref="IoCcNeighbor.Kind.OutBound"/> connection to a gossip peer
        /// </summary>
        /// <param name="neighbor">The verified neighbor associated with this connection</param>
        public void ConnectToPeer(IoCcNeighbor neighbor)
        {
            if (neighbor.Address != null && neighbor.Direction == IoCcNeighbor.Kind.OutBound &&
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
                                        _logger.Info($"Peer {neighbor.Direction}: Connected! ({task.Result.Id})");
                                        ((IoCcPeer) task.Result).AttachNeighbor(neighbor);
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
