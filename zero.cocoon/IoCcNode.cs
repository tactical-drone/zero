using System;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.AspNetCore.Mvc.ModelBinding.Binders;
using NLog;
using Proto;
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
    public class IoCcNode : IoNode<IoCcGossipMessage>
    {
        public IoCcNode(IoNodeAddress gossipAddress, IoNodeAddress peerAddress, IoNodeAddress fpcAddress, IoNodeAddress extAddress, int tcpReadAhead)
            : base(gossipAddress, (node, ioNetClient, extraData) => new IoCcPeer((IoCcNode)node, (IoCcNeighbor)extraData, ioNetClient), tcpReadAhead)
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
        public IoCcNeighborDiscovery DiscoveryService => (IoCcNeighborDiscovery)_autoPeering;

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
            _autoPeeringTask = Task.Factory.StartNew(async () => await _autoPeering.StartAsync().ConfigureAwait(false), TaskCreationOptions.LongRunning);

            await base.SpawnListenerAsync(async neighbor =>
            {

                if (Neighbors.Count > MaxClients)
                    return false;

                return await HandshakeAsync((IoCcPeer)neighbor);

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
            });
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="peer"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        private async Task<int> SendMessage(IoCcPeer peer, ByteString data)
        {
            var responsePacket = new Packet
            {
                Data = data,
                PublicKey = ByteString.CopyFrom(IoCcPeerMessage.CcId.PublicKey),
                Type = 0
            };

            responsePacket.Signature =
                ByteString.CopyFrom(IoCcPeerMessage.CcId.Sign(responsePacket.Data.ToByteArray(), 0, responsePacket.Data.Length));

            var msgRaw = responsePacket.ToByteArray();

            var sent = await ((IoNetClient<IoCcGossipMessage>)peer.IoSource).Socket.SendAsync(msgRaw, 0, msgRaw.Length);
            _logger.Debug($"{nameof(HandshakeAsync)}: Sent {sent} bytes to {((IoNetClient<IoCcGossipMessage>)peer.IoSource).Socket.RemoteAddress} ({Enum.GetName(typeof(IoCcPeerMessage.MessageTypes), responsePacket.Type)})");
            return sent;
        }

        /// <summary>
        /// Perform handshake
        /// </summary>
        /// <param name="peer"></param>
        /// <returns></returns>
        private async Task<bool> HandshakeAsync(IoCcPeer peer)
        {
            byte[] handshakeBuffer = new byte[512];
            var socket = ((IoNetClient<IoCcGossipMessage>)peer.IoSource).Socket;

            //inbound
            if (((IoNetClient<IoCcGossipMessage>)peer.IoSource).Socket.Ingress)
            {
                var verified = false;

                var bytesRead = await socket.ReadAsync(handshakeBuffer, 0, handshakeBuffer.Length);

                if (bytesRead == 0)
                    return false;

                var packet = Packet.Parser.ParseFrom(handshakeBuffer, 0, bytesRead);

                if (packet != null && packet.Data != null && packet.Data.Length > 0)
                {
                    var packetData = packet.Data.ToByteArray(); //TODO remove copy

                    if (packet.Signature != null || packet.Signature?.Length != 0)
                    {
                        verified = IoCcPeerMessage.CcId.Verify(packetData, 0,
                            packetData.Length, packet.PublicKey.ToByteArray(), 0,
                            packet.Signature.ToByteArray(), 0);
                    }

                    _logger.Debug($"HandshakeRequest [{(verified ? "signed" : "un-signed")}], {peer.IoSource.Key}");

                    //Don't process unsigned or unknown messages
                    if (!verified)
                        return false;

                    //Verify the connection 
                    var id = IoCcNeighbor.MakeId(IoCcIdentity.FromPK(packetData), socket.RemoteAddress);
                    if (_autoPeering.Neighbors.TryGetValue(id, out var neighbor))
                    {
                        if(((IoCcNeighbor)neighbor).Verified)
                            peer.AttachNeighbor((IoCcNeighbor)neighbor);
                        else
                            _logger.Debug($"Neighbor {id} not verified, dropping connection from {socket.RemoteAddress}");
                    }
                    else
                    {
                        _logger.Debug($"Neighbor {id} not found, dropping connection from {socket.RemoteAddress}");
                    }
                    
                    //process handshake request 
                    var handshakeRequest = HandshakeRequest.Parser.ParseFrom(packet.Data);
                    if (handshakeRequest != null)
                    {
                        var handshakeResponse = new HandshakeResponse
                        {
                            ReqHash = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(packet.Data.ToByteArray()))
                        };

                        await SendMessage(peer, handshakeResponse.ToByteString());
                    }

                }
                return true;
            }
            else if (((IoNetClient<IoCcGossipMessage>)peer.IoSource).Socket.Egress)//Outbound
            {
                var handshakeRequest = new HandshakeRequest
                {
                    Version = 0,
                    To = socket.ListeningAddress.Ip,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                };

                await SendMessage(peer, handshakeRequest.ToByteString());
                _logger.Trace($"{nameof(HandshakeRequest)}: Sent to {socket.Description}");

                var bytesRead = await socket.ReadAsync(handshakeBuffer, 0, handshakeBuffer.Length);

                var verified = false;
                var packet = Packet.Parser.ParseFrom(handshakeBuffer, 0, bytesRead);
                if (packet.Data != null && packet.Data.Length > 0)
                {
                    var packetData = packet.Data.ToByteArray(); //TODO remove copy

                    if (packet.Signature != null || packet.Signature?.Length != 0)
                    {
                        verified = IoCcPeerMessage.CcId.Verify(packetData, 0,
                            packetData.Length, packet.PublicKey.ToByteArray(), 0,
                            packet.Signature.ToByteArray(), 0);
                    }

                    _logger.Debug($"HandshakeResponse [{(verified ? "signed" : "un-signed")}], s = {bytesRead}");

                    //Don't process unsigned or unknown messages
                    if (!verified)
                    {
                        return false;
                    }

                    //Verify the connection 
                    var id = IoCcNeighbor.MakeId(IoCcIdentity.FromPK(packetData), socket.RemoteAddress);
                    if (_autoPeering.Neighbors.TryGetValue(id, out var neighbor))
                    {
                        if (((IoCcNeighbor)neighbor).Verified)
                            peer.AttachNeighbor((IoCcNeighbor)neighbor);
                        else
                            _logger.Debug($"Neighbor {id} not verified, dropping connection from {socket.RemoteAddress}");
                    }
                    else
                    {
                        _logger.Debug($"Neighbor {id} not found, dropping connection from {socket.RemoteAddress}");
                    }

                    var handshakeResponse = HandshakeResponse.Parser.ParseFrom(packet.Data);

                    if (handshakeResponse != null)
                    {
                        if (!IoCcIdentity.Sha256
                            .ComputeHash(handshakeRequest.ToByteArray())
                            .SequenceEqual(handshakeResponse.ReqHash))
                        {
                            _logger.Error($"Invalid handshake response! Closing {socket.Key}");
                            return false;
                        }
                    }
                }
            }
            return true;
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
                                        if (await HandshakeAsync((IoCcPeer)task.Result))
                                        {
                                            _logger.Info($"Peer {neighbor.Direction}: Connected! ({task.Result.Id})");
                                            ((IoCcPeer)task.Result).AttachNeighbor(neighbor);
                                            await task.Result.SpawnProcessingAsync(CancellationToken);
                                        }
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
