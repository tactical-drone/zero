﻿using System;
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

            Task.Run(async () =>
            {
                while (!_spinners.IsCancellationRequested)
                {
                    await Task.Delay(60000);
                    _logger.Fatal($"Peers connected: Inbound = {InboundCount}, Outbound = {OutboundCount}");
                }
            });
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
        /// Maximum size of a handshake message
        /// </summary>
        [IoParameter]
        public int parm_max_handshake_bytes = 256;

        /// <summary>
        /// Timeout for handshake messages
        /// </summary>
        [IoParameter]
        public int parm_handshake_timeout = 500;

        /// <summary>
        /// The discovery service
        /// </summary>
        public IoCcNeighborDiscovery DiscoveryService => (IoCcNeighborDiscovery)_autoPeering;

        /// <summary>
        /// Number of inbound neighbors
        /// </summary>
        public int InboundCount => Neighbors.Count(kv => ((IoCcPeer) kv.Value).Neighbor?.Inbound ?? false);

        /// <summary>
        /// Number of outbound neighbors
        /// </summary>
        public int OutboundCount => Neighbors.Count(kv => ((IoCcPeer)kv.Value).Neighbor?.Outbound ?? false);


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
        /// Protocol version
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public uint parm_version = 0;

        /// <summary>
        /// Protocol response message timeout in seconds
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_response_timeout = 20;


        /// <summary>
        /// Maximum clients allowed
        /// </summary>
        public int MaxClients => parm_max_outbound + parm_max_inbound;

        /// <summary>
        /// The node id
        /// </summary>
        public IoCcIdentity CcId { get; protected set; } = IoCcIdentity.Generate(true);

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
                //limit connects
                if (InboundCount >= parm_max_inbound)
                    return false;
                
                return await HandshakeAsync((IoCcPeer)neighbor);
            });
        }

        /// <summary>
        /// Sends a message to a peer
        /// </summary>
        /// <param name="peer">The destination</param>
        /// <param name="data">The message</param>
        /// <returns>The number of bytes sent</returns>
        private async Task<int> SendMessage(IoCcPeer peer, ByteString data)
        {
            var responsePacket = new Packet
            {
                Data = data,
                PublicKey = ByteString.CopyFrom(CcId.PublicKey),
                Type = 0
            };

            responsePacket.Signature =
                ByteString.CopyFrom(CcId.Sign(responsePacket.Data.ToByteArray(), 0, responsePacket.Data.Length));

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
            byte[] handshakeBuffer = new byte[parm_max_handshake_bytes];
            var socket = ((IoNetClient<IoCcGossipMessage>)peer.IoSource).Socket;

            //inbound
            if (((IoNetClient<IoCcGossipMessage>)peer.IoSource).Socket.Ingress)
            {
                var verified = false;

                //read from the socket
                var bytesRead = await socket.ReadAsync(handshakeBuffer, 0, handshakeBuffer.Length, parm_handshake_timeout);

                if (bytesRead == 0)
                    return false;

                //parse a packet
                var packet = Packet.Parser.ParseFrom(handshakeBuffer, 0, bytesRead);

                if (packet != null && packet.Data != null && packet.Data.Length > 0)
                {
                    var packetData = packet.Data.ToByteArray(); //TODO remove copy

                    //verify the signature
                    if (packet.Signature != null || packet.Signature?.Length != 0)
                    {
                        verified = CcId.Verify(packetData, 0,
                            packetData.Length, packet.PublicKey.ToByteArray(), 0,
                            packet.Signature.ToByteArray(), 0);
                    }

                    _logger.Debug($"HandshakeRequest [{(verified ? "signed" : "un-signed")}], read = {bytesRead}, {peer.IoSource.Key}");

                    //Don't process unsigned or unknown messages
                    if (!verified)
                        return false;

                    //Verify the connection 
                    var id = IoCcNeighbor.MakeId(IoCcIdentity.FromPK(packet.PublicKey.ToByteArray()), socket.RemoteAddress);
                    if (_autoPeering.Neighbors.TryGetValue(id, out var neighbor))
                    {
                        if(((IoCcNeighbor)neighbor).Verified)
                            peer.AttachNeighbor((IoCcNeighbor)neighbor);
                        else
                        {
                            _logger.Debug($"Neighbor {id} not verified, dropping connection from {socket.RemoteAddress}");
                            //await ((IoCcNeighbor)neighbor).SendPingMsgAsync();
                            return false;
                        }
                            
                    }
                    else
                    {
                        _logger.Debug($"Neighbor {id} not found, dropping connection from {socket.RemoteAddress}");
                        return false;
                    }
                    
                    //process handshake request 
                    var handshakeRequest = HandshakeRequest.Parser.ParseFrom(packet.Data);
                    if (handshakeRequest != null)
                    {
                        //reject old handshake requests
                        if (Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - handshakeRequest.Timestamp) >
                            parm_response_timeout)
                        {
                            _logger.Debug($"Rejected old handshake request from {socket.Key} - {DateTimeOffset.FromUnixTimeSeconds(handshakeRequest.Timestamp)}");
                            return false;
                        }

                        //reject invalid protocols
                        if (handshakeRequest.Version != parm_version)
                        {
                            _logger.Debug($"Invalid handshake protocol version from  {socket.Key} - got {handshakeRequest.Version}, wants {parm_version}");
                            return false;
                        }

                        //reject requests to invalid ext ip
                        if (handshakeRequest.To != ((IoCcNeighbor)neighbor)?.GossipAddress.IpPort)
                        {
                            _logger.Debug($"Invalid handshake received from {socket.Key} - got {handshakeRequest.To}, wants {((IoCcNeighbor)neighbor)?.GossipAddress.IpPort}");
                            return false;
                        }

                        //send response
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
                    Version = parm_version,
                    To = socket.ListeningAddress.IpPort,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                };

                //Send challenge
                await SendMessage(peer, handshakeRequest.ToByteString());
                _logger.Trace($"{nameof(HandshakeRequest)}: Sent to {socket.Description}");

                //Read challenge response 
                var bytesRead = await socket.ReadAsync(handshakeBuffer, 0, handshakeBuffer.Length, parm_handshake_timeout);

                var verified = false;
                var packet = Packet.Parser.ParseFrom(handshakeBuffer, 0, bytesRead);
                if (packet.Data != null && packet.Data.Length > 0)
                {
                    var packetData = packet.Data.ToByteArray(); //TODO remove copy

                    //verify signature
                    if (packet.Signature != null || packet.Signature?.Length != 0)
                    {
                        verified = CcId.Verify(packetData, 0,
                            packetData.Length, packet.PublicKey.ToByteArray(), 0,
                            packet.Signature.ToByteArray(), 0);
                    }

                    _logger.Debug($"HandshakeResponse [{(verified ? "signed" : "un-signed")}], read = {bytesRead}, {peer.IoSource.Key}");

                    //Don't process unsigned or unknown messages
                    if (!verified)
                    {
                        return false;
                    }

                    //Verify the connection 
                    var id = IoCcNeighbor.MakeId(IoCcIdentity.FromPK(packet.PublicKey.ToByteArray()), socket.RemoteAddress);
                    if (_autoPeering.Neighbors.TryGetValue(id, out var neighbor))
                    {
                        if (((IoCcNeighbor)neighbor).Verified)
                            peer.AttachNeighbor((IoCcNeighbor)neighbor);
                        else
                        {
                            _logger.Debug($"Neighbor {id} not verified, dropping connection from {socket.RemoteAddress}");
                            //await ((IoCcNeighbor) neighbor).SendPingMsgAsync();
                            return false;
                        }
                            
                    }
                    else
                    {
                        _logger.Debug($"Neighbor {id} not found, dropping connection from {socket.RemoteAddress}");
                        return false;
                    }

                    //validate handshake response
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
                OutboundCount < parm_max_outbound &&
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
                                            await task.Result.SpawnProcessingAsync();
                                        }
                                    }
                                    break;
                                case TaskStatus.Canceled:
                                case TaskStatus.Faulted:
                                    neighbor.DetachPeer();
                                    _logger.Error(task.Exception, $"Peer select {neighbor.Address} failed");
                                    break;
                            }
                        }).ConfigureAwait(false);
                }
                else
                {
                    neighbor.DetachPeer();
                    throw new ApplicationException($"Unexpected direction {neighbor.Direction}");
                }
            }
            else
            {
                neighbor.DetachPeer();
                _logger.Trace($"Handled {neighbor.Description}");
            }

        }
    }
}
