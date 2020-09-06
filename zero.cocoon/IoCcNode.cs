using System;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Base58Check;
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
        public IoCcNode(IoCcIdentity ioCcIdentity, IoNodeAddress gossipAddress, IoNodeAddress peerAddress,
            IoNodeAddress fpcAddress, IoNodeAddress extAddress, IoNodeAddress bootstrap, int tcpReadAhead)
            : base(gossipAddress, (node, ioNetClient, extraData) => new IoCcPeer((IoCcNode)node, (IoCcNeighbor)extraData, ioNetClient), tcpReadAhead)
        {
            _logger = LogManager.GetCurrentClassLogger();
            _gossipAddress = gossipAddress;
            _peerAddress = peerAddress;
            _fpcAddress = fpcAddress;
            _bootstrap = bootstrap;
            ExtAddress = extAddress;
            CcId = ioCcIdentity;

            Services.IoCcRecord.Endpoints.TryAdd(IoCcService.Keys.peering, _peerAddress);
            Services.IoCcRecord.Endpoints.TryAdd(IoCcService.Keys.gossip, _gossipAddress);
            Services.IoCcRecord.Endpoints.TryAdd(IoCcService.Keys.fpc, _fpcAddress);

            _autoPeering = ZeroOnCascade(new IoCcNeighborDiscovery(this, _peerAddress, (node, client, extraData) => new IoCcNeighbor((IoCcNeighborDiscovery)node, client, extraData), IoCcNeighbor.TcpReadAhead), true);

            Task.Factory.StartNew(async () =>
            {
                var inbound = 0;
                var outbound = 0;
                var available = 0;
                var secondsSinceEnsured = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                while (true)
                {
                    await Task.Delay(1000, AsyncTasks.Token).ConfigureAwait(false);
                    if (Zeroed())
                        break;

                    try
                    {
                        if (InboundCount != inbound || OutboundCount != outbound || _autoPeering.Neighbors.Count - 1 != available)
                        {
                            //_logger.Info($"Peers connected: Inbound = {InboundCount}, Outbound = {OutboundCount}, Available = {_autoPeering.Neighbors.Count - 1}");
                            inbound = InboundCount;
                            outbound = OutboundCount;
                            available = _autoPeering.Neighbors.Count - 1;
                        }

                        //Search for peers
                        if (Neighbors.Count < MaxClients * 0.75 && DateTimeOffset.UtcNow.ToUnixTimeSeconds() - secondsSinceEnsured > parm_discovery_force_time_delay)
                        {
                            secondsSinceEnsured = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                            _logger.Debug($"Neighbors running lean {Neighbors.Count} < {MaxClients * 0.75:0}, trying to discover new ones...");

                            foreach (var autoPeeringNeighbor in _autoPeering.Neighbors.Values.Where(n => ((IoCcNeighbor)n).RoutedRequest && ((IoCcNeighbor)n).Verified && ((IoCcNeighbor)n).Direction == IoCcNeighbor.Kind.Undefined && ((IoCcNeighbor)n).LastKeepAliveReceived < ((IoCcNeighbor)n).parm_zombie_max_ttl))
                            {
                                if (Zeroed())
                                    break;
                                await ((IoCcNeighbor)autoPeeringNeighbor).SendPeerRequestAsync().ConfigureAwait(false);

                                await Task.Delay(1000, AsyncTasks.Token).ConfigureAwait(false);
                            }

                            foreach (var autoPeeringNeighbor in _autoPeering.Neighbors.Values.Where(n => ((IoCcNeighbor)n).RoutedRequest && ((IoCcNeighbor)n).Verified && ((IoCcNeighbor)n).LastKeepAliveReceived < ((IoCcNeighbor)n).parm_zombie_max_ttl))
                            {
                                if (Zeroed())
                                    break;

                                await ((IoCcNeighbor) autoPeeringNeighbor).SendDiscoveryRequestAsync().ConfigureAwait(false);

                                await Task.Delay(1000, AsyncTasks.Token).ConfigureAwait(false);
                            }
                        }
                    }
                    catch (NullReferenceException) { }
                    catch (TaskCanceledException) { }
                    catch (OperationCanceledException) { }
                    catch (ObjectDisposedException) { }
                    catch (Exception e)
                    {
                        _logger.Error(e, $"Failed to ensure {_autoPeering.Neighbors.Count} peers");
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            _autoPeering = null;
            _autoPeeringTask = null;
            CcId = null;
            Services = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override void ZeroManaged()
        {
            Services.IoCcRecord.Endpoints.Clear();
            try
            {
                //_autoPeeringTask?.Wait();
            }
            catch
            {
                // ignored
            }

            base.ZeroManaged();
            //GC.Collect(GC.MaxGeneration);
        }

        private readonly Logger _logger;

        private string _description;
        public override string Description
        {
            get
            {
                if (_description != null)
                    return _description;
                return _description = $"`node({Address})'";
            }
        }

        private IoNode<IoCcPeerMessage> _autoPeering;
        private readonly IoNodeAddress _gossipAddress;
        private readonly IoNodeAddress _peerAddress;
        private readonly IoNodeAddress _fpcAddress;
        private readonly IoNodeAddress _bootstrap;

        /// <summary>
        /// Bootstrap
        /// </summary>
        public IoNodeAddress BootstrapAddress => _bootstrap;

        /// <summary>
        /// Reachable from NAT
        /// </summary>
        public IoNodeAddress ExtAddress { get; protected set; }

        /// <summary>
        /// Experimental support for detection of tunneled UDP connections (WSL)
        /// </summary>
        [IoParameter]
        public bool UdpTunnelSupport = false;

        /// <summary>
        /// Maximum size of a handshake message
        /// </summary>
        [IoParameter]
        public int parm_max_handshake_bytes = 256;

        /// <summary>
        /// Timeout for handshake messages
        /// </summary>
        [IoParameter]
        public int parm_handshake_timeout = 8000;

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
        public int parm_time_e = 20;


        /// <summary>
        /// Time between trying to re-aquire new neighbors using a discovery requests, if the node lacks <see cref="MaxClients"/>
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_discovery_force_time_delay = 60;

        /// <summary>
        /// Maximum clients allowed
        /// </summary>
        public int MaxClients => parm_max_outbound + parm_max_inbound;

        /// <summary>
        /// The node id
        /// </summary>
        public IoCcIdentity CcId { get; protected set;}

        /// <summary>
        /// Spawn the node listeners
        /// </summary>
        /// <param name="acceptConnection"></param>
        /// <returns></returns>
        protected override async Task SpawnListenerAsync(Func<IoNeighbor<IoCcGossipMessage>, Task<bool>> acceptConnection = null)
        {
            //start peering
            _autoPeeringTask = Task.Factory.StartNew(async () => await _autoPeering.StartAsync().ConfigureAwait(false), TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach);

            //DO the continuation inside this callback should always return on the same thread
            await base.SpawnListenerAsync(async peer =>
            {
                //limit connects
                if (InboundCount > parm_max_inbound || Zeroed())
                    return false;

                if (await HandshakeAsync((IoCcPeer) peer).ConfigureAwait(false))
                {
                    _logger.Debug($"Peer {((IoCcPeer)peer).Neighbor.Direction}: Connected! ({peer.Id}:{((IoCcPeer)peer).Neighbor.RemoteAddress.Port})");
                    return true;
                }
                
                return false;
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Sends a message to a peer
        /// </summary>
        /// <param name="peer">The destination</param>
        /// <param name="data">The message</param>
        /// <returns>The number of bytes sent</returns>
        private async Task<int> SendMessage(IoCcPeer peer, ByteString data, int timeout = 0)
        {
            var responsePacket = new Packet
            {
                Data = data,
                PublicKey = ByteString.CopyFrom(CcId.PublicKey),
                Type = (uint)IoCcPeerMessage.MessageTypes.Handshake
            };

            responsePacket.Signature =
                ByteString.CopyFrom(CcId.Sign(responsePacket.Data.ToByteArray(), 0, responsePacket.Data.Length));

            var msgRaw = responsePacket.ToByteArray();

            var sent = await ((IoNetClient<IoCcGossipMessage>)peer.IoSource).Socket.SendAsync(msgRaw, 0, msgRaw.Length, timeout: timeout).ConfigureAwait(false);
            _logger.Trace($"{nameof(HandshakeAsync)}: Sent {sent} bytes to {((IoNetClient<IoCcGossipMessage>)peer.IoSource).Socket.RemoteAddress} ({Enum.GetName(typeof(IoCcPeerMessage.MessageTypes), responsePacket.Type)})");
            return sent;
        }

        private Stopwatch _sw = Stopwatch.StartNew();
        /// <summary>
        /// Perform handshake
        /// </summary>
        /// <param name="peer"></param>
        /// <returns></returns>
        private async Task<bool> HandshakeAsync(IoCcPeer peer)
        {
            if (Zeroed()) return false;

            byte[] handshakeBuffer = new byte[parm_max_handshake_bytes];
            var socket = ((IoNetClient<IoCcGossipMessage>)peer.IoSource).Socket;

            //inbound
            if (((IoNetClient<IoCcGossipMessage>)peer.IoSource).Socket.Ingress)
            {
                var verified = false;

                _sw.Restart();
                //read from the socket
                var bytesRead = await socket.ReadAsync(handshakeBuffer, 0, handshakeBuffer.Length, parm_handshake_timeout).ConfigureAwait(false);

                if (bytesRead == 0)
                {
                    _logger.Debug($"Failed to read inbound challange request, waited = {_sw.ElapsedMilliseconds}ms, socket = {socket.Description}");
                    return false;
                }
                else
                {
                    _logger.Trace($"Read inbound handshake challange request size = {bytesRead} b,socket = {socket.Description}");
                }
                    

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

                    //process handshake request 
                    var handshakeRequest = HandshakeRequest.Parser.ParseFrom(packet.Data);
                    if (handshakeRequest != null)
                    {
                        //reject old handshake requests
                        if (Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - handshakeRequest.Timestamp) >
                            parm_time_e)
                        {
                            _logger.Error($"Rejected old handshake request from {socket.Key} - d = {Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - handshakeRequest.Timestamp)}s, {DateTimeOffset.FromUnixTimeSeconds(handshakeRequest.Timestamp)}");
                            return false;
                        }

                        //reject invalid protocols
                        if (handshakeRequest.Version != parm_version)
                        {
                            _logger.Error($"Invalid handshake protocol version from  {socket.Key} - got {handshakeRequest.Version}, wants {parm_version}");
                            return false;
                        }

                        //reject requests to invalid ext ip
                        //if (handshakeRequest.To != ((IoCcNeighbor)neighbor)?.ExtGossipAddress?.IpPort)
                        //{
                        //    _logger.Error($"Invalid handshake received from {socket.Key} - got {handshakeRequest.To}, wants {((IoCcNeighbor)neighbor)?.ExtGossipAddress.IpPort}");
                        //    return false;
                        //}

                        //send response
                        var handshakeResponse = new HandshakeResponse
                        {
                            ReqHash = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(packet.Data.ToByteArray()))
                        };

                        var sent = 0;
                        _sw.Restart();
                        if ((sent = await SendMessage(peer, handshakeResponse.ToByteString(), parm_handshake_timeout).ConfigureAwait(false)) == 0)
                        {
                            _logger.Debug($"Failed to send inbound handshake challange response, tried for = {_sw.ElapsedMilliseconds}ms");
                            return false;
                        }
                        else
                        {
                            _logger.Trace($"Sent inbound handshake challange response size = {sent} b, socket = {socket.Description}");
                        }
                    }

                    //Verify the connection 
                    var id = IoCcNeighbor.MakeId(IoCcIdentity.FromPubKey(packet.PublicKey.ToByteArray()), socket.RemoteAddress);
                    if (_autoPeering.Neighbors.TryGetValue(id, out var neighbor))
                    {
                        var direction = ((IoCcNeighbor) neighbor).Direction;
                        if (((IoCcNeighbor) neighbor).Verified &&
                            ((IoCcNeighbor) neighbor).Direction == IoCcNeighbor.Kind.Inbound &&
                            !((IoCcNeighbor)neighbor).Peered())
                        {
                            return peer.AttachNeighbor((IoCcNeighbor)neighbor);
                        }
                        else
                        {
                            _logger.Debug($"{direction} handshake [REJECT] {id} - {socket.RemoteAddress}: v = {((IoCcNeighbor)neighbor).Verified}");
                            return false;
                        }
                    }
                    else
                    {
                        _logger.Error($"Neighbor {id} not found, dropping {IoCcNeighbor.Kind.Inbound} connection from {socket.RemoteAddress}");
                        return false;
                    }
                }
            }
            else if (((IoNetClient<IoCcGossipMessage>)peer.IoSource).Socket.Egress)//Outbound
            {
                var handshakeRequest = new HandshakeRequest
                {
                    Version = parm_version,
                    To = socket.ListeningAddress.IpPort,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                };

                var sent = 0;
                _sw.Restart();
                //Send challenge
                if ((sent = await SendMessage(peer, handshakeRequest.ToByteString(), parm_handshake_timeout).ConfigureAwait(false)) == 0)
                {
                    _logger.Debug($"Failed to send handshake challange request, tried for = {_sw.ElapsedMilliseconds}ms, socket = {socket.Description}");
                }
                else
                {
                    _logger.Trace($"Sent handshake challange request size = {sent} b, socket = {socket.Description}");
                }

                //Read challenge response 
                var bytesRead = await socket.ReadAsync(handshakeBuffer, 0, handshakeBuffer.Length, parm_handshake_timeout).ConfigureAwait(false);
                if (bytesRead == 0)
                {
                    _logger.Debug($"Failed to read outbound handshake challange response, waited = {_sw.ElapsedMilliseconds}ms, address = {socket.RemoteAddress}");
                    return false;
                }
                else
                {
                    _logger.Trace($"Read outbound handshake challange response size = {bytesRead} b, addess = {socket.RemoteAddress}");
                }

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

                    _logger.Trace($"HandshakeResponse [{(verified ? "signed" : "un-signed")}], read = {bytesRead}, {peer.IoSource.Key}");

                    //Don't process unsigned or unknown messages
                    if (!verified)
                    {
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

                    //Verify the connection 
                    var id = IoCcNeighbor.MakeId(IoCcIdentity.FromPubKey(packet.PublicKey.ToByteArray()), socket.RemoteAddress);
                    if (_autoPeering.Neighbors.TryGetValue(id, out var neighbor))
                    {
                        var direction = ((IoCcNeighbor) neighbor).Direction;
                        if (((IoCcNeighbor) neighbor).Verified &&
                            ((IoCcNeighbor) neighbor).Direction == IoCcNeighbor.Kind.OutBound &&
                            !((IoCcNeighbor)neighbor).Peered())
                        {
                            return peer.AttachNeighbor((IoCcNeighbor)neighbor);
                        }
                        else
                        {
                            _logger.Debug($"{direction} handshake [REJECT] {id} - {socket.RemoteAddress}: v = {((IoCcNeighbor)neighbor).Verified}");
                            return false;
                        }
                    }
                    else
                    {
                        _logger.Error($"Neighbor {id} not found, dropping {IoCcNeighbor.Kind.OutBound} connection to {socket.RemoteAddress}");
                        return false;
                    }
                }
            }

            return false;
        }

        

        /// <summary>
        /// Opens an <see cref="IoCcNeighbor.Kind.OutBound"/> connection to a gossip peer
        /// </summary>
        /// <param name="neighbor">The verified neighbor associated with this connection</param>
        public async Task<bool> ConnectToPeer(IoCcNeighbor neighbor)
        {
            if (neighbor.RoutedRequest && neighbor.Direction == IoCcNeighbor.Kind.OutBound &&
                OutboundCount < parm_max_outbound &&
                //TODO add distance calc &&
                neighbor.Services.IoCcRecord.Endpoints.ContainsKey(IoCcService.Keys.gossip))
            {
                if (neighbor.Direction == IoCcNeighbor.Kind.OutBound)
                {
                    var neighborConnection = await SpawnConnectionAsync(neighbor.Services.IoCcRecord.Endpoints[IoCcService.Keys.gossip], neighbor)
                        .ContinueWith(async (peer) =>
                        {
                            switch (peer.Status)
                            {
                                case TaskStatus.RanToCompletion:
                                    if (peer.Result != null)
                                    {
                                        if (await HandshakeAsync((IoCcPeer)peer.Result).ConfigureAwait(false))
                                        {
                                            _logger.Debug($"Peer {neighbor.Direction}: Connected! ({peer.Result.Id}:{neighbor.RemoteAddress.Port})");
                                            NeighborTasks.Add(peer.Result.SpawnProcessingAsync());
                                        }
                                        else
                                        {
                                            peer.Result.Zero(this);
                                        }
                                    }
                                    break;
                                case TaskStatus.Canceled:
                                case TaskStatus.Faulted:
                                    neighbor.DetachPeer();
                                    _logger.Error(peer.Exception, $"Peer select {neighbor.RemoteAddress} failed");
                                    break;
                            }
                        }).ConfigureAwait(false);

                    //Connect success?
                    return neighborConnection != null;
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
                return false;
            }
        }
    }
}
