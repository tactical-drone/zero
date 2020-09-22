using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading;
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
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.misc;

namespace zero.cocoon
{
    /// <summary>
    /// Connects to cocoon
    /// </summary>
    public class IoCcNode : IoNode<IoCcGossipMessage>
    {
        public IoCcNode(IoCcIdentity ioCcIdentity, IoNodeAddress gossipAddress, IoNodeAddress peerAddress,
            IoNodeAddress fpcAddress, IoNodeAddress extAddress, List<IoNodeAddress> bootstrap, int tcpReadAhead)
            : base(gossipAddress, (node, ioNetClient, extraData) => new IoCcPeer((IoCcNode)node, (IoCcNeighbor)extraData, ioNetClient), tcpReadAhead)
        {
            _logger = LogManager.GetCurrentClassLogger();
            _gossipAddress = gossipAddress;
            _peerAddress = peerAddress;
            _fpcAddress = fpcAddress;
            BootstrapAddress = bootstrap;
            ExtAddress = extAddress;
            CcId = ioCcIdentity;

            Services.IoCcRecord.Endpoints.TryAdd(IoCcService.Keys.peering, _peerAddress);
            Services.IoCcRecord.Endpoints.TryAdd(IoCcService.Keys.gossip, _gossipAddress);
            Services.IoCcRecord.Endpoints.TryAdd(IoCcService.Keys.fpc, _fpcAddress);

            _autoPeering = ZeroOnCascade(new IoCcNeighborDiscovery(this, _peerAddress, (node, client, extraData) => new IoCcNeighbor((IoCcNeighborDiscovery)node, client, extraData), IoCcNeighbor.TcpReadAhead), true).target;

            // Calculate max handshake
            var handshakeRequest = new HandshakeRequest
            {
                Version = parm_version,
                To = "255.255.255.255:65535",
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
            };

            var protocolMsg = new Packet
            {
                Data = handshakeRequest.ToByteString(),
                PublicKey = ByteString.CopyFrom(CcId.PublicKey),
                Type = (uint)IoCcPeerMessage.MessageTypes.Handshake
            };
            protocolMsg.Signature = ByteString.CopyFrom(CcId.Sign(protocolMsg.Data.Memory.ToArray(), 0, protocolMsg.Data.Length));

            _handshakeRequestSize = protocolMsg.CalculateSize();

            var handshakeResponse = new HandshakeResponse
            {
                ReqHash = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(protocolMsg.Data.Memory.AsArray()))
            };

            protocolMsg = new Packet
            {
                Data = handshakeResponse.ToByteString(),
                PublicKey = ByteString.CopyFrom(CcId.PublicKey),
                Type = (uint)IoCcPeerMessage.MessageTypes.Handshake
            };
            protocolMsg.Signature = ByteString.CopyFrom(CcId.Sign(protocolMsg.Data.Memory.AsArray(), 0, protocolMsg.Data.Length));

            _handshakeResponseSize = protocolMsg.CalculateSize();

            _handshakeBufferSize = Math.Max(_handshakeResponseSize, _handshakeRequestSize);

            if(_handshakeBufferSize > parm_max_handshake_bytes)
                throw new ApplicationException($"{nameof(_handshakeBufferSize)} > {parm_max_handshake_bytes}");

            //print some stats
            var task = Task.Factory.StartNew(async () =>
            {
                var inbound = 0;
                var outbound = 0;
                var available = 0;
                var secondsSinceEnsured = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var random = new Random((int)DateTime.Now.Ticks);
                while (true)
                {
                    await Task.Delay(random.Next(30000) + 15000, AsyncTokenProxy.Token).ConfigureAwait(false);
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

                        if (Neighbors.Count == 0)
                        {
                            await BootStrapAsync().ConfigureAwait(false);
                        }
                        //Search for peers
                        if (Neighbors.Count < MaxClients * 0.75 && secondsSinceEnsured.UtDelta() > parm_discovery_force_time_multiplier * Neighbors.Count + 1)
                        {
                            secondsSinceEnsured = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                            _logger.Trace($"Neighbors running lean {Neighbors.Count} < {MaxClients * 0.75:0}, {Description}");

                            foreach (var autoPeeringNeighbor in _autoPeering.Neighbors.Values.Where(n => ((IoCcNeighbor)n).RoutedRequest && ((IoCcNeighbor)n).Verified && ((IoCcNeighbor)n).Direction == IoCcNeighbor.Kind.Undefined && ((IoCcNeighbor)n).SecondsSincePat < ((IoCcNeighbor)n).parm_zombie_max_ttl * 2))
                            {
                                if (Zeroed())
                                    break;

                                if (OutboundCount < parm_max_outbound)
                                {
                                    await ((IoCcNeighbor)autoPeeringNeighbor).SendPeerRequestAsync().ConfigureAwait(false);
                                }
                            }

                            foreach (var autoPeeringNeighbor in _autoPeering.Neighbors.Values.Where(n => ((IoCcNeighbor)n).RoutedRequest && ((IoCcNeighbor)n).Verified && ((IoCcNeighbor)n).SecondsSincePat < ((IoCcNeighbor)n).parm_zombie_max_ttl * 2))//TODO
                            {
                                if (Zeroed())
                                    break;

                                await ((IoCcNeighbor)autoPeeringNeighbor).SendDiscoveryRequestAsync().ConfigureAwait(false);
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
        public override void ZeroUnmanaged()
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
        public override async ValueTask ZeroManagedAsync()
        {
            //Services.IoCcRecord.Endpoints.Clear();
            try
            {
                //_autoPeeringTask?.Wait();
            }
            catch
            {
                // ignored
            }

            await base.ZeroManagedAsync().ConfigureAwait(false);
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

        /// <summary>
        /// Bootstrap
        /// </summary>
        public List<IoNodeAddress> BootstrapAddress { get; }

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
        public int InboundCount => Neighbors.Count(kv => ((IoCcPeer)kv.Value).Neighbor?.Inbound ?? false);

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
        public int parm_discovery_force_time_multiplier = 2;

        /// <summary>
        /// Maximum clients allowed
        /// </summary>
        public int MaxClients => parm_max_outbound + parm_max_inbound;

        /// <summary>
        /// The node id
        /// </summary>
        public IoCcIdentity CcId { get; protected set; }

        /// <summary>
        /// Spawn the node listeners
        /// </summary>
        /// <param name="acceptConnection"></param>
        /// <param name="bootstrapAsync"></param>
        /// <returns></returns>
        protected override async Task SpawnListenerAsync(Func<IoNeighbor<IoCcGossipMessage>, Task<bool>> acceptConnection = null, Func<Task> bootstrapAsync = null)
        {
            _autoPeeringTask = Task.Factory.StartNew(async () =>
            {
                //start peering
                await _autoPeering.StartAsync(BootStrapAsync).ConfigureAwait(false);

            }, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach);

            //start node listener
            await base.SpawnListenerAsync(async peer =>
            {
                //limit connects
                if (Zeroed() || InboundCount > parm_max_inbound || peer == null)
                    return false;

                //Handshake
                if (await HandshakeAsync((IoCcPeer)peer).ConfigureAwait(false))
                {
                    try
                    {
                        _logger.Info($"Connected {peer!.Description}");
                    }
                    catch 
                    {
                        return false;
                    }

                    //ACCEPT

                    return true; 
                }

                return false;
            }, bootstrapAsync).ConfigureAwait(false);
        }

        /// <summary>
        /// Sends a message to a peer
        /// </summary>
        /// <param name="peer">The destination</param>
        /// <param name="msg">The message</param>
        /// <param name="timeout"></param>
        /// <returns>The number of bytes sent</returns>
        private async Task<(int sent, ByteString msgRaw)> SendMessageAsync(IoCcPeer peer, IMessage msg, int timeout = 0)
        {
            var msgRaw = msg.ToByteString();
            var responsePacket = new Packet
            {
                Data = msgRaw,
                PublicKey = ByteString.CopyFrom(CcId.PublicKey),
                Type = (uint)IoCcPeerMessage.MessageTypes.Handshake
            };

            responsePacket.Signature = ByteString.CopyFrom(CcId.Sign(responsePacket.Data.Memory.AsArray(), 0, responsePacket.Data.Length));

            var protocolRaw = responsePacket.ToByteArray();

            var sent = await ((IoNetClient<IoCcGossipMessage>)peer.IoSource).Socket.SendAsync(protocolRaw, 0, protocolRaw.Length, timeout: timeout).ConfigureAwait(false);

            if (sent == protocolRaw.Length)
            {
                _logger.Trace($"{msg.GetType().Name}: Sent {sent} bytes to {((IoNetClient<IoCcGossipMessage>)peer.IoSource).Socket.RemoteAddress} ({Enum.GetName(typeof(IoCcPeerMessage.MessageTypes), responsePacket.Type)})");
                return (msgRaw.Length, msgRaw);
            }
            else
            {
                _logger.Error($"{msg.GetType().Name}: Sent {sent}/{protocolRaw.Length}...");
                return (0, msgRaw);
            }
        }


        private readonly Stopwatch _sw = Stopwatch.StartNew();
        private readonly int _handshakeRequestSize;
        private readonly int _handshakeResponseSize;
        private readonly int _handshakeBufferSize;
        public long Testing;

        /// <summary>
        /// Perform handshake
        /// </summary>
        /// <param name="peer"></param>
        /// <returns></returns>
        private async Task<bool> HandshakeAsync(IoCcPeer peer)
        {
            var bytesRead = 0;
            if (Zeroed()) return false;

            var handshakeBuffer = new byte[_handshakeBufferSize];
            //var handshakeBufferStream = new MemoryStream(handshakeBuffer);
            var socket = ((IoNetClient<IoCcGossipMessage>)peer.IoSource).Socket;
            try
            {
                //inbound
                if (((IoNetClient<IoCcGossipMessage>)peer.IoSource).Socket.Ingress)
                {
                    var verified = false;

                    _sw.Restart();
                    //read from the socket
                    do
                    {
                        bytesRead += await socket
                            .ReadAsync(handshakeBuffer, bytesRead, _handshakeRequestSize - bytesRead, parm_handshake_timeout)
                            .ConfigureAwait(false);
                    } while (
                        !Zeroed() &&
                         bytesRead < _handshakeRequestSize &&
                         socket.NativeSocket.Available > 0 && 
                         bytesRead < handshakeBuffer.Length && 
                         socket.NativeSocket.Available <= handshakeBuffer.Length - bytesRead
                    );
                

                    if (bytesRead == 0)
                    {
                        _logger.Debug($"Failed to read inbound challange request, waited = {_sw.ElapsedMilliseconds}ms, socket = {socket.Description}");
                        return false;
                    }
                    else
                    {
                        _logger.Trace($"{nameof(HandshakeRequest)}: size = {bytesRead}, socket = {socket.Description}");
                    }


                    //parse a packet
                    //var packet = Packet.Parser.ParseFrom(handshakeBufferStream);
                    var packet = Packet.Parser.ParseFrom(handshakeBuffer, 0, bytesRead);

                    if (packet != null && packet.Data != null && packet.Data.Length > 0)
                    {
                        var packetData = packet.Data.Memory.AsArray();

                        //verify the signature
                        if (packet.Signature != null || packet.Signature?.Length != 0)
                        {
                            verified = CcId.Verify(packetData, 0,
                                packetData.Length, packet.PublicKey.Memory.AsArray(), 0,
                                packet!.Signature!.Memory.AsArray(), 0);
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
                                ReqHash = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(packet.Data.Memory.AsArray()))
                            };
                            
                            _sw.Restart();
                            var (sent, handshake) = (await SendMessageAsync(peer, handshakeResponse, parm_handshake_timeout).ConfigureAwait(false));
                            if (sent == 0)
                            {
                                _logger.Debug($"{nameof(handshakeResponse)}: FAILED! {socket.Description}");
                                return false;
                            }
                            else
                            {
                                _logger.Trace($"Sent {sent } inbound handshake challange response, socket = {socket.Description}");
                            }
                        }

                        return ConnectForTheWin(IoCcNeighbor.Kind.Inbound, peer, packet, socket);
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

                    _sw.Restart();
                    var (sent, handshake) = await SendMessageAsync(peer, handshakeRequest, parm_handshake_timeout).ConfigureAwait(false);
                    if (sent == 0)
                    {
                        _logger.Debug($"Failed to send inbound handshake challange response, socket = {socket.Description}");
                        return false;
                    }
                    else 
                    {
                        _logger.Trace($"Sent {sent } inbound handshake challange response, socket = {socket.Description}");
                    }

                    do
                    {
                        bytesRead += await socket
                            .ReadAsync(handshakeBuffer, bytesRead, _handshakeResponseSize - bytesRead, parm_handshake_timeout)
                            .ConfigureAwait(false);
                    } while (
                        !Zeroed() && 
                         bytesRead < _handshakeResponseSize && 
                         socket.NativeSocket != null &&
                         socket.NativeSocket.Available > 0 &&
                         bytesRead < handshakeBuffer.Length && 
                         socket.NativeSocket.Available <= handshakeBuffer.Length - bytesRead
                    );

                    if (bytesRead == 0)
                    {
                        _logger.Debug($"Failed to read outbound handshake challange response, waited = {_sw.ElapsedMilliseconds}ms, address = {socket.RemoteAddress}, socket nill? = {socket.NativeSocket == null}, zeroed {Zeroed()}");
                        return false;
                    }

                    _logger.Trace($"Read outbound handshake challange response size = {bytesRead} b, addess = {socket.RemoteAddress}");

                    var verified = false;
                    
                    var packet = Packet.Parser.ParseFrom(handshakeBuffer, 0, bytesRead);

                    if (packet.Data != null && packet.Data.Length > 0)
                    {
                        var packetData = packet.Data.Memory.AsArray();

                        //verify signature
                        if (packet.Signature != null || packet.Signature?.Length != 0)
                        {
                            verified = CcId.Verify(packetData, 0,
                                packetData.Length, packet.PublicKey.Memory.AsArray(), 0,
                                packet!.Signature!.Memory.AsArray(), 0);
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
                                .ComputeHash(handshake.Memory.AsArray())
                                .SequenceEqual(handshakeResponse.ReqHash))
                            {
                                _logger.Error($"Invalid handshake response! Closing {socket.Key}");
                                return false;
                            }
                        }

                        return ConnectForTheWin(IoCcNeighbor.Kind.OutBound, peer, packet, socket);
                    }
                }

            }
            catch (Exception e)
            {
                _logger.Error(e, $"Handshake (size = {bytesRead}/{_handshakeRequestSize}/{_handshakeResponseSize}) for {Description} failed with:");
            }

            return false;
        }

        /// <summary>
        /// Race for a connection locked on the neighbor it finds
        /// </summary>
        /// <param name="direction">The direction of the lock</param>
        /// <param name="peer">The peer requesting the lock</param>
        /// <param name="packet">The handshake packet</param>
        /// <param name="socket">The socket</param>
        /// <returns>True if it won, false otherwise</returns>
        private bool ConnectForTheWin(IoCcNeighbor.Kind direction, IoCcPeer peer, Packet packet, IoNetSocket socket)
        {
            //Race for connection...
            var id = IoCcNeighbor.MakeId(IoCcIdentity.FromPubKey(packet.PublicKey.Memory.AsArray()), socket.RemoteAddress);
            if (_autoPeering.Neighbors.TryGetValue(id, out var neighbor))
            {
                var ccNeighbor = ((IoCcNeighbor) neighbor);
                if (ccNeighbor.IsAutopeering && !ccNeighbor.IsPeerAttached)
                {
                    //did we win?
                    return peer.AttachNeighbor((IoCcNeighbor) neighbor, direction);
                }
                else
                {
                    _logger.Debug(
                        $"{direction} handshake [REJECT] {id} - {socket.RemoteAddress}: s = {ccNeighbor.State}, a = {ccNeighbor.IsAutopeering}, p = {ccNeighbor.IsPeerConnected}, pa = {ccNeighbor.IsPeerAttached}");
                    return false;
                }
            }
            else
            {
                _logger.Error(
                    $"Neighbor {id} not found, dropping {direction} connection to {socket.RemoteAddress}");
                return false;
            }
        }

        /// <summary>
        /// Opens an <see cref="IoCcNeighbor.Kind.OutBound"/> connection to a gossip peer
        /// </summary>
        /// <param name="neighbor">The verified neighbor associated with this connection</param>
        public async Task<bool> ConnectToPeerAsync(IoCcNeighbor neighbor)
        {
            //Validate
            if (
                    !Zeroed() && 
                    neighbor.IsAutopeering &&
                    !neighbor.IsPeerConnected &&
                    OutboundCount < parm_max_outbound &&
                    //TODO add distance calc &&
                    neighbor.Services.IoCcRecord.Endpoints.ContainsKey(IoCcService.Keys.gossip)
                )
            {
                var peer = await RetryConnectionAsync(neighbor.Services.IoCcRecord.Endpoints[IoCcService.Keys.gossip], neighbor).ConfigureAwait(false);
                if (Zeroed() || peer == null)
                {
                    _logger.Debug($"{nameof(ConnectToPeerAsync)}: [ABORTED], {neighbor.Description}, {neighbor.MetaDesc}");
                    await neighbor.DetachPeerAsync().ConfigureAwait(false);
                    return false;
                }
                
                //Race for a connection
                if (await HandshakeAsync((IoCcPeer)peer).ConfigureAwait(false))
                {
                    _logger.Info($"Connected {peer!.Description}");
                    NeighborTasks.Add(peer.AssimilateAsync());
                    return true;
                }
                else
                {
                    await peer!.ZeroAsync(this).ConfigureAwait(false);
                    return false;
                }
            }
            else
            {
                _logger.Debug($"{nameof(ConnectToPeerAsync)}: Connect skipped: {neighbor.Description}");
                return false;
            }
        }

        /// <summary>
        /// Boots the node
        /// </summary>
        public async Task BootAsync()
        {
            Interlocked.Exchange(ref Testing, 1);
            
            foreach (var ioNeighbor in Neighbors.Values)
            {
                try
                {
                    await ((IoCcPeer) ioNeighbor).StartTestModeAsync().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    _logger.Debug(e,Description);
                }
            }
        }

        /// <summary>
        /// Boostrap node
        /// </summary>
        /// <returns></returns>
        private async Task BootStrapAsync()
        {
            _logger.Debug($"Bootstrapping {Description} from {BootstrapAddress.Count} bootnodes...");
            if (BootstrapAddress != null)
            {
                foreach (var ioNodeAddress in BootstrapAddress)
                {
                    _logger.Debug($"{Description} Boostrapping from {ioNodeAddress}");
                    if (!await DiscoveryService.LocalNeighbor.SendPingAsync(ioNodeAddress).ConfigureAwait(false))
                    {
                        _logger.Fatal($"{nameof(BootStrapAsync)}: Unable to boostrap {Description} from {ioNodeAddress}");
                    }
                }
            }
        }
    }
}
