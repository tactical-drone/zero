using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using Proto;
using zero.cocoon.autopeer;
using zero.cocoon.identity;
using zero.cocoon.models;
using zero.cocoon.models.services;
using zero.core.conf;
using zero.core.core;
using zero.core.misc;
using zero.core.network.ip;
using Packet = Proto.Packet;

namespace zero.cocoon
{
    /// <summary>
    /// Connects to cocoon
    /// </summary>
    public class CcNode : IoNode<CcGossipMessage>
    {
        public CcNode(CcIdentity ccIdentity, IoNodeAddress gossipAddress, IoNodeAddress peerAddress,
            IoNodeAddress fpcAddress, IoNodeAddress extAddress, List<IoNodeAddress> bootstrap, int udpPrefetch, int tcpPrefetch, int udpConcurrencyLevel, int tpcConcurrencyLevel)
            : base(gossipAddress, (node, ioNetClient, extraData) => new CcPeer((CcNode)node, (CcNeighbor)extraData, ioNetClient), tcpPrefetch, tpcConcurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
            _gossipAddress = gossipAddress;
            _peerAddress = peerAddress;
            _fpcAddress = fpcAddress;
            BootstrapAddress = bootstrap;
            ExtAddress = extAddress; //this must be the external or NAT address.
            CcId = ccIdentity;

            Services.CcRecord.Endpoints.TryAdd(CcService.Keys.peering, _peerAddress);
            Services.CcRecord.Endpoints.TryAdd(CcService.Keys.gossip, _gossipAddress);
            Services.CcRecord.Endpoints.TryAdd(CcService.Keys.fpc, _fpcAddress);

            _autoPeering = ZeroOnCascade(new CcNeighborDiscovery(this, _peerAddress, (node, client, extraData) => new CcNeighbor((CcNeighborDiscovery)node, client, extraData), udpPrefetch, udpConcurrencyLevel), true).target;

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
                Type = (uint)CcPeerMessage.MessageTypes.Handshake
            };
            protocolMsg.Signature = ByteString.CopyFrom(CcId.Sign(protocolMsg.Data.Memory.ToArray(), 0, protocolMsg.Data.Length));

            _handshakeRequestSize = protocolMsg.CalculateSize();

            var handshakeResponse = new HandshakeResponse
            {
                ReqHash = ByteString.CopyFrom(CcIdentity.Sha256.ComputeHash(protocolMsg.Data.Memory.AsArray()))
            };

            protocolMsg = new Packet
            {
                Data = handshakeResponse.ToByteString(),
                PublicKey = ByteString.CopyFrom(CcId.PublicKey),
                Type = (uint)CcPeerMessage.MessageTypes.Handshake
            };
            protocolMsg.Signature = ByteString.CopyFrom(CcId.Sign(protocolMsg.Data.Memory.AsArray(), 0, protocolMsg.Data.Length));

            _handshakeResponseSize = protocolMsg.CalculateSize();

            _handshakeBufferSize = Math.Max(_handshakeResponseSize, _handshakeRequestSize);

            if(_handshakeBufferSize > parm_max_handshake_bytes)
                throw new ApplicationException($"{nameof(_handshakeBufferSize)} > {parm_max_handshake_bytes}");

            //ensure some liveness
            var task = Task.Factory.StartNew(async () =>
            {
                var secondsSinceEnsured = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var random = new Random((int)DateTime.Now.Ticks);
                
                //while running
                while (!Zeroed())
                {
                    //periodically
                    await Task.Delay((random.Next(parm_mean_pat_delay)/3 * 1000), AsyncTasks.Token).ConfigureAwait(false);
                    if (Zeroed())
                        break;

                    try
                    {
                        //boostrap if alone
                        if (TotalConnections < 2)
                        {
                            await BootStrapAsync().ConfigureAwait(false);
                        }

                        var totalAdjuncts = TotalConnections;
                        double scanRatio = 1;
                        double peerAttempts = 0;
                        CcNeighbor susceptible = null;
                        
                        //Attempt to peer with standbys
                        if (totalAdjuncts < MaxAdjuncts * scanRatio && secondsSinceEnsured.Elapsed() > (parm_mean_pat_delay - (MaxAdjuncts - totalAdjuncts)) * 2)
                        {
                            secondsSinceEnsured = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                            _logger.Trace($"Scanning {Neighbors.Count} < {MaxAdjuncts * scanRatio:0}, {Description}");

                            //Send peer requests
                            foreach (var neighbor in _autoPeering.Neighbors.Values.Where(n =>
                                    ((CcNeighbor)n).Assimilated && 
                                    ((CcNeighbor)n).Direction == CcNeighbor.Heading.Undefined &&
                                    ((CcNeighbor)n).State > CcNeighbor.NeighborState.Unverified &&
                                    ((CcNeighbor)n).State < CcNeighbor.NeighborState.Peering &&
                                    ((CcNeighbor)n).TotalPats > ((CcNeighbor)n).parm_zombie_max_connection_attempts &&
                                    ((CcNeighbor)n).SecondsSincePat < ((CcNeighbor)n).parm_zombie_max_ttl).
                                OrderBy(n=>((CcNeighbor)n).Priority))
                            {
                                if (Zeroed())
                                    break;
                                
                                //We select the neighbor that makes least requests which means it is saturated,
                                //but that means it is probably not depleting its standby neighbors which is what 
                                //we are after. It's a long shot that relies on probability in the long run
                                //to work.
                                susceptible ??= (CcNeighbor) neighbor;

                                if (EgressConnections < parm_max_outbound)
                                {
                                    if (await ((CcNeighbor) neighbor).SendPeerRequestAsync()
                                        .ConfigureAwait(false))
                                    {
                                        peerAttempts++;
                                        await Task.Delay(parm_scan_throttle, AsyncTasks.Token).ConfigureAwait(false);
                                    }
                                }
                                else
                                {
                                    ((CcNeighbor) neighbor).State = CcNeighbor.NeighborState.Standby;
                                }
                            }
                            
                            //if we are not able to peer, use long range scanners
                            if (susceptible!= null && peerAttempts == 0 && totalAdjuncts == TotalConnections)
                            {
                                if (await susceptible.SendDiscoveryRequestAsync().ConfigureAwait(false))
                                {
                                    _logger.Debug($"& {susceptible.Description}");   
                                }
                            }
                        }
                        else if(secondsSinceEnsured.Elapsed() > parm_mean_pat_delay * 8 + 1) //scan for discovery
                        {
                            var maxP = _autoPeering.Neighbors.Values.Max(n => ((CcNeighbor) n).Priority);
                            var targetQ = _autoPeering.Neighbors.Values.Where(n => ((CcNeighbor) n).Assimilated && ((CcNeighbor) n).Priority < maxP/2)
                                .OrderBy(n => ((CcNeighbor) n).Priority).ToList();

                            //Have we found a suitable direction to scan in?
                            if (targetQ.Count > 0)
                            {
                                var target = targetQ[Math.Max(_random.Next(targetQ.Count) - 1, 0)];

                                //scan
                                if (target != null && !await ((CcNeighbor) target).SendDiscoveryRequestAsync()
                                    .ConfigureAwait(false))
                                {
                                    if(target != null)
                                        _logger.Trace($"{nameof(CcNeighbor.SendDiscoveryRequestAsync)}: [FAILED], c = {targetQ.Count}, {Description}");
                                }
                                else
                                {
                                    _logger.Debug($"* {Description}");   
                                }
                            }
                        }
                    }
                    catch (NullReferenceException e) { _logger.Trace(e, Description); }
                    catch (TaskCanceledException e) { _logger.Trace(e, Description);}
                    catch (ObjectDisposedException e) { _logger.Trace(e, Description);}
                    catch (OperationCanceledException e) { _logger.Trace(e, Description);}
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
            //Services.CcRecord.Endpoints.Clear();
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

        private IoNode<CcPeerMessage> _autoPeering;
        private readonly IoNodeAddress _gossipAddress;
        private readonly IoNodeAddress _peerAddress;
        private readonly IoNodeAddress _fpcAddress;

        readonly Random _random = new Random((int) DateTime.Now.Ticks);

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
        public int parm_handshake_timeout = 5000;
        
        /// <summary>
        /// Timeout for handshake messages
        /// </summary>
        [IoParameter]
        public int parm_scan_throttle = 2000;

        /// <summary>
        /// The discovery service
        /// </summary>
        public CcNeighborDiscovery DiscoveryService => (CcNeighborDiscovery)_autoPeering;


        /// <summary>
        /// Total number of connections
        /// </summary>
        public int TotalConnections => IngressConnections + EgressConnections;

        /// <summary>
        /// Number of inbound neighbors
        /// </summary>
        public int IngressConnections => Neighbors.Values.Count(kv => ((CcPeer)kv).Neighbor != null && ((CcPeer)kv).Neighbor.IsPeerConnected && (((CcPeer)kv).Neighbor.Inbound) && ((CcPeer)kv).Neighbor.State == CcNeighbor.NeighborState.Connected);

        /// <summary>
        /// Number of outbound neighbors
        /// </summary>
        public int EgressConnections => Neighbors.Values.Count(kv => ((CcPeer)kv).Neighbor != null && ((CcPeer)kv).Neighbor.IsPeerConnected && (((CcPeer)kv).Neighbor.Outbound) && ((CcPeer)kv).Neighbor.State == CcNeighbor.NeighborState.Connected);

        /// <summary>
        /// Connected nodes
        /// </summary>
        private List<IoNeighbor<CcGossipMessage>> Adjuncts => Neighbors.Values.Where(kv=> ((CcPeer)kv).Neighbor != null && ((CcPeer)kv).Neighbor.IsPeerConnected && ((CcPeer)kv).Neighbor.Inbound && ((CcPeer)kv).Neighbor.State == CcNeighbor.NeighborState.Connected).ToList();

        /// <summary>
        /// The services this node supports
        /// </summary>
        public CcService Services { get; set; } = new CcService();

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
        /// Time between trying to re-aquire new neighbors using a discovery requests, if the node lacks <see cref="MaxAdjuncts"/>
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_mean_pat_delay = 30;


        /// <summary>
        /// Client to neighbor ratio
        /// </summary>
        [IoParameter] public int parm_client_to_neighbor_ratio = 2;

        /// <summary>
        /// Maximum clients allowed
        /// </summary>
        public int MaxAdjuncts => parm_max_outbound + parm_max_inbound;


        /// <summary>
        /// Maximum number of allowed neighbors
        /// </summary>
        public int MaxNeighbors => MaxAdjuncts * parm_client_to_neighbor_ratio;

        /// <summary>
        /// The node id
        /// </summary>
        public CcIdentity CcId { get; protected set; }

        /// <summary>
        /// Spawn the node listeners
        /// </summary>
        /// <param name="acceptConnection"></param>
        /// <param name="bootstrapAsync"></param>
        /// <returns></returns>
        protected override async Task SpawnListenerAsync(Func<IoNeighbor<CcGossipMessage>, Task<bool>> acceptConnection = null, Func<Task> bootstrapAsync = null)
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
                if (Zeroed() || IngressConnections >= parm_max_inbound || peer == null)
                    return false;

                //Handshake
                if (await HandshakeAsync((CcPeer)peer).ConfigureAwait(false))
                {
                    //ACCEPT
                    _logger.Info($"+ {peer.Description}");
                    
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
        private async ValueTask<int> SendMessageAsync(CcPeer peer, ByteString msg, string type, int timeout = 0)
        {
            var responsePacket = new Packet
            {
                Data = msg,
                PublicKey = ByteString.CopyFrom(CcId.PublicKey),
                Type = (uint)CcPeerMessage.MessageTypes.Handshake
            };

            responsePacket.Signature = ByteString.CopyFrom(CcId.Sign(responsePacket.Data.Memory.AsArray(), 0, responsePacket.Data.Length));

            var protocolRaw = responsePacket.ToByteArray();

            var sent = await peer.IoSource.IoNetSocket.SendAsync(protocolRaw, 0, protocolRaw.Length, timeout: timeout).ConfigureAwait(false);

            if (sent == protocolRaw.Length)
            {
                _logger.Trace($"{type}: Sent {sent} bytes to {peer.IoSource.IoNetSocket.RemoteAddress} ({Enum.GetName(typeof(CcPeerMessage.MessageTypes), responsePacket.Type)})");
                return msg.Length;
            }
            else
            {
                _logger.Error($"{type}: Sent {sent}/{protocolRaw.Length}...");
                return 0;
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
        private async ValueTask<bool> HandshakeAsync(CcPeer peer)
        {
            return await ZeroAtomicAsync(async (s, u, d) =>
            {
                var _this = (CcNode)s;
                var __peer = (CcPeer) u;
                var bytesRead = 0;
                if (_this.Zeroed()) return false;

                try
                {
                    var handshakeBuffer = new byte[_this._handshakeBufferSize];
                    var ioNetSocket = __peer.IoSource.IoNetSocket;

                    //inbound
                    if (__peer.IoSource.IoNetSocket.Ingress)
                    {
                        var verified = false;
                        
                        _this._sw.Restart();
                        //read from the socket
                        //do
                        //{
                        bytesRead += await ioNetSocket
                            .ReadAsync(handshakeBuffer, bytesRead, _this._handshakeRequestSize - bytesRead,
                                timeout: _this.parm_handshake_timeout)
                            .ConfigureAwait(false);
                        //} while (
                        //    !Zeroed() &&
                        //    bytesRead < _handshakeRequestSize &&
                        //    ioNetSocket.NativeSocket.Available > 0 &&
                        //    bytesRead < handshakeBuffer.Length &&
                        //    ioNetSocket.NativeSocket.Available <= handshakeBuffer.Length - bytesRead
                        //);
                        
                        
                        if (bytesRead == 0)
                        {
                            _this._logger.Trace(
                                $"Failed to read inbound challange request, waited = {_this._sw.ElapsedMilliseconds}ms, socket = {ioNetSocket.Description}");
                            return false;
                        }
                        else
                        {
                            _this._logger.Trace(
                                $"{nameof(HandshakeRequest)}: size = {bytesRead}, socket = {ioNetSocket.Description}");
                        }
                        
                        //parse a packet
                        var packet = Packet.Parser.ParseFrom(handshakeBuffer, 0, bytesRead);
                        
                        if (packet != null && packet.Data != null && packet.Data.Length > 0)
                        {
                            if (!await _this.ConnectForTheWinAsync(CcNeighbor.Heading.Ingress, __peer, packet,
                                    (IPEndPoint) ioNetSocket.NativeSocket.RemoteEndPoint)
                                .ConfigureAwait(false))
                                return false;
                        
                            var packetData = packet.Data.Memory.AsArray();
                            
                            //verify the signature
                            if (packet.Signature != null || packet.Signature!.Length != 0)
                            {
                                verified = _this.CcId.Verify(packetData, 0,
                                    packetData.Length, packet.PublicKey.Memory.AsArray(), 0,
                                    packet!.Signature!.Memory.AsArray(), 0);
                            }
                            
                            _this._logger.Trace($"HandshakeRequest [{(verified ? "signed" : "un-signed")}], read = {bytesRead}, {__peer.IoSource.Key}");
                        
                            //Don't process unsigned or unknown messages
                            if (!verified)
                                return false;
                        
                            //process handshake request 
                            var handshakeRequest = HandshakeRequest.Parser.ParseFrom(packet.Data);
                            if (handshakeRequest != null)
                            {
                                //reject old handshake requests
                                if (handshakeRequest.Timestamp.ElapsedDelta() > _this.parm_time_e)
                                {
                                    _this._logger.Error(
                                        $"Rejected old handshake request from {ioNetSocket.Key} - d = {handshakeRequest.Timestamp.ElapsedDelta()}s, {handshakeRequest.Timestamp.Elapsed()}");
                                    return false;
                                }
                                
                                //reject invalid protocols
                                if (handshakeRequest.Version != _this.parm_version)
                                {
                                    _this._logger.Error(
                                        $"Invalid handshake protocol version from  {ioNetSocket.Key} - got {handshakeRequest.Version}, wants {_this.parm_version}");
                                    return false;
                                }
                        
                                //reject requests to invalid ext ip
                                //if (handshakeRequest.To != ((CcNeighbor)neighbor)?.ExtGossipAddress?.IpPort)
                                //{
                                //    _logger.Error($"Invalid handshake received from {socket.Key} - got {handshakeRequest.To}, wants {((CcNeighbor)neighbor)?.ExtGossipAddress.IpPort}");
                                //    return false;
                                //}
                        
                                //send response
                                var handshakeResponse = new HandshakeResponse
                                {
                                    ReqHash = ByteString.CopyFrom(CcIdentity.Sha256.ComputeHash(packet.Data.Memory.AsArray()))
                                };
                                
                                var handshake = handshakeResponse.ToByteString();
                                
                                _this._sw.Restart();
                                
                                var sent = await _this.SendMessageAsync(__peer, handshake, nameof(HandshakeResponse),
                                    _this.parm_handshake_timeout).ConfigureAwait(false);
                                if (sent > 0)
                                {
                                    _this._logger.Trace(
                                        $"Sent {sent} inbound handshake challange response, socket = {ioNetSocket.Description}");
                                }
                                else
                                {
                                    _this._logger.Trace($"{nameof(handshakeResponse)}: FAILED! {ioNetSocket.Description}");
                                    return false;
                                }
                            }
                            //Race
                            //return await ConnectForTheWinAsync(CcNeighbor.Kind.Inbound, peer, packet,
                            //        (IPEndPoint)ioNetSocket.NativeSocket.RemoteEndPoint)
                            //    .ConfigureAwait(false);
                            return true;
                        
                        }
                    }
                    else if (__peer.IoSource.IoNetSocket.Egress) //Outbound
                    {
                        var handshakeRequest = new HandshakeRequest
                        {
                            Version = _this.parm_version,
                            To = ioNetSocket.LocalNodeAddress.IpPort,
                            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                        };
                        
                        var handshake = handshakeRequest.ToByteString();
                        
                        _this._sw.Restart();
                        var sent = await _this.SendMessageAsync(__peer, handshake, nameof(HandshakeResponse),
                                _this.parm_handshake_timeout)
                            .ConfigureAwait(false);
                        if (sent > 0)
                        {
                            _this._logger.Trace(
                                $"Sent {sent} inbound handshake challange response, socket = {ioNetSocket.Description}");
                        }
                        else
                        {
                            _this._logger.Trace(
                                $"Failed to send inbound handshake challange response, socket = {ioNetSocket.Description}");
                            return false;
                        }
                        
                        //do
                        //{
                        bytesRead += await ioNetSocket
                            .ReadAsync(handshakeBuffer, bytesRead, _this._handshakeResponseSize - bytesRead,
                                timeout: _this.parm_handshake_timeout)
                            .ConfigureAwait(false);
                        //} while (
                        //    !Zeroed() &&
                        //    bytesRead < _handshakeResponseSize &&
                        //    ioNetSocket.NativeSocket.Available > 0 &&
                        //    bytesRead < handshakeBuffer.Length &&
                        //    ioNetSocket.NativeSocket.Available <= handshakeBuffer.Length - bytesRead
                        //);
                        
                        if (bytesRead == 0)
                        {
                            _this._logger.Trace(
                                $"Failed to read outbound handshake challange response, waited = {_this._sw.ElapsedMilliseconds}ms, remote = {ioNetSocket.RemoteAddress}, zeroed {_this.Zeroed()}");
                            return false;
                        }
                        
                        _this._logger.Trace(
                            $"Read outbound handshake challange response size = {bytesRead} b, addess = {ioNetSocket.RemoteAddress}");
                        
                        var verified = false;
                        
                        var packet = Packet.Parser.ParseFrom(handshakeBuffer, 0, bytesRead);
                        
                        if (packet != null && packet.Data != null && packet.Data.Length > 0)
                        {
                            if (!await _this.ConnectForTheWinAsync(CcNeighbor.Heading.Egress, __peer, packet,
                                    (IPEndPoint)ioNetSocket.NativeSocket.RemoteEndPoint)
                                .ConfigureAwait(false))
                                return false;
                        
                            var packetData = packet.Data.Memory.AsArray();
                        
                            //verify signature
                            if (packet.Signature != null || packet.Signature!.Length != 0)
                            {
                                verified = _this.CcId.Verify(packetData, 0,
                                    packetData.Length, packet.PublicKey.Memory.AsArray(), 0,
                                    packet.Signature.Memory.AsArray(), 0);
                            }
                        
                            _this._logger.Trace(
                                $"HandshakeResponse [{(verified ? "signed" : "un-signed")}], read = {bytesRead}, {__peer.IoSource.Key}");
                        
                            //Don't process unsigned or unknown messages
                            if (!verified)
                            {
                                return false;
                            }
                        
                            //validate handshake response
                            var handshakeResponse = HandshakeResponse.Parser.ParseFrom(packet.Data);
                        
                            if (handshakeResponse != null)
                            {
                                if (!CcIdentity.Sha256
                                    .ComputeHash(handshake.Memory.AsArray())
                                    .SequenceEqual(handshakeResponse.ReqHash))
                                {
                                    _this._logger.Error($"Invalid handshake response! Closing {ioNetSocket.Key}");
                                    return false;
                                }
                            }
                        
                            return true;
                        }
                    }
                }
                catch (TaskCanceledException e)
                {
                    _this._logger.Trace(e, _this.Description);
                }
                catch (ObjectDisposedException e)
                {
                    _this._logger.Trace(e, _this.Description);
                }
                catch (Exception e)
                {
                    _this._logger.Error(e,
                        $"Handshake (size = {bytesRead}/{_this._handshakeRequestSize}/{_this._handshakeResponseSize}) for {_this.Description} failed with:");
                }

                return false;
            }, peer, force: true).ConfigureAwait(false);
        }

        /// <summary>
        /// Race for a connection locked on the neighbor it finds
        /// </summary>
        /// <param name="direction">The direction of the lock</param>
        /// <param name="peer">The peer requesting the lock</param>
        /// <param name="packet">The handshake packet</param>
        /// <param name="remoteEp">The remote</param>
        /// <returns>True if it won, false otherwise</returns>
        private async ValueTask<bool> ConnectForTheWinAsync(CcNeighbor.Heading direction, CcPeer peer, Packet packet, IPEndPoint remoteEp)
        {
            if(_gossipAddress.IpEndPoint.ToString() == remoteEp.ToString())
                throw new ApplicationException($"Connection inception dropped from {remoteEp} on {_gossipAddress.IpEndPoint.ToString()}: {Description}");

            //Race for connection...
            var id = CcNeighbor.MakeId(CcIdentity.FromPubKey(packet.PublicKey.Memory.AsArray()), "");

            var neighbor = _autoPeering.Neighbors.Values.FirstOrDefault(n => n.Key.Contains(id));
            if (neighbor != null)
            {
                var ccNeighbor = (CcNeighbor) neighbor;
                if (ccNeighbor.Assimilated && !ccNeighbor.IsPeerAttached)
                {
                    //did we win?
                    return await peer.AttachNeighborAsync((CcNeighbor) neighbor, direction).ConfigureAwait(false);
                }
                else
                {
                    _logger.Trace($"{direction} handshake [LOST] {id} - {remoteEp}: s = {ccNeighbor.State}, a = {ccNeighbor.Assimilated}, p = {ccNeighbor.IsPeerConnected}, pa = {ccNeighbor.IsPeerAttached}, ut = {ccNeighbor.Uptime.TickSec()}");
                    return false;
                }
            }
            else
            {
                _logger.Error($"Neighbor {id} not found, dropping {direction} connection to {remoteEp}");
                return false;
            }
        }

        /// <summary>
        /// Opens an <see cref="CcNeighbor.Heading.Egress"/> connection to a gossip peer
        /// </summary>
        /// <param name="neighbor">The verified neighbor associated with this connection</param>
        public async ValueTask<bool> ConnectToPeerAsync(CcNeighbor neighbor)
        {
            //Validate
            if (
                    !Zeroed() && 
                    neighbor.Assimilated &&
                    !neighbor.IsPeerConnected &&
                    EgressConnections < parm_max_outbound &&
                    //TODO add distance calc &&
                    neighbor.Services.CcRecord.Endpoints.ContainsKey(CcService.Keys.gossip)
                )
            {
                var peer = await ConnectAsync(neighbor.Services.CcRecord.Endpoints[CcService.Keys.gossip], neighbor).ConfigureAwait(false);
                if (Zeroed() || peer == null)
                {
                    if (peer != null) await peer.ZeroAsync(this).ConfigureAwait(false);
                    _logger.Debug($"{nameof(ConnectToPeerAsync)}: [ABORTED], {neighbor.Description}, {neighbor.MetaDesc}");
                    return false;
                }
                
                //Race for a connection
                if (await HandshakeAsync((CcPeer)peer).ConfigureAwait(false))
                {
                    _logger.Info($"+ {peer.Description}");
                    NeighborTasks.Add(peer.AssimilateAsync());
                    return true;
                }
                else
                {
                    await peer.ZeroAsync(this).ConfigureAwait(false);
                    return false;
                }
            }
            else
            {
                _logger.Trace($"{nameof(ConnectToPeerAsync)}: Connect skipped: {neighbor.Description}");
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
                    await ((CcPeer) ioNeighbor).StartTestModeAsync().ConfigureAwait(false);
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
            _logger.Trace($"Bootstrapping {Description} from {BootstrapAddress.Count} bootnodes...");
            if (BootstrapAddress != null)
            {
                foreach (var ioNodeAddress in BootstrapAddress)
                {
                    if (!ioNodeAddress.Equals(_peerAddress))
                    {
                        //_logger.Trace($"{Description} Bootstrapping from {ioNodeAddress}");
                        if (!await DiscoveryService.Router.SendPingAsync(ioNodeAddress).ConfigureAwait(false))
                        {
                            if(!DiscoveryService.Router.Zeroed())
                                _logger.Trace($"{nameof(BootStrapAsync)}: Unable to boostrap {Description} from {ioNodeAddress}");
                        }
                    }
                }
            }
        }
    }
}
