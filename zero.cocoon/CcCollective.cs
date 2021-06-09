using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using MathNet.Numerics.Distributions;
using NLog;
using Proto;
using zero.cocoon.autopeer;
using zero.cocoon.events.services;
using zero.cocoon.identity;
using zero.cocoon.models;
using zero.cocoon.models.batches;
using zero.cocoon.models.services;
using zero.core.conf;
using zero.core.core;
using zero.core.misc;
using zero.core.models.protobuffer;
using zero.core.network.ip;
using zero.core.patterns.misc;
using Packet = Proto.Packet;

namespace zero.cocoon
{
    /// <summary>
    /// Connects to cocoon
    /// </summary>
    public class CcCollective : IoNode<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>
    {
        public CcCollective(CcDesignation ccDesignation, IoNodeAddress gossipAddress, IoNodeAddress peerAddress,
            IoNodeAddress fpcAddress, IoNodeAddress extAddress, List<IoNodeAddress> bootstrap, int udpPrefetch, int tcpPrefetch, int udpConcurrencyLevel, int tpcConcurrencyLevel)
            : base(gossipAddress, (node, ioNetClient, extraData) => new CcDrone((CcCollective)node, (CcAdjunct)extraData, ioNetClient), tcpPrefetch, tpcConcurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
            _gossipAddress = gossipAddress;
            _peerAddress = peerAddress;
            _fpcAddress = fpcAddress;
            BootstrapAddress = bootstrap;
            ExtAddress = extAddress; //this must be the external or NAT address.
            CcId = ccDesignation;

            Services.CcRecord.Endpoints.TryAdd(CcService.Keys.peering, _peerAddress);
            Services.CcRecord.Endpoints.TryAdd(CcService.Keys.gossip, _gossipAddress);
            Services.CcRecord.Endpoints.TryAdd(CcService.Keys.fpc, _fpcAddress);

            _autoPeering = ZeroOnCascade(new CcHub(this, _peerAddress, (node, client, extraData) => new CcAdjunct((CcHub)node, client, extraData), udpPrefetch, udpConcurrencyLevel), true).target;

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
                PublicKey = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcId.PublicKey)),
                Type = (uint)CcDiscoveries.MessageTypes.Handshake
            };
            protocolMsg.Signature = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcId.Sign(protocolMsg.Data.Memory.ToArray(), 0, protocolMsg.Data.Length)));

            _handshakeRequestSize = protocolMsg.CalculateSize();

            var handshakeResponse = new HandshakeResponse
            {
                ReqHash = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcDesignation.Sha256.ComputeHash(protocolMsg.Data.Memory.AsArray())))
            };

            protocolMsg = new Packet
            {
                Data = handshakeResponse.ToByteString(),
                PublicKey = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcId.PublicKey)),
                Type = (uint)CcDiscoveries.MessageTypes.Handshake
            };
            protocolMsg.Signature = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcId.Sign(protocolMsg.Data.Memory.AsArray(), 0, protocolMsg.Data.Length)));

            _handshakeResponseSize = protocolMsg.CalculateSize();

            _handshakeBufferSize = Math.Max(_handshakeResponseSize, _handshakeRequestSize);

            if(_handshakeBufferSize > parm_max_handshake_bytes)
                throw new ApplicationException($"{nameof(_handshakeBufferSize)} > {parm_max_handshake_bytes}");

            //ensure some liveness
            var task = Task.Factory.StartNew(async () =>
            {
                var secondsSinceEnsured = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var secondsSinceBoot = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var random = new Random((int)DateTime.Now.Ticks);
                
                //while running
                while (!Zeroed())
                {
                    //periodically
                    await Task.Delay(random.Next(parm_mean_pat_delay)/4 * 1000, AsyncTasks.Token).ConfigureAwait(false);
                    if (Zeroed())
                        break;

                    try
                    {
                        var totalConnections = TotalConnections;
                        double scanRatio = 1;
                        double peerAttempts = 0;
                        CcAdjunct susceptible = null;
                        
                        //Attempt to peer with standbys
                        if (totalConnections < MaxDrones * scanRatio &&
                            secondsSinceEnsured.Elapsed() > parm_mean_pat_delay / 3)
                        {
                            if (Neighbors.Count > 1)
                            {
                                _logger.Trace($"Scanning {Neighbors.Count} < {MaxDrones * scanRatio:0}, {Description}");

                                if (secondsSinceEnsured.Elapsed() > parm_mean_pat_delay / 3)
                                {
                                    //Send peer requests
                                    foreach (var adjunct in _autoPeering.Neighbors.Values.Where(n =>
                                            ((CcAdjunct) n).Assimilating &&
                                            ((CcAdjunct) n).Direction == CcAdjunct.Heading.Undefined &&
                                            ((CcAdjunct) n).State > CcAdjunct.AdjunctState.Unverified &&
                                            ((CcAdjunct) n).State < CcAdjunct.AdjunctState.Peering &&
                                            ((CcAdjunct) n).TotalPats >
                                            ((CcAdjunct) n).parm_zombie_max_connection_attempts &&
                                            ((CcAdjunct) n).SecondsSincePat < parm_mean_pat_delay * 4)
                                        .OrderBy(n => ((CcAdjunct) n).Priority))
                                    {
                                        if (Zeroed())
                                            break;

                                        //We select the neighbor that makes least requests which means it is saturated,
                                        //but that means it is probably not depleting its standby neighbors which is what 
                                        //we are after. It's a long shot that relies on probability in the long run
                                        //to work.
                                        susceptible ??= (CcAdjunct) adjunct;

                                        if (EgressConnections < parm_max_outbound)
                                        {
                                            if (await ((CcAdjunct) adjunct).SendPeerRequestAsync()
                                                .ConfigureAwait(false))
                                            {
                                                peerAttempts++;
                                                await Task.Delay(_random.Next(parm_scan_throttle), AsyncTasks.Token)
                                                    .ConfigureAwait(false);
                                            }
                                        }
                                    }
                                }

                                if (secondsSinceEnsured.Elapsed() > parm_mean_pat_delay)
                                {
                                    var newConnections = TotalConnections - totalConnections;
                                    //if we are not able to peer, use long range scanners
                                    if (susceptible != null && peerAttempts == 0 && newConnections == 0)
                                    {
                                        if (await susceptible.SendDiscoveryRequestAsync().ConfigureAwait(false))
                                        {
                                            secondsSinceEnsured = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                                            _logger.Debug($"& {susceptible.Description}");
                                        }
                                    }
                                }

                                //bootstrap every now and again
                                if (secondsSinceBoot.Elapsed() > parm_mean_pat_delay * 4)
                                {
                                    await BootStrapAsync().ConfigureAwait(false);
                                    secondsSinceBoot = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                                }
                            }
                            else
                            {
                                //bootstrap if alone
                                if (secondsSinceEnsured.Elapsed() > parm_mean_pat_delay)
                                {
                                    await BootStrapAsync().ConfigureAwait(false);
                                    secondsSinceEnsured = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                                }
                            }

                            if (peerAttempts > 0)
                            {
                                if (secondsSinceEnsured.Elapsed() > parm_mean_pat_delay)
                                {
                                    secondsSinceEnsured = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                                }
                            }
                        }
                        //else if(secondsSinceEnsured.Elapsed() > parm_mean_pat_delay * 3) //scan for discovery
                        //{
                        //    var maxP = _autoPeering.Neighbors.Values.Max(n => ((CcAdjunct) n).Priority);
                        //    var targetQ = _autoPeering.Neighbors.Values.Where(n => ((CcAdjunct) n).Assimilating && ((CcAdjunct) n).Priority < maxP/2)
                        //        .OrderBy(n => ((CcAdjunct) n).Priority).ToList();

                        //    //Have we found a suitable direction to scan in?
                        //    if (targetQ.Count > 0)
                        //    {
                        //        var target = targetQ[Math.Max(_random.Next(targetQ.Count) - 1, 0)];

                        //        //scan
                        //        if (target != null && !await ((CcAdjunct) target).SendDiscoveryRequestAsync()
                        //            .ConfigureAwait(false))
                        //        {
                        //            if(target != null)
                        //                _logger.Trace($"{nameof(CcAdjunct.SendDiscoveryRequestAsync)}: [FAILED], c = {targetQ.Count}, {Description}");
                        //        }
                        //        else
                        //        {
                        //            _logger.Debug($"* {Description}");   
                        //        }
                        //    }
                        //}
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
            }, TaskCreationOptions.LongRunning | TaskCreationOptions.PreferFairness);

            //Emit collective event
            AutoPeeringEventService.AddEvent(new AutoPeerEvent
            {
                EventType = AutoPeerEventType.AddCollective,
                Collective = new Collective
                {
                    Id = CcId.IdString(), 
                    Ip = ExtAddress.Url
                }
            });
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            _logger = null;
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

            AutoPeeringEventService.AddEvent(new AutoPeerEvent
            {
                EventType = AutoPeerEventType.RemoveCollective,
                Collective = new Collective()
                {
                    Id = Hub.Router.Designation.IdString()
                }
            });
        }

        private Logger _logger;

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

        private IoNode<CcProtocMessage<Packet, CcDiscoveryBatch>> _autoPeering;
        
        private readonly IoNodeAddress _gossipAddress;
        private readonly IoNodeAddress _peerAddress;
        private readonly IoNodeAddress _fpcAddress;

        readonly Random _random = new Random((int) DateTime.Now.Ticks);

        public ConcurrentDictionary<long, ConcurrentBag<string>> DupChecker { get; } = new ConcurrentDictionary<long, ConcurrentBag<string>>();

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
        public CcHub Hub => (CcHub)_autoPeering;


        /// <summary>
        /// Total number of connections
        /// </summary>
        public int TotalConnections => IngressConnections + EgressConnections;

        public List<IoNeighbor<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>> Ingress => Drones.Where(kv=>(((CcDrone) kv).Adjunct.Ingress)).ToList();

        public List<IoNeighbor<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>> Drones => Neighbors.Values.Where(kv =>
            ((CcDrone)kv).Adjunct != null && ((CcDrone)kv).Adjunct.IsDroneAttached).ToList();

        public List<CcDrone> WhisperingDrones => Neighbors.Values.Where(kv =>
            ((CcDrone)kv).Adjunct != null && ((CcDrone)kv).Adjunct.IsDroneAttached).ToList().Cast<CcDrone>().ToList();

        /// <summary>
        /// Number of inbound neighbors
        /// </summary>
        public volatile int IngressConnections;

        public List<IoNeighbor<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>> Egress => Drones.Where(kv => (((CcDrone)kv).Adjunct.Egress)).ToList();

        /// <summary>
        /// Number of outbound neighbors
        /// </summary>
        public volatile int EgressConnections;

        /// <summary>
        /// Connected nodes
        /// </summary>
        private List<IoNeighbor<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>> Adjuncts => Neighbors.Values.Where(kv=> ((CcDrone)kv).Adjunct != null && ((CcDrone)kv).Adjunct.IsDroneConnected && ((CcDrone)kv).Adjunct.Ingress && ((CcDrone)kv).Adjunct.State == CcAdjunct.AdjunctState.Connected).ToList();

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
        /// Max inbound neighbors
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_neighbor = 16;

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
        /// Time between trying to re-aquire new neighbors using a discovery requests, if the node lacks <see cref="MaxDrones"/>
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_mean_pat_delay = 60 * 5;


        /// <summary>
        /// Client to neighbor ratio
        /// </summary>
        [IoParameter] public int parm_client_to_neighbor_ratio = 2;

        /// <summary>
        /// Maximum clients allowed
        /// </summary>
        public int MaxDrones => parm_max_outbound + parm_max_inbound;


        /// <summary>
        /// Maximum number of allowed drones
        /// </summary>
        public int MaxAdjuncts => MaxDrones * parm_client_to_neighbor_ratio;

        /// <summary>
        /// The node id
        /// </summary>
        public CcDesignation CcId { get; protected set; }

        /// <summary>
        /// Spawn the node listeners
        /// </summary>
        /// <param name="acceptConnection"></param>
        /// <param name="bootstrapAsync"></param>
        /// <returns></returns>
        protected override async Task SpawnListenerAsync(Func<IoNeighbor<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>, Task<bool>> acceptConnection = null, Func<Task> bootstrapAsync = null)
        {
            _autoPeeringTask = Task.Factory.StartNew(async () =>
            {
                //start peering
                await _autoPeering.StartAsync(BootStrapAsync).ConfigureAwait(false);

            }, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach);

            
            //start node listener
            await base.SpawnListenerAsync(async drone =>
            {
                //limit connects
                if (Zeroed() || IngressConnections >= parm_max_inbound)
                    return false;

                //Handshake
                if (await HandshakeAsync((CcDrone)drone).ConfigureAwait(false))
                {
                    //ACCEPT
                    _logger.Info($"+ {drone.Description}");
                    
                    return true;
                }
                else
                {
                    _logger.Debug($">|{drone.Description}");
                }

                return false;
            }, bootstrapAsync).ConfigureAwait(false);
        }

        /// <summary>
        /// Sends a message to a peer
        /// </summary>
        /// <param name="drone">The destination</param>
        /// <param name="msg">The message</param>
        /// <param name="timeout"></param>
        /// <returns>The number of bytes sent</returns>
        private async ValueTask<int> SendMessageAsync(CcDrone drone, ByteString msg, string type, int timeout = 0)
        {
            var responsePacket = new Packet
            {
                Data = msg,
                PublicKey = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcId.PublicKey)),
                Type = (uint)CcDiscoveries.MessageTypes.Handshake
            };

            responsePacket.Signature = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcId.Sign(responsePacket.Data.Memory.AsArray(), 0, responsePacket.Data.Length)));

            var protocolRaw = responsePacket.ToByteArray();

            var sentTask = drone.IoSource.IoNetSocket.SendAsync(protocolRaw, 0, protocolRaw.Length, timeout: timeout);

            if (!sentTask.IsCompletedSuccessfully)
                await sentTask.ConfigureAwait(false);

            if (sentTask.Result == protocolRaw.Length)
            {
                _logger.Trace($"{type}: Sent {sentTask.Result} bytes to {drone.IoSource.IoNetSocket.RemoteAddress} ({Enum.GetName(typeof(CcDiscoveries.MessageTypes), responsePacket.Type)})");
                return msg.Length;
            }
            else
            {
                _logger.Error($"{type}: Sent {sentTask.Result}/{protocolRaw.Length}...");
                return 0;
            }
        }


        private readonly Stopwatch _sw = Stopwatch.StartNew();
        private readonly int _handshakeRequestSize;
        private readonly int _handshakeResponseSize;
        private readonly int _handshakeBufferSize;
        public long Testing;


        //public new ValueTask<bool> ZeroAtomicAsync(Func<IIoNanite, object, bool, ValueTask<bool>> ownershipAction, object userData = null, bool disposing = false, bool force = false)
        //{
        //    return base.ZeroAtomicAsync(ownershipAction, userData, disposing, force);
        //}

        /// <summary>
        /// Perform handshake
        /// </summary>
        /// <param name="drone"></param>
        /// <returns></returns>
        private async ValueTask<bool> HandshakeAsync(CcDrone drone)
        {
            return await ZeroAtomicAsync(async (s, u, d) =>
            {
                var handshakeSuccess = false;
                var _this = (CcCollective)s;
                var __peer = (CcDrone) u;
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
                        do
                        {
                            var readTask = ioNetSocket
                                .ReadAsync(handshakeBuffer, bytesRead, _this._handshakeRequestSize - bytesRead,
                                    timeout: _this.parm_handshake_timeout);

                            if (readTask.IsCompletedSuccessfully)
                                bytesRead += readTask.Result;
                            else
                                bytesRead += await readTask.ConfigureAwait(false);

                        } while (
                            !Zeroed() &&
                            bytesRead < _handshakeRequestSize &&
                            ioNetSocket.NativeSocket.Available > 0 &&
                            bytesRead < handshakeBuffer.Length &&
                            ioNetSocket.NativeSocket.Available <= handshakeBuffer.Length - bytesRead
                        );


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
                            var packetData = packet.Data.Memory.AsArray();
                            
                            //verify the signature
                            if (packet.Signature != null || packet.Signature!.Length != 0)
                            {
                                verified = _this.CcId.Verify(packetData, 0,
                                    packetData.Length, packet.PublicKey.Memory.AsArray(), 0,
                                    packet!.Signature!.Memory.AsArray(), 0);
                            }
                            
                            //Don't process unsigned or unknown messages
                            if (!verified)
                                return false;

                            
                            //process handshake request 
                            var handshakeRequest = HandshakeRequest.Parser.ParseFrom(packet.Data);
                            bool won = false;
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

                                //race for connection
                                won = await _this.ConnectForTheWinAsync(CcAdjunct.Heading.Ingress, __peer, packet, (IPEndPoint)ioNetSocket.NativeSocket.RemoteEndPoint).ConfigureAwait(false);

                                _this._logger.Trace($"HandshakeRequest [{(verified ? "signed" : "un-signed")}], won = {won}, read = {bytesRead}, {__peer.IoSource.Key}");


                                //send response
                                var handshakeResponse = new HandshakeResponse
                                {
                                    ReqHash = won? UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray()))): ByteString.Empty
                                };
                                
                                var handshake = handshakeResponse.ToByteString();
                                
                                _this._sw.Restart();
                                
                                var sent = await _this.SendMessageAsync(__peer, handshake, nameof(HandshakeResponse),
                                    _this.parm_handshake_timeout).ConfigureAwait(false);
                                if (sent == 0)
                                {
                                    _this._logger.Trace($"{nameof(handshakeResponse)}: FAILED! {ioNetSocket.Description}");
                                    return false;
                                }
                            }
                            //Race
                            //return await ConnectForTheWinAsync(CcNeighbor.Kind.Inbound, peer, packet,
                            //        (IPEndPoint)ioNetSocket.NativeSocket.RemoteEndPoint)
                            //    .ConfigureAwait(false);
                            handshakeSuccess = !Zeroed() && drone.Adjunct != null && !drone.Adjunct.Zeroed() && won && drone.Adjunct?.Direction == CcAdjunct.Heading.Ingress && drone.Source.IsOperational && IngressConnections < parm_max_inbound;
                            return handshakeSuccess;
                        }
                    }
                    //-----------------------------------------------------//
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
                                $"Sent {sent} egress handshake challange, socket = {ioNetSocket.Description}");
                        }
                        else
                        {
                            _this._logger.Trace(
                                $"Failed to send egress handshake challange, socket = {ioNetSocket.Description}");
                            return false;
                        }

                        do
                        {
                            var readTask = ioNetSocket
                            .ReadAsync(handshakeBuffer, bytesRead, _this._handshakeResponseSize - bytesRead,
                                timeout: _this.parm_handshake_timeout);

                            if (readTask.IsCompletedSuccessfully)
                                bytesRead += readTask.Result;
                            else
                                bytesRead += await readTask.ConfigureAwait(false);

                        } while (
                            !Zeroed() &&
                            bytesRead < _handshakeResponseSize &&
                            ioNetSocket.NativeSocket.Available > 0 &&
                            bytesRead < handshakeBuffer.Length &&
                            ioNetSocket.NativeSocket.Available <= handshakeBuffer.Length - bytesRead
                        );

                        if (bytesRead == 0)
                        {
                            _this._logger.Trace(
                                $"Failed to read egress  handshake challange response, waited = {_this._sw.ElapsedMilliseconds}ms, remote = {ioNetSocket.RemoteAddress}, zeroed {_this.Zeroed()}");
                            return false;
                        }
                        
                        _this._logger.Trace(
                            $"Read egress  handshake challange response size = {bytesRead} b, addess = {ioNetSocket.RemoteAddress}");
                        
                        var verified = false;
                        
                        var packet = Packet.Parser.ParseFrom(handshakeBuffer, 0, bytesRead);
                        
                        if (packet != null && packet.Data != null && packet.Data.Length > 0)
                        {

                            var packetData = packet.Data.Memory.AsArray();
                        
                            //verify signature
                            if (packet.Signature != null || packet.Signature!.Length != 0)
                            {
                                verified = _this.CcId.Verify(packetData, 0,
                                    packetData.Length, packet.PublicKey.Memory.AsArray(), 0,
                                    packet.Signature.Memory.AsArray(), 0);
                            }
                        
                            //Don't process unsigned or unknown messages
                            if (!verified)
                            {
                                return false;
                            }

                            _this._logger.Trace(
                                $"HandshakeResponse [signed], from = egress, read = {bytesRead}, {__peer.IoSource.Key}");

                            //race for connection
                            var won = await _this.ConnectForTheWinAsync(CcAdjunct.Heading.Egress, __peer, packet,
                                    (IPEndPoint)ioNetSocket.NativeSocket.RemoteEndPoint)
                                .ConfigureAwait(false);

                            if(!won)
                                return false;

                            //validate handshake response
                            var handshakeResponse = HandshakeResponse.Parser.ParseFrom(packet.Data);
                        
                            if (handshakeResponse != null)
                            {
                                if (handshakeResponse.ReqHash.Length == 32)
                                {
                                    if (!CcDesignation.Sha256
                                        .ComputeHash(handshake.Memory.AsArray())
                                        .SequenceEqual(handshakeResponse.ReqHash))
                                    {
                                        _this._logger.Error($"Invalid handshake response! Closing {ioNetSocket.Key}");
                                        return false;
                                    }
                                }
                                else
                                {
                                    return false;
                                }
                            }
                        
                            handshakeSuccess = !Zeroed() && drone.Adjunct != null && !drone.Adjunct.Zeroed() && drone.Adjunct?.Direction == CcAdjunct.Heading.Egress && drone.Source.IsOperational && EgressConnections < parm_max_outbound;
                            return handshakeSuccess;
                        }
                    }
                }
                catch (NullReferenceException e)
                {
                    _this._logger.Trace(e, _this.Description);
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
                finally
                {
                    if (handshakeSuccess)
                    {
                        if (__peer.IoSource.IoNetSocket.Egress)
                        {
                            drone.ZeroEvent(_ =>
                            {
                                Interlocked.Decrement(ref EgressConnections);
                                return ValueTask.CompletedTask;
                            });

                            Interlocked.Increment(ref EgressConnections);
                        }
                        else if (__peer.IoSource.IoNetSocket.Ingress)
                        {
                            drone.ZeroEvent(nanite =>
                            {
                                Interlocked.Decrement(ref IngressConnections);
                                return ValueTask.CompletedTask;
                            });
                            Interlocked.Increment(ref IngressConnections);
                        }
                    }
                }

                return false;
            }, drone).ConfigureAwait(false);
        }

        /// <summary>
        /// Race for a connection locked on the neighbor it finds
        /// </summary>
        /// <param name="direction">The direction of the lock</param>
        /// <param name="drone">The peer requesting the lock</param>
        /// <param name="packet">The handshake packet</param>
        /// <param name="remoteEp">The remote</param>
        /// <returns>True if it won, false otherwise</returns>
        private async ValueTask<bool> ConnectForTheWinAsync(CcAdjunct.Heading direction, CcDrone drone, Packet packet, IPEndPoint remoteEp)
        {
            if(_gossipAddress.IpEndPoint.ToString() == remoteEp.ToString())
                throw new ApplicationException($"Connection inception dropped from {remoteEp} on {_gossipAddress.IpEndPoint.ToString()}: {Description}");

            //Race for connection...
            var id = CcAdjunct.MakeId(CcDesignation.FromPubKey(packet.PublicKey.Memory.AsArray()), "");

            if ((direction == CcAdjunct.Heading.Ingress) && (drone.Adjunct = (CcAdjunct) _autoPeering.Neighbors.Values.FirstOrDefault(n => n.Key.Contains(id))) == null)
            {
                _logger.Error($"Neighbor {id} not found, dropping {direction} connection to {remoteEp}");
                return false;
            }

            if (drone.Adjunct.Assimilating && !drone.Adjunct.IsDroneAttached)
            {
                //did we win?
                return await drone.AttachViaAdjunctAsync(direction).ConfigureAwait(false) && TotalConnections <= MaxDrones;
            }
            else
            {
                _logger.Trace($"{direction} handshake [LOST] {id} - {remoteEp}: s = {drone.Adjunct.State}, a = {drone.Adjunct.Assimilating}, p = {drone.Adjunct.IsDroneConnected}, pa = {drone.Adjunct.IsDroneAttached}, ut = {drone.Adjunct.Uptime.TickSec()}");
                return false;
            }
        }

        /// <summary>
        /// Opens an <see cref="CcAdjunct.Heading.Egress"/> connection to a gossip peer
        /// </summary>
        /// <param name="adjunct">The verified neighbor associated with this connection</param>
        public async ValueTask<bool> ConnectToDroneAsync(CcAdjunct adjunct)
        {
            //Validate
            if (
                    !Zeroed() && 
                    adjunct.Assimilating &&
                    !adjunct.IsDroneConnected &&
                    //adjunct.State <= CcAdjunct.AdjunctState.Standby &&
                    EgressConnections < parm_max_outbound &&
                    //TODO add distance calc
                    adjunct.Services.CcRecord.Endpoints.ContainsKey(CcService.Keys.gossip)
                )
            {
                var drone = await ConnectAsync(adjunct.Services.CcRecord.Endpoints[CcService.Keys.gossip], adjunct).ConfigureAwait(false);
                if (Zeroed() || drone == null || ((CcDrone)drone).Adjunct.Zeroed())
                {
                    if (drone != null) await drone.ZeroAsync(this).ConfigureAwait(false);
                    _logger.Debug($"{nameof(ConnectToDroneAsync)}: [ABORTED], {adjunct.Description}, {adjunct.MetaDesc}");
                    return false;
                }
                
                //Race for a connection
                if (await HandshakeAsync((CcDrone)drone).ConfigureAwait(false))
                {
                    _logger.Info($"+ {drone.Description}");

                    NeighborTasks.Add(drone.AssimilateAsync());
                    return true;
                }
                else
                {
                    await drone.ZeroAsync(new IoNanoprobe("Lost connection race")).ConfigureAwait(false);
                    _logger.Debug($"|>{drone.Description}");
                    return false;
                }
            }
            else
            {
                _logger.Trace($"{nameof(ConnectToDroneAsync)}: Connect skipped: {adjunct.Description}");
                return false;
            }
        }

        private static readonly float _lambda = 10;
        
        private readonly Poisson _poisson = new Poisson(_lambda);
        /// <summary>
        /// Boots the node
        /// </summary>
        public async ValueTask<bool> BootAsync(long v = 0)
        {
            Interlocked.Exchange(ref Testing, 1);
            var s = 0;
            foreach (var ioNeighbor in Drones)
            {
                s = _poisson.Sample();
                if (Math.Abs(s - _lambda) > _lambda * 4/5)
                {
                    //Console.Write($"[{s}]");
                    continue;
                }
                try
                {
                    await ((CcDrone) ioNeighbor).EmitTestGossipMsgAsync(v).ConfigureAwait(false);
                    return true;
                }
                catch (Exception e)
                {
                    _logger.Debug(e,Description);
                }
            }

            return false;
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
                var c = 0;
                foreach (var ioNodeAddress in BootstrapAddress)
                {
                    if (!ioNodeAddress.Equals(_peerAddress))
                    {
                        if(Hub.Neighbors.Values.Count(a=>a.Key.Contains(ioNodeAddress.Key)) > 0)
                            continue;

                        await Task.Delay(++c * 2000).ConfigureAwait(false);
                        //_logger.Trace($"{Description} Bootstrapping from {ioNodeAddress}");
                        if (!await Hub.Router.SendPingAsync(ioNodeAddress, ioNodeAddress.Key).ConfigureAwait(false))
                        {
                            if(!Hub.Router.Zeroed())
                                _logger.Trace($"{nameof(BootStrapAsync)}: Unable to boostrap {Description} from {ioNodeAddress}");
                        }
                    }
                }
            }
        }
    }
}
