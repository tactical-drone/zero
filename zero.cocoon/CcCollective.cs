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
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore;
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
            : base(gossipAddress, static (node, ioNetClient, extraData) => new CcDrone((CcCollective)node, (CcAdjunct)extraData, ioNetClient), tcpPrefetch, tpcConcurrencyLevel, 16) //TODO config
        {
            _logger = LogManager.GetCurrentClassLogger();
            _gossipAddress = gossipAddress;
            _peerAddress = peerAddress;
            BootstrapAddress = bootstrap;
            ExtAddress = extAddress; //this must be the external or NAT address.
            CcId = ccDesignation;

            Services.CcRecord.Endpoints.TryAdd(CcService.Keys.peering, _peerAddress);
            Services.CcRecord.Endpoints.TryAdd(CcService.Keys.gossip, _gossipAddress);
            Services.CcRecord.Endpoints.TryAdd(CcService.Keys.fpc, fpcAddress);

            _autoPeering =  new CcHub(this, _peerAddress,static (node, client, extraData) => new CcAdjunct((CcHub) node, client, extraData), udpPrefetch, udpConcurrencyLevel);
            _autoPeering.ZeroHiveAsync(this, true).AsTask().GetAwaiter().GetResult();
            
            DupSyncRoot = new IoZeroSemaphoreSlim(AsyncTasks,  $"Dup checker for {ccDesignation.IdString()}", maxBlockers: parm_max_drone * tpcConcurrencyLevel, initialCount:1);
            DupSyncRoot.ZeroHiveAsync(DupSyncRoot).AsTask().GetAwaiter().GetResult();
            
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

            DupHeap = new IoHeap<IoBag<string>, CcCollective>($"{nameof(DupHeap)}: {Description}", _dupPoolSize * 2)
            {
                Make = static (o, s) => new IoBag<string>(null, s._dupPoolSize * 2, true),
                Prep = (popped, endpoint) =>
                {
                    popped.Add((string)endpoint);
                },
                Context = this
            };

            //ensure robotics
            ZeroAsync(RoboAsync,this,TaskCreationOptions.LongRunning | TaskCreationOptions.PreferFairness | TaskCreationOptions.DenyChildAttach).AsTask().GetAwaiter();
        }

        /// <summary>
        /// Robotics
        /// </summary>
        /// <param name="this"></param>
        /// <returns></returns>
        private async ValueTask RoboAsync(CcCollective @this)
        {
            var secondsSinceEnsured = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            
            //while running
            while (!@this.Zeroed())
            {
                //periodically
                await Task.Delay((_random.Next(@this.parm_mean_pat_delay / 5) + @this.parm_mean_pat_delay / 4) * 1000, @this.AsyncTasks.Token).ConfigureAwait(Zc);
                
                if (@this.Zeroed())
                    break;

                try
                {
                    var totalConnections = @this.TotalConnections;
                    double scanRatio = 1;

                    //Attempt to peer with standbys
                    if (totalConnections < @this.MaxDrones * scanRatio &&
                        secondsSinceEnsured.Elapsed() >= @this.parm_mean_pat_delay / 4) //delta q
                    {
                        @this._logger.Trace($"Scanning {@this._autoPeering.Neighbors.Values.Count}, {@this.Description}");

                        var c = 0;
                        //Send peer requests
                        foreach (var adjunct in @this._autoPeering.Neighbors.Values.Where(n =>
                                ((CcAdjunct)n).State is CcAdjunct.AdjunctState.Verified) //quick slot
                            .OrderBy(n => ((CcAdjunct)n).Priority)
                            .ThenBy(n => ((CcAdjunct)n).Uptime.ElapsedMs()))
                        {
                            await ((CcAdjunct)adjunct).SendPingAsync().FastPath().ConfigureAwait(Zc);
                            c++;
                        }

                        //Scan for peers
                        if (c == 0)
                        {
                            foreach (var adjunct in @this._autoPeering.Neighbors.Values.Where(n =>
                                    ((CcAdjunct)n).State is > CcAdjunct.AdjunctState.Verified) //quick slot
                                .OrderBy(n => ((CcAdjunct)n).Priority)
                                .ThenBy(n => ((CcAdjunct)n).Uptime.ElapsedMs()))
                            {
                                if (!Zeroed())
                                    await ((CcAdjunct)adjunct).SendDiscoveryRequestAsync().FastPath().ConfigureAwait(Zc);
                                else
                                    break;

                                c++;
                            }
                        }

                        if (@this.Neighbors.Count == 0 && secondsSinceEnsured.Elapsed() >= @this.parm_mean_pat_delay && !Zeroed())
                        {
                            //bootstrap if alone
                            await @this.DeepScanAsync().FastPath().ConfigureAwait(Zc);
                        }

                        if (secondsSinceEnsured.Elapsed() > @this.parm_mean_pat_delay)
                            secondsSinceEnsured = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                    }
                }
                catch when (@this.Zeroed()) { }
                catch (Exception e) when (!@this.Zeroed())
                {
                    @this._logger?.Error(e, $"Failed to ensure {@this._autoPeering.Neighbors.Count} peers");
                }
            }
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            DupHeap = null;
            _logger = null;
            _autoPeering = null;
            _autoPeeringTask = default;
            CcId = null;
            Services = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            var id = Hub?.Router?.Designation?.IdString();
            await DupSyncRoot.ZeroAsync(this).FastPath().ConfigureAwait(Zc);
            //Services.CcRecord.Endpoints.Clear();
            try
            {
                //_autoPeeringTask?.Wait();
            }
            catch
            {
                // ignored
            }

            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);

            try
            {
                if (!string.IsNullOrEmpty(id))
                {
                    if (AutoPeeringEventService.Operational)
                        await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.RemoveCollective,
                            Collective = new Collective()
                            {
                                Id = id
                            }
                        }).FastPath().ConfigureAwait(Zc);
                }
            }
            catch 
            {
                
            }

            await DupHeap.ClearAsync().FastPath().ConfigureAwait(Zc);
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

        private CcHub _autoPeering;
        
        private readonly IoNodeAddress _gossipAddress;
        private readonly IoNodeAddress _peerAddress;

        readonly Random _random = new((int)DateTime.Now.Ticks);
        public IoZeroSemaphoreSlim DupSyncRoot { get; init; }
        public ConcurrentDictionary<long, IoBag<string>> DupChecker { get; } = new();
        public IoHeap<IoBag<string>, CcCollective> DupHeap { get; protected set; }
        private uint _dupPoolSize = 50;

        /// <summary>
        /// Bootstrap
        /// </summary>
        private List<IoNodeAddress> BootstrapAddress { get; }

        /// <summary>
        /// Reachable from NAT
        /// </summary>
        public IoNodeAddress ExtAddress { get; }

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
        public CcHub Hub => _autoPeering;

        /// <summary>
        /// Total number of connections
        /// </summary>
        public int TotalConnections => IngressCount + EgressCount;

        public List<CcDrone> IngressDrones => Drones?.Where(kv=> kv.Adjunct.IsIngress).ToList();

        public List<CcDrone> EgressDrones => Drones?.Where(kv => kv.Adjunct.IsEgress).ToList();

        public List<CcDrone> Drones => Neighbors?.Values.Where(kv => ((CcDrone)kv).Adjunct is { IsDroneConnected: true }).Cast<CcDrone>().ToList();

        public List<CcAdjunct> Adjuncts => Hub?.Neighbors?.Values.Where(kv => ((CcAdjunct)kv).State > CcAdjunct.AdjunctState.Unverified).Cast<CcAdjunct>().ToList();

        public List<CcDrone> WhisperingDrones => Neighbors?.Values.Where(kv => ((CcDrone)kv).Adjunct is { IsGossiping: true }).Cast<CcDrone>().ToList();

        /// <summary>
        /// Number of inbound neighbors
        /// </summary>
        public volatile int IngressCount;

        /// <summary>
        /// Number of outbound neighbors
        /// </summary>
        public volatile int EgressCount;

        /// <summary>
        /// The services this node supports
        /// </summary>
        public CcService Services { get; set; } = new CcService();

        /// <summary>
        /// The autopeering task handler
        /// </summary>
        private ValueTask _autoPeeringTask;

        /// <summary>
        /// Max inbound neighbors
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public uint parm_max_inbound = 4;

        /// <summary>
        /// Max inbound neighbors
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public uint parm_max_outbound = 4;

        /// <summary>
        /// Max adjuncts
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_drone = 8;
        
        /// <summary>
        /// Max adjuncts
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public uint parm_max_adjunct = 16;

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
#if DEBUG        
        public int parm_time_e = 60;
#else
        public int parm_time_e = 4;
#endif


        /// <summary>
        /// Time between trying to re-aquire new neighbors using a discovery requests, if the node lacks <see cref="MaxDrones"/>
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_mean_pat_delay = 60 * 10;


        /// <summary>
        /// Client to neighbor ratio
        /// </summary>
        [IoParameter] public uint parm_client_to_neighbor_ratio = 2;

        /// <summary>
        /// Maximum clients allowed
        /// </summary>
        public uint MaxDrones => parm_max_outbound + parm_max_inbound;


        /// <summary>
        /// Maximum number of allowed drones
        /// </summary>
        public uint MaxAdjuncts => MaxDrones * parm_client_to_neighbor_ratio;

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
        protected override async ValueTask SpawnListenerAsync<T>(Func<IoNeighbor<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>, T,ValueTask<bool>> acceptConnection = null, T nanite = default, Func<ValueTask> bootstrapAsync = null)
        {
            //Start the hub
            await ZeroAsync(static async @this =>
            {
                while (!@this.Zeroed())
                {
                    @this._autoPeeringTask = @this._autoPeering.StartAsync(@this.DeepScanAsync);
                    await @this._autoPeeringTask;
                }
            }, this,  TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(Zc);

            //start node listener
            await base.SpawnListenerAsync(static async (drone,@this) =>
            {
                //limit connects
                if (@this.Zeroed() || @this.IngressCount >= @this.parm_max_inbound)
                    return false;

                //Handshake
                if (await @this.HandshakeAsync((CcDrone)drone).FastPath().ConfigureAwait(@this.Zc))
                {
                    //ACCEPT
                    @this._logger.Info($"+ {drone.Description}");
                    
                    return true;
                }
                else
                {
                    @this._logger.Debug($">|{drone.Description}");
                }

                return false;
            },this, bootstrapAsync).ConfigureAwait(Zc);
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

            var sent = 0;
            if ((sent = await drone.IoSource.IoNetSocket.SendAsync(protocolRaw, 0, protocolRaw.Length, timeout: timeout).FastPath().ConfigureAwait(Zc)) == protocolRaw.Length)
            {
                _logger.Trace($"~/> {type}({sent}): {drone.IoSource.IoNetSocket.LocalAddress} ~> {drone.IoSource.IoNetSocket.RemoteAddress} ({Enum.GetName(typeof(CcDiscoveries.MessageTypes), responsePacket.Type)})");
                return msg.Length;
            }
            else
            {
                _logger.Error($"~/> {type}({sent}):  {drone.IoSource.IoNetSocket.LocalAddress} ~> {drone.IoSource.IoNetSocket.RemoteAddress}");
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
        /// <param name="drone"></param>
        /// <returns></returns>
        private async ValueTask<bool> HandshakeAsync(CcDrone drone)
        {
            
            var handshakeSuccess = false;
            var bytesRead = 0;
            if (Zeroed()) return false;

            try
            {                
                var handshakeBuffer = new byte[_handshakeBufferSize];
                var ioNetSocket = drone.IoSource.IoNetSocket;

                //inbound
                if (drone.IoSource.IoNetSocket.IsIngress)
                {
                    var verified = false;
                    
                    _sw.Restart();
                    //read from the socket
                    do
                    {
                        bytesRead+= await ioNetSocket
                            .ReadAsync(handshakeBuffer, bytesRead, _handshakeRequestSize - bytesRead).FastPath().ConfigureAwait(Zc);

                    } while (
                        !Zeroed() &&
                        bytesRead < _handshakeRequestSize &&
                        ioNetSocket.NativeSocket.Available > 0 &&
                        bytesRead < handshakeBuffer.Length &&
                        ioNetSocket.NativeSocket.Available <= handshakeBuffer.Length - bytesRead
                    );


                    if (bytesRead == 0)
                    {
                        _logger.Trace(
                            $"Failed to read inbound challange request, waited = {_sw.ElapsedMilliseconds}ms, socket = {ioNetSocket.Description}");
                        return false;
                    }
                    else
                    {
                        _logger.Trace(
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
                            verified = CcId.Verify(packetData, 0,
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
                            if (handshakeRequest.Timestamp.ElapsedDelta() > parm_time_e)
                            {
                                _logger.Error(
                                    $"Rejected old handshake request from {ioNetSocket.Key} - d = {handshakeRequest.Timestamp.ElapsedDelta()}s, > {parm_time_e}");
                                return false;
                            }
                            
                            //reject invalid protocols
                            if (handshakeRequest.Version != parm_version)
                            {
                                _logger.Error(
                                    $"Invalid handshake protocol version from  {ioNetSocket.Key} - got {handshakeRequest.Version}, wants {parm_version}");
                                return false;
                            }

                            //reject requests to invalid ext ip
                            //if (handshakeRequest.To != ((CcNeighbor)neighbor)?.ExtGossipAddress?.IpPort)
                            //{
                            //    _logger.Error($"Invalid handshake received from {socket.Key} - got {handshakeRequest.To}, wants {((CcNeighbor)neighbor)?.ExtGossipAddress.IpPort}");
                            //    return false;
                            //}

                            //race for connection
                            won = await ConnectForTheWinAsync(CcAdjunct.Heading.Ingress, drone, packet, (IPEndPoint)ioNetSocket.NativeSocket.RemoteEndPoint).FastPath().ConfigureAwait(Zc);

                            _logger.Trace($"HandshakeRequest [{(verified ? "signed" : "un-signed")}], won = {won}, read = {bytesRead}, {drone.IoSource.Key}");

                            //send response
                            var handshakeResponse = new HandshakeResponse
                            {
                                ReqHash = won? UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray()))): ByteString.Empty
                            };
                            
                            var handshake = handshakeResponse.ToByteString();
                            
                            _sw.Restart();
                            
                            var sent = await SendMessageAsync(drone, handshake, nameof(HandshakeResponse),
                                parm_handshake_timeout).FastPath().ConfigureAwait(Zc);
                            if (sent == 0)
                            {
                                _logger.Trace($"{nameof(handshakeResponse)}: FAILED! {ioNetSocket.Description}");
                                return false;
                            }
                        }
                        //Race
                        //return await ConnectForTheWinAsync(CcNeighbor.Kind.Inbound, peer, packet,
                        //        (IPEndPoint)ioNetSocket.NativeSocket.RemoteEndPoint)
                        //    .FastPath().ConfigureAwait(ZC);
                        handshakeSuccess = !Zeroed() && drone.Adjunct != null && !drone.Adjunct.Zeroed() && won && drone.Adjunct?.Direction == CcAdjunct.Heading.Ingress && drone.Source.IsOperational && IngressCount < parm_max_inbound;
                        return handshakeSuccess;
                    }
                }
                //-----------------------------------------------------//
                else if (drone.IoSource.IoNetSocket.IsEgress) //Outbound
                {
                    var handshakeRequest = new HandshakeRequest
                    {
                        Version = parm_version,
                        To = ioNetSocket.LocalNodeAddress.IpPort,
                        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                    };
                    
                    var handshake = handshakeRequest.ToByteString();
                    
                    _sw.Restart();
                    var sent = await SendMessageAsync(drone, handshake, nameof(HandshakeResponse))
                        .FastPath().ConfigureAwait(Zc);
                    if (sent > 0)
                    {
                        _logger.Trace(
                            $"Sent {sent} egress handshake challange, socket = {ioNetSocket.Description}");
                    }
                    else
                    {
                        _logger.Trace(
                            $"Failed to send egress handshake challange, socket = {ioNetSocket.Description}");
                        return false;
                    }

                    do
                    {
                        bytesRead = await ioNetSocket
                        .ReadAsync(handshakeBuffer, bytesRead, _handshakeResponseSize - bytesRead,
                            timeout: parm_handshake_timeout).FastPath().ConfigureAwait(Zc);
                    } while (
                        !Zeroed() &&
                        bytesRead < _handshakeResponseSize &&
                        ioNetSocket.NativeSocket.Available > 0 &&
                        bytesRead < handshakeBuffer.Length &&
                        ioNetSocket.NativeSocket.Available <= handshakeBuffer.Length - bytesRead
                    );

                    if (bytesRead == 0)
                    {
                        _logger.Trace(
                            $"Failed to read egress  handshake challange response, waited = {_sw.ElapsedMilliseconds}ms, remote = {ioNetSocket.RemoteAddress}, zeroed {Zeroed()}");
                        return false;
                    }
                    
                    _logger.Trace(
                        $"Read egress  handshake challange response size = {bytesRead} b, addess = {ioNetSocket.RemoteAddress}");
                    
                    var verified = false;
                    
                    var packet = Packet.Parser.ParseFrom(handshakeBuffer, 0, bytesRead);
                    
                    if (packet != null && packet.Data != null && packet.Data.Length > 0)
                    {

                        var packetData = packet.Data.Memory.AsArray();
                    
                        //verify signature
                        if (packet.Signature != null || packet.Signature!.Length != 0)
                        {
                            verified = CcId.Verify(packetData, 0,
                                packetData.Length, packet.PublicKey.Memory.AsArray(), 0,
                                packet.Signature.Memory.AsArray(), 0);
                        }
                    
                        //Don't process unsigned or unknown messages
                        if (!verified)
                        {
                            return false;
                        }

                        _logger.Trace(
                            $"HandshakeResponse [signed], from = egress, read = {bytesRead}, {drone.IoSource.Key}");

                        //race for connection
                        var won = await ConnectForTheWinAsync(CcAdjunct.Heading.Egress, drone, packet,
                                (IPEndPoint)ioNetSocket.NativeSocket.RemoteEndPoint)
                            .FastPath().ConfigureAwait(Zc);

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
                                    _logger.Error($"Invalid handshake response! Closing {ioNetSocket.Key}");
                                    return false;
                                }
                            }
                            else
                            {
                                return false;
                            }
                        }
                    
                        handshakeSuccess = !Zeroed() && drone.Adjunct != null && !drone.Adjunct.Zeroed() && drone.Adjunct?.Direction == CcAdjunct.Heading.Egress && drone.Source.IsOperational && EgressCount < parm_max_outbound;
                        return handshakeSuccess;
                    }
                }
            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e, Description);
            }
            catch (TaskCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (ObjectDisposedException e)
            {
                _logger.Trace(e, Description);
            }
            catch (Exception e)
            {
                _logger.Error(e,
                    $"Handshake (size = {bytesRead}/{_handshakeRequestSize}/{_handshakeResponseSize}) for {Description} failed with:");
            }
            finally
            {
                if (handshakeSuccess)
                {
                    if (drone.IoSource.IoNetSocket.IsEgress)
                    {
                        await drone.ZeroSubAsync(static (_,@this) =>
                        {
                            Interlocked.Decrement(ref @this.EgressCount);
                            return ValueTask.FromResult(true);
                        }, this).FastPath().ConfigureAwait(Zc);                        
                    }
                    else if (drone.IoSource.IoNetSocket.IsIngress)
                    {
                        await drone.ZeroSubAsync(static (from, @this) =>
                        {
                            Interlocked.Decrement(ref @this.IngressCount);
                            return ValueTask.FromResult(true);
                        }, this).FastPath().ConfigureAwait(Zc);                        
                    }
                }
            }

            return false;
            
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

            var id = CcAdjunct.MakeId(CcDesignation.FromPubKey(packet.PublicKey.Memory.AsArray()), "");
            if ((direction == CcAdjunct.Heading.Ingress) && (drone.Adjunct = (CcAdjunct)_autoPeering.Neighbors.Values.FirstOrDefault(n => n.Key.Contains(id))) == null)
            {
                _logger.Error($"Neighbor {id} not found, dropping {direction} connection to {remoteEp}");
                return false;
            }
            
            if (drone.Adjunct.Assimilating && !drone.Adjunct.IsDroneAttached)
            {
                var attached = await drone.AttachViaAdjunctAsync(direction).FastPath().ConfigureAwait(Zc);

                return (await ZeroAtomicAsync(static (n, o, d) =>
                {
                    var (@this, id, direction, packet, drone, remoteEp) = o;
                                                            
                    if (direction == CcAdjunct.Heading.Ingress)
                    {
                        if(Interlocked.Increment(ref @this.IngressCount) - 1 < @this.parm_max_inbound)
                        {
                            return ValueTask.FromResult(true);
                        }
                        else
                        {
                            Interlocked.Decrement(ref @this.IngressCount);
                            return ValueTask.FromResult(false);
                        }
                    }
                    else
                    {
                        if (Interlocked.Increment(ref @this.EgressCount) - 1 < @this.parm_max_outbound)
                        {
                            return ValueTask.FromResult(true);
                        }
                        else
                        {
                            Interlocked.Decrement(ref @this.EgressCount);
                            return ValueTask.FromResult(false);
                        }
                    }
                }, (this, id, direction, packet, drone, remoteEp)).FastPath().ConfigureAwait(Zc) && attached);
            }
            else
            {
                _logger.Trace($"{direction} handshake [LOST] {id} - {remoteEp}: s = {drone.Adjunct.State}, a = {drone.Adjunct.Assimilating}, p = {drone.Adjunct.IsDroneConnected}, pa = {drone.Adjunct.IsDroneAttached}, ut = {drone.Adjunct.Uptime.ElapsedMs()}");
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
                    EgressCount < parm_max_outbound &&
                    //TODO add distance calc
                    adjunct.Services.CcRecord.Endpoints.ContainsKey(CcService.Keys.gossip)&&
                    _currentOutboundConnectionAttempts < _maxAsyncConnectionAttempts
                )
            {
                try
                {
                    Interlocked.Increment(ref _currentOutboundConnectionAttempts);

                    var drone = await ConnectAsync(adjunct.Services.CcRecord.Endpoints[CcService.Keys.gossip], adjunct, timeout:adjunct.parm_max_network_latency * 2).FastPath().ConfigureAwait(Zc);
                    if (Zeroed() || drone == null || ((CcDrone)drone).Adjunct.Zeroed())
                    {
                        if (drone != null) await drone.ZeroAsync(this).FastPath().ConfigureAwait(Zc);
                        _logger.Debug($"{nameof(ConnectToDroneAsync)}: [ABORTED], {adjunct.Description}, {adjunct.MetaDesc}");
                        return false;
                    }
                
                    //Race for a connection
                    if (await HandshakeAsync((CcDrone)drone).FastPath().ConfigureAwait(Zc))
                    {
                        _logger.Info($"+ {drone.Description}");

                        //Start processing
                        await ZeroAsync(static async state =>
                        {
                            var (@this, drone) = state;
                            await @this.BlockOnAssimilateAsync(drone).FastPath().ConfigureAwait(@this.Zc);
                        }, ValueTuple.Create(this, drone), TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(false);
                        
                        
                        return true;
                    }
                    else
                    {
                        _logger.Debug($"|>{drone.Description}");
                        await drone.ZeroAsync(this).FastPath().ConfigureAwait(Zc);
                    }

                    return false;
                }
                finally
                {
                    Interlocked.Decrement(ref _currentOutboundConnectionAttempts);
                }
            }
            else
            {
                _logger.Trace($"{nameof(ConnectToDroneAsync)}: Connect skipped: {adjunct.Description}");
                return false;
            }
        }

        private static readonly double _lambda = 20;
        private static readonly int _maxAsyncConnectionAttempts = 2;

        private int _currentOutboundConnectionAttempts;
        private readonly Poisson _poisson = new(_lambda, Random.Shared);

        /// <summary>
        /// Boots the node
        /// </summary>
        public async ValueTask<bool> BootAsync(long v = 0, int total = 1)
        {
            Interlocked.Exchange(ref Testing, 1);
            //foreach (var ioNeighbor in WhisperingDrones)
            //{
            //    var s = Poisson.Sample(Random.Shared, 1 / (double)total);
            //    Console.WriteLine(s);
            //    if (s == 0)
            //    {
            //        continue;
            //    }
            //    try
            //    {
            //        await ioNeighbor.EmitTestGossipMsgAsync(v).FastPath().ConfigureAwait(Zc);
            //        return true;
            //    }
            //    catch when(Zeroed()){}
            //    catch (Exception e)when(!Zeroed())
            //    {
            //        _logger.Debug(e,Description);
            //    }
            //}

            
            if(WhisperingDrones.Count > 0)
                await WhisperingDrones[_random.Next(0, WhisperingDrones.Count-1)].EmitTestGossipMsgAsync(v).FastPath().ConfigureAwait(Zc);
            
            return true;
        }

        /// <summary>
        /// Boostrap node
        /// </summary>
        /// <returns></returns>
        public async ValueTask DeepScanAsync()
        {
            
            var c = 0;
            var foundVector = false;
            foreach (var vector in Hub.Neighbors.Values.TakeWhile(n=>((CcAdjunct)n).Assimilating))
            {
                var adjunct = (CcAdjunct)vector;

                //Only probe when we are running lean
                if (adjunct.CcCollective.Hub.Neighbors.Count > adjunct.CcCollective.MaxAdjuncts)
                    break;
            
                //probe
                if (!await adjunct.SendPingAsync().FastPath().ConfigureAwait(Zc))
                {
                    if(!Zeroed())
                        _logger.Trace( $"{nameof(DeepScanAsync)}: {Description}, Unable to probe adjuncts");
                }
                else
                {
                    _logger.Debug($"? {Description}");
                    foundVector = true;
                }
                
                await Task.Delay(++c * ((CcAdjunct)vector).parm_max_network_latency*2, AsyncTasks.Token).ConfigureAwait(Zc);
            }
            
            if(foundVector)
                return;
            
            _logger.Trace($"Bootstrapping {Description} from {BootstrapAddress.Count} bootnodes...");
            if (BootstrapAddress != null)
            {
                c = 0;
                foreach (var ioNodeAddress in BootstrapAddress)
                {
                    if (!ioNodeAddress.Equals(_peerAddress))
                    {
                        if(Hub.Neighbors.Values.Count(a=>a.Key.Contains(ioNodeAddress.Key)) > 0)
                            continue;

                        await Task.Delay(++c * 2000, AsyncTasks.Token).ConfigureAwait(Zc);
                        //_logger.Trace($"{Description} Bootstrapping from {ioNodeAddress}");
                        if (!await Hub.Router.SendPingAsync(ioNodeAddress, ioNodeAddress.Key).FastPath().ConfigureAwait(Zc))
                        {
                            if(!Hub.Router.Zeroed())
                                _logger.Trace($"{nameof(DeepScanAsync)}: Unable to boostrap {Description} from {ioNodeAddress}");
                        }
                    }
                }
            }
        }

        public ValueTask EmitAsync()
        {
            //Emit collective event
            if (AutoPeeringEventService.Operational)
                return AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                {
                    EventType = AutoPeerEventType.AddCollective,
                    Collective = new Collective
                    {
                        Id = CcId.IdString(),
                        Ip = ExtAddress.Url
                    }
                });

            return ValueTask.CompletedTask;
        }
    }
}
