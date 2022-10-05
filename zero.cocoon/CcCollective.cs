﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using zero.cocoon.autopeer;
using zero.cocoon.events.services;
using zero.cocoon.identity;
using zero.cocoon.models;
using zero.cocoon.models.batches;
using zero.cocoon.models.services;
using zero.core.conf;
using zero.core.core;
using zero.core.feat.models.protobuffer;
using zero.core.feat.patterns.time;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore;
using zero.core.runtime.scheduler;
using Zero.Models.Protobuf;

namespace zero.cocoon
{
    /// <summary>
    /// Connects to cocoon
    /// </summary>
    public class CcCollective : IoNode<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>
    {
        public CcCollective(CcDesignation ccDesignation, IoNodeAddress gossipAddress, IoNodeAddress peerAddress,
            IoNodeAddress fpcAddress, IoNodeAddress extAddress, List<IoNodeAddress> bootstrap, int udpPrefetch,
            int tcpPrefetch, int udpConcurrencyLevel, int tcpConcurrencyLevel, bool zeroDrone)
            : base(gossipAddress, static (node, ioNetClient, extraData) => new CcDrone((CcCollective)node, (CcAdjunct)extraData, ioNetClient), tcpPrefetch, tcpConcurrencyLevel, 16 * 2) //TODO config
        {
            _logger = LogManager.GetCurrentClassLogger();
            _gossipAddress = gossipAddress;
            _peerAddress = peerAddress;
            _udpPrefetch = udpPrefetch;
            _udpConcurrencyLevel = udpConcurrencyLevel;

            BootstrapAddress = bootstrap;
            ExtAddress = extAddress; //this must be the external or NAT address.
            CcId = ccDesignation;

            _zeroDrone = zeroDrone;
            if (_zeroDrone)
            {
                _zeroDrone = true;
                parm_max_drone = 0;
                parm_max_adjunct = 512; //TODO tuning:
                udpPrefetch = 4;
                udpConcurrencyLevel = 3;
                NeighborTasks = new ConcurrentDictionary<string, Task>();
            }

            Services.CcRecord.Endpoints.TryAdd(CcService.Keys.Peering, _peerAddress);
            Services.CcRecord.Endpoints.TryAdd(CcService.Keys.Gossip, _gossipAddress);
            Services.CcRecord.Endpoints.TryAdd(CcService.Keys.Fpc, fpcAddress);

            DupSyncRoot = new IoZeroSemaphoreSlim(AsyncTasks,  $"Dup checker for {ccDesignation.IdString()}", maxBlockers: Math.Max(MaxDrones * tcpConcurrencyLevel,1), initialCount: 1);
            
            // Calculate max handshake
            var futileRequest = new CcFutileRequest
            {
                Protocol = parm_version,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            var protocolMsg = new chroniton
            {
                Signature = UnsafeByteOperations.UnsafeWrap(BitConverter.GetBytes((int)CcDiscoveries.MessageTypes.Handshake)),
                PublicKey = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcId.PublicKey)),
                Data = futileRequest.ToByteString(),
                Type = 1
            };
            protocolMsg.Signature = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcId.Sign(protocolMsg.Data.Memory.ToArray(), 0, protocolMsg.Data.Length)));

            _futileRequestSize = protocolMsg.CalculateSize();

            var futileResponse = new CcFutileResponse
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Protocol = parm_version,
                ReqHash = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcDesignation.Sha256.ComputeHash(protocolMsg.Data.Memory.AsArray())))
            };

            protocolMsg = new chroniton
            {
                Signature = UnsafeByteOperations.UnsafeWrap(BitConverter.GetBytes((int)CcDiscoveries.MessageTypes.Handshake)),
                Data = futileResponse.ToByteString(),
                PublicKey = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcId.PublicKey)),
                Type = (int)CcDiscoveries.MessageTypes.Handshake
            };

            protocolMsg.Signature = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcId.Sign(protocolMsg.Data.Memory.AsArray(), 0, protocolMsg.Data.Length)));

            _futileResponseSize = protocolMsg.CalculateSize();

            futileResponse.ReqHash = ByteString.CopyFrom(9);
            protocolMsg.Data = futileResponse.ToByteString();

            _futileRejectSize = protocolMsg.CalculateSize();

            _fuseBufSize = Math.Max(_futileResponseSize, _futileRequestSize);

            if(_fuseBufSize > parm_max_handshake_bytes)
                throw new ApplicationException($"{nameof(_fuseBufSize)} > {parm_max_handshake_bytes}");

            _dupPoolFpsTarget = 8192;  
            DupHeap = new IoHeap<List<string>, CcCollective>($"{nameof(DupHeap)}: {_description}", _dupPoolFpsTarget, static (_, @this) => new List<string>(@this.MaxDrones), autoScale: true)
            {
                PopAction = (popped, endpoint) =>
                {
                    popped.Add((string)endpoint);
                },
                Context = this
            };//ensure robotics
        }

        private async ValueTask StartHubAsync(int udpPrefetch, int udpConcurrencyLevel)
        {
            if (_autoPeering != null)
                await _autoPeering.DisposeAsync(this, "RESTART!").FastPath();

            Interlocked.Exchange(ref _autoPeering, new CcHub(this, _peerAddress, static (node, client, extraData) => new CcAdjunct((CcHub)node, client, extraData), udpPrefetch, udpConcurrencyLevel));
            
            await _autoPeering.ZeroSubAsync(static async (_, state) =>
            {
                var (@this, udpPrefetch, udpConcurrencyLevel) = (ValueTuple<CcCollective, int, int>)state;
                if (!@this.Zeroed())
                    await @this.StartHubAsync(udpPrefetch, udpConcurrencyLevel).FastPath();

                return true;
            }, (this, udpPrefetch, udpConcurrencyLevel));

            if (Zeroed())
                return;

            _autoPeeringTask = _autoPeering.StartAsync(static async @this =>
            {
                var spinWait = new SpinWait();

                //wait for the router to become available
                while (@this.Hub.Router == null)
                    spinWait.SpinOnce();

                await @this.DeepScanAsync().FastPath();
            }, this).AsTask();
        }

        /// <summary>
        /// Add Robotics to the start
        /// </summary>
        /// <typeparam name="TBoot"></typeparam>
        /// <param name="bootFunc">Bootstrap function</param>
        /// <param name="bootData">Bootstrap context</param>
        /// <param name="customScheduler">custom scheduler</param>
        /// <returns><see cref="ValueTask"/></returns>
        public override async ValueTask StartAsync<TBoot>(Func<TBoot, ValueTask> bootFunc = null, TBoot bootData = default, TaskScheduler customScheduler = null)
        {
            if (!ZeroDrone)
                await ZeroAsync(RoboAsync, this, TaskCreationOptions.LongRunning).FastPath();

            await base.StartAsync(static async state =>
            {
                var (@this, bootFunc, bootData) = state;
                if(bootFunc != null)
                    await bootFunc(bootData).FastPath();

                await @this.StartHubAsync(@this._udpPrefetch, @this._udpConcurrencyLevel).FastPath();

            }, (this, bootFunc, bootData)).FastPath();
        }

        /// <summary>
        /// Robotics
        /// </summary>
        /// <param name="this"></param>
        /// <returns></returns>
        private static async ValueTask RoboAsync(CcCollective @this)
        {
            while (!@this.Zeroed())
            {
                try
                {
                    var ts = Environment.TickCount;
                    var delay = (@this.EgressCount < @this.parm_max_outbound ? @this.parm_futile_timeout_ms/1000 : @this.parm_mean_pat_delay_s) * 1000;

                    await Task.Delay(TimeSpan.FromMilliseconds(@this._random.Next(delay) + delay / 2), @this.AsyncTasks.Token);

                    if (@this.Zeroed())
                        break;

                    @this._logger.Trace($"Robo - {TimeSpan.FromMilliseconds(ts.ElapsedMs())}, {@this.Description}");
                    
                    var force = false;
                    if (@this.Hub.Neighbors.Count <= 1 || @this.TotalConnections == 0)
                    {
                        //restart useless hubs
                        if (@this.UpTime.ElapsedMsToSec() > @this.parm_mean_pat_delay_s)
                            await @this.StartHubAsync(@this.Hub.PreFetch, @this.Hub.ZeroConcurrencyLevel).FastPath();

                        force = true;
                    }

                    if (@this.EgressCount < @this.parm_max_outbound || @this.IngressCount == 0) 
                        await @this.DeepScanAsync(force).FastPath();
                }
                catch when(@this.Zeroed()){}
                catch (Exception e) when (!@this.Zeroed())
                {
                    @this._logger.Error(e, "Error while scanning DMZ!");
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
            DupChecker = null;
            DupSyncRoot = null;
            _logger = null;
            _autoPeering = null;
            _autoPeeringTask = default;
            //CcId = null;
            Services = null;
            _badSigResponse = null;
            _gossipAddress = null;
            _peerAddress = null;
            //_poisson = null;
            _random = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();

            var autoPeeringDesc = _autoPeering.Description;
            await _autoPeering.DisposeAsync(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();

//#pragma warning disable VSTHRD103 // Call async methods when in an async method
//            if (!_autoPeeringTask.Wait(TimeSpan.FromSeconds(parm_hub_teardown_timeout_s)))
//            {
//                _logger.Warn(_autoPeeringTask.Exception,$"{nameof(CcCollective)}.{nameof(ZeroManagedAsync)}: {nameof(_autoPeeringTask)} exit slow..., {autoPeeringDesc}");
//            }
//#pragma warning restore VSTHRD103 // Call async methods when in an async method

            await DupSyncRoot.DisposeAsync(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();
            await DupHeap.ZeroManagedAsync<object>().FastPath();
            DupChecker.Clear();

            Services?.CcRecord?.Endpoints?.Clear();
            try
            {
                var id = CcId.IdString();
                if (!string.IsNullOrEmpty(id))
                {
                    if (!ZeroDrone && AutoPeeringEventService.Operational)
                        AutoPeeringEventService.AddEvent(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.RemoveCollective,
                            Collective = new Collective()
                            {
                                Id = id
                            }
                        });
                }
            }
            catch
            {
                // ignored
            }
        }

        /// <summary>
        /// Primes for DisposeAsync
        /// </summary>
        /// <returns>The task</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ZeroPrime()
        {
            base.ZeroPrime();
            _autoPeering.ZeroPrime();
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
        
        private IoNodeAddress _gossipAddress;
        private IoNodeAddress _peerAddress;

        Random _random = new((int)DateTime.Now.Ticks);
        public IoZeroSemaphoreSlim DupSyncRoot { get; protected set; }
        public ConcurrentDictionary<long, List<string>> DupChecker { get; private set; } = new();
        public IoHeap<List<string>, CcCollective> DupHeap { get; protected set; }
        private readonly int _dupPoolFpsTarget;
        public int DupPoolFpsTarget => _dupPoolFpsTarget;

        private long _eventCounter;
        public long EventCount => Interlocked.Read(ref _eventCounter);

        /// <summary>
        /// The node id
        /// </summary>
        public CcDesignation CcId { get; protected set; }

        /// <summary>
        /// Bootstrap
        /// </summary>
        private List<IoNodeAddress> BootstrapAddress { get; }

        /// <summary>
        /// Reachable from NAT
        /// </summary>
        public IoNodeAddress ExtAddress { get; }

        /// <summary>
        /// Number of inbound neighbors
        /// </summary>
        public int IngressCount;

        /// <summary>
        /// Number of outbound neighbors
        /// </summary>
        public int EgressCount;

        /// <summary>
        /// The services this node supports
        /// </summary>
        public CcService Services { get; set; } = new CcService();

        /// <summary>
        /// The autopeering task handler
        /// </summary>
        private Task _autoPeeringTask;
        private readonly int _futileRequestSize;
        private readonly int _futileResponseSize;
        private readonly int _futileRejectSize;
        private readonly int _fuseBufSize;
        private ByteString _badSigResponse = ByteString.CopyFrom(new byte[] { 9 });
        public long Testing;

        /// <summary>
        /// Experimental support for detection of tunneled UDP connections (WSL)
        /// </summary>
        [IoParameter]
        public bool UdpTunnelSupport = false;

        /// <summary>
        /// Maximum size of a futile message
        /// </summary>
        [IoParameter]
        public int parm_max_handshake_bytes = 256;

        /// <summary>
        /// Timeout for futile messages
        /// </summary>
        [IoParameter]
        public int parm_futile_timeout_ms = 1000;
        
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
        /// Max inbound neighbors
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_inbound = 5;

        /// <summary>
        /// Max outbound neighbors
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_outbound = 3;

        /// <summary>
        /// Max drones
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_drone = 8;

        /// <summary>
        /// Max adjuncts
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_adjunct = 10;

        /// <summary>
        /// Protocol version
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_version = 1;

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

#if DEBUG
        public int parm_mean_pat_delay_s = 60 * 3;
#else
        public int parm_mean_pat_delay_s = 60 * 10;
#endif

        /// <summary>
        /// Max adjuncts
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_hub_teardown_timeout_s = 60;


        /// <summary>
        /// Maximum clients allowed
        /// </summary>
        public int MaxDrones => parm_max_drone;

        /// <summary>
        /// Maximum number of allowed drones
        /// </summary>
        public int MaxAdjuncts => parm_max_adjunct;

        /// <summary>
        /// Spawn the node listeners
        /// </summary>
        /// <param name="handshake"></param>
        /// <param name="context"></param>
        /// <param name="bootFunc"></param>
        /// <param name="bootData"></param>
        /// <returns>ValueTask</returns>
        protected override ValueTask BlockOnListenerAsync<T, TContext>(Func<IoNeighbor<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>, T, ValueTask<bool>> handshake = null, T context = default, Func<TContext, ValueTask> bootFunc = null, TContext bootData = default)=>
            base.BlockOnListenerAsync(ZeroAcceptConAsync,this, bootFunc, bootData);

        private static async ValueTask<bool> ZeroAcceptConAsync(IoNeighbor<CcProtocMessage<CcWhisperMsg, CcGossipBatch>> drone, CcCollective collective)
        {
            if (drone == null || collective.Zeroed())
                return false;

            var success = false;
            var ccDrone = (CcDrone)drone;

            //limit connects
            try
            {
                if (collective.Zeroed() || collective.IngressCount >= collective.parm_max_inbound)
                    return false;

                //Handshake
                if (await collective.FutileAsync(ccDrone).FastPath())
                {
                    if (ccDrone.Zeroed())
                    {
                        collective._logger.Debug($"+|{drone.Description}");
                        return false;
                    }

                    ccDrone.Adjunct.WasAttached = true;

                    if (!collective.ZeroDrone && AutoPeeringEventService.Operational)
                        AutoPeeringEventService.AddEvent(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.AddDrone,
                            Drone = new Drone
                            {
                                CollectiveId = collective.Hub.Designation.IdString(),
                                Id = ccDrone.Adjunct.Designation.IdString(),
                                Direction = ccDrone.Adjunct.Direction.ToString()
                            }
                        });
                    
                    //ACCEPT
                    collective._logger.Info($"+ {drone.Description}");
                    return success = true;
                }
                else
                {
                    collective._logger.Debug($"+|{drone.Description}");
                }
                return false;
            }
            finally
            {
                if (!success && ccDrone.Adjunct != null)
                    await ccDrone.Adjunct.DeFuseAsync();
            }
        }

        /// <summary>
        /// Sends a message to a peer
        /// </summary>/// <param name="drone">The destination</param>
        /// <param name="msg">The message</param>
        /// <param name="type"></param>
        /// <param name="timeout"></param>
        /// <returns>The number of bytes sent</returns>
        private async ValueTask<int> SendMessageAsync(CcDrone drone, ByteString msg, string type, int timeout = 0)
        {
            var responsePacket = new chroniton
            {
                Data = msg,
                PublicKey = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcId.PublicKey)),
                Type = (int)CcDiscoveries.MessageTypes.Handshake
            };

            responsePacket.Signature = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcId.Sign(responsePacket.Data.Memory.AsArray(), 0, responsePacket.Data.Length)));

            var protocolRaw = responsePacket.ToByteArray();

            int sent = 0;
            if (!drone.Zeroed() && (sent = await drone.MessageService.IoNetSocket.SendAsync(protocolRaw, 0, protocolRaw.Length, timeout: timeout).FastPath()) == protocolRaw.Length)
            {
                _logger.Trace($"~/> {type}({sent}): {drone.MessageService.IoNetSocket.LocalAddress} ~> {drone.MessageService.IoNetSocket.RemoteAddress} ({Enum.GetName(typeof(CcDiscoveries.MessageTypes), responsePacket.Type)})");
                return msg.Length;
            }

            if(!drone.Zeroed())
                _logger.Error($"~/> {type}({sent}):  {drone.MessageService.IoNetSocket.LocalAddress} ~> {drone.MessageService.IoNetSocket.RemoteAddress}");
            
            return 0;
        }

        /// <summary>
        /// Futile request
        /// </summary>
        /// <param name="drone">The drone</param>
        /// <returns>True if accepted, false otherwise</returns>
        private async ValueTask<bool> FutileAsync(CcDrone drone)
        {
            
            var success = false;
            var bytesRead = 0;

            if (Zeroed()) return false;

            int ts = Environment.TickCount;
            try
            {                
                var futileBuffer = new byte[_fuseBufSize];
                var ioNetSocket = drone.MessageService.IoNetSocket;

                //inbound
                if (drone.MessageService?.IoNetSocket?.IsIngress??false)
                {
                    var verified = false;
                    
                    //read from the socket
                    int localRead;
                    do
                    {
                        bytesRead += localRead = await ioNetSocket.ReceiveAsync(futileBuffer, bytesRead, _futileRequestSize - bytesRead, timeout: parm_futile_timeout_ms).FastPath();
                    } while (bytesRead < _futileRequestSize && localRead > 0 && !Zeroed());

                    if ((bytesRead == 0 || bytesRead < _futileRequestSize) && ioNetSocket.IsConnected())
                    {
                        if(bytesRead > 0)
                            _logger.Error($"Handshake [FAILED]; read = {bytesRead}/{_futileRequestSize}, w = {ts.ElapsedMs()}ms, want = {parm_futile_timeout_ms}ms, socket = {ioNetSocket.Description}");
                        return false;
                    }
#if TRACE
                    else
                    {
                        _logger.Trace($"{nameof(CcFutileRequest)}: size = {bytesRead}, socket = {ioNetSocket.Description}");
                    }
#endif
                    
                    //parse a packet
                    var packet = chroniton.Parser.ParseFrom(futileBuffer, 0, bytesRead);

                    if (packet != null && packet.Data != null && packet.Data.Length > 0)
                    {
                        ////verify the public key
                        CcDesignation id = null;
                        if (packet.PublicKey.Length <= 0 || !Hub.Neighbors.TryGetValue(
                                (id = CcDesignation.FromPubKey(packet.PublicKey.Memory)).IdString(), out var adjunct))
                        {
                            _logger.Error(
                                $"bad public key: id = `{id?.IdString() ?? "null"}' not found in {Adjuncts.Count} adjuncts, {Description}");
                            return false;
                        }

                        drone.Adjunct = (CcAdjunct)adjunct;
                        var prevState = drone.Adjunct.CurrentState;
                        var stateIsValid = drone.Adjunct.CompareAndEnterState(CcAdjunct.AdjunctState.Connecting,
                                               CcAdjunct.AdjunctState.Fusing, overrideHung: parm_futile_timeout_ms) ==
                                           CcAdjunct.AdjunctState.Fusing;

                        if (!stateIsValid)
                        {
                            stateIsValid = drone.Adjunct.CompareAndEnterState(CcAdjunct.AdjunctState.Connecting,
                                               CcAdjunct.AdjunctState.Verified, overrideHung: parm_futile_timeout_ms) ==
                                           CcAdjunct.AdjunctState.Verified;
                            if (!stateIsValid)
                            {
                                if (drone.Adjunct.CurrentState.EnterTime.ElapsedMs() > parm_futile_timeout_ms)
                                {
                                    _logger.Warn($"{nameof(FutileAsync)} - {Description}: Invalid state, {prevState}, age = {prevState.EnterTime.ElapsedMs()}ms. Wanted {nameof(CcAdjunct.AdjunctState.Fusing)} - [RACE OK!]");
                                }
                                else
                                {
                                    _logger.Error($"{nameof(FutileAsync)} - {Description}: Invalid state, {prevState}, age = {prevState.EnterTime.ElapsedMs()}ms. Wanted {nameof(CcAdjunct.AdjunctState.Fusing)} - [RACE OK!]");
                                    return false;
                                }
                            }
                        }
                        
                        //verify the signature
                        var packetData = packet.Data.Memory.AsArray();
                        if (packet.Signature != null || packet.Signature!.Length != 0)
                        {
                            verified = CcDesignation.Verify(packetData, 0,
                                packetData.Length, packet.PublicKey.Memory.AsArray(), 0,
                                packet!.Signature!.Memory.AsArray(), 0);
                        }
                        
                        //Don't process unsigned or unknown messages
                        if (!verified)
                            return false;

                        //process futile request 
                        var ccFutileRequest = CcFutileRequest.Parser.ParseFrom(packet.Data);
                        var won = false;
                        if (ccFutileRequest != null)
                        {
                            //reject old futile requests
                            if (ccFutileRequest.Timestamp.ElapsedMs() > parm_futile_timeout_ms)
                            {
                                _logger.Error($"Rejected old futile request from {ioNetSocket.Key} - d = {ccFutileRequest.Timestamp.ElapsedMs()}ms, > {parm_futile_timeout_ms}");
                                return false;
                            }

                            //reject invalid protocols
                            if (ccFutileRequest.Protocol != parm_version)
                            {
                                _logger.Error(
                                    $"Invalid protocol version from  {ioNetSocket.Key} - got {ccFutileRequest.Protocol}, wants {parm_version}");
                                return false;
                            }

                            //reject requests to invalid ext ip
                            //if (CcFutileRequest.To != ((CcNeighbor)neighbor)?.DebugAddress?.IpPort)
                            //{
                            //    _logger.Error($"Invalid futile received from {socket.Key} - got {CcFutileRequest.To}, wants {((CcNeighbor)neighbor)?.DebugAddress.IpPort}");
                            //    return false;
                            //}

                            //race for connection
                            won = ConnectForTheWin(IIoSource.Heading.Ingress, drone, packet, (IPEndPoint)ioNetSocket.NativeSocket.RemoteEndPoint);
                            _logger.Trace($"{nameof(CcFutileRequest)}: won = {won}, read = {bytesRead}, {drone.IoSource?.Key}");

                            //send response
                            var futileResponse = new CcFutileResponse
                            {
                                ReqHash = won? UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray()))): _badSigResponse
                            };
                            
                            var futileResponseBuf = futileResponse.ToByteString();

                            ts = Environment.TickCount;

                            var sent = await SendMessageAsync(drone, futileResponseBuf, nameof(CcFutileResponse), parm_futile_timeout_ms).FastPath();
                            if (sent == 0)
                            {
                                _logger.Trace($"{nameof(futileResponse)}: Send FAILED!; time = {ts.ElapsedMs()} ms, {ioNetSocket.Description}");
                                return false;
                            }
                        }
                        //Race
                        //return await ConnectForTheWinAsync(CcNeighbor.Kind.Inbound, peer, packet,
                        //        (IPEndPoint)ioNetSocket.NativeSocket.RemoteEndPoint)
                        //    .FastPath().ConfigureAwait(ZC);
                        return success = !Zeroed() && drone.Adjunct != null && !drone.Adjunct.Zeroed() && won && drone.Adjunct.Direction == IIoSource.Heading.Ingress && drone.Source.IsOperational();
                    }
                }
                //-----------------------------------------------------//
                else if (drone.MessageService?.IoNetSocket?.IsEgress??false) //Outbound
                {
                    var ccFutileRequest = new CcFutileRequest
                    {
                        Protocol = parm_version,
                        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    };
                    
                    var futileRequestBuf = ccFutileRequest.ToByteString();

                    ts = Environment.TickCount;
                    var sent = await SendMessageAsync(drone, futileRequestBuf, nameof(CcFutileRequest)).FastPath();
                    if (sent > 0)
                    {
                        _logger.Trace($"h/> {nameof(SendMessageAsync)}({sent}): dest = {ioNetSocket.Description}");
                    }
                    else
                    {
                        _logger.Trace(
                            $"Failed to send egress futile challange, socket = {ioNetSocket.Description}");
                        return false;
                    }

                    int localRead;
                    var expectedChunk = _futileRejectSize;
                    var chunkSize = expectedChunk;
                    ts = Environment.TickCount;
                    do
                    {
                        bytesRead += localRead = await ioNetSocket.ReceiveAsync(futileBuffer, bytesRead, chunkSize, timeout: parm_futile_timeout_ms).FastPath();
                        if (chunkSize == _futileRejectSize)
                        {
                            expectedChunk = _futileResponseSize;
                            chunkSize = expectedChunk - localRead;
                        }
                        else
                            chunkSize -= localRead;
                    } while (localRead > 0 && bytesRead < expectedChunk && ioNetSocket.NativeSocket.Available > 0 && !Zeroed());

                    if (bytesRead == 0 || bytesRead < _futileRejectSize)
                    {
                        _logger.Trace($"<\\h {nameof(ioNetSocket.ReceiveAsync)}({bytesRead}): Failed to read egress futile challange response, waited = {ts.ElapsedMs()}ms, remote = {ioNetSocket.RemoteAddress}, z = {Zeroed()}");
                        return false;
                    }
                    
                    _logger.Trace($"<\\h {nameof(ioNetSocket.ReceiveAsync)}({bytesRead}): src = {ioNetSocket.RemoteAddress};");
                    
                    var verified = false;
                    
                    var packet = chroniton.Parser.ParseFrom(futileBuffer, 0, bytesRead);
                    
                    if (packet != null && packet.Data != null && packet.Data.Length > 0)
                    {

                        var packetData = packet.Data.Memory.AsArray();
                    
                        //verify signature
                        if (packet.Signature != null || packet.Signature!.Length != 0)
                        {
                            verified = CcDesignation.Verify(packetData, 0,
                                packetData.Length, packet.PublicKey.Memory.AsArray(), 0,
                                packet.Signature.Memory.AsArray(), 0);
                        }
                    
                        //Don't process unsigned or unknown messages
                        if (!verified)
                            return false;
                        
                        _logger.Trace($"{nameof(CcFuseRequest)}: [signed], from = egress, read = {bytesRead}, {drone.IoSource.Key}");
                        //race for connection
                        var won = ConnectForTheWin(IIoSource.Heading.Egress, drone, packet, drone.Adjunct.RemoteAddress.IpEndPoint);
                        if (!won)
                            return false;
                        //validate futile response
                        var futileResponse = CcFutileResponse.Parser.ParseFrom(packet.Data);
                        if (futileResponse != null)
                        {
                            if (futileResponse.ReqHash.Length == 32)
                            {
                                if (!CcDesignation.Sha256
                                    .ComputeHash(futileRequestBuf.Memory.AsArray())
                                    .ArrayEqual(futileResponse.ReqHash.Memory))
                                {
                                    _logger.Error($"{nameof(ConnectForTheWin)}: Invalid futile response! Closing {ioNetSocket.Key}");
                                    return false;
                                }
                            }
                            else
                            {
                                return false;
                            }
                        }

                        return success = !Zeroed() && drone.Adjunct != null && !drone.Adjunct.Zeroed() && drone.Adjunct?.Direction == IIoSource.Heading.Egress && drone.Source.IsOperational();
                    }
                }
            }            
            catch (Exception) when (Zeroed() || drone.Zeroed() || ts.ElapsedMs() >= parm_futile_timeout_ms) { }
            catch (Exception e) when(!Zeroed() && !drone.Zeroed())
            {
                _logger.Error(e, $"Futile request (size = {bytesRead}/{_futileRequestSize}/{_futileResponseSize}/{_futileRejectSize}) for {Description} failed with:");
            }
            finally
            {
                if (success)
                {
                    if (drone.MessageService.IoNetSocket.IsEgress)
                    {
                        await drone.ZeroSubAsync(static (_,@this) =>
                        {
                            Interlocked.Decrement(ref @this.EgressCount);
                            return new ValueTask<bool>(true);
                        }, this).FastPath();                        
                    }
                    else if (drone.MessageService.IoNetSocket.IsIngress)
                    {
                        await drone.ZeroSubAsync(static (_, @this) =>
                        {
                            Interlocked.Decrement(ref @this.IngressCount);
                            return new ValueTask<bool>(true);
                        }, this).FastPath();                        
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
        /// <param name="packet">The futile packet</param>
        /// <param name="remoteEp">The remote</param>
        /// <returns>True if it won, false otherwise</returns>
        private bool ConnectForTheWin(IIoSource.Heading direction, CcDrone drone, chroniton packet, IPEndPoint remoteEp)
        {
            if(Equals(_gossipAddress.IpEndPoint, remoteEp) || packet.PublicKey.Memory.ArrayEqual(CcId.PublicKey))
                throw new ApplicationException($"Connection inception from {remoteEp} (pk = {CcDesignation.FromPubKey(packet.PublicKey.Memory).IdString()}) on {_gossipAddress.IpEndPoint}: {Description}");

            var adjunct = drone.Adjunct;
            if (adjunct == null)
            {
                Debug.Assert(direction == IIoSource.Heading.Ingress);
                var id = CcDesignation.MakeKey(packet.PublicKey.Memory.AsArray());

                if (!_autoPeering.Neighbors.TryGetValue(id, out var existingAdjunct))
                {
                    _logger.Error($"Adjunct [{id}] not found, dropping {direction} connection to {remoteEp}");
                    return false;
                }

                drone.Adjunct = adjunct = (CcAdjunct)existingAdjunct;

                var stateIsValid = adjunct.CompareAndEnterState(CcAdjunct.AdjunctState.Connecting, CcAdjunct.AdjunctState.Fusing) == CcAdjunct.AdjunctState.Fusing;
                if (!stateIsValid)
                {
                    stateIsValid = adjunct.CompareAndEnterState(CcAdjunct.AdjunctState.Connecting, CcAdjunct.AdjunctState.Verified) == CcAdjunct.AdjunctState.Verified;
                    if (!stateIsValid)
                    {
                        if (adjunct.CurrentState.Value != CcAdjunct.AdjunctState.Connected)
                            _logger.Warn($"{nameof(ConnectForTheWin)} - {Description}: Invalid state, {adjunct.CurrentState.Value}, age = {adjunct.CurrentState.EnterTime.ElapsedMs()}ms. Wanted {nameof(CcAdjunct.AdjunctState.Fusing)} - [RACE OK!]");
                        return false;
                    }
                }
            }

            if (drone.AttachViaAdjunct(direction)) return true;

            adjunct.CompareAndEnterState(CcAdjunct.AdjunctState.Verified, CcAdjunct.AdjunctState.Connecting);

            _logger.Trace($"{direction} futile request [LOST] {CcDesignation.FromPubKey(packet.PublicKey.Memory.AsArray())} - {remoteEp}: s = {drone.Adjunct.State}, a = {drone.Adjunct.Assimilating}, p = {drone.Adjunct.IsDroneConnected}, pa = {drone.Adjunct.IsDroneAttached}, ut = {drone.Adjunct.UpTime.ElapsedUtcMs()}ms");
            return false;
        }

        /// <summary>
        /// Opens an <see cref="IIoSource.Heading.Egress"/> connection to a gossip peer
        /// </summary>
        /// <param name="adjunct">The verified neighbor associated with this connection</param>
        public async ValueTask<bool> ConnectToDroneAsync(CcAdjunct adjunct)
        {
            //Validate
            if (
                    !Zeroed() && 
                    adjunct.Assimilating &&
                    !adjunct.IsDroneConnected &&
                    adjunct.State == CcAdjunct.AdjunctState.Connecting &&
                    EgressCount < parm_max_outbound &&
                    _currentOutboundConnectionAttempts < MaxAsyncConnectionAttempts
                )
            {
                try
                {
                    Interlocked.Increment(ref _currentOutboundConnectionAttempts);

                    var drone = await ConnectAsync(ZeroAcceptConAsync,this, IoNodeAddress.CreateFromEndpoint("tcp", adjunct.RemoteAddress.IpEndPoint) , adjunct, false, parm_futile_timeout_ms).FastPath();
                    if (Zeroed() || drone == null || ((CcDrone)drone).Adjunct.Zeroed())
                    {
                        if (drone != null) await drone.DisposeAsync(this, $"{nameof(ConnectAsync)} was not successful [OK]").FastPath();
                        _logger.Trace($"{nameof(ConnectToDroneAsync)}: [ABORTED], {adjunct.Description}, {adjunct.MetaDesc}");
                        return false;
                    }

                    return true;
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

        //private static readonly double _lambda = 20;
        private const int MaxAsyncConnectionAttempts = 16;

        private int _currentOutboundConnectionAttempts;

        //private Poisson _poisson = new(_lambda, new Random(DateTimeOffset.Now.Ticks.GetHashCode() * DateTimeOffset.Now.Ticks.GetHashCode()));

        private readonly bool _zeroDrone;
        public bool ZeroDrone => _zeroDrone;

        /// <summary>
        /// Whether the drone complete the bootstrap process
        /// </summary>
        public bool Online { get; private set; }

        public long MaxReq = 0;

        /// <summary>
        /// Boots the node
        /// </summary>
        public async ValueTask<bool> BootAsync(long v = 1)
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
            //        await ioNeighbor.EmitTestGossipMsgAsync(v);
            //        return true;
            //    }
            //    catch when(Zeroed()){}
            //    catch (Exception e)when(!Zeroed())
            //    {
            //        _logger.Debug(e,Description);
            //    }
            //}


            if (Neighbors.Count > 0)
            {
                await WhisperingDrones[_random.Next(0, WhisperingDrones.Count)].EmitTestGossipMsgAsync(v).FastPath();
                return true;
            }
            
            return false;
        }

        private readonly int _udpPrefetch;
        private readonly int _udpConcurrencyLevel;

        /// <summary>
        /// Bootstrap node
        /// </summary>
        /// <returns></returns>
        public async ValueTask DeepScanAsync(bool ensureFuse = false)
        {
            try
            {
                if (Zeroed() || Hub?.Router == null)
                    return;

                var foundVector = false;
                foreach (var vector in Hub.Neighbors.Values.Where(n=>((CcAdjunct)n).State >= CcAdjunct.AdjunctState.Verified).OrderByDescending(n=>((CcAdjunct)n).OpenSlots))
                {
                    var adjunct = (CcAdjunct)vector;

                    if(ensureFuse)
                        adjunct.EnsureFuseChecks();

                    if (!await adjunct.ScanAsync().FastPath())
                    {
                        if (adjunct.Zeroed())
                            await adjunct.DisposeAsync(this, "Zombie on scan!");
                    }
                    else
                    {
                        foundVector = true;
                    }

                    await Task.Delay(((CcAdjunct)vector).parm_max_network_latency_ms>>1, AsyncTasks.Token);
                }

                if(foundVector)
                    return;

                if (BootstrapAddress != null)
                {
                    foreach (var ioNodeAddress in BootstrapAddress)
                    {
                        if (!ioNodeAddress.Equals(_peerAddress))
                        {
                            if (Hub.Neighbors.Values.Any(a => a.IoSource.Key.Contains(ioNodeAddress.Key)))
                                continue;

                            try
                            {
#if TRACE
                                if(Hub.Neighbors.Count == 1)
                                    _logger.Info($"{Description} Bootstrapping from {ioNodeAddress}");                       
#endif

                                if (!await Hub.Router.ProbeAsync("SYN-DMZ", ioNodeAddress).FastPath())
                                {
                                    if (!Hub.Router.Zeroed())
                                        _logger.Trace($"{nameof(DeepScanAsync)}: Unable to boostrap {Description} from {ioNodeAddress}");
                                }

                                if (ZeroDrone && !Online)
                                {
                                    Online = true;
                                    _logger.Warn($"Queen brought Online, {Description}");
                                }
                                    
#if TRACE
                                _logger.Trace($"Boostrap complete! {Description}");
#endif
                            }
                            catch when(Zeroed()){}
                            catch (Exception e)when (!Zeroed())
                            {
                                _logger.Error(e, $"{nameof(DeepScanAsync)}: {Description}");
                            }
                        }
                    }
                }
            }
            catch when(Zeroed()){}
            catch (Exception e) when(Zeroed())
            {
                _logger.Error(e,$"{nameof(DeepScanAsync)}:");
            }
        }

        public void Emit()
        {
            //Emit collective event
            if (!ZeroDrone && AutoPeeringEventService.Operational)
                AutoPeeringEventService.AddEvent(new AutoPeerEvent
                {
                    EventType = AutoPeerEventType.AddCollective,
                    Collective = new Collective
                    {
                        Id = CcId.IdString(),
                        Ip = ExtAddress.Url,
                        Os = new OsInfo
                        {
                            Kind = Environment.OSVersion.Platform switch
                            {
                                PlatformID.Win32NT => OSKind.Windows,
                                PlatformID.MacOSX => OSKind.Osx,
                                PlatformID.Unix => OSKind.Unix,
                                PlatformID.Win32Windows => OSKind.Windows,
                                PlatformID.WinCE => OSKind.Windows, 
                                PlatformID.Xbox => OSKind.Xbox,
                                _ => throw new ArgumentOutOfRangeException($"{Environment.OSVersion.Platform}")
                            },
                            UpSince = Environment.TickCount,
                            Version = Environment.OSVersion.ToString(),
                            Processors = Environment.ProcessorCount,
                            Memory = Environment.SystemPageSize,
                            X64 = Environment.Is64BitOperatingSystem,
                        },
                        Rt = new RuntimeInfo
                        {
                            Machine = Environment.MachineName,
                            Version = Environment.Version.ToString(),
                            Memory = GC.GetTotalMemory(false)
                        }
                    },
                });
        }

        public void PrintNeighborhood()
        {
            _logger.Info(Description);
            _logger.Info(Hub.Description);
            foreach (var adjunct in Adjuncts)
            {
                _logger.Info(adjunct.Description);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncEventCounter()
        {
            Interlocked.Increment(ref _eventCounter);
        }

        public void ClearEventCounter()
        {
            Interlocked.Exchange(ref _eventCounter, 0);
        }

        public void ClearDupBuf()
        {
            if (DupChecker.Count > DupHeap.Capacity * 4 / 5)
            {
                foreach (var mId in DupChecker.Keys)
                {
                    if (DupChecker.TryRemove(mId, out var del))
                    {
                        del.Clear();
                        DupHeap.Return(del);
                    }
                }
            }
        }

    }
}
