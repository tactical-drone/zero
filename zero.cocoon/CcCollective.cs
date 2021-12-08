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
using MathNet.Numerics.Distributions;
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
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore;
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
            int tcpPrefetch, int udpConcurrencyLevel, int tpcConcurrencyLevel, bool zeroDrone)
            : base(gossipAddress, static (node, ioNetClient, extraData) => new CcDrone((CcCollective)node, (CcAdjunct)extraData, ioNetClient), tcpPrefetch, tpcConcurrencyLevel, 16 * 2) //TODO config
        {
            _logger = LogManager.GetCurrentClassLogger();
            _gossipAddress = gossipAddress;
            _peerAddress = peerAddress;
            BootstrapAddress = bootstrap;
            ExtAddress = extAddress; //this must be the external or NAT address.
            CcId = ccDesignation;

            _zeroDrone = zeroDrone;
            if (_zeroDrone)
            {
                _zeroDrone = true;
                parm_max_drone = 0;
                parm_max_adjunct = 64;
                udpConcurrencyLevel = parm_max_adjunct * 2;
                udpPrefetch = udpConcurrencyLevel / 2;
            }

            Services.CcRecord.Endpoints.TryAdd(CcService.Keys.peering, _peerAddress);
            Services.CcRecord.Endpoints.TryAdd(CcService.Keys.gossip, _gossipAddress);
            Services.CcRecord.Endpoints.TryAdd(CcService.Keys.fpc, fpcAddress);

            _autoPeering =  new CcHub(this, _peerAddress,static (node, client, extraData) => new CcAdjunct((CcHub) node, client, extraData), udpPrefetch, udpConcurrencyLevel);
            _autoPeering.ZeroHiveAsync(this).AsTask().GetAwaiter().GetResult();
            
            DupSyncRoot = new IoZeroSemaphoreSlim(AsyncTasks,  $"Dup checker for {ccDesignation.IdString()}", maxBlockers: Math.Max(MaxDrones * tpcConcurrencyLevel,1), initialCount: 1);
            DupSyncRoot.ZeroHiveAsync(this).AsTask().GetAwaiter().GetResult();
            
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

            futileResponse.ReqHash = ByteString.CopyFrom(new byte[]{9});
            protocolMsg.Data = futileResponse.ToByteString();

            _futileRejectSize = protocolMsg.CalculateSize();

            _fuseBufSize = Math.Max(_futileResponseSize, _futileRequestSize);

            if(_fuseBufSize > parm_max_handshake_bytes)
                throw new ApplicationException($"{nameof(_fuseBufSize)} > {parm_max_handshake_bytes}");

            _dupPoolFpsTarget = 1000 * 2;  
            DupHeap = new IoHeap<IoHashCodes, CcCollective>($"{nameof(DupHeap)}: {Description}", _dupPoolFpsTarget)
            {
                Make = static (o, s) => new IoHashCodes(null, s.MaxDrones * 2, true),
                Prep = (popped, endpoint) =>
                {
                    popped.Add(endpoint.GetHashCode());
                },
                Context = this
            };

            //ensure robotics
            if(!ZeroDrone)
                ZeroAsync(RoboAsync,this,TaskCreationOptions.DenyChildAttach | TaskCreationOptions.LongRunning).AsTask().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Robotics
        /// </summary>
        /// <param name="this"></param>
        /// <returns></returns>
        private async ValueTask RoboAsync(CcCollective @this)
        {
            var secondsSinceEnsured = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            await ZeroAsync(static async @this =>
            {
                while (!@this.Zeroed())
                {
                    try
                    {
                        if (@this.Hub.Neighbors.Count <= 1)
                        {
                            await Task.Delay(@this._random.Next(@this.parm_mean_pat_delay_s/2) + @this.parm_mean_pat_delay_s / 4, @this.AsyncTasks.Token).ConfigureAwait(false);
                        }
                        else
                        {
                            await Task.Delay(@this._random.Next(@this.parm_mean_pat_delay_s) + @this.parm_mean_pat_delay_s / 2, @this.AsyncTasks.Token).ConfigureAwait(false);
                        }

                        if (@this.Neighbors.Count < @this.parm_max_outbound) 
                            await @this.DeepScanAsync().FastPath().ConfigureAwait(@this.Zc);
                    }
                    catch when(@this.Zeroed()){}
                    catch (Exception e)
                    {
                        @this._logger.Error(e, "Error while scanning DMZ!");
                    }
                }
                    
            }, this, TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(Zc);

            

            //while running
            while (!@this.Zeroed())
            {
                //periodically
                await Task.Delay((_random.Next(@this.parm_mean_pat_delay_s / 5) + @this.parm_mean_pat_delay_s / 4) * 1000, @this.AsyncTasks.Token).ConfigureAwait(Zc);
                
                if (@this.Zeroed())
                    break;

                try
                {
                    var totalConnections = @this.TotalConnections;
                    double scanRatio = 1;

                    //Attempt to peer with standbys
                    if (totalConnections < @this.MaxDrones * scanRatio &&
                        secondsSinceEnsured.Elapsed() >= @this.parm_mean_pat_delay_s / 4) //delta q
                    {
                        @this._logger.Trace($"Scanning {@this._autoPeering.Neighbors.Values.Count}, {@this.Description}");

                        var c = 0;
                        //Send peer requests
                        foreach (var adjunct in @this._autoPeering.Neighbors.Values.Where(n =>
                                ((CcAdjunct)n).State is >= CcAdjunct.AdjunctState.Verified) //quick slot
                            .OrderBy(n => ((CcAdjunct)n).Priority)
                            .ThenBy(n => ((CcAdjunct)n).Uptime.ElapsedMs()))
                        {
                            await ((CcAdjunct)adjunct).ProbeAsync("SYN").FastPath().ConfigureAwait(Zc);
                            c++;
                        }

                        ////Scan for peers
                        //if (c == 0)
                        //{
                        //    foreach (var adjunct in @this._autoPeering.Neighbors.Values.Where(n =>
                        //            ((CcAdjunct)n).State >= CcAdjunct.AdjunctState.Verified) //quick slot
                        //        .OrderBy(n => ((CcAdjunct)n).Priority)
                        //        .ThenBy(n => ((CcAdjunct)n).Uptime.ElapsedMs()))
                        //    {
                        //        if (!Zeroed())
                        //            await ((CcAdjunct)adjunct).FuseAsync().FastPath().ConfigureAwait(Zc);
                        //        else
                        //            break;

                        //        c++;
                        //    }
                        //}

                        //if (@this.Neighbors.Count < MaxDrones / 2 && !Zeroed())
                        //{
                        //    //bootstrap if alone
                        //    await @this.DeepScanAsync().FastPath().ConfigureAwait(Zc);
                        //}

                        if (secondsSinceEnsured.Elapsed() > @this.parm_mean_pat_delay_s)
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
            DupChecker = null;
            DupSyncRoot = null;
            _logger = null;
            _autoPeering = null;
            _autoPeeringTask = default;
            CcId = null;
            Services = null;
            _badSigResponse = null;
            _gossipAddress = null;
            _peerAddress = null;
            _poisson = null;
            _random = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);

            _autoPeering.Zero(this);

            var id = Hub?.Router?.Designation?.IdString();
            DupSyncRoot.Zero(this);
            await DupHeap.ZeroManagedAsync<object>().FastPath().ConfigureAwait(false);
            DupChecker.Clear();

            Services?.CcRecord?.Endpoints?.Clear();

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
                // ignored
            }
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
        public ConcurrentDictionary<long, IoHashCodes> DupChecker { get; private set; } = new();
        public IoHeap<IoHashCodes, CcCollective> DupHeap { get; protected set; }
        private readonly int _dupPoolFpsTarget;
        public int DupPoolFPSTarget => _dupPoolFpsTarget;

        private long _eventCounter;
        public long EventCount => Interlocked.Read(ref _eventCounter);

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
        private Task _autoPeeringTask;

        /// <summary>
        /// Max inbound neighbors
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_inbound = 5;

        /// <summary>
        /// Max inbound neighbors
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_outbound = 4;

        /// <summary>
        /// Max adjuncts
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_drone = 9;

        /// <summary>
        /// Max adjuncts
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_adjunct = 9 * 2;

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
        public int parm_mean_pat_delay_s = 60 * 5;
#endif

        /// <summary>
        /// Maximum clients allowed
        /// </summary>
        public int MaxDrones => parm_max_drone;

        /// <summary>
        /// Maximum number of allowed drones
        /// </summary>
        public int MaxAdjuncts => parm_max_adjunct;

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
                    @this._autoPeeringTask = @this._autoPeering.StartAsync(@this.DeepScanAsync).AsTask();
                    await @this._autoPeeringTask.ConfigureAwait(@this.Zc);
                    @this._logger.Warn($"Restarting hub... {@this.Description}");
                }
            }, this, TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(Zc);

            //start node listener
            await base.SpawnListenerAsync(static async (drone,@this) =>
            {
                var success = false;
                //limit connects
                try
                {
                    if (@this.Zeroed() || @this.IngressCount >= @this.parm_max_inbound)
                        return false;

                    //Handshake
                    if (await @this.FutileAsync((CcDrone)drone).FastPath().ConfigureAwait(@this.Zc))
                    {
                        await Task.Delay(@this.parm_futile_timeout_ms, @this.AsyncTasks.Token);
                        
                        if (drone.Zeroed())
                        {
                            @this._logger.Debug($"+|{drone.Description}");
                            return false;
                        }
                    
                        //ACCEPT
                        @this._logger.Info($"+ {drone.Description}");
                        return success = true;
                    }
                    else
                    {
                        @this._logger.Debug($"+|{drone.Description}");
                    }
                    return false;
                }
                finally
                {
                    if (!success)
                    {
                        //await @this.Hub.Router.DeFuseAsync().FastPath().ConfigureAwait(@this.Zc);
                    }
                }
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
            var responsePacket = new chroniton
            {
                Data = msg,
                PublicKey = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcId.PublicKey)),
                Type = (int)CcDiscoveries.MessageTypes.Handshake
            };

            responsePacket.Signature = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcId.Sign(responsePacket.Data.Memory.AsArray(), 0, responsePacket.Data.Length)));

            var protocolRaw = responsePacket.ToByteArray();

            int sent;
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
        private readonly int _futileRequestSize;
        private readonly int _futileResponseSize;
        private readonly int _futileRejectSize;
        private readonly int _fuseBufSize;
        private ByteString _badSigResponse = ByteString.CopyFrom(new byte[] { 9 });
        public long Testing;

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

            try
            {                
                var futileBuffer = new byte[_fuseBufSize];
                var ioNetSocket = drone.IoSource.IoNetSocket;

                //inbound
                if (drone.IoSource.IoNetSocket.IsIngress)
                {
                    var verified = false;
                    
                    _sw.Restart();
                    //read from the socket
                    int localRead;
                    do
                    {
                        bytesRead += localRead = await ioNetSocket
                            .ReadAsync(futileBuffer, bytesRead, _futileRequestSize - bytesRead, timeout:parm_futile_timeout_ms * 2).FastPath()
                            .ConfigureAwait(Zc);
                    } while (bytesRead < _futileRequestSize && localRead > 0 && !Zeroed());

                    if (bytesRead == 0 || bytesRead < _futileRequestSize)
                    {
                        _logger.Trace($"Failed to read inbound futile request, waited = {_sw.ElapsedMilliseconds}ms, socket = {ioNetSocket.Description}");
                        return false;
                    }
                    else
                    {
                        _logger.Trace(
                            $"{nameof(CcFutileRequest)}: size = {bytesRead}, socket = {ioNetSocket.Description}");
                    }
                    
                    //parse a packet
                    var packet = chroniton.Parser.ParseFrom(futileBuffer, 0, bytesRead);
                    
                    if (packet != null && packet.Data != null && packet.Data.Length > 0)
                    {
                        var packetData = packet.Data.Memory.AsArray();
                        
                        //verify the signature
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
                            if (ccFutileRequest.Timestamp.ElapsedMs() > parm_futile_timeout_ms * 2)
                            {
                                _logger.Error($"Rejected old futile request from {ioNetSocket.Key} - d = {ccFutileRequest.Timestamp.ElapsedMs()}ms, > {parm_futile_timeout_ms * 2}");
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
                            won = await ConnectForTheWinAsync(CcAdjunct.Heading.Ingress, drone, packet, (IPEndPoint)ioNetSocket.NativeSocket.RemoteEndPoint).FastPath().ConfigureAwait(Zc);

                            _logger.Trace($"{nameof(CcFutileRequest)}: won = {won}, read = {bytesRead}, {drone.IoSource.Key}");

                            //send response
                            var futileResponse = new CcFutileResponse
                            {
                                ReqHash = won? UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray()))): _badSigResponse
                            };
                            
                            var futileResponseBuf = futileResponse.ToByteString();
                            
                            _sw.Restart();

                            var sent = await SendMessageAsync(drone, futileResponseBuf, nameof(CcFutileResponse),
                                parm_futile_timeout_ms).FastPath().ConfigureAwait(Zc);
                            if (sent == 0)
                            {
                                _logger.Trace($"{nameof(futileResponse)}: Send FAILED! {ioNetSocket.Description}");
                                return false;
                            }
                        }
                        //Race
                        //return await ConnectForTheWinAsync(CcNeighbor.Kind.Inbound, peer, packet,
                        //        (IPEndPoint)ioNetSocket.NativeSocket.RemoteEndPoint)
                        //    .FastPath().ConfigureAwait(ZC);
                        success = !Zeroed() && drone.Adjunct != null && !drone.Adjunct.Zeroed() && won && drone.Adjunct.Direction == CcAdjunct.Heading.Ingress && drone.Source.IsOperational;
                        return success;
                    }
                }
                //-----------------------------------------------------//
                else if (drone.IoSource.IoNetSocket.IsEgress) //Outbound
                {
                    var ccFutileRequest = new CcFutileRequest
                    {
                        Protocol = parm_version,
                        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    };
                    
                    var futileRequestBuf = ccFutileRequest.ToByteString();
                    
                    _sw.Restart();
                    var sent = await SendMessageAsync(drone, futileRequestBuf, nameof(CcFutileRequest))
                        .FastPath().ConfigureAwait(Zc);
                    if (sent > 0)
                    {
                        _logger.Trace(
                            $"Sent {sent} egress futile challange, socket = {ioNetSocket.Description}");
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
                    do
                    {
                        bytesRead += localRead = await ioNetSocket
                            .ReadAsync(futileBuffer, bytesRead, chunkSize, timeout: parm_futile_timeout_ms * 2).FastPath()
                            .ConfigureAwait(Zc);
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
                        _logger.Trace(
                            $"Failed to read egress futile challange response, waited = {_sw.ElapsedMilliseconds}ms, remote = {ioNetSocket.RemoteAddress}, zeroed {Zeroed()}");
                        return false;
                    }
                    
                    _logger.Trace(
                        $"Read egress futile challange response size = {bytesRead} b, addess = {ioNetSocket.RemoteAddress}");
                    
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
                        {
                            return false;
                        }

                        _logger.Trace($"{nameof(CcFuseRequest)}: [signed], from = egress, read = {bytesRead}, {drone.IoSource.Key}");

                        //race for connection
                        var won = await ConnectForTheWinAsync(CcAdjunct.Heading.Egress, drone, packet,
                                (IPEndPoint)ioNetSocket.NativeSocket.RemoteEndPoint)
                            .FastPath().ConfigureAwait(Zc);

                        if(!won)
                            return false;

                        //validate futile response
                        var futileResponse = CcFutileResponse.Parser.ParseFrom(packet.Data);
                    
                        if (futileResponse != null)
                        {
                            if (futileResponse.ReqHash.Length == 32)
                            {
                                if (!CcDesignation.Sha256
                                    .ComputeHash(futileRequestBuf.Memory.AsArray())
                                    .SequenceEqual(futileResponse.ReqHash))
                                {
                                    _logger.Error($"{nameof(ConnectForTheWinAsync)}: Invalid futile response! Closing {ioNetSocket.Key}");
                                    return false;
                                }
                            }
                            else
                            {
                                return false;
                            }
                        }
                    
                        success = !Zeroed() && drone.Adjunct != null && !drone.Adjunct.Zeroed() && drone.Adjunct?.Direction == CcAdjunct.Heading.Egress && drone.Source.IsOperational;
                        return success;
                    }
                }
            }            
            catch (Exception) when (Zeroed() || drone.Zeroed()) { }
            catch (Exception e) when(!Zeroed() && !drone.Zeroed())
            {
                _logger.Error(e, $"Futile request (size = {bytesRead}/{_futileRequestSize}/{_futileResponseSize}/{_futileRejectSize}) for {Description} failed with:");
            }
            finally
            {
                if (success)
                {
                    if (drone.IoSource.IoNetSocket.IsEgress)
                    {
                        await drone.ZeroSubAsync(static (_,@this) =>
                        {
                            Interlocked.Decrement(ref @this.EgressCount);
                            return new ValueTask<bool>(true);
                        }, this).FastPath().ConfigureAwait(Zc);                        
                    }
                    else if (drone.IoSource.IoNetSocket.IsIngress)
                    {
                        await drone.ZeroSubAsync(static (from, @this) =>
                        {
                            Interlocked.Decrement(ref @this.IngressCount);
                            return new ValueTask<bool>(true);
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
        /// <param name="packet">The futile packet</param>
        /// <param name="remoteEp">The remote</param>
        /// <returns>True if it won, false otherwise</returns>
        private async ValueTask<bool> ConnectForTheWinAsync(CcAdjunct.Heading direction, CcDrone drone, chroniton packet, IPEndPoint remoteEp)
        {
            if(_gossipAddress.IpEndPoint.ToString() == remoteEp.ToString())
                throw new ApplicationException($"Connection inception dropped from {remoteEp} on {_gossipAddress.IpEndPoint}: {Description}");

            var adjunct = drone.Adjunct;

            if (adjunct == null)
            {
                var id = CcDesignation.ShortId(packet.PublicKey.Memory.AsArray());
                if (direction == CcAdjunct.Heading.Ingress && (drone.Adjunct = (CcAdjunct)_autoPeering.Neighbors.Values.FirstOrDefault(n => n.Key.Contains(id))) == null)
                {
                    //TODO: this code path is jammed
                    _logger.Error($"Neighbor [{id}] not found, dropping {direction} connection to {remoteEp}");
                    return false;
                }

                adjunct = drone.Adjunct;

                var stateIsValid = adjunct.CurrentState.Value != CcAdjunct.AdjunctState.Connected && adjunct.CompareAndEnterState(CcAdjunct.AdjunctState.Connecting, CcAdjunct.AdjunctState.Fusing, overrideHung: adjunct.parm_max_network_latency_ms * 4) == CcAdjunct.AdjunctState.Fusing;
                if (!stateIsValid)
                {
                    if(adjunct.CurrentState.Value != CcAdjunct.AdjunctState.Connected && adjunct.CurrentState.EnterTime.ElapsedMs() > adjunct.parm_max_network_latency_ms * 4)
                        _logger.Warn($"{nameof(ConnectForTheWinAsync)} - {Description}: Invalid state, {adjunct.CurrentState.Value}, age = {adjunct.CurrentState.EnterTime.ElapsedMs()}ms. Wanted {nameof(CcAdjunct.AdjunctState.Fusing)} - [RACE OK!]");
                    return false;
                }
            }

            if (adjunct.Assimilating && !adjunct.IsDroneAttached)
            {
                var attached = await drone.AttachViaAdjunctAsync(direction).FastPath().ConfigureAwait(Zc);

                var capped = attached && !ZeroAtomic(static (n, o, d) =>
                {
                    var (@this, direction) = o;
                                                            
                    if (direction == CcAdjunct.Heading.Ingress)
                    {
                        if(Interlocked.Increment(ref @this.IngressCount) - 1 < @this.parm_max_inbound)
                        {
                            return new ValueTask<bool>(true);
                        }
                        else
                        {
                            Interlocked.Decrement(ref @this.IngressCount);
                            return new ValueTask<bool>(false);
                        }
                    }
                    else
                    {
                        if (Interlocked.Increment(ref @this.EgressCount) - 1 < @this.parm_max_outbound)
                        {
                            return new ValueTask<bool>(true);
                        }
                        else
                        {
                            Interlocked.Decrement(ref @this.EgressCount);
                            return new ValueTask<bool>(false);
                        }
                    }
                }, (this, direction));

                if(capped)
                {
                    await drone.DetachNeighborAsync().FastPath().ConfigureAwait(Zc);
                }

                return !capped;
            }
            else
            {
                _logger.Trace($"{direction} futile request [LOST] {CcDesignation.FromPubKey(packet.PublicKey.Memory.AsArray())} - {remoteEp}: s = {drone.Adjunct.State}, a = {drone.Adjunct.Assimilating}, p = {drone.Adjunct.IsDroneConnected}, pa = {drone.Adjunct.IsDroneAttached}, ut = {drone.Adjunct.Uptime.ElapsedMs()}");
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
                    adjunct.State == CcAdjunct.AdjunctState.Connecting &&
                    EgressCount < parm_max_outbound &&
                    //TODO add distance calc
                    //adjunct.Services.CcRecord.Endpoints.ContainsKey(CcService.Keys.gossip) &&
                    _currentOutboundConnectionAttempts < _maxAsyncConnectionAttempts
                )
            {
                try
                {
                    Interlocked.Increment(ref _currentOutboundConnectionAttempts);

                    var drone = await ConnectAsync(IoNodeAddress.CreateFromEndpoint("tcp", adjunct.RemoteAddress.IpEndPoint) , adjunct, timeout:adjunct.parm_max_network_latency_ms * 2).FastPath().ConfigureAwait(Zc);
                    if (Zeroed() || drone == null || ((CcDrone)drone).Adjunct.Zeroed())
                    {
                        if (drone != null) drone.Zero(this);
                        _logger.Debug($"{nameof(ConnectToDroneAsync)}: [ABORTED], {adjunct.Description}, {adjunct.MetaDesc}");
                        return false;
                    }
                
                    //Race for a connection
                    if (await FutileAsync((CcDrone)drone).FastPath().ConfigureAwait(Zc))
                    {
                        //Start processing
                        await ZeroAsync(static async state =>
                        {
                            var (@this, drone) = state;
                            await Task.Delay(@this.parm_futile_timeout_ms, @this.AsyncTasks.Token);
                            
                            if (!drone.Zeroed())
                            {
                                @this._logger.Info($"+ {drone.Description}");
                                await @this.BlockOnAssimilateAsync(drone).FastPath().ConfigureAwait(@this.Zc);
                            }
                            else
                            {
                                @this._logger.Debug($"+|{drone.Description}");
                            }
                        }, ValueTuple.Create(this, drone), TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(false);
                        
                        
                        return true;
                    }
                    else
                    {
                        _logger.Debug($"+|{drone.Description}");
                        drone.Zero(this);
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
                
                //Console.WriteLine("--------------------");
                //Console.WriteLine(!Zeroed()?"[OK]": "ZEROED");
                //Console.WriteLine(adjunct.Assimilating ? "[OK]" : "not Assimilating");
                //Console.WriteLine(!adjunct.IsDroneConnected ? "[OK]" : "not IsDroneConnected");
                //if (adjunct.State != CcAdjunct.AdjunctState.Connecting)
                //{
                //    Console.WriteLine(adjunct.State);
                //    adjunct.PrintStateHistory();
                //}
                //else
                //{
                //    Console.WriteLine("[OK]");
                //}
                //Console.WriteLine(EgressCount < parm_max_outbound ? "[OK]" : "EgressCount");
                //Console.WriteLine(adjunct.Services.CcRecord.Endpoints.ContainsKey(CcService.Keys.gossip) ? "[OK]" : "gossip service missing");
                //Console.WriteLine(_currentOutboundConnectionAttempts < _maxAsyncConnectionAttempts ? "[OK]" : "_maxAsyncConnectionAttempts");
                //Console.WriteLine("--------------------");

                _logger.Trace($"{nameof(ConnectToDroneAsync)}: Connect skipped: {adjunct.Description}");
                return false;
            }
        }

        private static readonly double _lambda = 20;
        private static readonly int _maxAsyncConnectionAttempts = 16;

        private int _currentOutboundConnectionAttempts;
        private Poisson _poisson = new(_lambda, new Random(DateTimeOffset.Now.Ticks.GetHashCode() * DateTimeOffset.Now.Ticks.GetHashCode()));
        private readonly bool _zeroDrone;
        public bool ZeroDrone => _zeroDrone;

        /// <summary>
        /// Whether the drone complete the bootstrap process
        /// </summary>
        public bool Online { get; private set; }

        /// <summary>
        /// Boots the node
        /// </summary>
        public async ValueTask<bool> BootAsync(long v = 0)
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


            if (WhisperingDrones.Count > 0)
            {
                await WhisperingDrones[_random.Next(0, WhisperingDrones.Count - 1)].EmitTestGossipMsgAsync(v).FastPath().ConfigureAwait(Zc);
                return true;
            }
            
            return false;
        }

        /// <summary>
        /// Boostrap node
        /// </summary>
        /// <returns></returns>
        public async ValueTask DeepScanAsync()
        {
            try
            {
                if(Zeroed() || Hub?.Router == null)
                    return;

                //if (ZeroDrone)
                //    await Task.Delay(3000);

                //var foundVector = false;
                foreach (var vector in Hub.Neighbors.Values.Where(n=>((CcAdjunct)n).Assimilating))
                {
                    var adjunct = (CcAdjunct)vector;

                    adjunct.EnsureFuseChecks();

                    //Only probe when we are running lean
                    if (adjunct.CcCollective.Hub.Neighbors.Values.Where(n=>((CcAdjunct)n).Assimilating).ToList().Count >= adjunct.CcCollective.MaxAdjuncts)
                        break;

                    //if (await adjunct.SeduceAsync("SYN-SE", passive: false).FastPath().ConfigureAwait(Zc))
                    //{
                    //    _logger.Debug($"SE> {adjunct.Description}");
                    //    foundVector = true;
                    //}
                    if (adjunct.Probed)
                    {
                        if (!await adjunct.SweepAsync().FastPath().ConfigureAwait(Zc))
                        {
                            if (!Zeroed())
                                _logger.Trace($"{nameof(adjunct.SweepAsync)}: Unable to probe adjuncts, {Description}");
                        }
                        else
                        {
                            _logger.Debug($"SE> {adjunct.Description}");
                            //foundVector = true;
                        }
                    }
                    else
                    {
                        if (!await adjunct.SeduceAsync("SYN-SE", CcAdjunct.Heading.Both, force:true).FastPath().ConfigureAwait(Zc))
                        {
                            if (!Zeroed())
                                _logger.Trace($"{nameof(adjunct.SweepAsync)}: Unable to seduce adjuncts, {Description}");
                        }
                        else
                        {
                            _logger.Debug($"SE> {adjunct.Description}");
                            //foundVector = true;
                        }
                    }

                    //if (adjunct.Probed) 
                    //{
                    //    //probe
                    //    if (!await adjunct.SweepAsync().FastPath().ConfigureAwait(Zc))
                    //    {
                    //        if (!Zeroed())
                    //            _logger.Trace($"{nameof(DeepScanAsync)}: {Description}, Unable to probe adjuncts");
                    //    }
                    //    else
                    //    {
                    //        _logger.Debug($"R> {adjunct.Description}");
                    //        foundVector = true;
                    //    }
                    //}
                    //else
                    //{
                    //    //probe
                    //    if (!await adjunct.ProbeAsync("SYN!").FastPath().ConfigureAwait(Zc))
                    //    {
                    //        if (!Zeroed())
                    //            _logger.Trace($"{nameof(DeepScanAsync)}: {Description}, Unable to probe adjuncts");
                    //    }
                    //    else
                    //    {
                    //        _logger.Debug($"P> {adjunct.Description}");
                    //        foundVector = true;
                    //    }
                    //}

                    await Task.Delay(((CcAdjunct)vector).parm_max_network_latency_ms, AsyncTasks.Token).ConfigureAwait(Zc);
                }

                //if(foundVector)
                //    return;

                _logger.Trace($"Bootstrapping {Description} from {BootstrapAddress.Count} bootnodes...");
                if (BootstrapAddress != null)
                {
                    foreach (var ioNodeAddress in BootstrapAddress)
                    {
                        if (!ioNodeAddress.Equals(_peerAddress))
                        {
                            if (Hub.Neighbors.Values.Any(a => a.Key.Contains(ioNodeAddress.Key)))
                                continue;

                            try
                            {
                                await Task.Delay(parm_futile_timeout_ms / 2 + _random.Next(parm_futile_timeout_ms), AsyncTasks.Token).ConfigureAwait(Zc);
                                _logger.Trace($"{Description} Bootstrapping from {ioNodeAddress}");
                                if (!await Hub.Router.ProbeAsync("SYN-DMZ", ioNodeAddress).FastPath().ConfigureAwait(Zc))
                                {
                                    if (!Hub.Router.Zeroed())
                                        _logger.Trace($"{nameof(DeepScanAsync)}: Unable to boostrap {Description} from {ioNodeAddress}");
                                }

                                if (ZeroDrone && !Online)
                                {
                                    Online = true;
                                    _logger.Warn($"Queen brought Online, {Description}");
                                }
                                    
#if DEBUG
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

            return default;
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
            if (DupChecker.Count > DupHeap.MaxSize * 4 / 5)
            {
                foreach (var mId in DupChecker.Keys)
                {
                    if (DupChecker.TryRemove(mId, out var del))
                    {
                        del.ZeroManaged();
                        DupHeap.Return(del);
                    }
                }
            }
        }

    }
}
