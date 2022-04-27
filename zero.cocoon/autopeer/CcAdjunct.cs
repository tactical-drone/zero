//#define LOSS
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
#if DEBUG
using System.Text;
#endif
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using K4os.Compression.LZ4;
using MathNet.Numerics.Random;
using NLog;
using zero.cocoon.events.services;
using zero.cocoon.identity;
using zero.cocoon.models;
using zero.cocoon.models.batches;
using zero.core.conf;
using zero.core.core;
using zero.core.feat.misc;
using zero.core.feat.models.protobuffer;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore;
using zero.core.patterns.semaphore.core;
using zero.core.runtime.scheduler;
using Zero.Models.Protobuf;

namespace zero.cocoon.autopeer
{
    /// <summary>
    /// Processes (UDP) discovery messages from the collective.
    /// </summary>
    public class CcAdjunct : IoNeighbor<CcProtocMessage<chroniton, CcDiscoveryBatch>>
    {
        public CcAdjunct(CcHub node, IoNetClient<CcProtocMessage<chroniton, CcDiscoveryBatch>> ioNetClient, object extraData = null)
            : base
            (
                node,
                ioNetClient,
                static (ioZero, _) => new CcDiscoveries("discovery msg",
                    $"{((CcAdjunct)ioZero)?.Source.Key}",
                    // ReSharper disable once PossibleInvalidCastException
                    ((CcAdjunct)ioZero)?.Source, groupByEp: false),
                true, concurrencyLevel:ioNetClient.ZeroConcurrencyLevel()
            )
        {
            //sentinel
            if (Source == null)
                return;

            if (Source.Zeroed())
            {
                return;
            }
            
            _random = new CryptoRandomSource(true);

            _logger = LogManager.GetCurrentClassLogger();

            //TODO tuning
            var capMult = CcCollective.ZeroDrone ? 9 : 6;
            var capBase = 2;
            _probeRequest = new IoZeroMatcher(nameof(_probeRequest), Source.PrefetchSize, parm_max_network_latency_ms << 1, (int)Math.Pow(capBase, capMult));
            _fuseRequest = new IoZeroMatcher(nameof(_fuseRequest), Source.PrefetchSize, parm_max_network_latency_ms << 1, (int)Math.Pow(capBase, capMult));
            _scanRequest = new IoZeroMatcher(nameof(_scanRequest), (int)(CcCollective.MaxAdjuncts * parm_max_swept_drones + 1), parm_max_network_latency_ms << 1, (int)Math.Pow(capBase, capMult));

            if (extraData != null)
            {
                IsProxy = true;
                var (item1, ipEndPoint, verified) = (Tuple<CcDesignation, IPEndPoint, bool>) extraData;

                Designation = item1;
                Key = Designation.IdString();

                _remoteAddress = IoNodeAddress.CreateFromEndpoint("udp", ipEndPoint);

                //to prevent cascading into the hub we clone the source.
                Source = new IoUdpClient<CcProtocMessage<chroniton, CcDiscoveryBatch>>($"UDP Proxy ~> {base.Description}", MessageService, RemoteAddress.IpEndPoint);
                Source.ZeroHiveAsync(this).AsTask().GetAwaiter().GetResult();
                CompareAndEnterState(verified ? AdjunctState.Verified : AdjunctState.Unverified, AdjunctState.Undefined);
                
                Verified = verified;
            }
            else
            {
                Designation = CcCollective.CcId;
                
                //Services = services ?? node.Services;
                Key = Designation.IdString();

                CompareAndEnterState(AdjunctState.Local, AdjunctState.Undefined);
                _routingTable = new ConcurrentDictionary<string, CcAdjunct>();

                _zeroSync = new IoZeroResetValueTaskSource<bool>();
            }

#if DEBUG
            string packetHeapDesc = $"{nameof(_chronitonHeap)}, {_description}";
            string protoHeapDesc = $"{nameof(_sendBuf)}, {_description}";
#else
            string packetHeapDesc = string.Empty;
            string protoHeapDesc = string.Empty;
#endif

            //TODO tuning:
            _chronitonHeap = new IoHeap<chroniton, CcAdjunct>(packetHeapDesc, CcCollective.ZeroDrone & !IsProxy ? 32 : 16,
                static (_, @this) =>
                {
                    //sentinel
                    if (@this == null)
                        return new chroniton();

                    return new chroniton
                    {
                        Header = new z_header { Ip = new net_header() },
                        PublicKey = UnsafeByteOperations.UnsafeWrap(@this.CcCollective.CcId.PublicKey)
                    };
                }, true, this);

            _sendBuf = new IoHeap<Tuple<byte[],byte[]>>(protoHeapDesc, CcCollective.ZeroDrone & !IsProxy ? 32 : 16, static (_, _) => Tuple.Create(new byte[1492 / 2], new byte[1492 / 2]), autoScale: true);

            _stealthy = Environment.TickCount - parm_max_network_latency_ms;
            var nrOfStates = Enum.GetNames(typeof(AdjunctState)).Length;
            _lastScan = Environment.TickCount - CcCollective.parm_mean_pat_delay_s * 1000 * 2;
        }

        public enum AdjunctState
        {
            Undefined = 0,
            FinalState = 1,
            Local = 2,
            Unverified = 3,
            Verified = 4,
            Fusing = 5,
            Rusing = 6,
            Connecting = 7,
            Connected = 8
        }

        /// <summary>
        /// The current state
        /// </summary>

#if DEBUG
        private IoStateTransition<AdjunctState> _currState = new()
        {
            FinalState = AdjunctState.FinalState
        };
#else
        private volatile IoStateTransition<AdjunctState> _currState = new()
        {
            FinalState = AdjunctState.FinalState
        };
#endif


        /// <summary>
        /// logger
        /// </summary>
        private Logger _logger;


        private string _description;

        public override string Description
        {
            get
            {
                try
                {
#if DEBUG
                    if (!Zeroed())
#endif
                    {
                        if (IsProxy)
                            return _description = $"adjunct ({EventCount}, {_drone?.EventCount ?? 0}, {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{FuseCount}:{FusedCount}] local: |{MessageService.IoNetSocket.LocalAddress} ~> {MessageService.IoNetSocket.RemoteAddress}|, [{Hub?.Designation.IdString()}, {Designation.IdString()}], T = {UpTime.ElapsedMsToSec()/3600.0:0.00}h'";
                        else
                            return _description = $"hub ({EventCount}, {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{FuseCount}:{FusedCount}] local: |{MessageService.IoNetSocket.LocalAddress} ~> {MessageService.IoNetSocket.RemoteAddress}|, [{Hub?.Designation?.IdString()}, {Designation.IdString()}], T = {UpTime.ElapsedMsToSec() / 3600:0.00}h'";
                    }

#if DEBUG
                    if (IsProxy)
                        return _description ?? $"adjunct({EventCount}, {_drone?.EventCount ?? 0}, , {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{FuseCount}:{FusedCount}] local: |{MessageService?.IoNetSocket?.LocalAddress} ~> {MessageService?.IoNetSocket?.RemoteAddress}|,' [{Hub?.Designation?.IdString()}, {Designation?.IdString()}], T = {UpTime.ElapsedMsToSec() / 3600:0.00}h'";
                    else
                        return _description = $"hub ({EventCount}, {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{FuseCount}:{FusedCount}] local: |{MessageService?.IoNetSocket?.LocalAddress} ~> {MessageService?.IoNetSocket?.RemoteAddress}|, [{Hub?.Designation?.IdString()}, {Designation?.IdString()}], T = {UpTime.ElapsedMsToSec() / 3600:0.00}h'";
#endif
                }
                catch (Exception)
                {
                    if(IsProxy)
                        return _description?? $"adjunct({EventCount}, {_drone?.EventCount ?? 0}, , {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{FuseCount}:{FusedCount}] local: |{MessageService?.IoNetSocket?.LocalAddress} ~> {MessageService?.IoNetSocket?.RemoteAddress}|,' [{Hub?.Designation?.IdString()}, {Designation?.IdString()}], T = {UpTime.ElapsedMsToSec() / 3600:0.00}h'";    
                    else
                        return _description = $"hub ({EventCount}, {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{FuseCount}:{FusedCount}] local: ||{MessageService?.IoNetSocket?.LocalAddress} ~> {MessageService?.IoNetSocket?.RemoteAddress}||, [{Hub?.Designation?.IdString()}, {Designation?.IdString()}], T = {UpTime.ElapsedMsToSec() / 3600:0.00}h'";
                }
            }
        }

        /// <summary>
        /// return extra information about the state
        /// </summary>
#if DEBUG
        public string MetaDesc => $"{(Zeroed() ? "Zeroed!!!," : "")} d={Direction},s={State},v={(Verified?"1":"0")},a={(Assimilating ? "1":"0")},da={(IsDroneAttached ? "1":"0")},cc={(IsDroneConnected ? "1":"0")},g={(IsGossiping ? "1":"0")},arb={(IsArbitrating ? "1":"0")}";
#else
        public string MetaDesc => string.Empty;
#endif


        /// <summary>
        /// Random number generator
        /// </summary>
        private readonly Random _random;

        /// <summary>
        /// Discovery services
        /// </summary>
        protected internal CcHub Hub => (CcHub)Node;

        /// <summary>
        /// Source
        /// </summary>
        public IoNetClient<CcProtocMessage<chroniton, CcDiscoveryBatch>> MessageService => (IoNetClient<CcProtocMessage<chroniton, CcDiscoveryBatch>>) Source;


        /// <summary>
        /// Holds all messages
        /// </summary>
        private IoHeap<chroniton, CcAdjunct> _chronitonHeap;

        /// <summary>
        /// Holds all coded outputstreams used for sending messages
        /// </summary>
        private IoHeap<Tuple<byte[],byte[]>> _sendBuf;

        /// <summary>
        /// The udp routing table 
        /// </summary>
        private ConcurrentDictionary<string, CcAdjunct> _routingTable;

        /// <summary>
        /// The gossip drone associated with this neighbor
        /// </summary>
        private volatile CcDrone _drone;

        /// <summary>
        /// Whether The drone is attached
        /// </summary>
        public bool IsDroneAttached => _drone != null && !_drone.Zeroed();

        /// <summary>
        /// Whether the drone is nominal
        /// </summary>
        public bool IsDroneConnected => IsDroneAttached && State == AdjunctState.Connected && Direction != Heading.Undefined;
        
        /// <summary>
        /// If the adjunct is working 
        /// </summary>
        public bool Assimilating => !Zeroed() && Verified;

        /// <summary>
        /// Whether the node, drone and adjunct are nominal
        /// </summary>
        public bool IsGossiping => IsDroneConnected && Assimilating;

        /// <summary>
        /// Indicates whether we have extracted information from this drone
        /// </summary>
        public bool Assimilated { get; protected set; }

        private volatile int _fusedCount;
        /// <summary>
        /// The number of fuse requests received
        /// </summary>
        protected int FusedCount => _fusedCount;

        private volatile int _fuseCount;
        /// <summary>
        /// Number of fuse requests sent
        /// </summary>
        public int FuseCount => _fuseCount;

        private volatile int _zeroProbes;
        /// <summary>
        /// Number of successful probes
        /// </summary>
        public int ZeroProbes => _zeroProbes;

        private volatile int _scanCount;

        /// <summary>
        /// Gathers counter intelligence at a acceptable rates
        /// </summary>
        private long _lastSeduced = Environment.TickCount - 100000;

        /// <summary>
        /// Node broadcast priority. 
        /// </summary>
        public long Priority => FusedCount - FuseCount;

        //uptime
        private long _attachTimestamp;

        public long AttachTimestamp
        {
            get => Interlocked.Read(ref _attachTimestamp);
            private set => Interlocked.Exchange(ref _attachTimestamp, value);
        }


        private volatile IoNodeAddress _remoteAddress;
        /// <summary>
        /// The adjunct address
        /// </summary>
        public IoNodeAddress RemoteAddress => NatAddress ?? _remoteAddress;

        /// <summary>
        /// Whether this adjunct contains verified remote client connection information
        /// </summary>
        public bool IsProxy { get; }

#if DEBUG
        /// <summary>
        /// The our IP as seen by neighbor
        /// </summary>
        public IoNodeAddress DebugAddress { get; protected set; }
#endif

        /// <summary>
        /// The our discovery service IP as seen by neighbor
        /// </summary>
        public IoNodeAddress NatAddress { get; protected set; }
        
        /// <summary>
        /// The node identity
        /// </summary>
        public CcDesignation Designation { get; protected set; }

        /// <summary>
        /// Whether the node has been verified
        /// </summary>
        public bool Verified { get; protected set; }

        /// <summary>
        /// Backing cache
        /// </summary>
        private volatile int _direction;

        /// <summary>
        /// Who contacted who?
        /// </summary>
        //public Kind Direction { get; protected set; } = Kind.Undefined;
        public Heading Direction => (Heading) _direction;

        /// <summary>
        /// inbound
        /// </summary>
        public bool IsIngress =>  IsDroneConnected && Direction == Heading.Ingress;

        /// <summary>
        /// outbound
        /// </summary>
        public bool IsEgress => IsDroneConnected && Direction == Heading.Egress;

        /// <summary>
        /// Who contacted who?
        /// </summary>
        public enum Heading
        {
            Undefined = 0,
            Ingress = 1,
            Egress = 2,
            Both = 3
        }

        /// <summary>
        /// The node that this adjunct belongs to
        /// </summary>
        public CcCollective CcCollective => Hub.CcCollective;

        /// <summary>
        /// The router
        /// </summary>
        public CcAdjunct Router => Hub.Router;

        /// <summary>
        /// Receives protocol messages from here
        /// </summary>
        private IoConduit<CcProtocBatchJob<chroniton, CcDiscoveryBatch>> _protocolConduit;

        /// <summary>
        /// Syncs producer and consumer queues
        /// </summary>
        private readonly IoZeroResetValueTaskSource<bool> _zeroSync;

        /// <summary>
        /// Seconds since pat
        /// </summary>
        private long _lastPat = Environment.TickCount;

        /// <summary>
        /// Total pats
        /// </summary>
        private long _totalPats;

        /// <summary>
        /// Seconds since last adjunct pat
        /// </summary>
        protected long LastPat
        {
            get => Interlocked.Read(ref _lastPat);
            // ReSharper disable once ValueParameterNotUsed
            set
            {
                Interlocked.Exchange(ref _lastPat, value);
                Interlocked.Increment(ref _totalPats);
            }
        }

        /// <summary>
        /// Seconds since last adjunct pat
        /// </summary>
        public long TotalPats => Interlocked.Read(ref _totalPats);

        /// <summary>
        /// Seconds since valid
        /// </summary>
        public long SecondsSincePat => LastPat.ElapsedMsToSec() - 1;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private IoZeroMatcher _scanRequest;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private IoZeroMatcher _fuseRequest;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private IoZeroMatcher _probeRequest;

        /// <summary>
        /// Whether this adjunct exists in the Hub's routing table
        /// </summary>
        private volatile bool _routed;

        /// <summary>
        /// Whether we are probed
        /// </summary>
        public bool Probed => _routed;

        /// <summary>
        /// The CcId
        /// </summary>
        public override string Key { get; }

        /// <summary>
        /// Whether to enable syn triggers. Useful for seduction, 
        /// </summary>
        private const bool EnableSynTrigger = true;

        /// <summary>
        /// Enable or disable EP caching while unpacking batches. (if adjuncts move around regularly disable, or always disable)
        /// </summary>
        private bool _enableBatchEpCache = false;

        private long _lastScan = (long)(Environment.TickCount - TimeSpan.FromDays(1).TotalMilliseconds);
        /// <summary>
        /// Time since our last scan
        /// </summary>
        private long LastScan => Volatile.Read(ref _lastScan);

        /// <summary>
        /// The adjunct services
        /// </summary>
        //public CcService Services { get; protected set; }

        /// <summary>
        /// Salt length
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_salt_length = 20;

        /// <summary>
        /// Salt time to live
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_salt_ttl = 2 * 60 * 60;

        /// <summary>
        /// Max expected network delay
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
#if DEBUG
        public int parm_max_network_latency_ms = 5000;
#else
        public int parm_max_network_latency_ms = 10000;
#endif

        /// <summary>
        /// Maximum number of drones in discovery response
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public uint parm_max_swept_drones = 6;

        /// <summary>
        /// Maximum number of services supported
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_services = 3;
        
        /// <summary>
        /// Maximum connection retry on non responsive nodes 
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_zombie_max_connection_attempts = 5;

        /// <summary>
        /// Minimum pats before a node could be culled
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_min_pats_before_shuffle = 1;

        /// <summary>
        /// Minimum number of desired spare bays for sweep requests to yield returns.
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_min_spare_bays = 1;

        /// <summary>
        /// Network id
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public uint parm_network_id = 13;

        /// <summary>
        /// Protocol version
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_protocol_version = 0;

        /// <summary>
        /// Zeroed?
        /// </summary>
        /// <returns>True if zeroed</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Zeroed()
        {
            try
            {
                return base.Zeroed() || Source.Zeroed() || Hub.Zeroed() || CcCollective.Zeroed();
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
            
#if SAFE_RELEASE
            _logger = null;
            _routingTable = null;
            _probeRequest = null;
            _fuseRequest = null;
            _scanRequest = null;
            _drone = null;
            _protocolConduit = null;
            _chronitonHeap = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();

            if (IsProxy)
            {
                try
                {
                    //Remove from routing table
                    if (_routed && Router._routingTable.TryGetValue(RemoteAddress.Key, out var currentRoute))
                    {
                        if (currentRoute == this)
                        {
                            if (!Router._routingTable.TryRemove(RemoteAddress.Key, out _) && Verified)
                            {
                                _logger.Warn($"Failed to remove route {RemoteAddress.Key}, {Description}");
#if DEBUG
                                foreach (var route in Router._routingTable)
                                {
                                    _logger.Trace($"R/> {nameof(Router._routingTable)}[{route.Key}]=<{route.Value.RemoteAddress.Port}> {route.Value}");
                                }
#endif
                            }
                        }
                        else
                        {
                            _logger.Trace($"ROUTER: cull failed,wanted = [{Designation?.IdString()}], got [{currentRoute.Designation.IdString()}, serial1 = {Serial} vs {currentRoute.Serial}], {Description}");
                        }
                    }
                }
                catch
                {
                    // ignored
                }

                await _chronitonHeap.ZeroManagedAsync<object>().FastPath();
                await _sendBuf.ZeroManagedAsync<object>().FastPath();

                var from = ZeroedFrom == this? "self" : ZeroedFrom?.Description??"null";
                if (!CcCollective.ZeroDrone && Assimilated && WasAttached && UpTime.ElapsedMs() > parm_min_uptime_ms && EventCount > 10)
                    _logger.Info($"- `{(Assimilated ? "apex" : "sub")} {Direction}: {Description}, {from}: r ={ZeroReason}");

                await DetachDroneAsync().FastPath();
                
                //swarm 
                await ZeroAsync(static async @this =>
                {
                    await Task.Delay(@this._random.Next(@this.parm_max_network_latency_ms), @this.AsyncTasks.Token);
                    await @this.Router.ProbeAsync("SYN-BRG", @this.RemoteAddress.Copy());
                }, this, TaskCreationOptions.DenyChildAttach).FastPath();

                if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                    AutoPeeringEventService.AddEvent(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.RemoveAdjunct,
                        Adjunct = new Adjunct
                        {
                            Id = Designation.IdString(),
                            CollectiveId = Router.Designation.IdString()
                        }
                    });
            }
            else
            {
                _logger.Info($"ZERO HUB PROXY: from: {ZeroedFrom?.Description}, reason: {ZeroReason}, {Description}");
            }

            await _probeRequest.Zero(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();
            await _fuseRequest.Zero(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();
            await _scanRequest.Zero(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();

            ResetState(AdjunctState.FinalState);
        }

        /// <summary>
        /// Robotech
        /// </summary>
        /// <param name="this"></param>
        /// <returns></returns>
        private static async ValueTask RoboAsync(CcAdjunct @this)
        {
            try
            {
                //TODO: tuning, helps cluster test bootups not stalling on popdog spam
                await Task.Delay(@this.CcCollective.parm_mean_pat_delay_s * 1000);

                while (!@this.Zeroed())
                {
                    try
                    {
                        var targetDelay = (@this._random.Next(@this.CcCollective.parm_mean_pat_delay_s / 4) + @this.CcCollective.parm_mean_pat_delay_s / 4) * 1000;

                        if (@this.CcCollective.TotalConnections == 0)
                            targetDelay /= 4;

                        var ts = Environment.TickCount;
                        await Task.Delay(targetDelay, @this.AsyncTasks.Token);

                        if (@this.Zeroed())
                            break;

                        if (ts.ElapsedMs() > targetDelay * 1.25 && ts.ElapsedMs() > 0)
                        {
                            @this._logger.Warn($"{@this.Description}: Popdog is slow!!!, {(ts.ElapsedMs() - targetDelay) / 1000.0:0.0}s");
                        }

                        if (ts.ElapsedMs() < targetDelay && !@this.Zeroed())
                        {
                            @this._logger.Warn($"{@this.Description}: Popdog is FAST!!!, {(ts.ElapsedMs() - targetDelay):0.0}ms / {targetDelay}");
                        }

                        foreach (var ioNeighbor in @this.Hub.Neighbors.Values.Where(n=> ((CcAdjunct)n).IsProxy))
                        {
                            var adjunct = (CcAdjunct)ioNeighbor;
                            if(!adjunct.IsDroneConnected)
                                await adjunct.EnsureRoboticsAsync().FastPath();
                        }
                    }
                    catch (Exception) when (@this.Zeroed()) { }
                    catch (Exception e) when (!@this.Zeroed())
                    {
                        @this._logger.Fatal(e, $"{@this.Description}: Watchdog returned with errors!");
                    }
                }
            }
            finally
            {
                if (!@this.Zeroed())
                {
                    @this._logger.Fatal($"{@this.Description}: WATCHDOG died!!!");
                }
            }
        }

        /// <summary>
        /// Ensure robotic diagnostics
        /// </summary>
        public async ValueTask EnsureRoboticsAsync()
        {
            try
            {
                if (SecondsSincePat >= CcCollective.parm_mean_pat_delay_s / 2)
                {
                    //send PAT
                    if (!await ProbeAsync("SYN-PAT").FastPath())
                    {
                        if (!Zeroed())
                            _logger.Error($"-/> {nameof(ProbeAsync)}: PAT Send [FAILED], {Description}, {MetaDesc}");
                    }
#if DEBUG
                    else 
                        _logger.Trace($"-/> {nameof(ProbeAsync)}: PAT Send [SUCCESS], {Description}, {MetaDesc}");
#endif
                }

                if(Zeroed())
                    return;

                //Watchdog failure
                if (SecondsSincePat > CcCollective.parm_mean_pat_delay_s)
                {
                    _logger.Trace($"w {nameof(EnsureRoboticsAsync)} - {Description}, s = {SecondsSincePat} >> {CcCollective.parm_mean_pat_delay_s}, {MetaDesc}");
                    await Zero(CcCollective,$"-wd: l = {SecondsSincePat}s, up = {TimeSpan.FromMilliseconds(UpTime.ElapsedMs()).TotalHours:0.00}h").FastPath();
                }
            }
            catch when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"{nameof(EnsureRoboticsAsync)} - {Description}: Send PAT failed!");
            }
        }
        
        /// <summary>
        /// Start processors for this neighbor
        /// </summary>
        /// <returns></returns>
        public override async ValueTask BlockOnReplicateAsync()
        {
            try
            {
                if (!IsProxy)
                {
                    //Discoveries
                    await ZeroAsync(static async @this =>
                    {
                        await @this.ProcessDiscoveriesAsync().FastPath();
                    }, this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach).FastPath();

                    ////UDP traffic
                    await ZeroAsync(static async state =>
                    {
                        var (@this, assimilateAsync) = state;
                        await assimilateAsync().FastPath();
                    }, ValueTuple.Create<CcAdjunct, Func<ValueTask>>(this, base.BlockOnReplicateAsync), TaskCreationOptions.LongRunning).FastPath();

                    //Watchdogs
                    await ZeroAsync(RoboAsync, this, TaskCreationOptions.LongRunning).FastPath();
                    await AsyncTasks.Token.BlockOnNotCanceledAsync();
                }
                else
                {
                    await AsyncTasks.Token.BlockOnNotCanceledAsync();
                }
            }
            catch when(Zeroed()){}
            catch(Exception e)when (!Zeroed())
            {
                _logger.Error(e, $"{Description}: {nameof(BlockOnReplicateAsync)} Failed!");
            }
        }

        /// <summary>
        /// Connect to an adjunct, turning it into a drone for use
        /// </summary>
        /// <returns>True on success, false otherwise</returns>
        protected async ValueTask<bool> ConnectAsync()
        {
            try
            {
                //Validate request
                if (!Assimilating && !IsDroneConnected)
                {
                    _logger.Fatal($"{Description}: Connect aborted, wrong state: {MetaDesc}");
                    PrintStateHistory();
                    return false;
                }

                if (CcCollective.Neighbors.TryGetValue(Key, out var existingNeighbor))
                {
                    try
                    {
                        if (existingNeighbor.Source.IsOperational())
                        {
                            _logger.Trace($"Drone already operational, dropping {existingNeighbor.Description}");
                            return false;
                        }

                        await existingNeighbor.Zero(this, $"Dropped because stale from {existingNeighbor.Description}").FastPath();
                    }
                    catch when (existingNeighbor?.Source?.Zeroed()?? true) {}
                    catch (Exception e)when(!existingNeighbor.Source.Zeroed())
                    {
                        _logger.Error(e,$"{nameof(ConnectAsync)}:");
                    }
                }

                await ZeroAsync(static async @this =>
                {
                    try
                    {
                        //TODO: DELAYS
                        //await Task.Delay(@this._random.Next(@this.parm_max_network_latency_ms>>4), @this.AsyncTasks.Token);
                        //await Task.Yield();

                        var ts = Environment.TickCount;
                        AdjunctState oldState;
                        if ((oldState = @this.CompareAndEnterState(AdjunctState.Connecting, AdjunctState.Fusing)) != AdjunctState.Fusing)
                        {
                            @this._logger.Trace($"{nameof(ConnectAsync)} - {@this.Description}: Invalid state, {oldState}. Wanted {nameof(AdjunctState.Fusing)}");
                            return;
                        }
                        
                        //Attempt the connection, race to win
                        if (!await @this.CcCollective.ConnectToDroneAsync(@this).FastPath())
                        {
                            @this.CompareAndEnterState(AdjunctState.Verified, AdjunctState.Connecting);
                            @this._logger.Trace($"{@this.Description}: Leashing adjunct failed!");
                        }
                        else //Track some connection perf stats
                        {
                            Interlocked.Add(ref ConnectionTime, ts.ElapsedMs());
                            Interlocked.Increment(ref ConnectionCount);
                        }
                    }
                    catch when (@this.Zeroed()){}
                    catch (Exception e) when (!@this.Zeroed())
                    {
                        @this._logger.Trace(e);
                    }
                }, this, TaskCreationOptions.DenyChildAttach);

                return true;
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger?.Error(e, $"{nameof(ConnectAsync)}:");
            }

            return false;
        }

        /// <summary>
        /// Unpacks a batch of messages
        ///
        /// Batching strategies needs to be carefully matched with the nature of incoming traffic, or bad things will happen.
        /// </summary>
        /// <param name="batchJob">The consumer that need processing</param>
        /// <param name="channel">The arbiter</param>
        /// <param name="processCallback">The process callback</param>
        /// <param name="nanite"></param>
        /// <returns>Task</returns>
        private async ValueTask ZeroUnBatchAsync<T>(IoSink<CcProtocBatchJob<chroniton, CcDiscoveryBatch>> batchJob,
            IoConduit<CcProtocBatchJob<chroniton, CcDiscoveryBatch>> channel,
            Func<CcDiscoveryMessage, CcDiscoveryBatch, IoConduit<CcProtocBatchJob<chroniton, CcDiscoveryBatch>>, T, CcAdjunct, IPEndPoint, ValueTask> processCallback, T nanite)
        {
            if (batchJob == null)
                return;

            CcDiscoveryBatch msgBatch = default;
            try
            {
                
                var job = (CcProtocBatchJob<chroniton, CcDiscoveryBatch>)batchJob;
                msgBatch = job.Get();
                
                //Grouped by ingress endpoint batches (this needs to make sense or disable)
                if (msgBatch.GroupByEpEnabled)
                {
                    foreach (var epGroups in msgBatch.GroupBy)
                    {
                        if (Zeroed())
                            break;
                        if (msgBatch.GroupBy.TryGetValue(epGroups.Key, out var msgGroup))
                        {
                            if(!msgGroup.Item2.Any())
                                continue;

                            var srcEndPoint = epGroups.Value.Item1.GetEndpoint();
                            var proxy = await RouteAsync(srcEndPoint, epGroups.Value.Item2[0].Chroniton.PublicKey).FastPath();
                            if (proxy != null)
                            {
                                foreach (var message in msgGroup.Item2)
                                {
                                    if (Zeroed())
                                        break;
                                    try
                                    {
                                        if(Equals(message.Chroniton.Header.Ip.Dst.GetEndpoint(), Router.MessageService.IoNetSocket.NativeSocket.LocalEndPoint)) 
                                            await processCallback(message, msgBatch, channel, nanite, proxy, srcEndPoint).FastPath();
                                        message.Chroniton = null;
                                        message.EmbeddedMsg = null;
                                    }
                                    catch (Exception) when (!Zeroed()) { }
                                    catch (Exception e) when (Zeroed())
                                    {
                                        _logger.Debug(e, $"Processing protocol failed for {Description}: ");
                                    }
                                }
                            }
                        }
                        msgBatch.GroupBy.Clear();
                    }
                    
                }
                else //ZeroDefault and safe mode
                {
                    byte[] cachedEp = null;
                    ByteString cachedPk = null;
                    CcAdjunct proxy = null;

                    for (var i = 0; i < msgBatch.Count; i++)
                    {
                        if (Zeroed())
                            break;

                        var message = msgBatch[i];
                        try
                        {
                            //TODO, is this caching a good idea? 
                            if (!_enableBatchEpCache  || proxy is not { IsProxy: true } || cachedEp == null || !cachedEp.ArrayEqual(message.EndPoint) && cachedPk == null || !cachedPk.Memory.Span.ArrayEqual(message.Chroniton.PublicKey.Span))
                            {
                                cachedEp = message.EndPoint;
                                cachedPk = message.Chroniton.PublicKey;
#if DEBUG
                                proxy = await RouteAsync(cachedEp.GetEndpoint(), cachedPk, message.Chroniton.Header.Ip.Src.GetEndpoint()).FastPath();
#else
                                //proxy = RouteAsync(message.Chroniton.Header.Ip.Src.GetEndpoint(), cachedPk);
                                //proxy = RouteAsync(cachedEp.GetEndpoint(), cachedPk, message.Chroniton.Header.Ip.Src.GetEndpoint());
                                //TODO route issue
                                proxy = await RouteAsync(cachedEp.GetEndpoint(), cachedPk).FastPath();
#endif

                            }
                            if (proxy != null && Equals(message.Chroniton.Header.Ip.Dst.GetEndpoint(), Router.MessageService.IoNetSocket.NativeSocket.LocalEndPoint))
                                await processCallback(message, msgBatch, channel, nanite, proxy, message.EndPoint.GetEndpoint()).FastPath();
                            message.Chroniton = null;
                            message.EmbeddedMsg = null;
                        }
                        catch (Exception) when (!Zeroed()) { }
                        catch (Exception e) when (Zeroed())
                        {
                            _logger.Debug(e, $"Processing protocol failed for {Description}: ");
                        }
                    }
                }

                batchJob.State = IoJobMeta.JobState.Consumed;
            }
            catch when(Zeroed()){}
            catch (Exception e)when(!Zeroed())
            {
                _logger.Debug(e, "Error processing message batch");
            }
            finally
            {
                try
                {
                    msgBatch.ReturnToHeap();
                }
                catch
                {
                    // ignored
                }
            }
        }

        /// <summary>
        /// Routes Messages according to src IP
        /// </summary>
        /// <param name="srcEndPoint">The source of the request</param>
        /// <param name="publicKey">The public key of the request</param>
        /// <param name="alternate">for debug purposes</param>
        /// <returns>The proxy</returns>
        private async ValueTask<CcAdjunct> RouteAsync(IPEndPoint srcEndPoint, ByteString publicKey, IPEndPoint alternate = null)
        {
            var key = srcEndPoint.ToString();
            var routed = Router._routingTable.TryGetValue(key, out var proxy);
            //Router
            try
            {
                if (!routed)
                {
                    if (Node == null)
                        return null;

                    proxy = (CcAdjunct)Hub.Neighbors.Values.FirstOrDefault(n => ((CcAdjunct)n).IsProxy && Equals(((CcAdjunct)n).RemoteAddress.IpEndPoint, srcEndPoint));
                    if (proxy != null && !proxy.Zeroed())
                    {
                        //TODO, proxy adjuncts need to malloc the same way when listeners spawn them.
                        proxy._routed = true;
                        if (!Router._routingTable.TryAdd(key, proxy))
                        {
                            proxy._routed = false;
                            Router._routingTable.TryGetValue(key, out proxy);
                        }
                        else
                        {
#if DEBUG
                            _logger?.Trace($"Adjunct [ROUTED]: {MessageService.IoNetSocket.LocalAddress} ~> {srcEndPoint}");
#endif
                        }
                    }
                }

                //verify pk
                if (proxy is { IsProxy: true } && !proxy.Designation.PublicKey.ArrayEqual(publicKey.Memory) && proxy.State < AdjunctState.Verified)//TODO: why do we need this last check?
                {
                    var pk1 = proxy.Designation.IdString();
                    var pk2 = CcDesignation.MakeKey(publicKey);

                    var msg = $"{nameof(Router)}: Dropped route {proxy.RemoteAddress}/{pk1}, key = {key}/{pk2}: state = {proxy.State} | {proxy.Description}, up = {proxy.UpTime.ElapsedMs()}ms, events = {proxy.EventCount}";
#if DEBUG
                    _logger?.Warn(msg);           
#endif
                    await proxy.Zero(this, msg).FastPath();
                    proxy = null;
                }
            }
            catch when (Zeroed() || (proxy?.Zeroed() ?? false)) { }
            catch (Exception e) when (!Zeroed() && !(proxy?.Zeroed() ?? false))
            {
                _logger.Error(e, $"{nameof(Router)} failed with errors: ");
                return null;
            }

            if (proxy != null)
            {
#if DEBUG
                if (proxy.Designation.IdString() != CcDesignation.MakeKey(publicKey))
                {
                    if (alternate != null)
                    {
                        var badProxy = proxy;
                        proxy = await RouteAsync(alternate, publicKey).FastPath();
                        if (proxy.IsProxy && proxy.Designation.IdString() != CcDesignation.MakeKey(publicKey))
                        {
                            _logger!.Warn($"Invalid routing detected wanted = {proxy.Designation.IdString()} - {proxy.RemoteAddress.IpEndPoint}, got = {CcDesignation.MakeKey(publicKey)} - {srcEndPoint}");
                            proxy = Router;
                        }
                    }
                }
                else
#endif
                {
                    IncEventCounter();
                    proxy.IncEventCounter();
                }
            }
            else
            {
                proxy = Router;
            }

            return proxy;
        }

        /// <summary>
        /// Processes protocol messages
        /// </summary>
        /// <returns></returns>
        private async ValueTask ProcessDiscoveriesAsync()
        {
            try
            {
                //ensure the channel
                do
                {
                    _protocolConduit ??= MessageService.GetConduit<CcProtocBatchJob<chroniton, CcDiscoveryBatch>>(nameof(CcAdjunct));

                    if(_protocolConduit != null)
                        break;

                    //Get the conduit
                    _logger.Trace($"Waiting for {Description} stream to spin up...");
                    
                    await Task.Delay(parm_conduit_spin_up_wait_time++, AsyncTasks.Token);
                } while (_protocolConduit == null && !Zeroed());

            
                //fail fast on these
                if(_protocolConduit.Zeroed())
                    return;

                do{
//The producer
                    ValueTask consumer;
                    var producer = ZeroOptionAsync(static async @this =>
                    {
                        try
                        {
                            var width = @this._protocolConduit.Source.PrefetchSize;
                            var preload = new ValueTask<bool>[width];
                            while (!@this.Zeroed() && @this._protocolConduit.UpstreamSource.IsOperational())
                            {
                                for (var i = 0; i < width && @this._protocolConduit.UpstreamSource.IsOperational(); i++)
                                {
                                    try
                                    {
                                        preload[i] = @this._protocolConduit.ProduceAsync();
                                    }
                                    catch (Exception e)
                                    {
                                        @this._logger.Error(e, $"Production failed for {@this.Description}");
                                        break;
                                    }
                                }

                                var j = 0;
                                while (await preload[j].FastPath() && ++j < width) { }

                                if (j < width)
                                    break;

                                if(!await @this._zeroSync.WaitAsync().FastPath())
                                    break;
                            }
                        }
                        catch when (@this.Zeroed() || @this.Source.Zeroed() || @this._protocolConduit?.UpstreamSource == null) { }
                        catch (Exception e) when (!@this.Zeroed())
                        {
                            @this._logger?.Error(e, $"{@this.Description}");
                        }
                    
                    },this, TaskCreationOptions.LongRunning | TaskCreationOptions.PreferFairness);

                    consumer = ZeroOptionAsync(static async @this  =>
                    {
                        //the consumer
                        try
                        {
                            var width = @this._protocolConduit.Source.PrefetchSize;
                            var preload = new ValueTask<bool>[width];

                            while (!@this.Zeroed() && @this._protocolConduit.UpstreamSource.IsOperational())
                            {
                                //consume
                                for (var i = 0; i < width && @this._protocolConduit.UpstreamSource.IsOperational(); i++)
                                {
                                    
                                    preload[i] = @this._protocolConduit.ConsumeAsync(ProcessMessages(), @this);

                                    [MethodImpl(MethodImplOptions.AggressiveInlining)]
                                    static Func<IoSink<CcProtocBatchJob<chroniton, CcDiscoveryBatch>>, CcAdjunct, ValueTask> ProcessMessages()
                                    {
                                        return static async (batchJob, @this) =>
                                        {
                                            try
                                            {
                                                await @this.ZeroUnBatchAsync(batchJob, @this._protocolConduit, static async (batchItem, discoveryBatch, ioConduit, @this, currentRoute, srcEndPoint) =>
                                                {
                                                    IMessage message = default;
                                                    chroniton packet = default;
                                                    try
                                                    {
                                                        message = batchItem.EmbeddedMsg;
                                                        batchItem.EmbeddedMsg = null;
                                                        packet = batchItem.Chroniton;
                                                        batchItem.Chroniton = null;

                                                        if (packet.Data.Length == 0)
                                                        {
                                                            @this._logger.Warn($"Got zero message from {CcDesignation.MakeKey(packet.PublicKey.Memory.AsArray())}");
                                                            return;
                                                        }
                                                        
                                                        if (!@this.Zeroed() && !currentRoute.Zeroed())
                                                        {
                                                            try
                                                            {
                                                                //switch ((CcDiscoveries.MessageTypes)MemoryMarshal.Read<int>(packet.Signature.Span))
                                                                switch ((CcDiscoveries.MessageTypes)packet.Type)
                                                                {
                                                                    case CcDiscoveries.MessageTypes.Probe:
                                                                        if(((CcProbeMessage)message).Timestamp.ElapsedUtcMs() < @this.parm_max_network_latency_ms * 2)
                                                                            await currentRoute.ProcessAsync((CcProbeMessage)message, srcEndPoint, packet).FastPath();
                                                                        break;
                                                                    case CcDiscoveries.MessageTypes.Probed:
                                                                        if (((CcProbeResponse)message).Timestamp.ElapsedUtcMs() < @this.parm_max_network_latency_ms * 2)
                                                                            await currentRoute.ProcessAsync((CcProbeResponse)message, srcEndPoint, packet).FastPath();
                                                                        break;
                                                                    case CcDiscoveries.MessageTypes.Scan:
                                                                        if (!currentRoute.Verified)
                                                                        {
#if DEBUG
                                                                            @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.Scan)}: Unrouted request from {srcEndPoint} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}");
#endif
                                                                            break;
                                                                        }
                                                                        if (((CcScanRequest)message).Timestamp.ElapsedUtcMs() < @this.parm_max_network_latency_ms * 2)
                                                                            await currentRoute.ProcessAsync((CcScanRequest)message, srcEndPoint, packet).FastPath();
                                                                        break;
                                                                    case CcDiscoveries.MessageTypes.Adjuncts:
                                                                        if (!currentRoute.Verified)
                                                                        {
#if DEBUG
                                                                            @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.Adjuncts)}: Unrouted request from {srcEndPoint} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}");                                                               
#endif
                                                                            break;
                                                                        }
                                                                        if (((CcAdjunctResponse)message).Timestamp.ElapsedUtcMs() < @this.parm_max_network_latency_ms * 2)
                                                                            await currentRoute.ProcessAsync((CcAdjunctResponse) message, srcEndPoint, packet).FastPath();
                                                                        break;
                                                                    case CcDiscoveries.MessageTypes.Fuse:
                                                                        //TODO feat: add weak proxy test 
                                                                        if (((CcFuseRequest)message).Timestamp.ElapsedUtcMs() < @this.parm_max_network_latency_ms * 2)
                                                                            await currentRoute.ProcessAsync((CcFuseRequest)message, srcEndPoint, packet).FastPath();
                                                                        break;
                                                                    case CcDiscoveries.MessageTypes.Fused:
                                                                        if (!currentRoute.Verified)
                                                                        {
#if DEBUG
                                                                            @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.Fused)}: p = {currentRoute.IsProxy}: Unrouted request from {srcEndPoint} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}, {((CcFuseResponse)message).Timestamp.ElapsedMs()}ms");
#endif
                                                                            break;
                                                                        }

                                                                        if (((CcFuseResponse)message).Timestamp.ElapsedUtcMs() < @this.parm_max_network_latency_ms * 2)
                                                                            await currentRoute.ProcessAsync((CcFuseResponse) message, srcEndPoint, packet).FastPath();
                                                                        break;
                                                                    case CcDiscoveries.MessageTypes.Defuse:
                                                                        if (@this.CcCollective.ZeroDrone || !currentRoute.Verified)
                                                                        {
#if DEBUG
                                                                            @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.Defuse)}: Unrouted request from {srcEndPoint} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}");
#endif
                                                                            break;
                                                                        }

                                                                        if(((CcDefuseRequest)message).Timestamp.ElapsedUtcMs() < @this.parm_max_network_latency_ms * 2)
                                                                            await currentRoute.ProcessAsync((CcDefuseRequest) message, srcEndPoint, packet).FastPath();
                                                                        break;
                                                                }
                                                            }
                                                            catch when(@this.Zeroed() || currentRoute.Zeroed()){}
                                                            catch (Exception e) when (!@this.Zeroed() && !currentRoute.Zeroed())
                                                            {
                                                                @this._logger?.Error(e, $"{message!.GetType().Name} [FAILED]: l = {packet!.Data.Length}, {@this.Key}");
                                                            }
                                                        }
                                                    }
                                                    catch when(@this.Zeroed()) {}
                                                    catch (Exception e) when (!@this.Zeroed())
                                                    {
                                                        @this._logger?.Error(e, $"{message!.GetType().Name} [FAILED]: l = {packet!.Data.Length}, {@this.Key}");
                                                    }
                                                }, @this).FastPath();
                                            }
                                            finally
                                            {
                                                if (batchJob != null && batchJob.State != IoJobMeta.JobState.Consumed)
                                                    batchJob.State = IoJobMeta.JobState.ConsumeErr;
                                            }
                                        };
                                    }
                                }

                                var j = 0;
                                while (await preload[j].FastPath() && ++j < width) { }

                                @this._zeroSync.SetResult(j == width);

                                if (j < width)
                                    break;
                            }
                        }
                        catch when(@this.Zeroed() || @this._protocolConduit?.UpstreamSource == null) {}
                        catch (Exception e) when (!@this.Zeroed() && @this._protocolConduit?.UpstreamSource != null)
                        {
                            @this._logger?.Error(e, $"{@this.Description}");
                        }
                    }, this, TaskCreationOptions.LongRunning | TaskCreationOptions.PreferFairness);
                    await Task.WhenAll(producer.AsTask(), consumer.AsTask());
                }
                while (!Zeroed());
            }
            catch when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger?.Debug(e, $"Error processing {Description}");
            }

            if (!Zeroed())
            {
                _logger?.Fatal($"{Description} Stopped prosessing discoveries.");
            }
        }

        /// <summary>
        /// Defuse drop request
        /// </summary>
        /// <param name="request">The request</param>

        /// <param name="src"></param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcDefuseRequest request, IPEndPoint src, chroniton packet)
        {
            if (!Assimilating)
            {
                if(IsProxy)
                    _logger.Error($"{nameof(CcDefuseRequest)}: Ignoring {request.Timestamp.ElapsedMsToSec()}s old/invalid request, {Description}");
                return;
            }
            
            //PAT
            LastPat = Environment.TickCount;

            if (IsDroneAttached)
            {
                _logger.Trace($"{nameof(CcDefuseRequest)}: {Direction} Drone = {_drone.Key}: {Description}, {MetaDesc}");
                await DetachDroneAsync().FastPath();
            }
            //else
            //{
            //    //Hup
            //    if (Direction == Heading.Undefined && CcCollective.IngressCount < CcCollective.parm_max_inbound)
            //        await ProbeAsync();
            //}
        }

        /// <summary>
        /// Fusing Request message from client
        /// </summary>
        /// <param name="request">The request</param>
        /// <param name="src">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcFuseRequest request, IPEndPoint src, chroniton packet)
        { 
            //emit event
            if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                AutoPeeringEventService.AddEvent(new AutoPeerEvent
                {
                    EventType = AutoPeerEventType.AddAdjunct,
                    Adjunct = new Adjunct
                    {
                        Id = Designation.IdString(),
                        CollectiveId = CcCollective.Hub.Router.Designation.IdString()
                    }
                });

            //fail fast
            int sent;
            if (!Assimilating || CcCollective.ZeroDrone)
            {
                //send reject so that the sender's state can be fixed
                var reject = new CcFuseResponse()
                {
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    ReqHash = UnsafeByteOperations.UnsafeWrap(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                    Accept = false
                };

                var remote = IoNodeAddress.CreateFromEndpoint("udp", src);
                //var remote = IoNodeAddress.CreateFromEndpoint("udp", packet.Header.Ip.Src.GetEndpoint());
                if ((sent = await Router.SendMessageAsync(reject.ToByteArray(), CcDiscoveries.MessageTypes.Fused, remote).FastPath()) > 0)
                {
                    _logger.Trace($"-/> {nameof(CcFuseResponse)}({sent}): Reply reject to {remote} [OK], {Description}");
                }
                else
                    _logger.Error($"<\\- {nameof(CcFuseRequest)}({sent}): Reply to {remote} [FAILED], {Description}, {MetaDesc}");


                //if (!Collected) return;

                //_stealthy = Environment.TickCount;
                ////DMZ-syn
                ////We syn here (Instead of in process ping) to force the other party to do some work (prepare to connect) before we do work (verify).
                //if ((CcCollective.Hub.Neighbors.Count <= CcCollective.MaxDrones || CcCollective.ZeroDrone) && !await Router
                //        .ProbeAsync("ACK-SYN-ACK", remote, CcDesignation.FromPubKey(packet.PublicKey.Memory).IdString())
                //        .FastPath())
                //{
                //    _logger.Trace($"Failed to send ACK-SYN-ACK to {remote}! {Description}");
                //}
                return;
            }

            Interlocked.Increment(ref _fusedCount);
            //PAT
            LastPat = Environment.TickCount;

            var fuseResponse = new CcFuseResponse
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                ReqHash = UnsafeByteOperations.UnsafeWrap(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                Accept = CcCollective.IngressCount < CcCollective.parm_max_inbound && _direction == 0
            };

            if (fuseResponse.Accept)
            {
                AdjunctState curState = AdjunctState.Undefined;
                var stateIsValid = (curState = CompareAndEnterState(AdjunctState.Fusing, AdjunctState.Verified, overrideHung: parm_max_network_latency_ms)) == AdjunctState.Verified;

                if (!stateIsValid)
                {
                    if (curState == AdjunctState.Fusing &&
                        Hub.Designation < CcDesignation.FromPubKey(packet.PublicKey.Memory))
                    {
                        stateIsValid = true;//avert deadlock
                        var recover = CompareAndEnterState(AdjunctState.Rusing, AdjunctState.Fusing, overrideHung: 0) == curState;
                    }
                }
                else if(curState == AdjunctState.Fusing)//revert hung state
                {
                    var recover = CompareAndEnterState(AdjunctState.Verified, curState, overrideHung: 0) == curState;
                    _logger.Trace($"recover = {recover}, {Hub.Designation} < {CcDesignation.FromPubKey(packet.PublicKey.Memory)} = {Hub.Designation < CcDesignation.FromPubKey(packet.PublicKey.Memory)}");
                }
                
                fuseResponse.Accept &= stateIsValid;
                if (!stateIsValid && _currState.EnterTime.ElapsedMs() > parm_max_network_latency_ms)
                {
                    _logger.Warn($"{Description}: Invalid state ~{_currState.Value}, age = {_currState.EnterTime.ElapsedMs()}ms. Wanted {nameof(AdjunctState.Verified)} -  [RACE OK!]");
                }
            }

            if ((sent= await SendMessageAsync(fuseResponse.ToByteArray(),
                    CcDiscoveries.MessageTypes.Fused).FastPath()) > 0)
            {
                var response = $"{(fuseResponse.Accept ? "accept" : "reject")}";

                _logger.Debug($"-/> {nameof(CcFuseResponse)}({sent}): Reply {response}, {Description}");

                if(fuseResponse.Accept)
                    _logger.Debug($"# {Description}");

                if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                    AutoPeeringEventService.AddEvent(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.SendProtoMsg,
                        Msg = new ProtoMsg
                        {
                            CollectiveId = Hub.Router.Designation.IdString(),
                            Id = Designation.IdString(),
                            Type = $"peer response {response}"
                        }
                    });
            }
            else
                _logger.Debug($"<\\- {nameof(CcFuseRequest)}: Send fuse response [FAILED], {Description}, {MetaDesc}");

            if (!CcCollective.ZeroDrone && !fuseResponse.Accept)
            {
                await SeduceAsync("SYN-FU", Heading.Both, force:true).FastPath();
            }
        }

        private long _stealthy;

        /// <summary>
        /// Peer response message from client
        /// </summary>
        /// <param name="response">The request</param>
        /// <param name="src">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcFuseResponse response, IPEndPoint src, chroniton packet)
        {
            var f1 = _fuseRequest.Count;
            var f2 = Router._fuseRequest.Count;
            var fuseMessage = await _fuseRequest.ResponseAsync(src.ToString(), response.ReqHash).FastPath();

            if(!fuseMessage)
                fuseMessage = await Router._fuseRequest.ResponseAsync(src.ToString(), response.ReqHash).FastPath();

            if (!fuseMessage)
            {
                if (IsProxy)
                {
                    if (response.ReqHash.Length > 0 && _fuseRequest.Count > 0)
                    {
                        _logger.Error($"<\\- {nameof(CcFuseResponse)}({packet.CalculateSize()}, {response.CalculateSize()}, {response.Timestamp.ElapsedMs()}ms) - {packet.Data.Memory.HashSig()}({packet.Data.Length}): No-Hash!!!, r = {response.ReqHash.Memory.HashSig()}({response.ReqHash.Length}), _fuseRequest = {f1}({_fuseRequest.Count}), {f2}({Router._fuseRequest.Count}), {MessageService.IoNetSocket.LocalNodeAddress} ~> {src}({RemoteAddress})");
                    }
#if DEBUG
                    _logger.Warn($"<\\- {nameof(CcFuseResponse)}({packet.CalculateSize()}, {response.CalculateSize()}, {response.Timestamp.ElapsedMs()}ms) - {packet.Data.Memory.HashSig()}({packet.Data.Length}): No-Hash!!!, r = {response.ReqHash.Memory.HashSig()}({response.ReqHash.Length}), _fuseRequest = {f1}({_fuseRequest.Count}), {f2}({Router._fuseRequest.Count}), {MessageService.IoNetSocket.LocalNodeAddress} ~> {src}({RemoteAddress})");
#endif
                }

                return;
            }
            
            //Validated
            _logger.Debug($"<\\- {nameof(CcFuseResponse)}: Accepted = {response.Accept}, {Description}");

            //PAT
            LastPat = Environment.TickCount;

            switch (response.Accept)
            {
                //Race for 
                case true when _direction == 0 && _currState.Value == AdjunctState.Fusing:
                {
                    if (!await ConnectAsync().FastPath())
                    {
                        _logger.Trace($"{nameof(CcFuseResponse)}: FAILED to connect! Defusing..., {Description}");
                    }
                    break;
                }
                //at least probe
                case false when CcCollective.Neighbors.Count < CcCollective.MaxDrones:
                {
                    AdjunctState oldState;
                    if (_currState.Value != AdjunctState.Unverified &&
                        !(_currState.Value == AdjunctState.Verified && _currState is not { Prev.Value: AdjunctState.Unverified }) &&
                        (oldState = CompareAndEnterState(AdjunctState.Verified, AdjunctState.Fusing)) != AdjunctState.Fusing)
                    {
                        if(oldState != AdjunctState.Connected && oldState != AdjunctState.Rusing && _currState.EnterTime.ElapsedMs() > parm_max_network_latency_ms)
                            _logger.Warn($"{nameof(CcFuseResponse)} - {Description}: Invalid state, {oldState}, age = {_currState.EnterTime.ElapsedMs()}ms. Wanted {nameof(AdjunctState.Fusing)}");
                    }
                    else
                    {
                        if (!await SeduceAsync("SYN-FM", Heading.Ingress, force: true).FastPath())
                        {
                            if (!await ScanAsync(CcCollective.parm_mean_pat_delay_s * 1000).FastPath())
                            {
                                _logger.Trace($"{nameof(ScanAsync)} skipped!");
                            }
                        }

                        if (IsProxy)
                        {
                            var setting = -1;
                            if (CcCollective.Hub.Neighbors.Count <= CcCollective.parm_max_drone)
                                setting = 0;
                            await ScanAsync(setting).FastPath();
                        }
                    }
                    break;
                }
                case false:
                {
                    AdjunctState oldState;
                    if (_currState.Value != AdjunctState.Unverified &&
                        !(_currState.Value == AdjunctState.Verified && _currState is not { Prev.Value: AdjunctState.Unverified }) &&
                        (oldState = CompareAndEnterState(AdjunctState.Verified, AdjunctState.Fusing)) != AdjunctState.Fusing)
                    {
                        if (oldState != AdjunctState.Connected && _currState.EnterTime.ElapsedMs() > parm_max_network_latency_ms)
                            _logger.Warn($"{nameof(CcFuseResponse)}(f) - {Description}: Invalid state, {oldState}, age = {_currState.EnterTime.ElapsedMs()}ms. Wanted {nameof(AdjunctState.Fusing)}");
                    }

                    break;
                }
            }
        }

#pragma warning disable CA2211 // Non-constant fields should not be visible
        public static long ConnectionTime;
        public static long ConnectionCount;
#pragma warning restore CA2211 // Non-constant fields should not be visible

        /// <summary>
        /// Sends a message to the neighbor
        /// </summary>
        /// <param name="data">The message data</param>
        /// <param name="dest">The destination address</param>
        /// <param name="type">The message type</param>
        /// <returns></returns>
        private async ValueTask<int> SendMessageAsync(byte[] data, CcDiscoveries.MessageTypes type = CcDiscoveries.MessageTypes.Undefined, IoNodeAddress dest = null)
        {
            try
            {
                if (Zeroed())
                    return 0;
                
                if (data == null || data.Length == 0 || dest != null && Equals(dest, Router.MessageService.IoNetSocket.LocalNodeAddress))
                    return 0;
                
                dest ??= RemoteAddress;

                chroniton packet = null;
                try
                { 
                    packet = _chronitonHeap.Take();

                    if (packet == null)
                        throw new OutOfMemoryException($"{nameof(_chronitonHeap)}: {_chronitonHeap.Description}, {Description}");

                    packet.Header.Ip.Dst = dest.IpEndPoint.AsByteString();

#if DEBUG
                    packet.Header.Ip.Src = ((IPEndPoint)MessageService.IoNetSocket.NativeSocket.LocalEndPoint).AsByteString();
#else
                    //TODO routing bug
                    //packet.Header.Ip.Src = ((IPEndPoint)Router.MessageService.IoNetSocket.NativeSocket.LocalEndPoint).AsByteString();
#endif
                    packet.Data = UnsafeByteOperations.UnsafeWrap(data);
                    packet.Type = (int)type;
                    
                    var packetMsgRaw = packet.Data.Memory.AsArray();
                    packet.Signature = UnsafeByteOperations.UnsafeWrap(CcCollective.CcId.Sign(packetMsgRaw, 0, packetMsgRaw.Length));

                    Tuple<byte[],byte[]> buf = null;
                    try
                    {
                        buf = _sendBuf.Take();
                        if (buf == null)
                            throw new OutOfMemoryException($"{nameof(_sendBuf)}, {_sendBuf.Description}");

                        var packetLen = packet.CalculateSize();

                        if (packetLen > short.MaxValue)
                            throw new InvalidOperationException($"Max message size supported is {short.MaxValue} bytes, got {packetLen} bytes");

                        packet.WriteTo(buf.Item1.AsSpan(0, packetLen));
                        var length = (ulong)(packetLen + sizeof(ulong));
                        buf.Item1[length] = 0;

                        length = (ulong)LZ4Codec.Encode(buf.Item1,0, packetLen, buf.Item2, sizeof(ulong), buf.Item2.Length - sizeof(ulong));
                        MemoryMarshal.Write(buf.Item2, ref length);
                        //Console.WriteLine($"[0] = {buf.Item2[8]}, [1] = {buf.Item2[9]}, [2] = {buf.Item2[10]}, [3] = {buf.Item2[11]}");
                        //Console.WriteLine($"[0] = {buf.Item2[length - 4] + sizeof(ulong)}, [1] = {buf.Item2[length - 3 + sizeof(ulong)]}, [2] = {buf.Item2[length - 2 + sizeof(ulong)]}, [3] = {buf.Item2[length - 1 + sizeof(ulong)]}");
                        //Console.WriteLine($"[0] = {buf.Item1[0]}, [1] = {buf.Item1[1]}, [2] = {buf.Item1[2]}, [3] = {buf.Item1[3]}");
                        //Console.WriteLine($"[0] = {buf.Item1[packetLen - 4]}, [1] = {buf.Item1[packetLen - 3]}, [2] = {buf.Item1[packetLen - 2]}, [3] = {buf.Item1[packetLen - 1]}");
                        //Console.WriteLine($"original len = {packetLen}");
                        //simulate byzantine failure.                
#if LOSS
                        var sent = 0;
                        var loss = 51;
                        if (_random.Next(100) < loss) //drop
                        {
                            sent = msgRaw.Length;
                        } 
                        else if(_random.Next(100) < loss) //duplicate
                        {
                            sent = await MessageService.IoNetSocket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint)
                                .FastPath().ConfigureAwait(ZC);
                            
                            sent = await MessageService.IoNetSocket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint)
                                .FastPath().ConfigureAwait(ZC);
                        }
                        else //nominal
                        {
                            sent = await MessageService.IoNetSocket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint)
                                .FastPath().ConfigureAwait(ZC);
                        }

                        return sent;
#else
#if MOCK_ZERO_CONNECTION
                        if (RandomNumberGenerator.GetInt32(0, 100) > 50)
                        {
                            var s = RandomNumberGenerator.GetInt32(20, (int)(length / 2));
                            var s2 = RandomNumberGenerator.GetInt32(s + 1, (int)(length - 20));
                            //Console.WriteLine($"S = {s}/{s2}/{length - sizeof(ulong)}");
                            //Console.WriteLine($"S = {s}/{s2 - s}/{(int)(length - (ulong)s2)}");
                            await MessageService.IoNetSocket.SendAsync(buf.Item2, 0, s, dest.IpEndPoint).FastPath();
                            await MessageService.IoNetSocket.SendAsync(buf.Item2, s, s2 - s, dest.IpEndPoint).FastPath();
                            return await MessageService.IoNetSocket.SendAsync(buf.Item2, s2, (int)(length - (ulong)s2) + sizeof(ulong), dest.IpEndPoint).FastPath();
                        }
                        else
                        {
                            return await MessageService.IoNetSocket.SendAsync(buf.Item2, 0, (int)length + sizeof(ulong), dest.IpEndPoint);
                        }
#else
                        return await MessageService.IoNetSocket.SendAsync(buf.Item2, 0, (int)length + sizeof(ulong), dest.IpEndPoint).FastPath();
#endif
                    }
                    finally
                    {   
                        _sendBuf.Return(buf);
                    }
#endif
                }
                catch when (Zeroed()) { }
                catch (Exception e) when (!Zeroed())
                {
                    _logger.Debug(e, $"{nameof(SendMessageAsync)} Failed to send message {Description}");
                }
                finally
                {
                    if (packet != null)
                    {
                        packet.Data = ByteString.Empty;
                        packet.Signature = ByteString.Empty;
                        packet.Header.Ip.Dst = ByteString.Empty;
                        _chronitonHeap.Return(packet);
                    }
                }
            }
            catch when(Zeroed()){}
            catch (Exception e)when(!Zeroed())
            {
                _logger.Debug(e, $"Failed to send message {Description}");
            }

            return 0;
        }

        /// <summary>
        /// Scan response
        /// </summary>
        /// <param name="response">The response</param>
        /// <param name="src">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcAdjunctResponse response, IPEndPoint src, chroniton packet)
        {
            Interlocked.Increment(ref _scanCount);

            var matchRequest = await _scanRequest.ResponseAsync(src.ToString(), response.ReqHash).FastPath();
            if(!matchRequest)
                matchRequest = await Router._scanRequest.ResponseAsync(src.ToString(), response.ReqHash).FastPath();

            if (!matchRequest || !Assimilating || response.Contacts.Count > parm_max_swept_drones)
            {
                if (IsProxy && response.Contacts.Count <= parm_max_swept_drones)
                    _logger.Debug($"{nameof(CcAdjunctResponse)}{response.ToByteArray().PayloadSig()}: Reject, rq = {response.ReqHash.Memory.HashSig()}, {response.Contacts.Count} > {parm_max_swept_drones}? from {CcDesignation.FromPubKey(packet.PublicKey.Memory).IdString()}, RemoteAddress = {RemoteAddress}, c = {_scanRequest.Count}, matched[{src}]");
                return;
            }

            _logger.Debug($"<\\- {nameof(CcAdjunctResponse)}: Received {response.Contacts.Count} potentials from {Description}");

            _lastScan = Environment.TickCount;

            //PAT
            LastPat = Environment.TickCount;
            var processed = 0;
            foreach (var sweptDrone in response.Contacts)
            {
                //Any services attached?
                //if (responsePeer.Services?.Map == null || responsePeer.Services.Map.Count == 0)
                //{
                //    _logger.Trace(
                //        $"<\\- {nameof(CcAdjunctResponse)}: Invalid services recieved!, map = {responsePeer.Services?.Map}, count = {responsePeer.Services?.Map?.Count ?? -1}");
                //    continue;
                //}

                ////ignore strange services
                //if (count > parm_max_services)
                //    continue;

                //Never add ourselves (by ID)
                if (sweptDrone.PublicKey.Memory.ArrayEqual(CcCollective.CcId.PublicKey))
                    continue;

                //Don't add already known neighbors
                var id = CcDesignation.MakeKey(sweptDrone.PublicKey);
                
                if (Hub.Neighbors.Values.Any(n => ((CcAdjunct) n).Designation.IdString() == id))
                    continue;

                var newRemoteEp = sweptDrone.Url.GetEndpoint();
                
                if (!newRemoteEp.Equals(Router.MessageService.IoNetSocket.NativeSocket.RemoteEndPoint) && 
                    !await Router.ProbeAsync("DMZ-SYN-SCA", IoNodeAddress.CreateFromEndpoint("udp", newRemoteEp)).FastPath())
                {
#if DEBUG
                    _logger.Trace($"{Description}: Probing swept {newRemoteEp.Address} failed!");
#endif
                }
                processed++;
            }
        }

        /// <summary>
        /// Collect another adjunct into the collective
        /// </summary>
        /// <param name="newRemoteEp">The location</param>
        /// <param name="designation">The adjunctId</param>
        /// <param name="verified">Whether this designation  has already been verified</param>
        /// <returns>A task, true if successful, false otherwise</returns>
        private async ValueTask<bool> CollectAsync(IPEndPoint newRemoteEp, CcDesignation designation, bool verified)
        {
            if (Zeroed() || Hub.Zeroed() || newRemoteEp != null && newRemoteEp.Equals(RemoteAddress?.IpEndPoint) || CcCollective.Hub.Neighbors.ContainsKey(designation.IdString()))
            {
                _logger.Trace($"<x {designation.IdString()}");
                return false;
            }

            var newAdjunct = (CcAdjunct) Hub.MallocNeighbor(Hub, MessageService, Tuple.Create(designation, newRemoteEp, verified));

            if (!Zeroed() && await Hub.ZeroAtomic(static async (s, state, ___) =>
                {
                    var (@this, newAdjunct) = state;
                    try
                    {
                        if (@this.Hub.Neighbors.Count > @this.CcCollective.MaxAdjuncts)
                        {
                            //drop something
                            var bad = @this.Hub.Neighbors.Values.Where(n =>
                                    ((CcAdjunct)n).IsProxy &&
                                    (
                                        ((CcAdjunct) n).SecondsSincePat > @this.CcCollective.parm_mean_pat_delay_s ||
                                        ((CcAdjunct) n).UpTime.ElapsedMs() > @this.parm_min_uptime_ms &&
                                        ((CcAdjunct) n).State < AdjunctState.Verified)
                                )
                                .OrderByDescending(n => ((CcAdjunct)n).UpTime.ElapsedMs());

                            var good = @this.Hub.Neighbors.Values.Where(n =>
                                    ((CcAdjunct)n).IsProxy &&
                                    (
                                        ((CcAdjunct)n).UpTime.ElapsedMs() > @this.parm_min_uptime_ms &&
                                        ((CcAdjunct)n).State < AdjunctState.Fusing &&
                                        (@this.CcCollective.ZeroDrone || ((CcAdjunct)n).TotalPats > @this.parm_min_pats_before_shuffle))
                                )
                                .OrderByDescending(n => ((CcAdjunct)n).Priority)
                                .ThenByDescending(n => ((CcAdjunct)n).UpTime.ElapsedMs());

                            var badList = bad.ToList();
                            if (badList.Any())
                            {
                                var dropped = badList.FirstOrDefault();
                                if (dropped != default && ((CcAdjunct)dropped).State < AdjunctState.Verified) 
                                {
                                    await ((CcAdjunct)dropped).Zero(@this,"got collected").FastPath();
                                    @this._logger.Debug($"@ {dropped.Description}");
                                }
                            }
                            else //try harder 
                            {
                                foreach (var ioNeighbor in good)
                                {
                                    if (((CcAdjunct)ioNeighbor).State < AdjunctState.Connected)
                                    {
                                        await ((CcAdjunct)ioNeighbor).Zero(@this,"Assimilated!").FastPath();
                                        @this._logger.Debug($"@ {ioNeighbor.Description}");
                                        break;
                                    }
                                }
                            }
                        }
                    
                        //Transfer?
                        if (@this.Hub.Neighbors.Count > @this.CcCollective.MaxAdjuncts || !@this.Hub.Neighbors.TryAdd(newAdjunct.Key, newAdjunct))
                        {
                            await newAdjunct.Zero(@this,$"Adjunct already exists, dropping {newAdjunct.Description}").FastPath();
                            return false;    
                        }
                        return true;
                    }
                    catch when(@this.Zeroed()){}
                    catch (Exception e) when(!@this.Zeroed())
                    {
                        @this._logger.Error(e, $"{@this.Description??"N/A"}");
                    }

                    return false;
                }, ValueTuple.Create(this, newAdjunct)).FastPath())
            {
                //Start processing on adjunct
                await ZeroAsync(static async state =>
                {
                    var (@this, newAdjunct) = state;
                    await newAdjunct.BlockOnReplicateAsync();
                },(this, newAdjunct), TaskCreationOptions.DenyChildAttach).FastPath();
                
                //setup conduits to messages
#if DEBUG
                newAdjunct.DebugAddress = DebugAddress;
#endif
                if (!verified)
                {
                    //await Task.Delay(parm_max_network_latency_ms / 2 + RandomNumberGenerator.GetInt32(0, parm_max_network_latency_ms), AsyncTasks.Token);
                    
                    if (!await newAdjunct.ProbeAsync("SYN-VCK").FastPath())
                    {
                        _logger.Trace($"{nameof(ProbeAsync)}: SYN-VCK [FAILED], {newAdjunct.Description}");
                        newAdjunct?.Zero(this, "CollectAsync failed! -> SYN-VCK [FAILED]");
                        return false;
                    }
                }
                else
                {
                    //PAT
                    LastPat = Environment.TickCount;

                    //TODO: vector?
                    //set ext address as seen by neighbor
#if DEBUG
                    DebugAddress = IoNodeAddress.CreateFromEndpoint("udp", newRemoteEp);
#endif
                    NatAddress = IoNodeAddress.CreateFromEndpoint("udp", newRemoteEp);

                    Verified = true;
#if DEBUG
                    if (CcCollective.ZeroDrone)
                        _logger.Warn($"Verified with queen `{newRemoteEp}' ~> {MessageService.IoNetSocket.LocalAddress} ");

                    _logger.Trace($"* {nameof(CollectAsync)}: Collected: {Description}");
#endif
                    //TODO: DELAYS
                    //await Task.Delay(parm_max_network_latency_ms / 2 + RandomNumberGenerator.GetInt32(0, parm_max_network_latency_ms), AsyncTasks.Token);

                    await SeduceAsync("ACK-SYN", Heading.Ingress, NatAddress, force: true).FastPath();
                }
                
                return true;
            }

            newAdjunct?.Zero(this,"CollectAsync failed!");
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="src"></param>
        /// <param name="packet"></param>
        /// <returns></returns>
        private async ValueTask ProcessAsync(CcScanRequest request, IPEndPoint src, chroniton packet)
        {
            if (!(CcCollective.ZeroDrone || Assimilating))
            {
                if (Assimilating)
                {
                    _logger.Trace(
                        $"<\\- {nameof(CcScanRequest)}: [ABORTED] {!Assimilating}, age = {request.Timestamp.CurrentUtcMsDelta():0.0}ms/{parm_max_network_latency_ms:0.0}ms, {Description}, {MetaDesc}");
                }
                    
                return;
            }

            //PAT
            LastPat = Environment.TickCount;

            var sweptResponse = new CcAdjunctResponse
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                ReqHash = UnsafeByteOperations.UnsafeWrap(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                Contacts = {}
            };

            var count = 0;
            // var certified = Hub.Neighbors.Values.Where(n =>
            //         ((CcNeighbor) n).Verified && n != this && !((CcNeighbor) n).IsLocal)
            //     .OrderByDescending(n => (int) ((CcNeighbor) n).Priority).ToList();
            var certified = Hub.Neighbors.Values.Where(
                    n => n != this && ((CcAdjunct) n).Assimilating)
                .OrderByDescending(n => ((CcAdjunct)n).IsDroneConnected? 0 : 1)
                .ThenByDescending(n => (int) ((CcAdjunct) n).Priority)
                .ThenBy(n => ((CcAdjunct)n).UpTime.ElapsedMs()).ToList();

            //if (!certified.Any())
            //{
            //    certified = Hub.Neighbors.Values.Where(n => n != this)
            //        .OrderByDescending(n => ((CcAdjunct)n).IsDroneConnected ? 0 : 1)
            //        .ThenByDescending(n => (int)((CcAdjunct)n).Priority)
            //        .ThenBy(n => ((CcAdjunct)n).UpTime.ElapsedMs()).ToList();
            //}
            
            foreach (var ioNeighbor in certified.Take((int)parm_max_swept_drones))
            {
                try
                {
                    //var remoteEp = ((CcAdjunct)ioNeighbor).NatAddress?.IpEndPoint.AsByteString();
                    //remoteEp ??= ((CcAdjunct)ioNeighbor).MessageService.IoNetSocket.LocalNodeAddress.IpEndPoint.AsByteString();

                    sweptResponse.Contacts.Add(new CcAdjunctModel
                    {
                        PublicKey = UnsafeByteOperations.UnsafeWrap(((CcAdjunct) ioNeighbor).Designation.PublicKey),
                        Url = ((CcAdjunct)ioNeighbor).MessageService.IoNetSocket.RemoteNodeAddress.IpEndPoint.AsByteString()
                    });
                }
                catch when (ioNeighbor.Zeroed()){}
                catch (Exception e) when (!ioNeighbor.Zeroed())
                {
                    _logger.Error(e,
                        $"{nameof(CcAdjunctResponse)}: Failed to add adjunct, contacts = {sweptResponse.Contacts}, pk = {((CcAdjunct)ioNeighbor)?.Designation?.PublicKey}, remote = {RemoteAddress} ");
                    continue;
                }

                if (CcCollective.ZeroDrone)
                {
                    Interlocked.Exchange(ref ((CcAdjunct)ioNeighbor)._fusedCount,
                        ((CcAdjunct)ioNeighbor)._fusedCount / 2);
                }

                count++;
            }

            int sent;
            var dst = IsProxy ? RemoteAddress : IoNodeAddress.CreateFromEndpoint("udp", src);
            if ((sent = await SendMessageAsync(sweptResponse.ToByteArray(), CcDiscoveries.MessageTypes.Adjuncts, dst).FastPath()) > 0)
            {
                if(count > 0)
                    _logger.Trace($"-/> {nameof(CcAdjunctResponse)}({sent}): Sent {count} discoveries to {(IsProxy?Description:src)}");

                //Emit message event
                if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                    AutoPeeringEventService.AddEvent(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.SendProtoMsg,
                        Msg = new ProtoMsg
                        {
                            CollectiveId = Hub.Router.Designation.IdString(),
                            Id = Designation.IdString(),
                            Type = "discovery response"
                        }
                    });
            }
            else
            {
                _logger.Debug($"<\\- {nameof(CcScanRequest)}({sent}): Send [FAILED], {Description}, {MetaDesc}");
            }
        }

        [Flags]
        enum DroneStatus
        {
            Undefined = 0,
            Drone = 1 << 0,
        }

#if DEBUG
        /// <summary>
        /// dbg info
        /// </summary>
        private CcAdjunct _sibling;
#endif

        /// <summary>
        /// Probe message
        /// </summary>
        /// <param name="probeMessage">The probe packet</param>
        /// <param name="remoteEp">The originating src</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcProbeMessage probeMessage, IPEndPoint remoteEp, chroniton packet)
        {
            //Drop old API calls
            if(probeMessage.Protocol != parm_protocol_version)
                return;

            //Drop if we are saturated
            if(!IsProxy && !CcCollective.ZeroDrone && Hub.Neighbors.Count >= CcCollective.MaxAdjuncts)
                return;

            var response = new CcProbeResponse
            {
                Protocol = parm_protocol_version,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                ReqHash = UnsafeByteOperations.UnsafeWrap(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                Status = (long)(IsDroneAttached? DroneStatus.Drone : DroneStatus.Undefined),
#if DEBUG
                DbgPtr = GCHandle.ToIntPtr(GCHandle.Alloc(this, GCHandleType.WeakTrackResurrection)).ToInt64() 
#endif
            };

            //ensure ingress delta trigger
            if (!CcCollective.ZeroDrone && EnableSynTrigger && Volatile.Read(ref _lastSeduced).ElapsedMs() < parm_max_network_latency_ms)
                _fuseCount = -parm_zombie_max_connection_attempts;
            
            Interlocked.Exchange(ref _lastSeduced, Environment.TickCount);

            //PROCESS DMZ-SYN
            var sent = 0;
            if (!IsProxy)
            {
                var toAddress = IoNodeAddress.CreateFromEndpoint("udp", remoteEp);

                //IoNodeAddress toProxyAddress = null;
                //if (CcCollective.UdpTunnelSupport)
                //{
                //    if (ping.SrcAddr != "0.0.0.0" && remoteEp.Address.ToString() != ping.SrcAddr)
                //    {
                //        toProxyAddress = IoNodeAddress.Create($"udp://{ping.SrcAddr}:{ping.SrcPort}");
                //        _logger.Trace(
                //            $"<\\- {nameof(Probe)}: static peer address received: {toProxyAddress}, source detected = udp://{remoteEp}");
                //    }
                //    else
                //    {
                //        toProxyAddress = IoNodeAddress.CreateFromEndpoint("udp", remoteEp);
                //        _logger.Trace(
                //            $"<\\- {nameof(Probe)}: automatic peer address detected: {toProxyAddress}, source declared = udp://{ping.SrcAddr}:{ping.SrcPort}");
                //    }
                //}

                //SEND SYN-ACK
                if ((sent = await SendMessageAsync(response.ToByteArray(), CcDiscoveries.MessageTypes.Probed, toAddress).FastPath()) > 0)
                {
#if DEBUG
                    _logger.Trace($"-/> {nameof(CcProbeResponse)}({sent})[{response.ToByteArray().PayloadSig()} ~ {response.ReqHash.Memory.HashSig()}]: Sent [[SYN-ACK]], [{MessageService.IoNetSocket.LocalAddress} ~> {toAddress}]");
#endif
                    
                    var ccId = CcDesignation.FromPubKey(packet.PublicKey.Memory);
#if DEBUG
                    var fromAddress = IoNodeAddress.CreateFromEndpoint("udp", packet.Header.Ip.Src.GetEndpoint());
#else
                    //var fromAddress = IoNodeAddress.CreateFromEndpoint("udp", packet.Header.Ip.Src.GetEndpoint());
                    var fromAddress = IoNodeAddress.CreateFromEndpoint("udp", remoteEp);
                    //TODO route issue
#endif

                    await ZeroAsync(static async state =>
                    {
                        var (@this, fromAddress, ccId) = state;
                        if (!await @this.CollectAsync(fromAddress.IpEndPoint, ccId, false).FastPath())
                        {
#if DEBUG
                            @this._logger.Trace($"{@this.Description}: Collecting {fromAddress.IpEndPoint} failed!");
#else
                    @this._logger.Trace($"{@this.Description}: Collecting {fromAddress.IpEndPoint} failed!");
#endif
                        }
                    }, ValueTuple.Create(this, fromAddress, ccId), TaskCreationOptions.DenyChildAttach).FastPath();
                    

                    if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                        AutoPeeringEventService.AddEvent(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = CcDesignation.MakeKey(packet.PublicKey),
                                Type = "pong"
                            }
                        });
                }

//                if (toProxyAddress != null && CcCollective.UdpTunnelSupport && toAddress.Ip != toProxyAddress.Ip)
//                {
//                    if (await SendMessageAsync(pong.ToByteString(), CcDiscoveries.MessageTypes.Probed, toAddress)
//                            .FastPath() > 0)
//                    {
//#if DEBUG
//                        _logger.Trace($"-/> {nameof(CcProbeResponse)}({sent})[{pong.ToByteString().Memory.PayloadSig()} ~ {pong.ReqHash.Memory.HashSig()}]: Sent [[SYN-ACK]], [{MessageService.IoNetSocket.LocalAddress} ~> {toAddress}]");
//#endif
//                    }
//                }
            }
            else //PROCESS SYN
            {
                //if (UpTime.ElapsedMsToSec() > CcCollective.parm_mean_pat_delay_s && SecondsSincePat > CcCollective.parm_mean_pat_delay_s/2)
                //{
                //    _logger.Error($"PAT Success! {Description}");
                //}
                LastPat = Environment.TickCount;

                if ((sent = await SendMessageAsync(data: response.ToByteArray(), type: CcDiscoveries.MessageTypes.Probed).FastPath()) > 0)
                {
#if DEBUG
                    _logger.Trace(IsDroneConnected
                        ? $"-/> {nameof(CcProbeResponse)}({sent})[{response.ToByteArray().PayloadSig()} ~ {response.ReqHash.Memory.HashSig()}]: Sent [[KEEP-ALIVE]], {Description}"
                        : $"-/> {nameof(CcProbeResponse)}({sent})[{response.ToByteArray().PayloadSig()} ~ {response.ReqHash.Memory.HashSig()}]: Sent [[SYN-VCK]], {Description}");
#endif

                    //ensure ingress delta trigger
                    if (!CcCollective.ZeroDrone)
                    {
                        await SeduceAsync("ACK", Heading.Both).FastPath();
                    }

                    if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                        AutoPeeringEventService.AddEvent(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = Designation.IdString(),
                                Type = "pong"
                            }
                        });
                }
                else
                {
                    _logger.Error($"-/> {nameof(CcProbeMessage)}: [FAILED] Send [[SYN ACK/KEEP-ALIVE]], to = {Description}");
                }
            }
        }

        /// <summary>
        /// Prepares the services for this adjunct
        /// </summary>
        /// <param name="packet"></param>
        /// <param name="localEp"></param>
        /// <param name="remoteEp"></param>
        /// <returns></returns>
        //private CcProbeResponse PrepareServices(chroniton packet, IPEndPoint localEp, IPEndPoint remoteEp)
        //{
        //    //TODO optimize
        //    var gossipAddress =
        //        Hub.Services.CcRecord.Endpoints[CcService.Keys.gossip];
        //    var peeringAddress =
        //        Hub.Services.CcRecord.Endpoints[CcService.Keys.peering];
        //    var fpcAddress =
        //        Hub.Services.CcRecord.Endpoints[CcService.Keys.fpc];

        //    var probedMessage = new CcProbeResponse
        //    {
        //        Protocol = parm_protocol_version,
        //        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        //        ReqHash = UnsafeByteOperations.UnsafeWrap(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),

        //        //DstAddr = $"{remoteEp.Address}", //TODO, add port somehow
        //        //Services = _serviceMapLocal = new CcServiceMapModel
        //        //{
        //        //    Map =
        //        //    {
        //        //        {
        //        //            CcService.Keys.peering.ToString(),
        //        //            new NetworkAddress { Network = "udp", Port = (uint)peeringAddress.Port }
        //        //        },
        //        //        {
        //        //            CcService.Keys.gossip.ToString(),
        //        //            new NetworkAddress { Network = "tcp", Port = (uint)gossipAddress.Port }
        //        //        },
        //        //        {
        //        //            CcService.Keys.fpc.ToString(),
        //        //            new NetworkAddress { Network = "tcp", Port = (uint)fpcAddress.Port }
        //        //        }
        //        //    }
        //        //}
        //    };
        //    return probedMessage;
        //}

        /// <summary>
        /// Probed message
        /// </summary>
        /// <param name="response">The Probed packet</param>
        /// <param name="src">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcProbeResponse response, IPEndPoint src, chroniton packet)
        {
            var matchRequest = await _probeRequest.ResponseAsync(src.ToString(), response.ReqHash).FastPath();
            
            //Try the router
            if (!matchRequest && IsProxy)
                matchRequest = await Hub.Router._probeRequest.ResponseAsync(src.ToString(), response.ReqHash).FastPath();

#if DEBUG
            //Try the DBG source info //TODO: is this fixed?
            if (!matchRequest && IsProxy)
                matchRequest = await _probeRequest.ResponseAsync(packet.Header.Ip.Src.GetEndpoint().ToString(), response.ReqHash).FastPath();
#endif
            if (!matchRequest)
            {
#if DEBUG
                if (IsProxy && _probeRequest.Count > 0)
                {
                    _logger.Error($"<\\- {nameof(CcProbeResponse)} {packet.Data.Memory.PayloadSig()}: SEC! age = {response.Timestamp.ElapsedUtcMs()}ms, matcher = ({_probeRequest.Count}, {Router._probeRequest.Count}) ,{response.ReqHash.Memory.HashSig()}, d = {_probeRequest.Count}, pats = {TotalPats},  " +
                                  $"PK={Designation.IdString()} != {CcDesignation.MakeKey(packet.PublicKey)} (proxy = {IsProxy}),  ssp = {SecondsSincePat}, d = {(AttachTimestamp > 0 ? (AttachTimestamp - LastPat).ToString() : "N/A")}, v = {Verified}, s = {src}, nat = {NatAddress}, dmz = {packet.Header.Ip.Src.GetEndpoint()}");
                    _probeRequest.DumpToLog();
                }
#endif
                return;
            }

            Interlocked.Exchange(ref _zeroProbes, 0);

            //Process SYN-ACK
            if (!IsProxy)
            {
                var ccId = CcDesignation.FromPubKey(packet.PublicKey.Memory);
#if DEBUG
                var fromAddress = IoNodeAddress.CreateFromEndpoint("udp", packet.Header.Ip.Src.GetEndpoint());
#else
                //var fromAddress = IoNodeAddress.CreateFromEndpoint("udp", packet.Header.Ip.Src.GetEndpoint());
                //TODO route issue
                var fromAddress = IoNodeAddress.CreateFromEndpoint("udp", src);
#endif


                //var remoteServices = new CcService();
                //foreach (var key in pong.Services.Map.Keys.ToList())
                //    remoteServices.CcRecord.Endpoints.TryAdd(Enum.Parse<CcService.Keys>(key),
                //        IoNodeAddress.Create(
                //            $"{pong.Services.Map[key].Network}://{((IPEndPoint) extraData).Address}:{pong.Services.Map[key].Port}"));


                //Collect...
                if (Hub.Neighbors.Count <= CcCollective.MaxAdjuncts)
                {
                    await ZeroAsync(static async state =>
                    {
                        var (@this, fromAddress, ccId) = state;
                        if (!await @this.CollectAsync(fromAddress.IpEndPoint, ccId, true).FastPath())
                        {
#if DEBUG
                            @this._logger.Trace($"{@this.Description}: Collecting {fromAddress.IpEndPoint} failed!");
#else
                            @this._logger.Trace($"{@this.Description}: Collecting {fromAddress.IpEndPoint} failed!");
#endif
                        }
                    }, ValueTuple.Create(this, fromAddress, ccId), TaskCreationOptions.DenyChildAttach).FastPath();
                }
            }
            else if (!Verified) //Process ACK
            {
                //PAT
                LastPat = Environment.TickCount;

#if DEBUG
                //TODO: vector?
                //set ext address as seen by neighbor
                DebugAddress = IoNodeAddress.CreateFromEndpoint("udp", packet.Header.Ip.Src.GetEndpoint());
                NatAddress = DebugAddress;
#else
                NatAddress = IoNodeAddress.CreateFromEndpoint("udp", src);
#endif
                Verified = true;

                AdjunctState oldState;
                if ((oldState = CompareAndEnterState(AdjunctState.Verified, AdjunctState.Unverified)) != AdjunctState.Unverified)
                {
                    _logger.Warn($"{nameof(CcProbeResponse)} - {Description}: Invalid state, {oldState}. Wanted {nameof(AdjunctState.Unverified)}");
                    return;
                }

                if(CcCollective.ZeroDrone)
                    _logger.Warn($"Verified with queen `{src}'");

#if DEBUG
                _logger.Trace($"<\\- {nameof(CcProbeResponse)}[{response.ToByteArray().PayloadSig()} ~ {response.ReqHash.Memory.HashSig()}]: Processed <<SYN-ACK>>: {Description}");
#endif
            }
            else 
            {
#if DEBUG
                _sibling = (CcAdjunct)GCHandle.FromIntPtr(new IntPtr(response.DbgPtr)).Target;
#endif
                //PAT
                LastPat = Environment.TickCount;
#if DEBUG
                if(NatAddress == null)
                    NatAddress = IoNodeAddress.CreateFromEndpoint("udp", src);
                else if (!Equals(src, NatAddress.IpEndPoint))
                {
                    _logger.Fatal($"Bad NAT address, switching {NatAddress} to {src}");
                    NatAddress = IoNodeAddress.CreateFromEndpoint("udp", src);
                }
#endif

                if (!CcCollective.ZeroDrone)
                    await SeduceAsync("ACK-SYN-VCK", Heading.Both).FastPath();

                //Ask drones that we are not connected to, to drop us
                if (((DroneStatus)response.Status).HasFlag(DroneStatus.Drone) && !IsDroneAttached && UpTime.ElapsedMs() > parm_min_uptime_ms)
                {
                    await DeFuseAsync().FastPath();
                }
            }
        }

        /// <summary>
        /// Seduces another adjunct
        /// </summary>
        /// <returns>A valuable task</returns>s
        public async ValueTask<bool> SeduceAsync(string desc, Heading heading, IoNodeAddress dmzEndpoint = null, bool force = false)
        {
            if (!force && Volatile.Read(ref _stealthy).ElapsedMs() < parm_max_network_latency_ms || Zeroed() || IsDroneAttached ||
                _currState.Value > AdjunctState.Connecting /*don't change this from Connecting*/)
            {
                return false;
            }
            
            try
            {
                if (IsProxy && Probed && heading > Heading.Ingress &&
                    !CcCollective.ZeroDrone && Direction == Heading.Undefined &&
                    CcCollective.EgressCount < CcCollective.parm_max_outbound &&
                    FuseCount < parm_zombie_max_connection_attempts)
                {
                    if (!await FuseAsync().FastPath())
                    {
                        _logger.Trace($"<\\- {nameof(FuseAsync)}: [FAILED] Send Drone request, {Description}");
                    }
                    else
                    {
                        Interlocked.Exchange(ref _stealthy, Environment.TickCount);
#if DEBUG
                        _logger.Trace($"-/> {nameof(FuseAsync)}: Sent Drone >FUSE< [[{desc}]]({heading}) ), {Description}");
#endif
                        return true;
                    }
                }

                if (!CcCollective.ZeroDrone && ((int)heading & (int)Heading.Ingress) > 0 && CcCollective.IngressCount < CcCollective.parm_max_inbound)
                {
                    var proxy = dmzEndpoint == null ? this : Router;
                    //delta trigger
                    if (proxy != null && !await proxy.ProbeAsync(desc, dmzEndpoint).FastPath())
                        _logger.Trace($"<\\- {nameof(ProbeAsync)}({desc}): [FAILED] to seduce {Description}");
                    else
                    {
                        Interlocked.Exchange(ref _stealthy, Environment.TickCount);
#if DEBUG
                        _logger.Trace($"-/> {nameof(ProbeAsync)}: Send [[{desc}]]({heading}) Probe ");
#endif
                        return true;
                    }
                }
            }
            catch when(Zeroed()){}
            catch (Exception e)when(!Zeroed())
            {
                _logger.Error(e,$"{nameof(SeduceAsync)}: ");
            }
            return false;
        }

        /// <summary>
        /// Sends a probe to scout ahead for adjuncts
        /// </summary>
        /// <param name="desc">Probe description</param>
        /// <param name="dest">The destination address</param>
        /// <param name="id">Optional Id of the node pinged</param>
        /// <returns>Task</returns>
        public async ValueTask<bool> ProbeAsync(string desc, IoNodeAddress dest = null, string id = null)
        {
            try
            {
                dest ??= RemoteAddress;
                
                if (dest == null)
                    throw new ArgumentException($"dest cannot be null at this point!");

                //Create the ping request
                var probeMessage = new CcProbeMessage
                {
                    Protocol = parm_protocol_version,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                };

                var probeMsgBuf = probeMessage.ToByteArray();

                IoQueue<IoZeroMatcher.IoChallenge>.IoZNode challenge;
                if ((challenge = await _probeRequest.ChallengeAsync(dest.IpPort, probeMsgBuf).FastPath()) == null)
                {
                    if(!Zeroed())
                        _logger.Fatal($"{nameof(ProbeAsync)} No challange, {_probeRequest.Count}/{_probeRequest.Capacity}");
                    return false;
                }
                    
                
                // Is this a routed request?
                if (IsProxy)
                {
                    if (ZeroProbes > parm_zombie_max_connection_attempts)
                    {
#if DEBUG
                        await Zero(this, $"drone left, T = {TimeSpan.FromMilliseconds(UpTime.ElapsedMs()).TotalMinutes:0.0}min ~ {_sibling?.UpTime.ElapsedMsToSec()/60.0:0.0}min, {_sibling?.Description}").FastPath();
#else
                        await Zero(this, $"wd: s = {SecondsSincePat}, probed = {ZeroProbes} << {parm_zombie_max_connection_attempts}, T = {TimeSpan.FromMilliseconds(UpTime.ElapsedMs()).TotalMinutes:0.0}min");
#endif

                        return false;
                    }

                    //send
                    var sent = await SendMessageAsync(data: probeMsgBuf, type: CcDiscoveries.MessageTypes.Probe).FastPath();
                    if (sent > 0)
                    {
                        Interlocked.Increment(ref _zeroProbes);
#if DEBUG
                        try
                        {
                            _logger.Trace($"-/> {nameof(CcProbeMessage)}({sent})[{probeMsgBuf.PayloadSig()}, hash = {challenge.Value.Hash.HashSig()}]: sent [[{desc}]] {Description}");
                        }
                        catch when (Zeroed()){}
                        catch (Exception e)when(!Zeroed())
                        {
                            _logger.Trace(e);
                        }
#endif
                        if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                            AutoPeeringEventService.AddEvent(new AutoPeerEvent
                            {
                                EventType = AutoPeerEventType.SendProtoMsg,
                                Msg = new ProtoMsg
                                {
                                    CollectiveId = Hub.Router.Designation.IdString(),
                                    Id = Designation.IdString(),
                                    Type = "ping"
                                }
                            });

                        return true;
                    }

                    await _probeRequest.RemoveAsync(challenge).FastPath();

                    if(!Zeroed())
                        _logger.Error($"-/> {nameof(CcProbeMessage)}: [FAILED], {Description}");

                    return false;
                }
                else //The destination state was undefined, this is local
                {
                    var sent = await SendMessageAsync(probeMsgBuf, CcDiscoveries.MessageTypes.Probe, dest).FastPath();

                    if (sent > 0)
                    {
#if DEBUG
                        _logger.Trace($"-/> {nameof(CcProbeMessage)}({sent})[{probeMsgBuf.PayloadSig()}]: sent [[{desc}]], dest = {dest}, {Description}");
#endif
                        if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                            AutoPeeringEventService.AddEvent(new AutoPeerEvent
                            {
                                EventType = AutoPeerEventType.SendProtoMsg,
                                Msg = new ProtoMsg
                                {
                                    CollectiveId = Hub.Router.Designation.IdString(),
                                    Id = id??Designation.IdString(),
                                    Type = "ping"
                                }
                            });

                        return true;
                    }

                    await _probeRequest.RemoveAsync(challenge).FastPath();

                    if(!Zeroed())
                        _logger.Error($"-/> {nameof(CcProbeMessage)}:({desc}) [FAILED], {Description}");

                    return false;
                }
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger.Debug(e,$"ERROR z = {Zeroed()}, dest = {dest}, source = {MessageService}, _scanRequest = {_scanRequest.Count}");
            }

            return false;
        }

        /// <summary>
        /// Scans adjuncts for more
        /// </summary>
        /// <returns>Task</returns>
        public async ValueTask<bool> ScanAsync(int cooldown = -1)
        {
            try
            {
                if (!Assimilating || !Probed && !CcCollective.ZeroDrone)
                {
                    _logger.Trace($"{nameof(ScanAsync)}: [ABORTED], {Description}, s = {State}, a = {Assimilating}");
                    return false;
                }

                if (_scanCount > parm_zombie_max_connection_attempts)
                {
                    _logger.Trace($"{nameof(ScanAsync)}: [skipped], no replies {Description}, s = {State}, a = {Assimilating}");
                    if (!IsDroneAttached)
                    {
                        await Zero(this, $"{nameof(ScanAsync)}: Unable to scan adjunct, count = {_scanCount} << {parm_zombie_max_connection_attempts}").FastPath();
                        return false;
                    }
                        
                    return true;
                }

                if (cooldown == -1)
                    cooldown = CcCollective.parm_mean_pat_delay_s * 1000 / 5;

                //rate limit
                if (LastScan.ElapsedMs() < cooldown)
                    return false;

                var sweepMessage = new CcScanRequest
                {
                     Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                };

                var sweepMsgBuf = sweepMessage.ToByteArray();

                IoQueue<IoZeroMatcher.IoChallenge>.IoZNode challenge;
                if ((challenge = await _scanRequest.ChallengeAsync(RemoteAddress.IpPort, sweepMsgBuf).FastPath()) == null)
                {
                    return false;
                }

                var sent = await SendMessageAsync(sweepMsgBuf,CcDiscoveries.MessageTypes.Scan, RemoteAddress).FastPath();
                if (sent > 0)
                {
                    Interlocked.Increment(ref _scanCount);
                    _logger.Trace($"-/> {nameof(CcScanRequest)}({sent}){sweepMsgBuf.PayloadSig()}: Sent, {Description}");

                    //Emit message event
                    if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                        AutoPeeringEventService.AddEvent(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = Designation.IdString(),
                                Type = "scan request"
                            }
                        });

                    return true;
                }

                await _scanRequest.RemoveAsync(challenge).FastPath();
                _logger.Debug($"-/> {nameof(CcScanRequest)}: [FAILED], {Description} ");
            }
            catch when (Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Debug(e,$"{nameof(CcScanRequest)}: [ERROR] z = {Zeroed()}, state = {State}, dest = {RemoteAddress}, source = {MessageService}, _scanRequest = {_scanRequest.Count}");
            }

            return false;
        }

        /// <summary>
        /// Causes adjunct to fuse with another adjunct forming a drone. Drones are useful. 
        /// </summary>
        /// <returns>Task</returns>
        public async ValueTask<bool> FuseAsync()
        {
            if (IsDroneAttached || CcCollective.ZeroDrone || _currState.Value == AdjunctState.Connected)
            {
                if (!CcCollective.ZeroDrone)
                    _logger.Warn($"{nameof(FuseAsync)}: [ABORTED], {Description}, s = {State}, a = {Assimilating}, D = {IsDroneConnected}, d = {IsDroneAttached}");
                return false;
            }

            if (_currState.Value <= AdjunctState.Unverified) return true;

            AdjunctState oldState;

            var stateIsValid = (oldState = CompareAndEnterState(AdjunctState.Fusing, AdjunctState.Verified, overrideHung: parm_max_network_latency_ms)) == AdjunctState.Verified;
            if (!stateIsValid)
            {
                if (oldState != AdjunctState.Fusing && _currState.EnterTime.ElapsedMs() > parm_max_network_latency_ms)
                    _logger.Warn($"{nameof(FuseAsync)} - {Description}: Invalid state, {oldState}, age = {_currState.EnterTime.ElapsedMs()}ms. Wanted {nameof(AdjunctState.Verified)} - [RACE OK!] ");
                return false;
            }

            //oneshot
            await ZeroAsync(static async (@this) =>
            {
                try
                {
                    //TODO DELAYS:
                    //await Task.Delay(@this._random.Next(@this.parm_max_network_latency_ms / 5 + 1), @this.AsyncTasks.Token);
                    //await Task.Delay(@this._random.Next(@this.parm_max_network_latency_ms>>3), @this.AsyncTasks.Token);

                    var fuseRequest = new CcFuseRequest
                    {
                        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    };

                    var fuseRequestBuf = fuseRequest.ToByteArray();

                    IoQueue<IoZeroMatcher.IoChallenge>.IoZNode challenge;
                    if ((challenge = await @this._fuseRequest.ChallengeAsync(@this.RemoteAddress.IpPort, fuseRequestBuf).FastPath()) == null)
                    {
                        return;
                    }

                    var sent = await @this.SendMessageAsync(fuseRequestBuf, CcDiscoveries.MessageTypes.Fuse).FastPath();
                    if (sent > 0)
                    {
                        Interlocked.Increment(ref @this._fuseCount);
                        @this._logger.Debug($"-/> {nameof(CcFuseRequest)}({sent})[{fuseRequestBuf.PayloadSig()}]: Sent, {@this.Description}");
                        
                        if (!@this.CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                            AutoPeeringEventService.AddEvent(new AutoPeerEvent
                            {
                                EventType = AutoPeerEventType.SendProtoMsg,
                                Msg = new ProtoMsg
                                {
                                    CollectiveId = @this.Hub.Router.Designation.IdString(),
                                    Id = @this.Designation.IdString(),
                                    Type = "peer request"
                                }
                            });
                        return;
                    }

                    await @this._fuseRequest.RemoveAsync(challenge).FastPath();
                    @this._logger.Debug($"-/> {nameof(CcDefuseRequest)}: [FAILED], {@this.Description}, {@this.MetaDesc}");
                }
                catch when (@this.Zeroed()) { }
                catch (Exception e) when (!@this.Zeroed())
                {
                    @this._logger.Debug(e, $"{nameof(CcDefuseRequest)}: [FAILED], {@this.Description}, {@this.MetaDesc}");
                }
            }, this, TaskCreationOptions.DenyChildAttach).FastPath();

            return true;
        }

        /// <summary>
        /// Causes adjuncts to de fuse
        /// </summary>
        /// <returns></returns>
        internal async ValueTask<bool> DeFuseAsync(IoNodeAddress dest = null)
        {
            try
            {
                dest ??= RemoteAddress;

                var dropRequest = new CcDefuseRequest
                {
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                };

                var sent = await SendMessageAsync(dropRequest.ToByteArray(), CcDiscoveries.MessageTypes.Defuse, dest).FastPath();

#if DEBUG
                _logger.Trace(
                    (sent) > 0
                        ? $"-/> {nameof(CcDefuseRequest)}({sent}): Sent, {Description}"
                        : $"-/> {nameof(CcDefuseRequest)}: [FAILED], {Description}, {MetaDesc}");
#endif

                if (sent > 0)
                {
                    if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                        AutoPeeringEventService.AddEvent(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = Designation.IdString(),
                                Type = $"peer disconnect"
                            }
                        });

                    return true;
                }
            }
            catch (Exception) when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger.Debug(e, $"{nameof(DeFuseAsync)}: [ERROR], {Description}, s = {State}, a = {Assimilating}, p = {IsDroneConnected}, d = {dest}, s = {MessageService}");
            }

            return false;
        }

        //TODO complexity
        /// <summary>
        /// Attaches a gossip drone to this neighbor
        /// </summary>
        /// <param name="ccDrone">The drone</param>
        /// <param name="direction"></param>
        public async ValueTask<bool> AttachDroneAsync(CcDrone ccDrone, Heading direction)
        {
            try
            {
                //raced?
                if (IsDroneAttached)
                {
                    return false;
                }

                if (!await ZeroAtomic( static (_, state, _) =>
                    {
                        var (@this, ioCcDrone, direction) = state;
                   
                        //Race for direction
                        if (Interlocked.CompareExchange(ref @this._direction, (int) direction, (int) Heading.Undefined) != (int) Heading.Undefined)
                        {
                            @this._logger.Warn($"oz: race for {direction} lost {ioCcDrone.Description}, current = {@this.Direction}, {@this._drone?.Description}");
                            return new ValueTask<bool>(false);
                        }
                    
                        @this._drone = ioCcDrone ?? throw new ArgumentNullException($"{nameof(ccDrone)}");
                    
                        return new ValueTask<bool>(@this.CompareAndEnterState(AdjunctState.Connected, AdjunctState.Connecting) == AdjunctState.Connecting);
                    }, ValueTuple.Create(this, ccDrone, direction)).FastPath())
                {
                    return false;
                }

                _logger.Trace($"{nameof(AttachDroneAsync)}: [WON] {_drone?.Description}");
                
                Assimilated = true;
                AttachTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                //emit event
                //if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                //    AutoPeeringEventService.AddEvent(new AutoPeerEvent
                //    {
                //        EventType = AutoPeerEventType.AddDrone,
                //        Drone = new Drone
                //        {
                //            CollectiveId = Router.Designation.IdString(),
                //            Adjunct = ccDrone.Adjunct.Designation.IdString(),
                //            Direction = ccDrone.Adjunct.Direction.ToString()
                //        }
                //    });

                if(CcCollective.Hub.Neighbors.Count <= CcCollective.MaxAdjuncts)
                    await ScanAsync().FastPath();

                return true;
            }
            catch when (!Zeroed()){}
            catch (Exception e) when(Zeroed())
            {
                _logger.Trace(e, Description);
            }

            //if ((oldState = CompareAndEnterState(AdjunctState.Verified, AdjunctState.Fusing)) != AdjunctState.Fusing)
            //    _logger.Warn($"{nameof(AttachDroneAsync)} - {Description}:5 Invalid state, {oldState}. Wanted {nameof(AdjunctState.Fusing)}");

            return false;
        }

        /// <summary>
        /// If a connection was ever made
        /// </summary>
        public bool WasAttached { get; protected internal set; }

        /// <summary>
        /// Detaches a drone from this neighbor
        /// </summary>
        public async ValueTask DetachDroneAsync()
        {
            var latch = _drone;
            CcDrone severedDrone;
            if(latch == null || (severedDrone = Interlocked.CompareExchange(ref _drone, null, latch)) != latch)
                return;

            try
            {
                AttachTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                Interlocked.Exchange(ref _direction, 0);
                
                _fusedCount = _fuseCount = _scanCount = 0;

                if (!Zeroed() && Node != null && Direction == Heading.Ingress &&
                    CcCollective.IngressCount < CcCollective.parm_max_inbound)
                {
                    //back off for a while... Try to re-establish a link 
                    await ZeroAsync(static async @this =>
                    {
                        var backOff = @this.parm_max_network_latency_ms + @this._random.Next(@this.parm_max_network_latency_ms);
                        await Task.Delay(backOff, @this.AsyncTasks.Token);

                        var router = !@this.Zeroed() ? @this : @this.Router;
                        await router.ProbeAsync("SYN-BRG", @this.RemoteAddress.Copy()).FastPath();

                    }, this, TaskCreationOptions.DenyChildAttach).FastPath();
                }

                //emit event
                if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                    AutoPeeringEventService.AddEvent(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.RemoveDrone,
                        Drone = new Drone
                        {
                            CollectiveId = CcCollective.Hub.Router.Designation.IdString(),
                            Adjunct = Designation.IdString()
                        }
                    });
            }
            catch when (Zeroed())
            { }
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"{nameof(DetachDroneAsync)}: ");
            }
            finally
            {
                ResetState(AdjunctState.Verified);
                await severedDrone.Zero(this, $"detached from adjunct, was attached = {WasAttached}").FastPath();
                if(WasAttached)
                    await DeFuseAsync().FastPath();
            }

            if (CcCollective.TotalConnections < CcCollective.MaxDrones)
            {
                //load hot backup
                await ZeroAsync(async @this =>
                {
                    await CcCollective.Adjuncts.Where(a => a.State == AdjunctState.Verified).ToList().ForEachAsync<CcAdjunct, IIoNanite>(static async (@this, _) =>
                    {
                        await @this.ProbeAsync("SYN-HOT");
                    }).FastPath();
                }, this, TaskCreationOptions.DenyChildAttach).FastPath();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AdjunctState ResetState(AdjunctState state)
        {
            Interlocked.Exchange(ref _direction, 0);
            _fusedCount = _fuseCount = 0;

            _currState.Set((int)state);

            return _currState.Value;
        }

        /// <summary>
        /// Makes fusing more likely
        /// </summary>
        public void EnsureFuseChecks()
        {
            if (!IsDroneAttached)
            {
                _fusedCount = 0;
                _fuseCount = 0;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AdjunctState CompareAndEnterState(AdjunctState state, AdjunctState cmp, bool compare = true, int overrideHung = 0)
        {
            var oldValue = _currState.Value;
#if DEBUG
            try
            {
                //TODO Make HEAP
                var nextState = new IoStateTransition<AdjunctState>((int)state);
                
                //force hung states
                if (overrideHung > 0)
                {
                    var hung = _currState.EnterTime.ElapsedMs() > overrideHung;
                    if (hung)
                    {
                        oldValue = cmp;
                        Interlocked.Exchange(ref _direction, 0);
                        _fusedCount = _fuseCount = 0;
                    }

                    compare = compare && !hung;
                }

                if (compare)
                {
#if DEBUG
                    if (oldValue != cmp)
                        return oldValue;
#else
                    return _currState.CompareAndEnterState((int)state, (int)cmp);
#endif
                }
#if !DEBUG
                else
                {
                    _currState.Set((int)state);
                    return oldValue;
                }
#endif
#if DEBUG
                _currState = _currState.Exit(nextState);      
#endif
            }
            catch when(!Zeroed()){}
            catch (Exception e)when(Zeroed())
            {
                _logger.Error(e, $"{nameof(CompareAndEnterState)} failed!");
            }
#else
            if (overrideHung > 0)
            {
                var hung = _currState.EnterTime.ElapsedMs() > overrideHung;

                if (hung)
                {
                    oldValue = cmp;
                    Interlocked.Exchange(ref _direction, 0);
                    _fusedCount = _fuseCount = _scanCount = 0;
                    compare = false;
                }
            }

            if (compare)
            {
                return _currState.CompareAndEnterState((int)state, (int)cmp);
            }
            else
            {
                _currState.Set((int)state);
            }
#endif
            return oldValue;
        }

        /// <summary>
        /// Print state history
        /// </summary>
#if DEBUG
        public void PrintStateHistory()
#else
        public static void PrintStateHistory()
#endif
        {
#if DEBUG
            var ioStateTransition = _currState.GetStartState();
            var sb = new StringBuilder();

            while (ioStateTransition != null)
            {
                sb.Append($"{ioStateTransition.Value} ~> ");
                ioStateTransition = ioStateTransition.Next;
            }

            if(sb.Length > 0)
                LogManager.GetCurrentClassLogger().Fatal(sb.ToString()[..(sb.Length-3)]);
#else
            //return default;
#endif
        }

        public IoStateTransition<AdjunctState> CurrentState => _currState;

        /// <summary>
        /// Gets and sets the state of the work
        /// </summary>
        public AdjunctState State
        {
            get => _currState.Value;
//            set
//            {
//#if DEBUG
//                var nextState = new IoStateTransition<AdjunctState>();
//                nextState.CompareAndEnterState(value, _currState);

//                var prevState = _currState;
//                _currState = nextState;

//                if (StateTransitionHistory[(int)prevState.Value] != null)
//                {
//                    var c = StateTransitionHistory[(int)prevState.Value];
//                    while (c.Repeat != null)
//                        c = c.Repeat;
//                    StateTransitionHistory[(int) prevState.Value].Repeat = prevState;
//                }
//                else
//                    StateTransitionHistory[(int) prevState.Value] = prevState;
//#else
//                _currState.CompareAndEnterState(value);
//#endif
//                //terminate
//                if (value == AdjunctState.ZeroState)
//                {
//                    State = AdjunctState.FinalState;
//                }
//            }
        }
    }
}