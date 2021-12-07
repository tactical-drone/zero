//#define LOSS
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Runtime.CompilerServices;
#if DEBUG
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using System.Text;
#endif
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using MathNet.Numerics.Random;
using NLog;
using SimpleBase;
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
using Zero.Models.Protobuf;

namespace zero.cocoon.autopeer
{
    /// <summary>
    /// Processes (UDP) discovery messages from the collective.
    /// </summary>
    public class CcAdjunct : IoNeighbor<CcProtocMessage<chroniton, CcDiscoveryBatch>>
    {
        public CcAdjunct(CcHub node, IoNetClient<CcProtocMessage<chroniton, CcDiscoveryBatch>> ioNetClient,
            object extraData = null)
            : base
            (
                node,
                ioNetClient,
                static (o, ioNetClient) => new CcDiscoveries("adjunct msgs", $"{((IoNetClient<CcProtocMessage<chroniton, CcDiscoveryBatch>>)ioNetClient).Key}", (IoSource<CcProtocMessage<chroniton, CcDiscoveryBatch>>)ioNetClient),
                false
            )
        {
            
            _random = new CryptoRandomSource(true);

            if (Source.Zeroed())
            {
                return;
            }
            
            _logger = LogManager.GetCurrentClassLogger();

            //TODO tuning
            var capMult = CcCollective.ZeroDrone ? 100 : 1;
            var ttlMult = CcCollective.ZeroDrone ? 10 : 1;

            _probeRequest = new IoZeroMatcher(nameof(_probeRequest), Source.ZeroConcurrencyLevel(), parm_max_network_latency_ms * 3 * ttlMult, (int)(CcCollective.MaxAdjuncts * parm_max_swept_drones * 2 * capMult));
            _fuseRequest = new IoZeroMatcher(nameof(_fuseRequest), Source.ZeroConcurrencyLevel(), parm_max_network_latency_ms * 3 * ttlMult, (int)(CcCollective.MaxAdjuncts* parm_max_swept_drones * 2 * capMult));
            _sweepRequest = new IoZeroMatcher(nameof(_sweepRequest), (int)(CcCollective.MaxAdjuncts * parm_max_swept_drones + 1), parm_max_network_latency_ms * 3 * ttlMult, (int)(CcCollective.MaxAdjuncts * parm_max_swept_drones * 2));

            if (extraData != null)
            {
                var (item1, ipEndPoint) = (Tuple<CcDesignation, IPEndPoint>) extraData;
                Designation = item1;
                RemoteAddress = IoNodeAddress.CreateFromEndpoint("udp", ipEndPoint);
                Key = MakeId(Designation, RemoteAddress);
                //to prevent cascading into the hub we clone the source.
                Source = new IoUdpClient<CcProtocMessage<chroniton, CcDiscoveryBatch>>($"UDP Proxy ~> {Description}", MessageService, RemoteAddress.IpEndPoint);
                Source.ZeroHiveAsync(this).AsTask().GetAwaiter().GetResult();
            }
            else
            {
                Designation = CcCollective.CcId;
                
                //Services = services ?? node.Services;
                Key = MakeId(Designation, CcCollective.ExtAddress);
            }

            if (Proxy)
            {
                CompareAndEnterState(AdjunctState.Unverified, AdjunctState.Undefined);
                ZeroAsync(RoboAsync, this,TaskCreationOptions.DenyChildAttach).AsTask().GetAwaiter().GetResult();
            }
            else
            {
                CompareAndEnterState(AdjunctState.Local, AdjunctState.Undefined);
                _routingTable = new ConcurrentDictionary<string, CcAdjunct>();
            }

            string desc;
#if DEBUG
            desc = $"{nameof(_chronitonHeap)}, {_description}";
#else
            desc = $"";
#endif
            
            _chronitonHeap = new IoHeap<chroniton>(desc, CcCollective.ZeroDrone & !Proxy? 4096: 4, autoScale: true) { Make = (o, o1) => new chroniton
                {
                    Header = new z_header { Ip = new net_header() }
                }
            };

            _stealthy = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - parm_max_network_latency_ms * 10;
            var nrOfStates = Enum.GetNames(typeof(AdjunctState)).Length;
            _lastScan = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - CcCollective.parm_mean_pat_delay_s * 1000 * 2;
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
                while (!@this.Zeroed())
                {
                    var targetDelay = (@this._random.Next(@this.CcCollective.parm_mean_pat_delay_s / 5) + @this.CcCollective.parm_mean_pat_delay_s / 4) * 1000;
                    var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    await Task.Delay(targetDelay, @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);

                    if (@this.Zeroed())
                        break;

                    if (ts.ElapsedMs() > targetDelay * 1.25 && ts.ElapsedMsToSec() > 0)
                    {
                        @this._logger.Warn($"{@this.Description}: Popdog is slow!!!, {(ts.ElapsedMs() - targetDelay) / 1000.0:0.0}s");
                    }

                    try
                    {
                        await @this.EnsureRoboticsAsync().FastPath().ConfigureAwait(@this.Zc);
                    }
                    catch (Exception) when (@this.Zeroed()) {}
                    catch (Exception e) when (!@this.Zeroed())
                    {
                        if (@this.Collected)
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

        public enum AdjunctState
        {
            Undefined = 0,
            FinalState = 1,
            Local = 2,
            Unverified = 3,
            Verified = 4,
            Fusing = 5,
            Connecting = 6,
            Connected = 7
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
        private readonly IoStateTransition<AdjunctState> _currState = new()
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
                    if(Proxy)
                        return _description = $"`adjunct ({EventCount}, {_drone?.EventCount??0}, {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{FuseRequestsSentCount}:{FuseRequestsRecvCount}] local: ||{MessageService.IoNetSocket.LocalAddress} ~> {MessageService.IoNetSocket.RemoteAddress}||, [{Hub?.Designation.IdString()}, {Designation.IdString()}], uptime = {TimeSpan.FromSeconds(Uptime.ElapsedMsToSec())}'";
                    else
                        return _description = $"`hub ({EventCount}, {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{FuseRequestsSentCount}:{FuseRequestsRecvCount}] local: ||{MessageService.IoNetSocket.LocalAddress} ~> {MessageService.IoNetSocket.RemoteAddress}||, [{Hub?.Designation?.IdString()}, {Designation.IdString()}], uptime = {TimeSpan.FromSeconds(Uptime.ElapsedMsToSec())}'";
                }
                catch (Exception)
                {
                    if(Proxy)
                        return _description?? $"`adjunct({EventCount}, {_drone?.EventCount ?? 0}, , {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{FuseRequestsSentCount}:{FuseRequestsRecvCount}] local: ||{MessageService?.IoNetSocket?.LocalAddress} ~> {MessageService?.IoNetSocket?.RemoteAddress}||,' [{Hub?.Designation?.IdString()}, {Designation?.IdString()}], uptime = {TimeSpan.FromSeconds(Uptime.ElapsedMsToSec())}'";    
                    else
                        return _description = $"`hub ({EventCount}, {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{FuseRequestsSentCount}:{FuseRequestsRecvCount}] local: ||{MessageService?.IoNetSocket?.LocalAddress} ~> {MessageService?.IoNetSocket?.RemoteAddress}||, [{Hub?.Designation?.IdString()}, {Designation?.IdString()}], uptime = {TimeSpan.FromSeconds(Uptime.ElapsedMsToSec())}'";
                }
            }
        }

        /// <summary>
        /// return extra information about the state
        /// </summary>
        public string MetaDesc => $"{(Zeroed()?"Zeroed!!!,":"")} d={Direction}, s={State}, v={Verified}, a={Assimilating}, da={IsDroneAttached}, cc={IsDroneConnected}, g={IsGossiping}, arb={IsArbitrating}, s={MessageService?.IsOperational}";

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
        private IoHeap<chroniton> _chronitonHeap;

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
        public volatile bool Assimilated;

        private volatile int _fuseRequestsRecvCount;
        /// <summary>
        /// Indicates whether we have successfully established a connection before
        /// </summary>
        protected int FuseRequestsRecvCount => _fuseRequestsRecvCount;

        private volatile int _fuseRequestsSentCount;
        /// <summary>
        /// Number of fuse requests
        /// </summary>
        public int FuseRequestsSentCount => _fuseRequestsSentCount;

        /// <summary>
        /// Gathers counter intelligence at a acceptable rates
        /// </summary>
        private long _triggerTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 100000;

        /// <summary>
        /// Node broadcast priority. 
        /// </summary>
        public long Priority => FuseRequestsRecvCount - (parm_zombie_max_connection_attempts - FuseRequestsSentCount);

        //uptime
        private long _attachTimestamp;

        public long AttachTimestamp
        {
            get => Interlocked.Read(ref _attachTimestamp);
            private set => Interlocked.Exchange(ref _attachTimestamp, value);
        }

        /// <summary>
        /// The adjunct address
        /// </summary>
        public IoNodeAddress RemoteAddress { get; }

        /// <summary>
        /// Whether this adjunct contains verified remote client connection information
        /// </summary>
        public bool Proxy => RemoteAddress != null;

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
            Both = 4
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
        /// Are we in the hive?
        /// </summary>
        public bool Collected => !Zeroed() && !Source.Zeroed() && Source.IsOperational;

        /// <summary>
        /// Receives protocol messages from here
        /// </summary>
        private IoConduit<CcProtocBatchJob<chroniton, CcDiscoveryBatch>> _protocolConduit;

        /// <summary>
        /// Seconds since pat
        /// </summary>
        private long _lastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

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
        public long SecondsSincePat => LastPat.Elapsed();

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private IoZeroMatcher _sweepRequest;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private IoZeroMatcher _fuseRequest;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private IoZeroMatcher _probeRequest;

        /// <summary>
        /// salt timestamp
        /// </summary>
        private long _curSaltStamp = DateTimeOffset.UnixEpoch.ToUnixTimeSeconds();

        /// <summary>
        /// Current salt value
        /// </summary>
        private volatile ByteString _curSalt;

        /// <summary>
        /// Generates a new salt
        /// </summary>
        private ByteString GetSalt
        {
            get
            {
                if (_curSalt != null && _curSaltStamp.Elapsed() <= parm_salt_ttl) return _curSalt;
                
                _curSalt = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(new byte[parm_salt_length]));
                RandomNumberGenerator.Fill(_curSalt.ToByteArray());
                _curSaltStamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                return _curSalt;
            }
        }

        /// <summary>
        /// Producer tasks
        /// </summary>
        private ValueTask<bool>[] _produceTaskPool;

        /// <summary>
        /// Consumer tasks
        /// </summary>
        private ValueTask<bool>[] _consumeTaskPool;


        public bool Probed => _fuseRequestsRecvCount > 0 || Verified;

        /// <summary>
        /// Create an CcId string
        /// </summary>
        /// <param name="designation">The crypto identity</param>
        /// <param name="address">The transport identity</param>
        /// <returns></returns>
        public static string MakeId(CcDesignation designation, IoNodeAddress address)
        {
            return MakeId(designation, address.Key);
        }

        /// <summary>
        /// Makes an CcId string
        /// </summary>
        /// <param name="designation">The identity</param>
        /// <param name="key">The ip</param>
        /// <returns>A key</returns>
        public static string MakeId(CcDesignation designation, string key)
        {
            //return $"{designation.PkString()}@{key}";
            return $"{designation.PkShort()}";
        }

        /// <summary>
        /// The CcId
        /// </summary>
        public override string Key { get; }

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
        public int parm_max_network_latency_ms = 3000;
#else
        public int parm_max_network_latency_ms = 1000;
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
            _drone = null;
            _protocolConduit = null;
            _fuseRequest = null;
            _sweepRequest = null;
            _produceTaskPool = null;
            _consumeTaskPool = null;
            _chronitonHeap = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);

            if (Proxy)
            {
                try
                {
                    //Remove from routing table
                    if (Router._routingTable.TryGetValue(RemoteAddress.Key, out var currentRoute))
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
                            _logger.Trace($"ROUTER: cull failed,wanted = [{Designation?.PkString()}], got [{currentRoute.Designation.PkString()}, serial1 = {Serial} vs {currentRoute.Serial}], {Description}");
                        }
                    }
                }
                catch
                {
                    // ignored
                }

                await _chronitonHeap.ZeroManagedAsync<object>().FastPath().ConfigureAwait(Zc);

                var from = ZeroedFrom == this? "self" : ZeroedFrom?.Description??"null";
                if (Assimilated && WasAttached && Uptime.ElapsedMs() > parm_min_uptime_ms)
                    _logger.Info($"- `{(Assimilated ? "apex" : "sub")} {Direction}: {Description}, from: {from}");

                await DetachDroneAsync().FastPath().ConfigureAwait(Zc);
                
                //swarm 
                await ZeroAsync(static async @this =>
                {
                    await Task.Delay(@this.parm_max_network_latency_ms * 2, @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);
                    await @this.Router.ProbeAsync("SYN-BRG", @this.RemoteAddress.Copy()).FastPath().ConfigureAwait(@this.Zc);
                }, this, TaskCreationOptions.DenyChildAttach);

                //await ZeroAsync(static async @this =>
                //{
                //    await Task.Delay(@this.parm_min_uptime_ms, @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);
                //    if (AutoPeeringEventService.Operational)
                //        await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                //        {
                //            EventType = AutoPeerEventType.RemoveAdjunct,
                //            Adjunct = new Adjunct
                //            {
                //                Id = @this.Designation.IdString(),
                //                CollectiveId = @this.Router.Designation.IdString()
                //            }
                //        }).FastPath().ConfigureAwait(@this.Zc);

                //}, this, TaskCreationOptions.DenyChildAttach);

                if (AutoPeeringEventService.Operational)
                    await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.RemoveAdjunct,
                        Adjunct = new Adjunct
                        {
                            Id = Designation.IdString(),
                            CollectiveId = Router.Designation.IdString()
                        }
                    }).FastPath().ConfigureAwait(Zc);
            }
            else
            {
                _logger.Info($"~> {Description}, from: {ZeroedFrom?.Description}");
            }

            _probeRequest.Zero(this);
            _fuseRequest.Zero(this);
            _sweepRequest.Zero(this);
        }

        /// <summary>
        /// Ensure robotic diagnostics
        /// </summary>
        public async ValueTask EnsureRoboticsAsync()
        {
            try
            {
                if (SecondsSincePat >= CcCollective.parm_mean_pat_delay_s * 2 / 3)
                {
                    //send PAT
                    if (!await ProbeAsync("SYN-PAT").FastPath().ConfigureAwait(Zc))
                    {
                        if (Collected)
                            _logger.Error($"-/> {nameof(ProbeAsync)}: PAT Send [FAILED], {Description}, {MetaDesc}");
                    }
                }

                //Watchdog failure
                if (SecondsSincePat > CcCollective.parm_mean_pat_delay_s * 2)
                {
                    //swarm attempt
                    for (int j = 0; j < 3; j++)
                    {
                        await ProbeAsync("SYN-SWA").FastPath().ConfigureAwait(Zc);
                        await Task.Delay(parm_max_network_latency_ms).ConfigureAwait(Zc);
                    }

                    //cull
                    if (SecondsSincePat > CcCollective.parm_mean_pat_delay_s)
                    {
                        _logger.Trace($"w {nameof(EnsureRoboticsAsync)} - {Description}, s = {SecondsSincePat} >> {CcCollective.parm_mean_pat_delay_s}, {MetaDesc}");
                        Zero(new IoNanoprobe($"-wd: l = {SecondsSincePat}s ago, uptime = {TimeSpan.FromMilliseconds(Uptime.ElapsedMs()).TotalHours:0.00}h"));
                    }
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
                if (!Proxy)
                {
                    //Discoveries
                    await ZeroAsync(static async @this =>
                    {
                        await @this.ProcessDiscoveriesAsync().FastPath().ConfigureAwait(@this.Zc);
                    }, this, TaskCreationOptions.None ).FastPath().ConfigureAwait(Zc);

                    //UDP traffic
                    await ZeroAsync(static async state =>
                    {
                        var (@this, assimilateAsync) = state;
                        await assimilateAsync().FastPath().ConfigureAwait(@this.Zc);
                    }, ValueTuple.Create<CcAdjunct, Func<ValueTask>>(this, base.BlockOnReplicateAsync), TaskCreationOptions.None).FastPath().ConfigureAwait(Zc);

                    await AsyncTasks.Token.BlockOnNotCanceledAsync().FastPath().ConfigureAwait(Zc);
                }
                else
                {
                    await AsyncTasks.Token.BlockOnNotCanceledAsync().FastPath().ConfigureAwait(Zc);
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
                    if (existingNeighbor.Source?.IsOperational??false)
                    {
                        _logger.Trace($"Drone already operational, dropping {existingNeighbor.Description}");
                        return false;
                    }
                    existingNeighbor.Zero(new IoNanoprobe($"Dropped because stale from {existingNeighbor.Description}"));
                }

                await ZeroAsync(static async @this =>
                {
                    try
                    {
                        await Task.Delay(@this._random.Next(@this.parm_max_network_latency_ms / 5 + 1), @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);
                        //await Task.Yield();

                        var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        AdjunctState oldState;
                        if ((oldState = @this.CompareAndEnterState(AdjunctState.Connecting, AdjunctState.Fusing)) != AdjunctState.Fusing)
                        {
                            @this._logger.Trace($"{nameof(ConnectAsync)} - {@this.Description}: Invalid state, {oldState}. Wanted {nameof(AdjunctState.Fusing)}");
                            return;
                        }
                        
                        //Attempt the connection, race to win
                        if (!await @this.CcCollective.ConnectToDroneAsync(@this).FastPath().ConfigureAwait(@this.Zc))
                        {
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
                }, this, TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(Zc);

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
        /// Processes protocol messages
        /// </summary>
        /// <param name="batchJob">The consumer that need processing</param>
        /// <param name="channel">The arbiter</param>
        /// <param name="processCallback">The process callback</param>
        /// <param name="nanite"></param>
        /// <returns>Task</returns>
        private async ValueTask ProcessMsgBatchAsync<T>(IoSink<CcProtocBatchJob<chroniton, CcDiscoveryBatch>> batchJob,
            IoConduit<CcProtocBatchJob<chroniton, CcDiscoveryBatch>> channel,
            Func<CcDiscoveryMessage, CcDiscoveryBatch, IoConduit<CcProtocBatchJob<chroniton, CcDiscoveryBatch>>, T, ValueTask> processCallback, T nanite)
        {
            if (batchJob == null)
                return;

            CcDiscoveryBatch msgBatch = default;
            try
            {
                var job = (CcProtocBatchJob<chroniton, CcDiscoveryBatch>)batchJob;
                msgBatch = job.Get();
                for (int i = 0; i < msgBatch.Count; i++)
                {
                    var message = msgBatch[i];
                    try
                    {
                        await processCallback(message, msgBatch, channel, nanite).FastPath();
                    }
                    catch (Exception) when (!Zeroed()){}
                    catch (Exception e)when(Zeroed())
                    {
                        if (Collected)
                            _logger.Debug(e, $"Processing protocol failed for {Description}: ");
                    }
                }
                
                batchJob.State = IoJobMeta.JobState.Consumed;
            }
            catch when(Zeroed()){}
            catch (Exception e)when(!Zeroed())
            {
                if (Collected)
                    _logger.Debug(e, "Error processing message batch");
            }
            finally
            {
                if (msgBatch != null) 
                    msgBatch.ReturnToHeap();
            }
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
                    if (_protocolConduit == null)
                    {
                        _protocolConduit = MessageService.GetConduit<CcProtocBatchJob<chroniton, CcDiscoveryBatch>>(nameof(CcAdjunct));
                        if (_protocolConduit != null)
                        {
                            _produceTaskPool = new ValueTask<bool>[_protocolConduit.ZeroConcurrencyLevel()];
                            _consumeTaskPool = new ValueTask<bool>[_protocolConduit.ZeroConcurrencyLevel()];
                            break;
                        }
                    }
                
                    if(_protocolConduit != null)
                        break;

                    //Get the conduit
                    _logger.Trace($"Waiting for {Description} stream to spin up...");
                    
                    //Init the conduit
                    if (_protocolConduit != null)
                    {
                        _produceTaskPool = new ValueTask<bool>[_protocolConduit.ZeroConcurrencyLevel()];
                        _consumeTaskPool = new ValueTask<bool>[_protocolConduit.ZeroConcurrencyLevel()];
                    }
                    
                    await Task.Delay(parm_conduit_spin_up_wait_time++, AsyncTasks.Token).ConfigureAwait(Zc);
                } while (_protocolConduit == null && !Zeroed());

            
                //fail fast on these
                if(_protocolConduit.Zeroed())
                    return;

                do{
//The producer
                    ValueTask consumer = default;
                    var producer = ZeroOptionAsync(static async @this =>
                    {
                        try
                        {
                            while (!@this.Zeroed() && @this._protocolConduit.Source.IsOperational)
                            {
                                for (var i = 0; i < @this._produceTaskPool.Length && @this._protocolConduit.Source.IsOperational; i++)
                                {
                                    try
                                    {
#pragma warning disable CA2012 // Use ValueTasks correctly
                                        @this._produceTaskPool[i] = @this._protocolConduit.ProduceAsync();
#pragma warning restore CA2012 // Use ValueTasks correctly
                                    }
                                    catch (Exception e)
                                    {
                                        @this._logger.Error(e, $"Production failed for {@this.Description}");
                                        break;
                                    }
                                }

                                if (!await @this._produceTaskPool[^1])
                                    break;
                            }
                        }
                        catch when (@this.Zeroed() || @this.Source.Zeroed() || @this._protocolConduit?.Source == null) { }
                        catch (Exception e) when (!@this.Zeroed())
                        {
                            @this._logger?.Error(e, $"{@this.Description}");
                        }
                    
                    },this, TaskCreationOptions.DenyChildAttach);

                    consumer = ZeroOptionAsync(static async @this  =>
                    {
                        //the consumer
                        try
                        {
                            while (!@this.Zeroed() && @this._protocolConduit.Source.IsOperational)
                            {
                                //consume
                                for (var i = 0; i < @this._consumeTaskPool.Length && @this._protocolConduit.Source.IsOperational; i++)
                                {
#pragma warning disable CA2012 // Use ValueTasks correctly
                                    @this._consumeTaskPool[i] = @this._protocolConduit.ConsumeAsync(static async (batchJob, @this) =>
                                    {
                                        try
                                        {
                                            await @this.ProcessMsgBatchAsync(batchJob, @this._protocolConduit,static async (msgBatch, discoveryBatch, ioConduit, @this) =>
                                            {
                                                IMessage message = default;
                                                chroniton packet = default;
                                                try
                                                {
                                                    message = msgBatch.EmbeddedMsg;
                                                    packet = msgBatch.Message;

                                                    if (packet.Data.Length == 0)
                                                    {
                                                        @this._logger.Warn($"Got zero message from {CcDesignation.ShortId(packet.PublicKey.Memory.AsArray())}");
                                                        return;
                                                    }
                                                    var srcEndPoint = discoveryBatch.RemoteEndPoint.GetEndpoint();
                                                    var key = srcEndPoint.ToString();
                                                    var routed = @this.Router._routingTable.TryGetValue(key, out var currentRoute);
                                                    //Router
                                                    try
                                                    {
                                                        if (!routed)
                                                        {
                                                            currentRoute = (CcAdjunct)@this.Hub.Neighbors.Values.FirstOrDefault(n => ((CcAdjunct)n).Proxy && ((CcAdjunct)n).Collected && Equals(((CcAdjunct)n).RemoteAddress.IpEndPoint, srcEndPoint));
                                                            if (currentRoute != null)
                                                            { 
                                                                //TODO, proxy adjuncts need to malloc the same way when listeners spawn them.
                                                                if (!@this.Router._routingTable.TryAdd(key, currentRoute))
                                                                {
                                                                    @this.Router._routingTable.TryGetValue(key, out currentRoute);
                                                                }
                                                                else
                                                                {
#if DEBUG
                                                                    @this?._logger?.Debug($"Adjunct [ROUTED]: {discoveryBatch.RemoteEndPoint} ~> {@this.MessageService.IoNetSocket.LocalAddress}");
#endif
                                                                }
                                                            }
                                                            else
                                                            {
                                                                //Console.WriteLine("");
                                                            }
                                                        }
                                                        else
                                                        {
                                                            //validate
                                                            for (var j = 2; j < 4; j++)
                                                            {
                                                                for (var i = j; i < currentRoute.Designation.PublicKey.Length; i += i)
                                                                {
                                                                    if (currentRoute.Zeroed())
                                                                    {
                                                                        currentRoute = null;
                                                                        break;
                                                                    }

                                                                    if (i < currentRoute.Designation.PublicKey.Length && currentRoute.Designation.PublicKey[i] != packet.PublicKey[i] && !currentRoute.Probed /*TODO: What is going on here?*/)
                                                                    {
                                                                        var pk1 = currentRoute.Designation.PkShort();
                                                                        var pk2 = CcDesignation.ShortId(packet.PublicKey);

                                                                        var msg = $"{nameof(@this.Router)}[{i}]: Dropped route {currentRoute.MessageService.IoNetSocket.RemoteNodeAddress.IpEndPoint}/{pk1} ~> {discoveryBatch.RemoteEndPoint.GetEndpoint()}/{pk2}: {currentRoute.Description}";

                                                                        @this._logger.Warn(msg);
                                                                        currentRoute.Zero(new IoNanoprobe(msg));
                                                                        currentRoute = null;
                                                                        break;
                                                                    }
                                                                }
                                                                if (currentRoute == null)
                                                                    break;
                                                            }
                                                        }
                                                    }
                                                    catch when(@this.Zeroed() || (currentRoute?.Zeroed()?? false)) {}
                                                    catch (Exception e) when(!@this.Zeroed() && !(currentRoute?.Zeroed() ?? false))
                                                    {
                                                        @this._logger.Error(e, $"{nameof(Router)} failed with errors: ");
                                                        return;
                                                    }

                                                    if (currentRoute != null)
                                                    {
#if DEBUG
                                                        if (currentRoute.Designation.PkShort() !=
                                                            CcDesignation.ShortId(packet.PublicKey))
                                                        {
                                                            @this._logger!.Warn($"Invalid routing detected wanted = {currentRoute.Designation.PkShort()} - {currentRoute.RemoteAddress.IpEndPoint}, got = {CcDesignation.ShortId(packet.PublicKey)} - {srcEndPoint}, {(CcDiscoveries.MessageTypes)packet.Type}");
                                                            currentRoute = @this;
                                                        }
                                                        else
#endif
                                                            currentRoute.IncEventCounter();
                                                    }
                                                    else
                                                    {
                                                        currentRoute = @this;
                                                    }
                                                    
                                                    if (!@this.Zeroed() && !currentRoute.Zeroed())
                                                    {
                                                        try
                                                        {
                                                            //switch ((CcDiscoveries.MessageTypes)MemoryMarshal.Read<int>(packet.Signature.Span))
                                                            switch ((CcDiscoveries.MessageTypes)packet.Type)
                                                            {
                                                                case CcDiscoveries.MessageTypes.Probe:
                                                                    if(((CcProbeMessage)message).Timestamp.ElapsedMs() < @this.parm_max_network_latency_ms * 2)
                                                                        await currentRoute.ProcessAsync((CcProbeMessage)message, srcEndPoint, packet).FastPath().ConfigureAwait(@this.Zc);
                                                                    break;
                                                                case CcDiscoveries.MessageTypes.Probed:
                                                                    if (((CcProbeResponse)message).Timestamp.ElapsedMs() < @this.parm_max_network_latency_ms * 2)
                                                                        await currentRoute.ProcessAsync((CcProbeResponse)message, srcEndPoint, packet).FastPath().ConfigureAwait(@this.Zc);
                                                                    break;
                                                                case CcDiscoveries.MessageTypes.Sweep:
                                                                    if (!currentRoute.Proxy && !@this.CcCollective.ZeroDrone)
                                                                    {
#if DEBUG
                                                                        @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.Sweep)}: Unrouted request from {srcEndPoint} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}");
#endif
                                                                        break;
                                                                    }
                                                                    if (((CcSweepRequest)message).Timestamp.ElapsedMs() < @this.parm_max_network_latency_ms * 2)
                                                                        await currentRoute.ProcessAsync((CcSweepRequest)message, srcEndPoint, packet).FastPath().ConfigureAwait(@this.Zc);
                                                                    break;
                                                                case CcDiscoveries.MessageTypes.Swept:
                                                                    if (!currentRoute.Proxy)
                                                                    {
#if DEBUG
                                                                        @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.Swept)}: Unrouted request from {srcEndPoint} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}");                                                               
#endif
                                                                        break;
                                                                    }
                                                                    if (((CcSweepResponse)message).Timestamp.ElapsedMs() < @this.parm_max_network_latency_ms * 2)
                                                                        await currentRoute.ProcessAsync((CcSweepResponse) message, srcEndPoint, packet).FastPath().ConfigureAwait(@this.Zc);
                                                                    break;
                                                                case CcDiscoveries.MessageTypes.Fuse:
                                                                    if (((CcFuseRequest)message).Timestamp.ElapsedMs() < @this.parm_max_network_latency_ms * 2)
                                                                        await currentRoute.ProcessAsync((CcFuseRequest) message, srcEndPoint, packet).FastPath().ConfigureAwait(@this.Zc);
                                                                    break;
                                                                case CcDiscoveries.MessageTypes.Fused:
//                                                                    if (!currentRoute.Proxy)
//                                                                    {
//#if DEBUG
//                                                                        @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.Fused)}: Unrouted request from {srcEndPoint} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}");
//#endif
//                                                                        break;
//                                                                    }

                                                                    if (!@this.CcCollective.ZeroDrone && ((CcFuseResponse)message).Timestamp.ElapsedMs() < @this.parm_max_network_latency_ms * 2)
                                                                        await currentRoute.ProcessAsync((CcFuseResponse) message, srcEndPoint, packet).FastPath().ConfigureAwait(@this.Zc);
                                                                    break;
                                                                case CcDiscoveries.MessageTypes.Defuse:
                                                                    if (!currentRoute.Proxy)
                                                                    {
#if DEBUG
                                                                        @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.Defuse)}: Unrouted request from {srcEndPoint} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}");
#endif
                                                                        break;
                                                                    }

                                                                    if (!@this.CcCollective.ZeroDrone && !@this.CcCollective.ZeroDrone && ((CcDefuseRequest)message).Timestamp.ElapsedMs() < @this.parm_max_network_latency_ms * 2)
                                                                        await currentRoute.ProcessAsync((CcDefuseRequest) message, srcEndPoint, packet).FastPath().ConfigureAwait(@this.Zc);
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
                                            }, @this);
                                        }
                                        finally
                                        {
                                            if (batchJob != null && batchJob.State != IoJobMeta.JobState.Consumed)
                                                batchJob.State = IoJobMeta.JobState.ConsumeErr;
                                        }
                                    }, @this);
#pragma warning restore CA2012 // Use ValueTasks correctly
                                }

                                if (!await @this._consumeTaskPool[^1].FastPath().ConfigureAwait(@this.Zc))
                                    break;
                            }
                        }
                        catch when(@this.Zeroed() || @this._protocolConduit?.Source == null) {}
                        catch (Exception e) when (!@this.Zeroed() && @this._protocolConduit?.Source != null)
                        {
                            @this._logger?.Error(e, $"{@this.Description}");
                        }
                    }, this, TaskCreationOptions.DenyChildAttach | TaskCreationOptions.PreferFairness);
                    await Task.WhenAll(producer.AsTask(), consumer.AsTask()).ConfigureAwait(Zc);
                }
                while (!Zeroed());
            }
            catch when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger?.Debug(e, $"Error processing {Description}");
            }

            if (!Proxy && !Zeroed())
            {
                _logger?.Fatal($"{Description} Stopped prosessing discoveries.");
            }
            else if (!Proxy)
            {
                _logger?.Debug($"{Description} Stopped prosessing discoveries!");
            }
        }

        /// <summary>
        /// Defuse drop request
        /// </summary>
        /// <param name="request">The request</param>

        /// <param name="endpoint"></param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcDefuseRequest request, IPEndPoint endpoint, chroniton packet)
        {
            if (!Assimilating)
            {
                if(Proxy)
                    _logger.Error($"{nameof(CcDefuseRequest)}: Ignoring {request.Timestamp.ElapsedMsToSec()}s old/invalid request, {Description}");
                return;
            }
            
            //PAT
            LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            if (IsDroneAttached)
            {
                _logger.Trace($"{nameof(CcDefuseRequest)}: {Direction} Drone = {_drone.Key}: {Description}, {MetaDesc}");
                await DetachDroneAsync().FastPath().ConfigureAwait(Zc);
            }
            //else
            //{
            //    //Hup
            //    if (Direction == Heading.Undefined && CcCollective.IngressCount < CcCollective.parm_max_inbound)
            //        await ProbeAsync().FastPath().ConfigureAwait(Zc);
            //}
        }

        /// <summary>
        /// Fusing Request message from client
        /// </summary>
        /// <param name="request">The request</param>
        /// <param name="endpoint">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcFuseRequest request, IPEndPoint endpoint, chroniton packet)
        {
            Interlocked.Increment(ref _fuseRequestsRecvCount);

            //emit event
            if (AutoPeeringEventService.Operational)
                await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                {
                    EventType = AutoPeerEventType.AddAdjunct,
                    Adjunct = new Adjunct
                    {
                        Id = Designation.IdString(),
                        CollectiveId = CcCollective.Hub.Router.Designation.IdString()
                    }
                }).FastPath().ConfigureAwait(Zc);

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

                var remote = IoNodeAddress.CreateFromEndpoint("udp", endpoint);
                //var remote = IoNodeAddress.CreateFromEndpoint("udp", packet.Header.Ip.Src.GetEndpoint());
                if ((sent = await Router.SendMessageAsync(reject.ToByteArray(), CcDiscoveries.MessageTypes.Fuse, remote).FastPath().ConfigureAwait(Zc)) > 0)
                {
                    _logger.Trace($"-/> {nameof(CcFuseRequest)}({sent}): Sent {(reject.Accept ? "ACCEPT" : "REJECT")}, {Description}");
                }
                else
                    _logger.Debug($"<\\- {nameof(CcFuseRequest)}({sent}): [FAILED], {Description}, {MetaDesc}");

                if (!Collected) return;

                //DMZ-syn
                //We syn here (Instead of in process ping) to force the other party to do some work (prepare to connect) before we do work (verify).
                if ((CcCollective.Hub.Neighbors.Count <= CcCollective.MaxDrones || CcCollective.ZeroDrone) && !await Router
                        .ProbeAsync("ACK-SYN-ACK", remote, CcDesignation.FromPubKey(packet.PublicKey.Memory).IdString())
                        .FastPath().ConfigureAwait(Zc))
                {
                    _logger.Trace($"Failed to send ACK-SYN-ACK to {remote}! {Description}");
                }
                
                return;
            }

            //PAT
            LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            var fuseResponse = new CcFuseResponse
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                ReqHash = UnsafeByteOperations.UnsafeWrap(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                Accept = CcCollective.IngressCount < CcCollective.parm_max_inbound && _direction == 0
            };

            if (fuseResponse.Accept)
            {
                var stateIsValid = _currState.Value != AdjunctState.Connected && CompareAndEnterState(AdjunctState.Fusing, AdjunctState.Verified, overrideHung: parm_max_network_latency_ms * 2) == AdjunctState.Verified;

                fuseResponse.Accept &= stateIsValid;

                if (!stateIsValid)
                {
                    _logger.Trace($"{Description}: Invalid state ~{_currState.Value}, age = {_currState.EnterTime.ElapsedMs()}ms. Wanted {nameof(AdjunctState.Verified)} -  [RACE OK!]");
                }
            }

            if ((sent= await SendMessageAsync(fuseResponse.ToByteArray(),
                    CcDiscoveries.MessageTypes.Fused).FastPath().ConfigureAwait(Zc)) > 0)
            {
                var response = $"{(fuseResponse.Accept ? "accept" : "reject")}";
                _logger.Debug($"-/> {nameof(CcFuseRequest)}({sent}): Sent {response}, {Description}");

                if(fuseResponse.Accept)
                    _logger.Debug($"# {Description}");

                if (AutoPeeringEventService.Operational)
                    await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.SendProtoMsg,
                        Msg = new ProtoMsg
                        {
                            CollectiveId = Hub.Router.Designation.IdString(),
                            Id = Designation.IdString(),
                            Type = $"peer response {response}"
                        }
                    }).FastPath().ConfigureAwait(Zc);
            }
            else
                _logger.Debug($"<\\- {nameof(CcFuseRequest)}: Send fuse response [FAILED], {Description}, {MetaDesc}");

            if(!fuseResponse.Accept)
                await SeduceAsync("SYN-SE", Heading.Ingress).FastPath().ConfigureAwait(Zc);
        }

        private long _stealthy;

        /// <summary>
        /// Peer response message from client
        /// </summary>
        /// <param name="response">The request</param>
        /// <param name="endpoint">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcFuseResponse response, IPEndPoint endpoint, chroniton packet)
        {
            //Validate
            if (!Collected)
            {
                _logger.Trace($"{nameof(CcFuseResponse)}: Dropped, not collected");
                return;
            }

            var f1 = _fuseRequest.Count;
            var f2 = Router._fuseRequest.Count;
            var fuseMessage = await _fuseRequest.ResponseAsync(endpoint.ToString(), response.ReqHash).FastPath().ConfigureAwait(Zc);

            if(!fuseMessage)
                fuseMessage = await Router._fuseRequest.ResponseAsync(endpoint.ToString(), response.ReqHash).FastPath().ConfigureAwait(Zc);

            if (!fuseMessage)
            {
                if (Collected && Proxy)
                {
                    if (response.ReqHash.Length > 0)
                    {
                        _logger.Error($"<\\- {nameof(CcFuseResponse)}({packet.CalculateSize()}, {response.CalculateSize()}, {response.Timestamp.ElapsedMs()}ms) - {response.ToByteArray().HashSig()}({response.ReqHash.Length}): No-Hash!!!, r = {response.ReqHash.Memory.HashSig()}, _fuseRequest = {f1}({_fuseRequest.Count}), {f2}({Router._fuseRequest.Count}), {MessageService.IoNetSocket.LocalNodeAddress} ~> {endpoint}({RemoteAddress})");
                    }
#if DEBUG
                    _logger.Error($"<\\- {nameof(CcFuseResponse)}({packet.CalculateSize()}, {response.CalculateSize()}, {response.Timestamp.ElapsedMs()}ms) - {response.ToByteArray().HashSig()}({response.ReqHash.Length}): No-Hash!!!, r = {response.ReqHash.Memory.HashSig()}, _fuseRequest = {f1}({_fuseRequest.Count}), {f2}({Router._fuseRequest.Count}), {MessageService.IoNetSocket.LocalNodeAddress} ~> {endpoint}({RemoteAddress})");
#endif
                }
                    
                return;
            }
            
            //Validated
            _logger.Debug($"<\\- {nameof(CcFuseResponse)}: Accepted = {response.Accept}, {Description}");

            //PAT
            LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            switch (response.Accept)
            {
                //Race for 
                case true when _direction == 0 && _currState.Value > AdjunctState.Unverified:
                {
                    if (!await ConnectAsync().FastPath().ConfigureAwait(Zc))
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
                        if(oldState != AdjunctState.Connected && _currState.EnterTime.ElapsedMs() > parm_max_network_latency_ms)
                            _logger.Warn($"{nameof(CcFuseResponse)} - {Description}: Invalid state, {oldState}, age = {_currState.EnterTime.ElapsedMs()}ms. Wanted {nameof(AdjunctState.Fusing)}");
                    }
                    else
                    {
                        if (!await SeduceAsync("SYN-SE", Heading.Ingress).FastPath().ConfigureAwait(Zc))
                            _logger.Trace($"{nameof(SeduceAsync)} skipped!");
                        
                        if (Proxy)
                            await SweepAsync(CcCollective.parm_mean_pat_delay_s * 1000).FastPath().ConfigureAwait(Zc);
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
                {
                    return 0;
                }

                if (data == null || data.Length == 0 || dest == null && !Proxy)
                {
                    return 0;
                }

                dest ??= RemoteAddress;

                chroniton packet = null;
                try
                { 
                    packet = _chronitonHeap.Take();

                    if (packet == null)
                        throw new OutOfMemoryException($"{nameof(_chronitonHeap)}: {_chronitonHeap.Description}, {Description}");

                    packet.Header.Ip.Dst = dest.IpEndPoint.AsByteString();

#if DEBUG
                    packet.Header.Ip.Src = MessageService.IoNetSocket.LocalNodeAddress.IpEndPoint.AsByteString();
#endif

                    packet.Data = UnsafeByteOperations.UnsafeWrap(data);

                    if(packet.PublicKey.Length == 0)
                        packet.PublicKey = UnsafeByteOperations.UnsafeWrap(CcCollective.CcId.PublicKey);

                    packet.Type = (int)type;
                    
                    var packetMsgRaw = packet.Data.Memory.AsArray();

                    packet.Signature = UnsafeByteOperations.UnsafeWrap(CcCollective.CcId.Sign(packetMsgRaw, 0, packetMsgRaw.Length));

                    var msgRaw = packet.ToByteArray();
                    
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
                    return await MessageService.IoNetSocket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint).FastPath().ConfigureAwait(Zc);
#endif
                
#if DEBUG
                //await sent.OverBoostAsync().FastPath().ConfigureAwait(ZC);
                //_logger.Trace(
                //    $"=/> {Enum.GetName(typeof(CcDiscoveries.MessageTypes), packet.Type)} {MessageService.IoNetSocket.LocalAddress} /> {dest.IpEndPoint}>>{data.Memory.PayloadSig()}: s = {sent}");
#endif
                }
                catch when (Zeroed()) { }
                catch (Exception e) when (!Zeroed())
                {
                    if (Collected)
                        _logger.Debug(e, $"Failed to send message {Description}");
                }
                finally
                {
                    if (packet != null)
                    {
                        packet.Data = ByteString.Empty;
                        packet.Signature = ByteString.Empty;
                        packet.Header.Ip.Dst = ByteString.Empty;
                        Thread.MemoryBarrier();
                        _chronitonHeap.Return(packet);
                    }
                }
            }
            catch when(Zeroed()){}
            catch (Exception e)when(!Zeroed())
            {
                if (Collected)
                    _logger.Debug(e, $"Failed to send message {Description}");
            }

            return 0;
        }

        /// <summary>
        /// Sweep response
        /// </summary>
        /// <param name="response">The response</param>
        /// <param name="endpoint">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcSweepResponse response, IPEndPoint endpoint, chroniton packet)
        {
            var matchRequest = await _sweepRequest.ResponseAsync(endpoint.ToString(), response.ReqHash).FastPath().ConfigureAwait(Zc);

            if(!matchRequest)
                matchRequest = await Router._sweepRequest.ResponseAsync(endpoint.ToString(), response.ReqHash).FastPath().ConfigureAwait(Zc);

            if (!matchRequest || !Assimilating || response.Contacts.Count > parm_max_swept_drones)
            {
                if (Proxy && Collected && response.Contacts.Count <= parm_max_swept_drones)
                    _logger.Debug($"{nameof(CcSweepResponse)}{response.ToByteArray().PayloadSig()}: Reject, rq = {response.ReqHash.Memory.HashSig()}, {response.Contacts.Count} > {parm_max_swept_drones}? from {MakeId(CcDesignation.FromPubKey(packet.PublicKey.Memory), IoNodeAddress.CreateFromEndpoint("udp", endpoint))}, RemoteAddress = {RemoteAddress}, c = {_sweepRequest.Count}, matched[{endpoint}]");
                return;
            }

            _logger.Trace($"<\\- {nameof(CcSweepResponse)}: Received {response.Contacts.Count} potentials from {Description}");

            if(response.Contacts.Count > 0)
                _lastScan = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            //PAT
            LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            foreach (var sweptDrone in response.Contacts)
            {
                //Any services attached?
                //if (responsePeer.Services?.Map == null || responsePeer.Services.Map.Count == 0)
                //{
                //    _logger.Trace(
                //        $"<\\- {nameof(CcSweepResponse)}: Invalid services recieved!, map = {responsePeer.Services?.Map}, count = {responsePeer.Services?.Map?.Count ?? -1}");
                //    continue;
                //}

                ////ignore strange services
                //if (count > parm_max_services)
                //    continue;

                //var remoteEp = responsePeer.Services.Map[CcService.Keys.peering.ToString()];
                ////Never add ourselves (by NAT)
                //if (responsePeer.Header.Src.Zrl.AsString() == CcCollective.ExtAddress.IpPort)
                //    continue;

                //Never add ourselves (by ID)
                if (sweptDrone.PublicKey.SequenceEqual(CcCollective.CcId.PublicKey))
                    continue;

                //Don't add already known neighbors
                var id = CcDesignation.ShortId(sweptDrone.PublicKey);
                if (Hub.Neighbors.Values.Any(n => ((CcAdjunct) n).Designation.PkShort() == id))
                    continue;

                //var services = new CcService {CcRecord = new CcRecord()};

                //foreach (var (key, value) in responsePeer.Services.Map)
                //{
                //    services.CcRecord.Endpoints.TryAdd(Enum.Parse<CcService.Keys>(key),
                //        IoNodeAddress.Create($"{value.Network}://{responsePeer.Ip}:{value.Port}"));
                //}

                //sanity check
                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                //if (services == null || services.CcRecord.Endpoints.IsEmpty)
                //    continue;

                if (!CcCollective.ZeroDrone)
                {
                    await Router.ProbeAsync("SYN-DMZ", dest: IoNodeAddress.CreateFromEndpoint("udp",sweptDrone.Url.GetEndpoint())).FastPath().ConfigureAwait(false);
                }
                else
                {
                    await ZeroAsync(static async state =>
                    {
                        var (@this, newRemoteEp, id) = state;
                        await Task.Delay(@this._random.Next(@this.parm_max_network_latency_ms) + @this.parm_max_network_latency_ms, @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);

                        if (!await @this.CollectAsync(newRemoteEp, id).FastPath().ConfigureAwait(@this.Zc))
                        {
#if DEBUG
                            @this._logger.Trace($"{@this.Description}: Collecting {newRemoteEp.Address} failed!");
#endif
                        }

                    }, ValueTuple.Create(this, sweptDrone.Url.GetEndpoint(), CcDesignation.FromPubKey(sweptDrone.PublicKey.Memory)), TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(Zc);

                }
            }
            
        }
        
        /// <summary>
        /// Collect another adjunct into the collective
        /// </summary>
        /// <param name="newRemoteEp">The location</param>
        /// <param name="id">The adjunctId</param>
        /// <returns>A task, true if successful, false otherwise</returns>
        private async ValueTask<bool> CollectAsync(IPEndPoint newRemoteEp, CcDesignation id)
        {
            if (Zeroed() || Hub.Zeroed() || newRemoteEp != null && newRemoteEp.Equals(NatAddress?.IpEndPoint) || CcCollective.Hub.Neighbors.ContainsKey(id.PkShort()))
            {
                _logger.Trace($"x {id.PkShort()}");
                return false;
            }

            var newAdjunct = (CcAdjunct) Hub.MallocNeighbor(Hub, MessageService, Tuple.Create(id, newRemoteEp));

            if (!Zeroed() && Hub.ZeroAtomic(static (s, state, ___) =>
            {
                var (@this, newAdjunct) = state;
                try
                {
                    if (@this.Hub.Neighbors.Count >= @this.CcCollective.MaxAdjuncts)
                    {
                        //drop something
                        var bad = @this.Hub.Neighbors.Values.Where(n =>
                                ((CcAdjunct)n).Proxy && ((CcAdjunct)n).SecondsSincePat > @this.CcCollective.parm_mean_pat_delay_s ||
                                ((CcAdjunct) n).Proxy &&
                                ((CcAdjunct) n).Direction == Heading.Undefined &&
                                ((CcAdjunct) n).Uptime.ElapsedMs() > @this.parm_min_uptime_ms &&
                                ((CcAdjunct) n).State < AdjunctState.Verified)
                                .OrderByDescending(n => ((CcAdjunct)n).Uptime.ElapsedMs());

                        var good = @this.Hub.Neighbors.Values.Where(n =>
                                ((CcAdjunct)n).Proxy &&
                                ((CcAdjunct)n).Direction == Heading.Undefined &&
                                ((CcAdjunct)n).Uptime.ElapsedMs() > @this.parm_min_uptime_ms &&
                                ((CcAdjunct)n).State < AdjunctState.Fusing &&
                                ((CcAdjunct)n).TotalPats > @this.parm_min_pats_before_shuffle)
                            .OrderByDescending(n => ((CcAdjunct)n).Priority)
                            .ThenByDescending(n => ((CcAdjunct)n).Uptime.ElapsedMs());

                        var badList = bad.ToList();
                        if (badList.Any())
                        {
                            var dropped = badList.FirstOrDefault();
                            if (dropped != default && ((CcAdjunct)dropped).State < AdjunctState.Verified) 
                            {
                                ((CcAdjunct)dropped).Zero(new IoNanoprobe("got collected"));
                                @this._logger.Debug($"@ {dropped.Description}");
                            }
                        }
                        else //try harder 
                        {
                            foreach (var ioNeighbor in good)
                            {
                                if (((CcAdjunct)ioNeighbor).State < AdjunctState.Fusing)
                                {
                                    ((CcAdjunct)ioNeighbor).Zero(new IoNanoprobe("Assimilated!"));
                                    @this._logger.Debug($"@ {ioNeighbor.Description}");
                                    break;
                                }
                            }
                        }
                    }
                    
                    //Transfer?
                    if (@this.Hub.Neighbors.Count >= @this.CcCollective.MaxAdjuncts || !@this.Hub.Neighbors.TryAdd(newAdjunct.Key, newAdjunct))
                    {
                        newAdjunct.Zero(@this);
                        return new ValueTask<bool>(false);    
                    }
                    return new ValueTask<bool>(true);
                }
                catch when(@this.Zeroed()){}
                catch (Exception e) when(!@this.Zeroed())
                {
                    @this._logger.Error(e, $"{@this.Description??"N/A"}");
                }

                return new ValueTask<bool>(false);
            }, ValueTuple.Create(this, newAdjunct)))
            {
                //setup conduits to messages
#if DEBUG
                newAdjunct.DebugAddress = DebugAddress;
#endif
                //Start processing
                //await ZeroAsync(static async state =>
                //{
                //    var (@this, newAdjunct) = state;
                //    await @this.CcCollective.Hub.BlockOnAssimilateAsync(newAdjunct).FastPath().ConfigureAwait(@this.Zc);
                //}, ValueTuple.Create(this, newAdjunct), TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(false);
                //if (!await newAdjunct.SeduceAsync("ACK", ignoreZeroDrone: true).FastPath()
                //        .ConfigureAwait(Zc))
                //{
                //    _logger.Error($"{nameof(CollectAsync)}: Failed to send ACK");
                //    return false;
                //}

                if (!await newAdjunct.ProbeAsync("SYN").FastPath().ConfigureAwait(Zc))
                {
                    _logger.Debug($"{newAdjunct.Description}: Unable to send ping!!!");
                    return false;
                }

                return true;
            }
            else
            {
                newAdjunct?.Zero(new IoNanoprobe("CollectAsync failed!"));
            }

            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="endpoint"></param>
        /// <param name="packet"></param>
        /// <returns></returns>
        private async ValueTask ProcessAsync(CcSweepRequest request, IPEndPoint endpoint, chroniton packet)
        {
            if (!(CcCollective.ZeroDrone || Assimilating))
            {
                if (Assimilating)
                {
                    _logger.Trace(
                        $"<\\- {nameof(CcSweepRequest)}: [ABORTED] {!Assimilating}, age = {request.Timestamp.CurrentMsDelta():0.0}ms/{parm_max_network_latency_ms * 2:0.0}ms, {Description}, {MetaDesc}");
                }
                    
                return;
            }

            //PAT
            LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            var sweptResponse = new CcSweepResponse
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
                .ThenBy(n => ((CcAdjunct)n).Uptime.ElapsedMs()).ToList();

            if (!certified.Any())
            {
                certified = Hub.Neighbors.Values.Where(n => n != this)
                    .OrderByDescending(n => ((CcAdjunct)n).IsDroneConnected ? 0 : 1)
                    .ThenByDescending(n => (int)((CcAdjunct)n).Priority)
                    .ThenBy(n => ((CcAdjunct)n).Uptime.ElapsedMs()).ToList();
            }

            foreach (var ioNeighbor in certified.Take((int)parm_max_swept_drones))
            {
                try
                {
                    var remoteEp = ((CcAdjunct)ioNeighbor).NatAddress?.IpEndPoint.AsByteString();
                    remoteEp ??= ((CcAdjunct)ioNeighbor).MessageService.IoNetSocket.RemoteNodeAddress.IpEndPoint.AsByteString();

                    sweptResponse.Contacts.Add(new CcAdjunctModel
                    {
                        PublicKey = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(((CcAdjunct) ioNeighbor).Designation.PublicKey)),
                        Url = remoteEp
                    });
                }
                catch when (ioNeighbor.Zeroed()){}
                catch (Exception e) when (!ioNeighbor.Zeroed())
                {
                    _logger.Error(e,
                        $"{nameof(CcSweepResponse)}: Failed to add adjunct, contacts = {sweptResponse.Contacts}, pk = {((CcAdjunct)ioNeighbor)?.Designation?.PublicKey}, remote = {RemoteAddress} ");
                    continue;
                }

                if (CcCollective.ZeroDrone)
                {
                    Interlocked.Exchange(ref ((CcAdjunct)ioNeighbor)._fuseRequestsRecvCount,
                        ((CcAdjunct)ioNeighbor)._fuseRequestsRecvCount / 2);
                }

                count++;
            }

            int sent;
            if ((sent = await SendMessageAsync(sweptResponse.ToByteArray(), CcDiscoveries.MessageTypes.Swept).FastPath().ConfigureAwait(Zc)) > 0)
            {
                if(count > 0)
                    _logger.Debug($"-/> {nameof(CcSweepResponse)}({sent}): Sent {count} discoveries to {Description}");

                //Emit message event
                if (AutoPeeringEventService.Operational)
                    await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.SendProtoMsg,
                        Msg = new ProtoMsg
                        {
                            CollectiveId = Hub.Router.Designation.IdString(),
                            Id = Designation.IdString(),
                            Type = "discovery response"
                        }
                    }).FastPath().ConfigureAwait(Zc);
            }
            else
            {
                if (Collected)
                    _logger.Debug($"<\\- {nameof(CcSweepRequest)}({sent}): Send [FAILED], {Description}, {MetaDesc}");
            }
        }

        /// <summary>
        /// Probe message
        /// </summary>
        /// <param name="probeMessage">The probe packet</param>
        /// <param name="remoteEp">The originating endpoint</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcProbeMessage probeMessage, IPEndPoint remoteEp, chroniton packet)
        {
            //syn trigger
            static async ValueTask CheckSynTriggerAsync(CcAdjunct @this, long delay, IoNodeAddress ioNodeAddress)
            {
                if (delay < @this.parm_max_network_latency_ms * 2)
                {
                    if (@this.Direction == Heading.Undefined && @this.CcCollective.IngressCount < @this.CcCollective.parm_max_inbound &&
                        @this.Hub?.Router != null)
                    {
                        @this._fuseRequestsSentCount = -@this.parm_zombie_max_connection_attempts;
                        if (!await @this.ProbeAsync("SYN-FLD", ioNodeAddress).FastPath().ConfigureAwait(@this.Zc))
                        {
                            @this._logger.Trace($"<\\- {nameof(ProbeAsync)}(SYN-FLD): [FAILED] {@this.Description}");
                        }
                        else
                        {
                            Interlocked.Add(ref @this._triggerTime, -@this.parm_max_network_latency_ms * 2);
                        }
                    }
                }
                else
                {
                    Interlocked.Exchange(ref @this._triggerTime, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                }
            }

            //Drop old API calls
            if(probeMessage.Protocol != parm_protocol_version)
                return;

            //Drop if we are saturated
            if(!Proxy && !CcCollective.ZeroDrone && Hub.Neighbors.Count > CcCollective.MaxAdjuncts && _random.Next(1000000) > 1000000/4)
                return;

            var response = new CcProbeResponse
            {
                Protocol = parm_protocol_version,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                ReqHash = UnsafeByteOperations.UnsafeWrap(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray()))
            };

            //if (_serviceMapLocal == null)
            //{ 
            //    pong = PrepareServices(packet, RemoteAddress.IpEndPoint ,remoteEp);
            //}
            //else
            //{
            //    pong = new CcProbeResponse
            //    {
            //        ReqHash = UnsafeByteOperations.UnsafeWrap(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
            //        DstAddr = $"{remoteEp.Address}", //TODO, add port somehow
            //        Services = _serviceMapLocal
            //    };
            //}

            var triggerDelta = CcCollective.ZeroDrone ? parm_max_network_latency_ms : Volatile.Read(ref _triggerTime).ElapsedMs();

            var toAddress = IoNodeAddress.CreateFromEndpoint("udp", remoteEp);
            //PROCESS DMZ-SYN
            var sent = 0;
            if (!Proxy)
            {
                //zero adjunct 
                if(Hub.CcCollective.Neighbors.Count > CcCollective.MaxAdjuncts)
                    return;

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
                if (await SendMessageAsync(response.ToByteArray(), CcDiscoveries.MessageTypes.Probed, toAddress)
                        .FastPath().ConfigureAwait(Zc) > 0)
                {
#if DEBUG
                    _logger.Debug($"-/> {nameof(CcProbeResponse)}({sent})[{response.ToByteString().Memory.PayloadSig()} ~ {response.ReqHash.Memory.HashSig()}]: Sent [[SYN-ACK]], [{MessageService.IoNetSocket.LocalAddress} ~> {toAddress}]");
#endif

                    //ensure ingress delta trigger
                    if (!CcCollective.ZeroDrone)
                        await CheckSynTriggerAsync(this, triggerDelta, toAddress).FastPath().ConfigureAwait(Zc);

                    if (AutoPeeringEventService.Operational)
                        await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = CcDesignation.ShortId(packet.PublicKey),
                                Type = "pong"
                            }
                        }).FastPath().ConfigureAwait(Zc);
                }

//                if (toProxyAddress != null && CcCollective.UdpTunnelSupport && toAddress.Ip != toProxyAddress.Ip)
//                {
//                    if (await SendMessageAsync(pong.ToByteString(), CcDiscoveries.MessageTypes.Probed, toAddress)
//                            .FastPath().ConfigureAwait(Zc) > 0)
//                    {
//#if DEBUG
//                        _logger.Trace($"-/> {nameof(CcProbeResponse)}({sent})[{pong.ToByteString().Memory.PayloadSig()} ~ {pong.ReqHash.Memory.HashSig()}]: Sent [[SYN-ACK]], [{MessageService.IoNetSocket.LocalAddress} ~> {toAddress}]");
//#endif
//                    }
//                }
            }
            else //PROCESS SYN
            {
                LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                if ((sent = await SendMessageAsync(data: response.ToByteArray(), type: CcDiscoveries.MessageTypes.Probed)
                        .FastPath().ConfigureAwait(Zc)) > 0)
                {
#if DEBUG
                    _logger.Debug(IsDroneConnected
                        ? $"-/> {nameof(CcProbeResponse)}({sent})[{response.ToByteString().Memory.PayloadSig()} ~ {response.ReqHash.Memory.HashSig()}]: Sent [[KEEPALIVE]], {Description}"
                        : $"-/> {nameof(CcProbeResponse)}({sent})[{response.ToByteString().Memory.PayloadSig()} ~ {response.ReqHash.Memory.HashSig()}]: Sent [[SYN-ACK]], {Description}");
#endif
                    if (!CcCollective.ZeroDrone)
                        await CheckSynTriggerAsync(this, triggerDelta, toAddress).FastPath().ConfigureAwait(Zc);

                    if (AutoPeeringEventService.Operational)
                        await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = Designation.IdString(),
                                Type = "pong"
                            }
                        }).FastPath().ConfigureAwait(Zc);
                }
                else
                {
                    if (Collected)
                        _logger.Error($"-/> {nameof(CcProbeMessage)}: [FAILED] Send [[SYN ACK/KEEPALIVE]], to = {Description}");
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
        /// <param name="endpoint">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcProbeResponse response, IPEndPoint endpoint, chroniton packet)
        {
            var matchRequest = await _probeRequest.ResponseAsync(endpoint.ToString(), response.ReqHash).FastPath().ConfigureAwait(Zc);
            
            //Try the router
            if (!matchRequest && Proxy)
                matchRequest = await Hub.Router._probeRequest.ResponseAsync(endpoint.ToString(), response.ReqHash).FastPath().ConfigureAwait(Zc);

            if (!matchRequest)
            {
#if DEBUG
                if (Collected && Proxy)
                {
                    _logger.Error($"<\\- {nameof(CcProbeResponse)} {packet.Data.Memory.PayloadSig()}: SEC! {response.ReqHash.Memory.HashSig()}, d = {_probeRequest.Count}, pats = {TotalPats},  " +
                                  $"PK={Designation.PkShort()} != {CcDesignation.ShortId(packet.PublicKey)} (proxy = {Proxy}),  ssp = {SecondsSincePat}, d = {(AttachTimestamp > 0 ? (AttachTimestamp - LastPat).ToString() : "N/A")}, v = {Verified}, s = {endpoint}, nat = {NatAddress}, dmz = {packet.Header.Ip.Src.GetEndpoint()}, {Description}");
                }
#endif
                return;
            }

            //Process SYN-ACK
            if (!Proxy)
            {
                var idCheck = CcDesignation.FromPubKey(packet.PublicKey.Memory);
                var fromAddress = IoNodeAddress.CreateFromEndpoint("udp", endpoint);
                
                //var remoteServices = new CcService();
                //foreach (var key in pong.Services.Map.Keys.ToList())
                //    remoteServices.CcRecord.Endpoints.TryAdd(Enum.Parse<CcService.Keys>(key),
                //        IoNodeAddress.Create(
                //            $"{pong.Services.Map[key].Network}://{((IPEndPoint) extraData).Address}:{pong.Services.Map[key].Port}"));

                if (Hub.Neighbors.Count <= CcCollective.MaxAdjuncts)
                {
                    await ZeroAsync(static async state =>
                    {
                        var (@this, fromAddress, idCheck) = state;
                        if (!await @this.CollectAsync(fromAddress.IpEndPoint, idCheck).FastPath().ConfigureAwait(@this.Zc))
                        {
#if DEBUG
                            @this._logger.Trace($"{@this.Description}: Collecting {fromAddress.IpEndPoint} failed!");
#else
                            @this._logger.Trace($"{@this.Description}: Collecting {fromAddress.IpEndPoint} failed!");
#endif
                        }
                    }, ValueTuple.Create(this, fromAddress, idCheck), TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(Zc);
                }
            }
            else if (!Verified) //Process SYN-ACK
            {
                //PAT
                LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                //TODO: vector?
                //set ext address as seen by neighbor
#if DEBUG
                DebugAddress = IoNodeAddress.Create($"tcp://{packet.Header.Ip.Src.GetEndpoint()}");
#endif
                NatAddress = IoNodeAddress.Create($"udp://{endpoint}");

                Verified = true;
                Thread.MemoryBarrier();

                AdjunctState oldState;
                if ((oldState = CompareAndEnterState(AdjunctState.Verified, AdjunctState.Unverified)) != AdjunctState.Unverified)
                {
                    _logger.Warn($"{nameof(CcProbeResponse)} - {Description}: Invalid state, {oldState}. Wanted {nameof(AdjunctState.Unverified)}");
                    return;
                }

                if(CcCollective.ZeroDrone)
                    _logger.Warn($"Verified with queen `{endpoint}'");

#if DEBUG
                _logger.Trace($"<\\- {nameof(CcProbeResponse)}[{response.ToByteString().Memory.PayloadSig()} ~ {response.ReqHash.Memory.HashSig()}]: Processed <<SYN-ACK>>: {Description}");
#endif

                await SeduceAsync("ACK", Heading.Egress, ignoreZeroDrone: true).FastPath().ConfigureAwait(Zc);
            }
            else 
            {
                //PAT
                LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                if (!CcCollective.ZeroDrone)
                {
                    await SeduceAsync("ZRO", Heading.Both).FastPath().ConfigureAwait(Zc);
                }
                else if(CcCollective.ZeroDrone || !IsDroneConnected && CcCollective.Neighbors.Count < CcCollective.MaxDrones)
                {
                    await SweepAsync().FastPath().ConfigureAwait(Zc);
                }
            }
        }

        /// <summary>
        /// Seduces another adjunct
        /// </summary>
        /// <returns>A valuable task</returns>
        public async ValueTask<bool> SeduceAsync(string desc, Heading heading = Heading.Undefined, IoNodeAddress address = null, bool ignoreZeroDrone = false)
        {
            if (Volatile.Read(ref _stealthy).ElapsedMs() < parm_max_network_latency_ms * 2 || Zeroed() || !Collected || IsDroneAttached ||
                _currState.Value > AdjunctState.Connecting /*don't change this from Connecting*/)
            {
                return false;
            }
            
            try
            {
                if (Proxy && (Probed || heading >= Heading.Egress) &&
                    !CcCollective.ZeroDrone && Direction == Heading.Undefined &&
                    CcCollective.EgressCount < CcCollective.parm_max_outbound &&
                    FuseRequestsSentCount < parm_zombie_max_connection_attempts)
                {
                    if (!await FuseAsync().FastPath().ConfigureAwait(Zc))
                    {
                        _logger.Trace($"<\\- {nameof(FuseAsync)}: [FAILED] Send Drone request, {Description}");
                    }
                    else
                    {
                        if(((int)heading & (int)Heading.Ingress) == 0)
                            Interlocked.Exchange(ref _stealthy, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
#if DEBUG
                        _logger.Trace($"-/> {nameof(SeduceAsync)}: Sent [[{desc}]] Drone REQUEST), {Description}");
#endif
                        return true;
                    }
                }

                if(!(CcCollective.ZeroDrone && !ignoreZeroDrone) && ((int)heading & (int)Heading.Ingress) > 0)
                {
                    var proxy = address == null ? this : Router;
                    //delta trigger
                    if (proxy != null && !await proxy.ProbeAsync(desc, address).FastPath().ConfigureAwait(Zc))
                        _logger.Trace($"<\\- {nameof(ProbeAsync)}({desc}): [FAILED] to seduce {Description}");
                    else
                    {
                        if (((int)heading & (int)Heading.Ingress) == 0)
                            Interlocked.Exchange(ref _stealthy, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
#if DEBUG
                        _logger.Debug($"-/> {nameof(ProbeAsync)}: Send [[{desc}]] Probe");
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
                //Check for teardown
                if (!Collected)
                    return false;

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

                // Is this a routed request?
                if (Proxy)
                {
                    //send
                    IoQueue<IoZeroMatcher.IoChallenge>.IoZNode challenge;
                    if ((challenge = await _probeRequest.ChallengeAsync(RemoteAddress.IpPort, probeMsgBuf)
                        .FastPath().ConfigureAwait(Zc)) == null) 
                        return false;

                    var sent = await SendMessageAsync(data: probeMsgBuf, type: CcDiscoveries.MessageTypes.Probe).FastPath().ConfigureAwait(Zc);
                    if (sent > 0)
                    {
#if DEBUG
                        try
                        {
                            _logger.Debug($"-/> {nameof(CcProbeMessage)}({sent})[payload = {probeMsgBuf.PayloadSig()}, hash = {challenge.Value.Hash}]: sent [[{desc}]] {Description}");
                        }
                        catch when (Zeroed()){}
                        catch (Exception e)when(!Zeroed())
                        {
                            _logger.Trace(e);
                        }
#endif
                        if (AutoPeeringEventService.Operational)
                            await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                            {
                                EventType = AutoPeerEventType.SendProtoMsg,
                                Msg = new ProtoMsg
                                {
                                    CollectiveId = Hub.Router.Designation.IdString(),
                                    Id = Designation.IdString(),
                                    Type = "ping"
                                }
                            }).FastPath().ConfigureAwait(Zc);

                        return true;
                    }

                    await _probeRequest.RemoveAsync(challenge).FastPath().ConfigureAwait(Zc);

                    if (Collected)
                        _logger.Error($"-/> {nameof(CcProbeMessage)}: [FAILED], {Description}");

                    return false;
                }
                else //The destination state was undefined, this is local
                {
                    IoQueue<IoZeroMatcher.IoChallenge>.IoZNode challenge;
                    if ((challenge = await _probeRequest.ChallengeAsync(dest.IpPort, probeMsgBuf).FastPath().ConfigureAwait(Zc)) == null)
                        return false;

                    var sent = await SendMessageAsync(probeMsgBuf, CcDiscoveries.MessageTypes.Probe, dest)
                        .FastPath().ConfigureAwait(Zc);

                    if (sent > 0)
                    {
#if DEBUG
                        _logger.Debug($"-/> {nameof(CcProbeMessage)}({sent})[{probeMsgBuf.PayloadSig()}]: sent [[{desc}]], dest = {dest}, {Description}");
#endif
                        if (AutoPeeringEventService.Operational)
                            await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                            {
                                EventType = AutoPeerEventType.SendProtoMsg,
                                Msg = new ProtoMsg
                                {
                                    CollectiveId = Hub.Router.Designation.IdString(),
                                    Id = id??Designation.IdString(),
                                    Type = "ping"
                                }
                            }).FastPath().ConfigureAwait(Zc);

                        return true;
                    }

                    await _probeRequest.RemoveAsync(challenge).FastPath().ConfigureAwait(Zc);
                    if(Collected)
                        _logger.Trace($"-/> {nameof(CcProbeMessage)}:({desc}) [FAILED], {Description}");
                    return false;
                }
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger.Debug(e,$"ERROR z = {Zeroed()}, dest = {dest}, source = {MessageService}, _sweepRequest = {_sweepRequest.Count}");
            }

            return false;
        }

        private long _lastScan;

        private long LastScan => Volatile.Read(ref _lastScan);
        /// <summary>
        /// Sweeps adjuncts for more
        /// </summary>
        /// <returns>Task</returns>
        public async ValueTask<bool> SweepAsync(long cooldown = -1)
        {
            try
            {
                if (!Assimilating || (!Probed && !CcCollective.ZeroDrone))
                {
                    _logger.Trace($"{nameof(SweepAsync)}: [ABORTED], {Description}, s = {State}, a = {Assimilating}");
                    return false;
                }

                if (cooldown == -1)
                    cooldown = CcCollective.parm_mean_pat_delay_s * 1000 / 5;

                //rate limit
                if (LastScan.ElapsedMs() < cooldown)
                    return false;

                var sweepMessage = new CcSweepRequest
                {
                     Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                };

                var sweepMsgBuf = sweepMessage.ToByteArray();

                IoQueue<IoZeroMatcher.IoChallenge>.IoZNode challenge;
                if ((challenge = await _sweepRequest.ChallengeAsync(RemoteAddress.IpPort, sweepMsgBuf)
                    .FastPath().ConfigureAwait(Zc)) == null)
                {
                    return false;
                }

                var sent = await SendMessageAsync(sweepMsgBuf,CcDiscoveries.MessageTypes.Sweep, RemoteAddress).FastPath().ConfigureAwait(Zc);
                if (sent > 0)
                {
                    _logger.Trace($"-/> {nameof(CcSweepRequest)}({sent}){sweepMsgBuf.PayloadSig()}: Sent, {Description}");

                    //Emit message event
                    if (AutoPeeringEventService.Operational)
                        await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = Designation.IdString(),
                                Type = "discovery request"
                            }
                        }).FastPath().ConfigureAwait(Zc);

                    return true;
                }

                await _sweepRequest.RemoveAsync(challenge).FastPath().ConfigureAwait(Zc);
                if (Collected)
                    _logger.Debug($"-/> {nameof(CcSweepRequest)}: [FAILED], {Description} ");
            }
            catch when (Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Debug(e,$"{nameof(CcSweepRequest)}: [ERROR] z = {Zeroed()}, state = {State}, dest = {RemoteAddress}, source = {MessageService}, _sweepRequest = {_sweepRequest.Count}");
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
                if (Collected && !CcCollective.ZeroDrone)
                    _logger.Warn($"{nameof(FuseAsync)}: [ABORTED], {Description}, s = {State}, a = {Assimilating}, D = {IsDroneConnected}, d = {IsDroneAttached}");
                return false;
            }

            if (_currState.Value > AdjunctState.Unverified)
            {
                AdjunctState oldState;

                var stateIsValid = (oldState = CompareAndEnterState(AdjunctState.Fusing, AdjunctState.Verified,
                    overrideHung: parm_max_network_latency_ms * 2)) == AdjunctState.Verified;

                if (!stateIsValid)
                {
                    if (oldState != AdjunctState.Fusing)
                        _logger.Warn($"{nameof(FuseAsync)} - {Description}: Invalid state, {oldState}, age = {_currState.EnterTime.ElapsedMs()}ms. Wanted {nameof(AdjunctState.Verified)} - [RACE OK!] ");
                    return false;
                }
                
                if(oldState != AdjunctState.Verified)
                {
                    _logger.Warn($"{nameof(FuseAsync)} - {Description}: Invalid state, {oldState}, age = {_currState.EnterTime.ElapsedMs()}ms. Wanted {nameof(AdjunctState.Verified)} - [PUSH OK?] ");
                }
            }

            //oneshot
            await ZeroAsync(async (@this) =>
            {
                try
                {
                    //TODO prod:
                    await Task.Delay(_random.Next(parm_max_network_latency_ms / 5 + 1), AsyncTasks.Token).ConfigureAwait(Zc);
                    //await Task.Yield();

                    var fuseRequest = new CcFuseRequest
                    {
                        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    };

                    var fuseRequestBuf = fuseRequest.ToByteArray();

                    IoQueue<IoZeroMatcher.IoChallenge>.IoZNode challenge;
                    if ((challenge = await _fuseRequest.ChallengeAsync(RemoteAddress.IpPort, fuseRequestBuf).FastPath().ConfigureAwait(Zc)) == null)
                    {
                        return;
                    }

                    var sent = await SendMessageAsync(fuseRequestBuf, CcDiscoveries.MessageTypes.Fuse).FastPath().ConfigureAwait(Zc);
                    if (sent > 0)
                    {
                        
                        Interlocked.Increment(ref _fuseRequestsSentCount);
                        _logger.Trace($"-/> {nameof(CcFuseRequest)}({sent})[{fuseRequestBuf.PayloadSig()}]: Sent, {Description}");
                        
                        if (AutoPeeringEventService.Operational)
                            await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                            {
                                EventType = AutoPeerEventType.SendProtoMsg,
                                Msg = new ProtoMsg
                                {
                                    CollectiveId = Hub.Router.Designation.IdString(),
                                    Id = Designation.IdString(),
                                    Type = "peer request"
                                }
                            }).FastPath().ConfigureAwait(Zc);
                        return;
                    }

                    await _fuseRequest.RemoveAsync(challenge).FastPath().ConfigureAwait(Zc);

                    if (Collected)
                        _logger.Debug($"-/> {nameof(CcDefuseRequest)}: [FAILED], {Description}, {MetaDesc}");
                }
                catch when (Zeroed()) { }
                catch (Exception e) when (!Zeroed())
                {
                    _logger.Debug(e, $"{nameof(CcDefuseRequest)}: [FAILED], {Description}, {MetaDesc}");
                }
            }, this, TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(Zc);

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

                var sent = await SendMessageAsync(dropRequest.ToByteArray(), CcDiscoveries.MessageTypes.Defuse, dest)
                    .FastPath().ConfigureAwait(Zc);

#if DEBUG
                _logger.Trace(
                    (sent) > 0
                        ? $"-/> {nameof(CcDefuseRequest)}({sent}): Sent, {Description}"
                        : $"-/> {nameof(CcDefuseRequest)}: [FAILED], {Description}, {MetaDesc}");
#endif

                if (sent > 0)
                {
                    if (AutoPeeringEventService.Operational)
                        await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = Designation.IdString(),
                                Type = $"peer disconnect"
                            }
                        }).FastPath().ConfigureAwait(Zc);

                    return true;
                }
            }
            catch (Exception) when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                if (Collected)
                    _logger.Debug(e,
                        $"{nameof(DeFuseAsync)}: [ERROR], {Description}, s = {State}, a = {Assimilating}, p = {IsDroneConnected}, d = {dest}, s = {MessageService}");
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

                if (!ZeroAtomic( static (_, state, _) =>
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
                }, ValueTuple.Create(this, ccDrone, direction)))
                {
                    return false;
                }

                _logger.Trace($"{nameof(AttachDroneAsync)}: [WON] {_drone?.Description}");
                
                Assimilated = true;
                AttachTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                //emit event
                if (AutoPeeringEventService.Operational)
                    await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.AddDrone,
                        Drone = new Drone
                        {
                            CollectiveId = Router.Designation.IdString(),
                            Adjunct = ccDrone.Adjunct.Designation.IdString(),
                            Direction = ccDrone.Adjunct.Direction.ToString()
                        }
                    }).FastPath().ConfigureAwait(Zc);

                if(CcCollective.Hub.Neighbors.Count < CcCollective.MaxAdjuncts)
                    await SweepAsync().FastPath().ConfigureAwait(Zc);

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
                
                if (!Zeroed() && Node != null && Direction == Heading.Ingress &&
                    CcCollective.IngressCount < CcCollective.parm_max_inbound)
                {
                    //back off for a while... Try to re-establish a link 
                    await ZeroAsync(static async @this =>
                    {
                        var backOff = @this.parm_max_network_latency_ms * 2 + @this._random.Next(@this.parm_max_network_latency_ms * 2);
                        
                        await Task.Delay(backOff, @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);

                        if (@this.State == AdjunctState.Verified)
                        {
                            await @this.ProbeAsync("SYN-BRG", @this.RemoteAddress.Copy()).FastPath().ConfigureAwait(@this.Zc);
                        }
                    }, this, TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(Zc);
                }

                //emit event
                if (AutoPeeringEventService.Operational)
                    await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.RemoveDrone,
                        Drone = new Drone
                        {
                            CollectiveId = CcCollective.Hub.Router.Designation.IdString(),
                            Adjunct = Designation.IdString()
                        }
                    }).FastPath().ConfigureAwait(Zc);
            }
            catch when (Zeroed())
            {
            }
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"{nameof(DetachDroneAsync)}: ");
            }
            finally
            {
                severedDrone.Zero(this);
                if(WasAttached)
                    await DeFuseAsync().FastPath().ConfigureAwait(Zc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AdjunctState ResetState(AdjunctState state)
        {
            Interlocked.Exchange(ref _direction, 0);
            _fuseRequestsRecvCount = _fuseRequestsSentCount = 0;

            _currState.Set((int)state);

            return _currState.Value;
        }

        /// <summary>
        /// Makes fusing more likely
        /// </summary>
        public void EnsureFuseChecks()
        {
            _fuseRequestsRecvCount = 0;
            _fuseRequestsSentCount = 0;
        }

#if DEBUG
        [MethodImpl(MethodImplOptions.Synchronized)]
#endif
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
                        _fuseRequestsRecvCount = _fuseRequestsSentCount = 0;
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
                    Interlocked.Exchange(ref _direction, 0);
                    _fuseRequestsRecvCount = _fuseRequestsSentCount = 0;
                }

                compare = compare && !hung;
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