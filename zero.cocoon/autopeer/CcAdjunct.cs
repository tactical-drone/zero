//#define LOSS
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
#if DEBUG
using System.Runtime.InteropServices;
using System.Text;
#endif
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using MathNet.Numerics.Financial;
using MathNet.Numerics.Random;
using NLog;
using Proto;
using SimpleBase;
using zero.cocoon.events.services;
using zero.cocoon.identity;
using zero.cocoon.models;
using zero.cocoon.models.batches;
using zero.cocoon.models.services;
using zero.core.conf;
using zero.core.core;
using zero.core.feat.misc;
using zero.core.feat.models.protobuffer;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;
using zero.core.patterns.queue;

namespace zero.cocoon.autopeer
{
    /// <summary>
    /// Processes (UDP) discovery messages from the collective.
    /// </summary>
    public class CcAdjunct : IoNeighbor<CcProtocMessage<Packet, CcDiscoveryBatch>>
    {
        public CcAdjunct(CcHub node, IoNetClient<CcProtocMessage<Packet, CcDiscoveryBatch>> ioNetClient,
            object extraData = null, CcService services = null)
            : base
            (
                node,
                ioNetClient,
                static (o, ioNetClient) => new CcDiscoveries("adjunct msgs", $"{((IoNetClient<CcProtocMessage<Packet, CcDiscoveryBatch>>)ioNetClient).Key}", (IoSource<CcProtocMessage<Packet, CcDiscoveryBatch>>)ioNetClient),
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
            var mult = CcCollective.ZeroDrone ? 100 : 1;
            _pingRequest = new IoZeroMatcher<ByteString>(nameof(_pingRequest), Source.ZeroConcurrencyLevel(), parm_max_network_latency_ms * 2, (int)(CcCollective.MaxAdjuncts * parm_max_discovery_peers * 2 * mult));
            _peerRequest = new IoZeroMatcher<ByteString>(nameof(_peerRequest), Source.ZeroConcurrencyLevel(), parm_max_network_latency_ms * 2, (int)(CcCollective.MaxAdjuncts* parm_max_discovery_peers * 2 * mult));
            _discoveryRequest = new IoZeroMatcher<ByteString>(nameof(_discoveryRequest), (int)(CcCollective.MaxAdjuncts * parm_max_discovery_peers + 1), parm_max_network_latency_ms * 2, (int)(CcCollective.MaxAdjuncts * parm_max_discovery_peers * 2));

            if (extraData != null)
            {
                var (item1, item2, item3) = (Tuple<CcDesignation, CcService, IPEndPoint>) extraData;
                Designation = item1;
                Services = services ?? item2;
                RemoteAddress = IoNodeAddress.CreateFromEndpoint("udp", item3);
                Key = MakeId(Designation, RemoteAddress);

                //to prevent cascading into the hub we clone the source.
                Source = new IoUdpClient<CcProtocMessage<Packet, CcDiscoveryBatch>>($"UDP Proxy ~> {Description}", MessageService, RemoteAddress.IpEndPoint);
                Source.ZeroHiveAsync(this).AsTask().GetAwaiter().GetResult();
            }
            else
            {
                Designation = CcCollective.CcId;
                Services = services ?? node.Services;
                Key = MakeId(Designation, CcCollective.ExtAddress);
            }

            if (Proxy)
            {
                CompareAndEnterState(AdjunctState.Unverified, AdjunctState.Undefined);
                ZeroAsync(RoboAsync, this,TaskCreationOptions.DenyChildAttach | TaskCreationOptions.LongRunning).AsTask().GetAwaiter().GetResult();
            }
            else
            {
                CompareAndEnterState(AdjunctState.Local, AdjunctState.Undefined);
                _routingTable = new ConcurrentDictionary<string, CcAdjunct>();
            }

            _stealthy = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - parm_max_network_latency_ms * 10;
            var nrOfStates = Enum.GetNames(typeof(AdjunctState)).Length;
            _lastScan = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - parm_max_network_latency_ms * 2;
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
            Peering = 5,
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
                        return _description = $"`adjunct ({EventCount}, {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{PeerRequestsSentCount}:{PeerRequestsRecvCount}] local: ||{MessageService.IoNetSocket.LocalAddress} ~> {MessageService.IoNetSocket.RemoteAddress}||, [{Hub?.Designation.IdString()}, {Designation.IdString()}], uptime = {TimeSpan.FromSeconds(Uptime.ElapsedMsToSec())}'";
                    else
                        return _description = $"`hub ({(Zeroed() ? "z" : "a")}, {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{PeerRequestsSentCount}:{PeerRequestsRecvCount}] local: ||{MessageService.IoNetSocket.LocalAddress} ~> {MessageService.IoNetSocket.RemoteAddress}||, [{Hub?.Designation?.IdString()}, {Designation.IdString()}], uptime = {TimeSpan.FromSeconds(Uptime.ElapsedMsToSec())}'";
                }
                catch (Exception)
                {
                    if(Proxy)
                        return _description?? $"`adjunct({EventCount}, {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{PeerRequestsSentCount}:{PeerRequestsRecvCount}] local: ||{MessageService?.IoNetSocket?.LocalAddress} ~> {MessageService?.IoNetSocket?.RemoteAddress}||,' [{Hub?.Designation?.IdString()}, {Designation?.IdString()}], uptime = {TimeSpan.FromSeconds(Uptime.ElapsedMsToSec())}'";    
                    else
                        return _description = $"`hub ({(Zeroed() ? "z" : "a")}, {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{PeerRequestsSentCount}:{PeerRequestsRecvCount}] local: ||{MessageService?.IoNetSocket?.LocalAddress} ~> {MessageService?.IoNetSocket?.RemoteAddress}||, [{Hub?.Designation?.IdString()}, {Designation?.IdString()}], uptime = {TimeSpan.FromSeconds(Uptime.ElapsedMsToSec())}'";
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
        protected IoNetClient<CcProtocMessage<Packet, CcDiscoveryBatch>> MessageService => (IoNetClient<CcProtocMessage<Packet, CcDiscoveryBatch>>) Source;

        /// <summary>
        /// The udp routing table 
        /// </summary>
        private ConcurrentDictionary<string, CcAdjunct> _routingTable;

        /// <summary>
        /// The gossip peer associated with this neighbor
        /// </summary>
        private volatile CcDrone _drone;

        /// <summary>
        /// Whether The peer is attached
        /// </summary>
        public bool IsDroneAttached => _drone != null && !_drone.Zeroed();

        /// <summary>
        /// Whether the peer is nominal
        /// </summary>
        public bool IsDroneConnected => IsDroneAttached && State == AdjunctState.Connected && Direction != Heading.Undefined;
        
        /// <summary>
        /// If the adjunct is working 
        /// </summary>
        public bool Assimilating => !Zeroed() && Verified;

        /// <summary>
        /// Whether the node, peer and adjunct are nominal
        /// </summary>
        public bool IsGossiping => IsDroneConnected && Assimilating;

        /// <summary>
        /// Indicates whether we have extracted information from this drone
        /// </summary>
        public volatile bool Assimilated;

        private volatile int _peerRequestsRecvCount;
        /// <summary>
        /// Indicates whether we have successfully established a connection before
        /// </summary>
        protected int PeerRequestsRecvCount => _peerRequestsRecvCount;

        private volatile int _peerRequestsSentCount;
        /// <summary>
        /// Number of peer requests
        /// </summary>
        public int PeerRequestsSentCount => _peerRequestsSentCount;

        /// <summary>
        /// Gathers counter intelligence at a acceptable rates
        /// </summary>
        private long _triggerTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 100000;

        /// <summary>
        /// Node broadcast priority. 
        /// </summary>
        public long Priority => PeerRequestsRecvCount;

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

        /// <summary>
        /// The our IP as seen by neighbor
        /// </summary>
        public IoNodeAddress ExtGossipAddress { get; protected set; }
        
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
            Egress = 2
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
        private IoConduit<CcProtocBatchJob<Packet, CcDiscoveryBatch>> _protocolConduit;

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
                Interlocked.Exchange(ref _lastPat, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
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
        private IoZeroMatcher<ByteString> _discoveryRequest;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private IoZeroMatcher<ByteString> _peerRequest;

        /// <summary>
        /// Holds un-routed ping requests
        /// </summary>
        private IoZeroMatcher<ByteString> _pingRequest;

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


        public bool Probed => _peerRequestsRecvCount > 0 || Verified;

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
            return $"{designation.PkString()}@{key}";
        }

        /// <summary>
        /// The CcId
        /// </summary>
        public override string Key { get; }

        /// <summary>
        /// The adjunct services
        /// </summary>
        public CcService Services { get; protected set; }

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
        public int parm_max_network_latency_ms = 2000;
#else
        public int parm_max_network_latency_ms = 1000;
#endif

        /// <summary>
        /// Maximum number of peers in discovery response
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public uint parm_max_discovery_peers = 6;

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
        public int parm_zombie_max_connection_attempts = 3;

        /// <summary>
        /// Minimum pats before a node could be culled
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_min_pats_before_shuffle = 1;

        /// <summary>
        /// Minimum number of desired spare bays for discovery requests to yield returns.
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
        public uint parm_protocol_version = 0;

        /// <summary>
        /// The service map
        /// </summary>
        private ServiceMap _serviceMap;
        /// <summary>
        /// A service map helper
        /// </summary>
        private ServiceMap ServiceMap
        {
            get
            {
                if (_serviceMap != null)
                    return _serviceMap;
                
                var mapping = new ServiceMap();
                foreach (var (key, value) in Services.CcRecord.Endpoints)
                {
                    if (value is { Validated: true })
                        mapping.Map.Add(key.ToString(),
                            new NetworkAddress
                            {
                                Network = $"{value.Protocol().ToString().ToLower()}",
                                Port = (uint) value.Port
                            });
                    else
                    {
                        _logger.Warn($"Invalid endpoints found ({value?.ValidationErrorString})");
                    }
                }

                return _serviceMap = mapping;
            }
        }

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
            _pingRequest = null;
            _drone = null;
            _protocolConduit = null;
            _peerRequest = null;
            _discoveryRequest = null;
            _produceTaskPool = null;
            _consumeTaskPool = null;
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

                var from = ZeroedFrom == this? "self" : ZeroedFrom?.Description??"null";
                if (Assimilated && Direction != Heading.Undefined && WasAttached && Uptime.ElapsedMs() > parm_min_uptime_ms)
                    _logger.Info($"- `{(Assimilated ? "apex" : "sub")} {Direction}: {Description}, from: {from}");

                await DetachPeerAsync().FastPath().ConfigureAwait(Zc);
                
                //swarm 
                await ZeroAsync(static async @this =>
                {
                    await Task.Delay(@this.parm_max_network_latency_ms * 2, @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);
                    await @this.Router.SendPingAsync("SYN-BRG", @this.RemoteAddress).FastPath().ConfigureAwait(@this.Zc);
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

            _pingRequest.Zero(this);
            _peerRequest.Zero(this);
            _discoveryRequest.Zero(this);
            
        }

        /// <summary>
        /// Ensure robotic diagnostics
        /// </summary>
        public async ValueTask EnsureRoboticsAsync()
        {
            try
            {
                //send PAT
                if (!await SendPingAsync("SYN-PAT").FastPath().ConfigureAwait(Zc))
                {
                    if (Collected)
                        _logger.Error($"-/> {nameof(SendPingAsync)}: PAT Send [FAILED], {Description}, {MetaDesc}");
                }

                var threshold = IsDroneConnected ? CcCollective.parm_mean_pat_delay_s * 2 : CcCollective.parm_mean_pat_delay_s;
                //Watchdog failure
                if (SecondsSincePat > threshold * 2)
                {
                    _logger.Trace($"w {nameof(EnsureRoboticsAsync)} - {Description}, s = {SecondsSincePat} >> {CcCollective.parm_mean_pat_delay_s}, {MetaDesc}");
                    Zero(new IoNanoprobe($"-wd: l = {SecondsSincePat}s ago, uptime = {TimeSpan.FromMilliseconds(Uptime.ElapsedMs()).TotalHours:0.00}h"));
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
                        await Task.Delay(@this._random.Next(@this.parm_max_network_latency_ms / 16 + 1), @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);
                        //await Task.Yield();

                        var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        AdjunctState oldState;
                        if ((oldState = @this.CompareAndEnterState(AdjunctState.Connecting, AdjunctState.Peering)) != AdjunctState.Peering)
                        {
                            @this._logger.Trace($"{nameof(ConnectAsync)} - {@this.Description}: Invalid state, {oldState}. Wanted {nameof(AdjunctState.Peering)}");
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
        private async ValueTask ProcessMsgBatchAsync<T>(IoSink<CcProtocBatchJob<Packet, CcDiscoveryBatch>> batchJob,
            IoConduit<CcProtocBatchJob<Packet, CcDiscoveryBatch>> channel,
            Func<CcDiscoveryMessage, CcDiscoveryBatch, IoConduit<CcProtocBatchJob<Packet, CcDiscoveryBatch>>, T, ValueTask> processCallback, T nanite)
        {
            if (batchJob == null)
                return;

            CcDiscoveryBatch msgBatch = default;
            try
            {
                var job = (CcProtocBatchJob<Packet, CcDiscoveryBatch>)batchJob;
                msgBatch = job.Get();
                for (int i = 0; i < msgBatch.Filled; i++)
                {
                    var message = msgBatch[i];
                    if (message == default)
                        break;
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
                        _protocolConduit = MessageService.GetConduit<CcProtocBatchJob<Packet, CcDiscoveryBatch>>(nameof(CcAdjunct));
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
                                                Packet packet = default;
                                                try
                                                {
                                                    message = msgBatch.EmbeddedMsg;
                                                    packet = msgBatch.Message;
                                                
                                                    var routed = @this.Router._routingTable.TryGetValue(discoveryBatch.RemoteEndPoint, out var currentRoute);
                                                    //Router
                                                    try
                                                    {
                                                        if (!routed)
                                                        {
                                                            currentRoute = (CcAdjunct)@this.Hub.Neighbors.Values.FirstOrDefault(n => ((CcAdjunct)n).Proxy && ((CcAdjunct)n).Collected && ((CcAdjunct)n).RemoteAddress.EndpointIpPort == discoveryBatch.RemoteEndPoint);
                                                            if (currentRoute != null)
                                                            { 
                                                                //TODO, proxy adjuncts need to malloc the same way when listeners spawn them.
                                                                if (!@this.Router._routingTable.TryAdd(discoveryBatch.RemoteEndPoint, currentRoute))
                                                                {
                                                                    @this.Router._routingTable.TryGetValue(discoveryBatch.RemoteEndPoint, out currentRoute);
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
                                                            //for (var j = 2; j < 4; j++)
                                                            //{
                                                            //    for (var i = j; i < currentRoute.Designation.PublicKey.Length; i += i)
                                                            //    {
                                                            //        if (currentRoute.Designation.PublicKey[i] != packet.PublicKey[i])
                                                            //        {
                                                            //            @this._logger.Warn($"{nameof(@this.Router)}[{i}]: Dropped current stale route {currentRoute.RemoteAddress}/{currentRoute.Designation.PkString()[..10]} ~> {discoveryBatch.RemoteEndPoint}/{Base58.Bitcoin.Encode(packet.PublicKey.Span)[..10]}: {currentRoute.Description}");
                                                            //            currentRoute.Zero(@this);
                                                            //            currentRoute = null;
                                                            //            break;
                                                            //        }
                                                            //    }
                                                            //    if(currentRoute == null)
                                                            //        break;
                                                            //}
                                                        }
                                                    }
                                                    catch when(@this.Zeroed()){}
                                                    catch (Exception e) when(!@this.Zeroed())
                                                    {
                                                        @this._logger.Error(e, @this.Description);
                                                        return;
                                                    }

                                                    currentRoute ??= @this.Hub.Router;

                                                    var ip = discoveryBatch.RemoteEndPoint.Split(':');
                                                    
                                                    var extraData = new IPEndPoint(IPAddress.Parse(ip[0]), int.Parse(ip[1]));

                                                    if (!@this.Zeroed() && !currentRoute.Zeroed())
                                                    {
                                                        try
                                                        {
                                                            switch ((CcDiscoveries.MessageTypes) packet.Type)
                                                            {
                                                                case CcDiscoveries.MessageTypes.Ping:
                                                                    await currentRoute.ProcessAsync((Ping)message, extraData, packet).FastPath().ConfigureAwait(@this.Zc);
                                                                    break;
                                                                case CcDiscoveries.MessageTypes.Pong:
                                                                    await currentRoute.ProcessAsync((Pong)message, extraData, packet).FastPath().ConfigureAwait(@this.Zc);
                                                                    break;
                                                                case CcDiscoveries.MessageTypes.DiscoveryRequest:
                                                                    if (!currentRoute.Proxy && !@this.CcCollective.ZeroDrone)
                                                                    {
#if DEBUG
                                                                        @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.DiscoveryRequest)}: Unrouted request from {extraData} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}");
#endif
                                                                        break;
                                                                    }
                                                                    await currentRoute.ProcessAsync((DiscoveryRequest)message, extraData, packet).FastPath().ConfigureAwait(@this.Zc);
                                                                    break;
                                                                case CcDiscoveries.MessageTypes.DiscoveryResponse:
                                                                    if (!currentRoute.Proxy)
                                                                    {
#if DEBUG
                                                                        @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.DiscoveryResponse)}: Unrouted request from {extraData} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}");                                                               
#endif

                                                                        break;
                                                                    }

                                                                    await currentRoute.ProcessAsync((DiscoveryResponse) message, extraData, packet).FastPath().ConfigureAwait(@this.Zc);
                                                                    break;
                                                                case CcDiscoveries.MessageTypes.PeeringRequest:
                                                                    await currentRoute.ProcessAsync((PeeringRequest) message, extraData, packet).FastPath().ConfigureAwait(@this.Zc);
                                                                    break;
                                                                case CcDiscoveries.MessageTypes.PeeringResponse:
                                                                    if (!currentRoute.Proxy)
                                                                    {
#if DEBUG
                                                                        @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.PeeringResponse)}: Unrouted request from {extraData} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}");
#endif
                                                                        break;
                                                                    }

                                                                    if (!@this.CcCollective.ZeroDrone)
                                                                        await currentRoute.ProcessAsync((PeeringResponse) message, extraData, packet).FastPath().ConfigureAwait(@this.Zc);
                                                                    break;
                                                                case CcDiscoveries.MessageTypes.PeeringDrop:
                                                                    if (!currentRoute.Proxy)
                                                                    {
#if DEBUG
                                                                        @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.PeeringDrop)}: Unrouted request from {extraData} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}");
#endif
                                                                        break;
                                                                    }

                                                                    if (!@this.CcCollective.ZeroDrone)
                                                                        await currentRoute.ProcessAsync((PeeringDrop) message, extraData, packet).FastPath().ConfigureAwait(@this.Zc);
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
        /// Peer drop request
        /// </summary>
        /// <param name="request">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(PeeringDrop request, object extraData, Packet packet)
        {
            if (!Assimilating || request.Timestamp.ElapsedMs() > parm_max_network_latency_ms * 10)
            {
                if(Proxy)
                    _logger.Error($"{nameof(PeeringDrop)}: Ignoring {request.Timestamp.ElapsedMsToSec()}s old/invalid request, {Description}");
                return;
            }
            
            //PAT
            LastPat = 0;

            if (IsDroneAttached)
            {
                _logger.Trace($"{nameof(PeeringDrop)}: {Direction} Peer = {_drone.Key}: {Description}, {MetaDesc}");
                await DetachPeerAsync().FastPath().ConfigureAwait(Zc);
            }
            //else
            //{
            //    //Hup
            //    if (Direction == Heading.Undefined && CcCollective.IngressCount < CcCollective.parm_max_inbound)
            //        await SendPingAsync().FastPath().ConfigureAwait(Zc);
            //}
        }

        /// <summary>
        /// Peering Request message from client
        /// </summary>
        /// <param name="request">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(PeeringRequest request, object extraData, Packet packet)
        {
            Interlocked.Increment(ref _peerRequestsRecvCount);

            //Drop old requests
            if (request.Timestamp.ElapsedMs() > parm_max_network_latency_ms * 2)
            {
                _logger.Trace($"{nameof(PeeringRequest)}: Dropped!, {(Verified ? "verified" : "un-verified")}, age = {request.Timestamp.ElapsedDelta()}");
                return;
            }

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
                var reject = new PeeringResponse
                {
                    ReqHash = UnsafeByteOperations.UnsafeWrap(
                        CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                    Status = false
                };

                var remote = IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint)extraData);
                if ((sent = await Router.SendMessageAsync(reject.ToByteString(), CcDiscoveries.MessageTypes.PeeringResponse, remote).FastPath().ConfigureAwait(Zc)) > 0)
                {
                    _logger.Trace($"-/> {nameof(PeeringResponse)}({sent}): Sent {(reject.Status ? "ACCEPT" : "REJECT")}, {Description}");
                }
                else
                    _logger.Debug($"<\\- {nameof(PeeringRequest)}({sent}): [FAILED], {Description}, {MetaDesc}");

                if (!Collected) return;

                //DMZ-syn
                //We syn here (Instead of in process ping) to force the other party to do some work (prepare to connect) before we do work (verify).
                if ((CcCollective.Hub.Neighbors.Count <= CcCollective.MaxDrones || CcCollective.ZeroDrone) && !await Router
                        .SendPingAsync("ACK-SYN-ACK", remote, CcDesignation.FromPubKey(packet.PublicKey.Memory).IdString())
                        .FastPath().ConfigureAwait(Zc))
                {
                    _logger.Trace($"Failed to send ACK-SYN-ACK to {remote}! {Description}");
                }
                
                return;
            }

            //PAT
            LastPat = 0;

            var peeringResponse = new PeeringResponse
            {
                ReqHash = UnsafeByteOperations.UnsafeWrap(
                    CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                Status = CcCollective.IngressCount < CcCollective.parm_max_inbound && _direction == 0
            };

            if (peeringResponse.Status)
            {
                var stateIsValid = _currState.Value != AdjunctState.Connected && CompareAndEnterState(AdjunctState.Peering, AdjunctState.Verified, overrideHung: parm_max_network_latency_ms * 2) == AdjunctState.Verified;
                
                peeringResponse.Status &= stateIsValid;

                if (!stateIsValid)
                {
                    _logger.Trace($"{Description}: Invalid state ~{_currState.Value}, age = {_currState.EnterTime.ElapsedMs()}ms. Wanted {nameof(AdjunctState.Verified)} -  [RACE OK!]");
                }
            }

            if ((sent= await SendMessageAsync(peeringResponse.ToByteString(),
                    CcDiscoveries.MessageTypes.PeeringResponse).FastPath().ConfigureAwait(Zc)) > 0)
            {
                var response = $"{(peeringResponse.Status ? "accept" : "reject")}";
                _logger.Trace($"-/> {nameof(PeeringResponse)}({sent}): Sent {response}, {Description}");

                if(peeringResponse.Status)
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
                _logger.Debug($"<\\- {nameof(PeeringRequest)}: Send peer response [FAILED], {Description}, {MetaDesc}");

            if(!peeringResponse.Status)
                await SeduceAsync("SYN-SE").FastPath().ConfigureAwait(Zc);
        }

        private long _stealthy;

        /// <summary>
        /// Peer response message from client
        /// </summary>
        /// <param name="response">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(PeeringResponse response, object extraData, Packet packet)
        {
            //Validate
            if (!Collected)
            {
                _logger.Trace($"{nameof(PeeringResponse)}: Dropped, not collected");
                return;
            }

            var peerRequest = await _peerRequest.ResponseAsync(extraData.ToString(), response.ReqHash).FastPath().ConfigureAwait(Zc);

            if (!peerRequest)
            {
                if (Collected)
                    _logger.Debug($"<\\- {nameof(PeeringResponse)}{response.ToByteArray().PayloadSig()}: No-Hash {extraData}, {RemoteAddress}, r = {response.ReqHash.Memory.HashSig()}, _peerRequest = {_peerRequest.Count}");
                return;
            }
            
            //Validated
            _logger.Trace($"<\\- {nameof(PeeringResponse)}: Accepted = {response.Status}, {Description}");

            //PAT
            LastPat = 0;

            switch (response.Status)
            {
                //Race for 
                case true when _direction == 0 && _currState.Value > AdjunctState.Unverified:
                {
                    if (!await ConnectAsync().FastPath().ConfigureAwait(Zc))
                    {
                        _logger.Trace($"{nameof(PeeringResponse)}: FAILED to connect! Sending Peer drop, {Description}");
                    }
                    break;
                }
                //at least probe
                case false when CcCollective.Neighbors.Count <= CcCollective.MaxDrones:
                {
                    AdjunctState oldState;
                    if (_currState.Value != AdjunctState.Unverified &&
                        !(_currState.Value == AdjunctState.Verified && _currState is not { Prev.Value: AdjunctState.Unverified }) &&
                        (oldState = CompareAndEnterState(AdjunctState.Verified, AdjunctState.Peering)) != AdjunctState.Peering)
                    {
                        if(oldState != AdjunctState.Connected && _currState.EnterTime.ElapsedMs() > parm_max_network_latency_ms)
                            _logger.Warn($"{nameof(PeeringResponse)} - {Description}: Invalid state, {oldState}, age = {_currState.EnterTime.ElapsedMs()}ms. Wanted {nameof(AdjunctState.Peering)}");
                    }
                    else
                    {
                        if (!await SeduceAsync("SYN-SE").FastPath().ConfigureAwait(Zc))
                            await SendDiscoveryRequestAsync().FastPath().ConfigureAwait(Zc);
                        else
                        {
                            if (_lastScan.ElapsedMs() > CcCollective.parm_mean_pat_delay_s * 1000 / 4)
                                await SendDiscoveryRequestAsync().FastPath().ConfigureAwait(Zc);
                        }
                    }
                    break;
                }
                case false:
                {
                    AdjunctState oldState;
                    if ((oldState = CompareAndEnterState(AdjunctState.Verified, AdjunctState.Peering)) != AdjunctState.Peering)
                        _logger.Warn($"{nameof(PeeringResponse)}: Invalid state, {oldState}. Wanted {nameof(AdjunctState.Peering)}, {Description}");
                    
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
        private async ValueTask<int> SendMessageAsync(ByteString data, CcDiscoveries.MessageTypes type = CcDiscoveries.MessageTypes.Undefined, IoNodeAddress dest = null)
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

                var packet = new Packet
                {
                    Data = data,
                    PublicKey = UnsafeByteOperations.UnsafeWrap(CcCollective.CcId.PublicKey),
                    Type = (uint) type
                };

                var packetMsgRaw = packet.Data.Memory.AsArray();
                packet.Signature = UnsafeByteOperations.UnsafeWrap(CcCollective.CcId.Sign(packetMsgRaw, 0, packetMsgRaw.Length));
                
// #if DEBUG
//                 var verified = CcCollective.CcId.Verify(packetMsgRaw, 0, packetMsgRaw.Length, packet.PublicKey.Memory.AsArray(),
//                     0, packet.Signature.Memory.AsArray(), 0);
//                 if (!verified)
//                 {
//                     _logger.Fatal($"{Description}: Unable to sign message!!!");
//                 }
// #endif

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
            catch (NullReferenceException e)
            {
                _logger.Trace(e, Description);
            }
            catch (TaskCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (OperationCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (ObjectDisposedException e)
            {
                _logger.Trace(e, Description);
            }
            catch (Exception e)
            {
                if (Collected)
                    _logger.Debug(e, $"Failed to send message {Description}");
            }

            return 0;
        }

        /// <summary>
        /// Discovery response message
        /// </summary>
        /// <param name="response">The response</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(DiscoveryResponse response, object extraData, Packet packet)
        {
            var discoveryRequest = await _discoveryRequest.ResponseAsync(extraData.ToString(), response.ReqHash).FastPath().ConfigureAwait(Zc);

            if (!discoveryRequest || !Assimilating || response.Peers.Count > parm_max_discovery_peers)
            {
                if (Proxy && Collected && response.Peers.Count <= parm_max_discovery_peers)
                    _logger.Debug($"{nameof(DiscoveryResponse)}{response.ToByteArray().PayloadSig()}: Reject, rq = {response.ReqHash.Memory.HashSig()}, {response.Peers.Count} > {parm_max_discovery_peers}? from {MakeId(CcDesignation.FromPubKey(packet.PublicKey.Memory), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData))}, RemoteAddress = {RemoteAddress}, c = {_discoveryRequest.Count}, matched[{extraData}]");
                return;
            }

            var count = 0;

            _logger.Trace($"<\\- {nameof(DiscoveryResponse)}: Received {response.Peers.Count} potentials from {Description}");

            //PAT
            LastPat = 0;

            foreach (var responsePeer in response.Peers)
            {
                //Any services attached?
                if (responsePeer.Services?.Map == null || responsePeer.Services.Map.Count == 0)
                {
                    _logger.Trace(
                        $"<\\- {nameof(DiscoveryResponse)}: Invalid services recieved!, map = {responsePeer.Services?.Map}, count = {responsePeer.Services?.Map?.Count ?? -1}");
                    continue;
                }

                //ignore strange services
                if (count > parm_max_services)
                    continue;

                var remoteEp = responsePeer.Services.Map[CcService.Keys.peering.ToString()];
                //Never add ourselves (by NAT)
                if (responsePeer.Ip == CcCollective.ExtAddress.Ip &&
                    CcCollective.ExtAddress.Port == remoteEp.Port)
                    continue;

                //Never add ourselves (by ID)
                if (responsePeer.PublicKey.SequenceEqual(CcCollective.CcId.PublicKey))
                    continue;

                //Don't add already known neighbors
                var id = CcDesignation.FromPubKey(responsePeer.PublicKey.Memory);
                if (Hub.Neighbors.Values.Any(n => ((CcAdjunct) n).Designation.Equals(id)))
                    continue;

                var services = new CcService {CcRecord = new CcRecord()};
                var newRemoteEp = new IPEndPoint(IPAddress.Parse(responsePeer.Ip),
                    (int) responsePeer.Services.Map[CcService.Keys.peering.ToString()].Port);

                foreach (var (key, value) in responsePeer.Services.Map)
                {
                    services.CcRecord.Endpoints.TryAdd(Enum.Parse<CcService.Keys>(key),
                        IoNodeAddress.Create($"{value.Network}://{responsePeer.Ip}:{value.Port}"));
                }

                //sanity check
                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (services == null || services.CcRecord.Endpoints.IsEmpty)
                    continue;

                if (!CcCollective.ZeroDrone)
                {
                    await Router.SendPingAsync("SYN-DMZ", dest: services.CcRecord.Endpoints[CcService.Keys.peering]).FastPath().ConfigureAwait(false);
                }
                else
                {
                    await ZeroAsync(static async state =>
                    {
                        var (@this, newRemoteEp, id, services) = state;
                        await Task.Delay(@this._random.Next(@this.parm_max_network_latency_ms) + @this.parm_max_network_latency_ms, @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);

                        if (!await @this.CollectAsync(newRemoteEp, id, services).FastPath().ConfigureAwait(@this.Zc))
                        {
#if DEBUG
                            @this._logger.Trace($"{@this.Description}: Collecting {newRemoteEp.Address} failed!");
#endif
                        }

                    }, ValueTuple.Create(this, newRemoteEp, id, services), TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(Zc);

                }

                count++;
            }
            
        }
        
        /// <summary>
        /// Collect another adjunct into the collective
        /// </summary>
        /// <param name="newRemoteEp">The location</param>
        /// <param name="id">The adjunctId</param>
        /// <param name="services">The adjunct services</param>
        /// <returns>A task, true if successful, false otherwise</returns>
        private async ValueTask<bool> CollectAsync(IPEndPoint newRemoteEp, CcDesignation id, CcService services)
        {
            if (Zeroed() || Hub.Zeroed() || newRemoteEp != null && newRemoteEp.Equals(NatAddress?.IpEndPoint))
            {
                _logger.Trace($"x {Description}");
                return false;
            }

            //var source = new IoUdpClient<CcProtocMessage<Packet, CcDiscoveryBatch>>($"UDP Proxy ~> {Description}",MessageService, newRemoteEp);
            var newAdjunct = (CcAdjunct) Hub.MallocNeighbor(Hub, MessageService, Tuple.Create(id, services, newRemoteEp));

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
                                ((CcAdjunct)n).State < AdjunctState.Peering &&
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
                                if (((CcAdjunct)ioNeighbor).State < AdjunctState.Peering)
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
                newAdjunct.ExtGossipAddress = ExtGossipAddress;
                
                //Start processing
                //await ZeroAsync(static async state =>
                //{
                //    var (@this, newAdjunct) = state;
                //    await @this.CcCollective.Hub.BlockOnAssimilateAsync(newAdjunct).FastPath().ConfigureAwait(@this.Zc);
                //}, ValueTuple.Create(this, newAdjunct), TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(false);

                await newAdjunct.SeduceAsync("ACK", ignoreZeroDrone:true, passive:false).FastPath().ConfigureAwait(Zc);
                //if (!await newAdjunct.SendPingAsync("SYN").FastPath().ConfigureAwait(Zc))
                //{
                //    _logger.Debug($"{newAdjunct.Description}: Unable to send ping!!!");
                //    return false;
                //}
                
                
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
        /// <param name="extraData"></param>
        /// <param name="packet"></param>
        /// <returns></returns>
        private async ValueTask ProcessAsync(DiscoveryRequest request, IPEndPoint extraData, Packet packet)
        {
            if (!(CcCollective.ZeroDrone || Assimilating) || request.Timestamp.ElapsedMs() > parm_max_network_latency_ms * 2)
            {
                if (Assimilating)
                {
                    _logger.Trace(
                        $"<\\- {nameof(DiscoveryRequest)}: [ABORTED] {!Assimilating}, age = {request.Timestamp.CurrentMsDelta():0.0}ms/{parm_max_network_latency_ms * 2:0.0}ms, {Description}, {MetaDesc}");
                }
                    
                return;
            }

            //PAT
            LastPat = 0;

            var discoveryResponse = new DiscoveryResponse
            {
                ReqHash = UnsafeByteOperations.UnsafeWrap(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray()))
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
            foreach (var ioNeighbor in certified.Take((int)parm_max_discovery_peers))
            {
                discoveryResponse.Peers.Add(new Peer
                {
                    PublicKey = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(((CcAdjunct) ioNeighbor).Designation.PublicKey)),
                    Services = ((CcAdjunct) ioNeighbor).ServiceMap,
                    Ip = ((CcAdjunct) ioNeighbor).RemoteAddress.Ip
                });

                if(CcCollective.ZeroDrone)
                    ((CcAdjunct)ioNeighbor)._peerRequestsRecvCount = 0;

                count++;
            }

            int sent;
            if ((sent = await SendMessageAsync(discoveryResponse.ToByteString(), CcDiscoveries.MessageTypes.DiscoveryResponse).FastPath().ConfigureAwait(Zc)) > 0)
            {
                if(count > 0)
                    _logger.Debug($"-/> {nameof(DiscoveryResponse)}({sent}): Sent {count} discoveries to {Description}");

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
                    _logger.Debug($"<\\- {nameof(DiscoveryRequest)}({sent}): [FAILED], {Description}, {MetaDesc}");
            }
        }

        private volatile ServiceMap _serviceMapLocal;
        /// <summary>
        /// Ping message
        /// </summary>
        /// <param name="ping">The ping packet</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(Ping ping, object extraData, Packet packet)
        {
            //syn trigger
            static async ValueTask CheckSynTriggerAsync(CcAdjunct @this, long delay, IoNodeAddress ioNodeAddress)
            {
                if (delay < @this.parm_max_network_latency_ms)
                {
                    if (@this.Direction == Heading.Undefined && @this.CcCollective.IngressCount < @this.CcCollective.parm_max_inbound &&
                        @this.Hub?.Router != null)
                    {
                        if (!await @this.SendPingAsync("SYN-FLD", ioNodeAddress).FastPath().ConfigureAwait(@this.Zc))
                        {
                            @this._logger.Trace($"<\\- {nameof(SendPingAsync)}(SYN-FLD): [FAILED] {@this.Description}");
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

            var remoteEp = (IPEndPoint) extraData;
            
            //Drop old API calls
            if(ping.NetworkId != parm_network_id || ping.Version != parm_protocol_version)
                return;

            //Drop old messages
            if (ping.Timestamp.ElapsedMs() > parm_max_network_latency_ms * 2) //TODO params
            {
                _logger.Error($"<\\- {nameof(Ping)}: [WARN] Dropped stale, age = {ping.Timestamp.ElapsedMs()}ms");
                return;
            }

            //Drop if we are saturated
            if(!Proxy && !CcCollective.ZeroDrone && Hub.Neighbors.Count > CcCollective.MaxAdjuncts && _random.Next(1000000) > 1000000/4)
                return;

            Pong pong;
            if (_serviceMapLocal == null)
            {
                pong = PrepareServices(packet, remoteEp);
            }
            else
            {
                pong = new Pong
                {
                    ReqHash = UnsafeByteOperations.UnsafeWrap(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                    DstAddr = $"{remoteEp.Address}", //TODO, add port somehow
                    Services = _serviceMapLocal
                };
            }

            long triggerDelta;
            if (CcCollective.ZeroDrone)
            {
                //Interlocked.Exchange(ref _triggerTime, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                triggerDelta = parm_max_network_latency_ms;
            }
            else
            {
                triggerDelta = Volatile.Read(ref _triggerTime).ElapsedMs();
            }

            var toAddress = IoNodeAddress.CreateFromEndpoint("udp", remoteEp);
            //PROCESS DMZ-SYN
            var sent = 0;
            if (!Proxy)
            {
                //zero adjunct 
                if(Hub.CcCollective.Neighbors.Count > CcCollective.MaxAdjuncts)
                    return;

                IoNodeAddress toProxyAddress = null;
                if (CcCollective.UdpTunnelSupport)
                {
                    if (ping.SrcAddr != "0.0.0.0" && remoteEp.Address.ToString() != ping.SrcAddr)
                    {
                        toProxyAddress = IoNodeAddress.Create($"udp://{ping.SrcAddr}:{ping.SrcPort}");
                        _logger.Trace(
                            $"<\\- {nameof(Ping)}: static peer address received: {toProxyAddress}, source detected = udp://{remoteEp}");
                    }
                    else
                    {
                        toProxyAddress = IoNodeAddress.CreateFromEndpoint("udp", remoteEp);
                        _logger.Trace(
                            $"<\\- {nameof(Ping)}: automatic peer address detected: {toProxyAddress}, source declared = udp://{ping.SrcAddr}:{ping.SrcPort}");
                    }
                }

                //SEND SYN-ACK
                if (await SendMessageAsync(pong.ToByteString(), CcDiscoveries.MessageTypes.Pong, toAddress)
                        .FastPath().ConfigureAwait(Zc) > 0)
                {
                    //ensure ingress delta trigger
                    if(!CcCollective.ZeroDrone)
                        await CheckSynTriggerAsync(this, triggerDelta, toAddress).FastPath().ConfigureAwait(Zc);

#if DEBUG
                    _logger.Debug($"-/> {nameof(Pong)}({sent})[{pong.ToByteString().Memory.PayloadSig()} ~ {pong.ReqHash.Memory.HashSig()}]: Sent [[SYN-ACK]], [{MessageService.IoNetSocket.LocalAddress} ~> {toAddress}]");
#endif

                    if (AutoPeeringEventService.Operational)
                        await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = Base58.Bitcoin.Encode(packet.PublicKey.Span[..8].ToArray()),
                                Type = "pong"
                            }
                        }).FastPath().ConfigureAwait(Zc);
                }

                if (toProxyAddress != null && CcCollective.UdpTunnelSupport && toAddress.Ip != toProxyAddress.Ip)
                {
                    if (await SendMessageAsync(pong.ToByteString(), CcDiscoveries.MessageTypes.Pong, toAddress)
                            .FastPath().ConfigureAwait(Zc) > 0)
                    {
#if DEBUG
                        _logger.Trace($"-/> {nameof(Pong)}({sent})[{pong.ToByteString().Memory.PayloadSig()} ~ {pong.ReqHash.Memory.HashSig()}]: Sent [[SYN-ACK]], [{MessageService.IoNetSocket.LocalAddress} ~> {toAddress}]");
#endif
                    }
                }
            }
            else //PROCESS SYN
            {
                LastPat = 0;

                if ((sent = await SendMessageAsync(data: pong.ToByteString(), type: CcDiscoveries.MessageTypes.Pong)
                        .FastPath().ConfigureAwait(Zc)) > 0)
                {
#if DEBUG
                    _logger.Debug(IsDroneConnected
                        ? $"-/> {nameof(Pong)}({sent})[{pong.ToByteString().Memory.PayloadSig()} ~ {pong.ReqHash.Memory.HashSig()}]: Sent [[KEEPALIVE]], {Description}"
                        : $"-/> {nameof(Pong)}({sent})[{pong.ToByteString().Memory.PayloadSig()} ~ {pong.ReqHash.Memory.HashSig()}]: Sent [[SYN-ACK]], {Description}");
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
                        _logger.Error($"-/> {nameof(Ping)}: [FAILED] Send [[SYN ACK/KEEPALIVE]], to = {Description}");
                }
            }
        }

        /// <summary>
        /// Prepares the services for this adjunct
        /// </summary>
        /// <param name="packet"></param>
        /// <param name="remoteEp"></param>
        /// <returns></returns>
        private Pong PrepareServices(Packet packet, IPEndPoint remoteEp)
        {
            Pong pong;
            //TODO optimize
            var gossipAddress =
                Hub.Services.CcRecord.Endpoints[CcService.Keys.gossip];
            var peeringAddress =
                Hub.Services.CcRecord.Endpoints[CcService.Keys.peering];
            var fpcAddress =
                Hub.Services.CcRecord.Endpoints[CcService.Keys.fpc];

            pong = new Pong
            {
                ReqHash = UnsafeByteOperations.UnsafeWrap(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                DstAddr = $"{remoteEp.Address}", //TODO, add port somehow
                Services = _serviceMapLocal = new ServiceMap
                {
                    Map =
                    {
                        {
                            CcService.Keys.peering.ToString(),
                            new NetworkAddress { Network = "udp", Port = (uint)peeringAddress.Port }
                        },
                        {
                            CcService.Keys.gossip.ToString(),
                            new NetworkAddress { Network = "tcp", Port = (uint)gossipAddress.Port }
                        },
                        {
                            CcService.Keys.fpc.ToString(),
                            new NetworkAddress { Network = "tcp", Port = (uint)fpcAddress.Port }
                        }
                    }
                }
            };
            return pong;
        }

        /// <summary>
        /// Pong message
        /// </summary>
        /// <param name="pong">The Pong packet</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(Pong pong, object extraData, Packet packet)
        {
            var pingRequest = await _pingRequest.ResponseAsync(extraData.ToString(), pong.ReqHash).FastPath().ConfigureAwait(Zc);
            
            //Try the router
            if (!pingRequest && Proxy)
                pingRequest = await Hub.Router._pingRequest.ResponseAsync(extraData.ToString(), pong.ReqHash).FastPath().ConfigureAwait(Zc);
            

            if (!pingRequest)
            {
#if DEBUG
                if (Collected && Proxy)
                {
                    _logger.Error($"<\\- {nameof(Pong)} {packet.Data.Memory.PayloadSig()}: SEC! {pong.ReqHash.Memory.HashSig()}, d = {_pingRequest.Count}, pats = {TotalPats},  " +
                                  $"PK={Designation.IdString()} != {Base58.Bitcoin.Encode(packet.PublicKey.Span[..8])} (proxy = {Proxy}),  ssp = {SecondsSincePat}, d = {(AttachTimestamp > 0 ? (AttachTimestamp - LastPat).ToString() : "N/A")}, v = {Verified}, s = {extraData}, {Description}");
                }
#endif

                return;
            }
            
            //Process SYN-ACK
            if (!Proxy)
            {
                var idCheck = CcDesignation.FromPubKey(packet.PublicKey.Memory);
                var fromAddress = IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData);
                var keyStr = MakeId(idCheck, fromAddress);
                var pkString = idCheck.PkString();

                // remove stale adjunctPKs
                var staleId = Hub.Neighbors.Where(kv => ((CcAdjunct) kv.Value).Proxy).Any(kv => kv.Value.Key.Contains(pkString));
                if (staleId && Hub.Neighbors.TryRemove(pkString, out var staleNeighbor))
                {
                    _logger.Warn($"Removing stale adjunct {staleNeighbor.Key}:{((CcAdjunct) staleNeighbor).RemoteAddress.Port} ==> {keyStr}:{((IPEndPoint) extraData).Port}");
                    staleNeighbor.Zero(new IoNanoprobe($"{nameof(staleNeighbor)}"));
                }

                var remoteServices = new CcService();
                foreach (var key in pong.Services.Map.Keys.ToList())
                    remoteServices.CcRecord.Endpoints.TryAdd(Enum.Parse<CcService.Keys>(key),
                        IoNodeAddress.Create(
                            $"{pong.Services.Map[key].Network}://{((IPEndPoint) extraData).Address}:{pong.Services.Map[key].Port}"));

                if (Hub.Neighbors.Count <= CcCollective.MaxAdjuncts)
                {
                    await ZeroAsync(static async state =>
                    {
                        var (@this, fromAddress, remoteServices, idCheck) = state;
                        if (!await @this.CollectAsync(fromAddress.IpEndPoint, idCheck, remoteServices).FastPath().ConfigureAwait(@this.Zc))
                        {
#if DEBUG
                            @this._logger.Trace($"{@this.Description}: Collecting {fromAddress.IpEndPoint} failed!");
#else
                            @this._logger.Trace($"{@this.Description}: Collecting {fromAddress.IpEndPoint} failed!");
#endif
                        }
                    }, ValueTuple.Create(this, fromAddress, remoteServices, idCheck), TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(Zc);
                }
            }
            else if (!Verified) //Process SYN-ACK
            {
                //PAT
                LastPat = 0;

                //TODO: vector?
                //set ext address as seen by neighbor
                ExtGossipAddress =
                    IoNodeAddress.Create(
                        $"tcp://{pong.DstAddr}:{CcCollective.Services.CcRecord.Endpoints[CcService.Keys.gossip].Port}");

                NatAddress =
                    IoNodeAddress.Create(
                        $"udp://{pong.DstAddr}:{CcCollective.Services.CcRecord.Endpoints[CcService.Keys.peering].Port}");

                Verified = true;
                Thread.MemoryBarrier();

                AdjunctState oldState;
                if ((oldState = CompareAndEnterState(AdjunctState.Verified, AdjunctState.Unverified)) != AdjunctState.Unverified)
                {
                    _logger.Warn($"{nameof(Pong)} - {Description}: Invalid state, {oldState}. Wanted {nameof(AdjunctState.Unverified)}");
                    return;
                }

#if DEBUG
                _logger.Trace($"<\\- {nameof(Pong)}: Processed <<SYN-ACK>>: {Description}");
#endif

                await SeduceAsync("ACK",false, ignoreZeroDrone: true).FastPath().ConfigureAwait(Zc);
            }
            else 
            {
                //PAT
                LastPat = 0;

                if (!this.CcCollective.ZeroDrone)
                {
                    await SeduceAsync("ZRO").FastPath().ConfigureAwait(Zc);
                }
                else
                {
                    await SendDiscoveryRequestAsync().FastPath().ConfigureAwait(Zc);
                }
            }
        }

        /// <summary>
        /// Seduces another adjunct
        /// </summary>
        /// <returns>A valuable task</returns>
        public async ValueTask<bool> SeduceAsync(string desc, bool passive = true, IoNodeAddress address = null, bool ignoreZeroDrone = false)
        {
            if(_stealthy.ElapsedMs() < parm_max_network_latency_ms || Zeroed() || !Collected || IsDroneAttached || _currState.Value > AdjunctState.Connecting /*don't change this from Connecting*/)
                return false;

            try
            {
                if (Proxy && (Probed || !passive) &&
                    !CcCollective.ZeroDrone && Direction == Heading.Undefined &&
                    CcCollective.EgressCount < CcCollective.parm_max_outbound &&
                    PeerRequestsSentCount < parm_zombie_max_connection_attempts)
                {
                    if (!await SendPeerRequestAsync().FastPath().ConfigureAwait(Zc))
                    {
                        _logger.Trace($"<\\- {nameof(SendPeerRequestAsync)}: [FAILED] Send Peer request, {Description}");
                    }
                    else
                    {
                        if(passive)
                            _stealthy = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
#if DEBUG
                        _logger.Trace($"-/> {nameof(SeduceAsync)}: Sent [[{desc}]] Peer REQUEST), {Description}");
#endif
                        return true;
                    }
                }
                else if(!(CcCollective.ZeroDrone && !ignoreZeroDrone) && passive)
                {
                    var proxy = address == null ? this : Router;
                    //delta trigger
                    if (proxy != null && !await proxy.SendPingAsync(desc, address).FastPath().ConfigureAwait(Zc))
                        _logger.Trace($"<\\- {nameof(SendPingAsync)}({desc}): [FAILED] Send Peer HUP");
                    else
                    {
                        _logger.Debug($"-/> {nameof(SendPingAsync)}: Send [[{desc}]] Ping");
                        _stealthy = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        return true;
                    }
                }
            }
            catch when(Zeroed()){}
            catch (Exception e)when(!Zeroed())
            {
                _logger.Error(e,$"SeduceAsync: ");
            }
            return false;
        }

        /// <summary>
        /// Sends a universal ping packet
        /// </summary>
        /// <param name="desc">Ping description</param>
        /// <param name="dest">The destination address</param>
        /// <param name="id">Optional Id of the node pinged</param>
        /// <returns>Task</returns>
        public async ValueTask<bool> SendPingAsync(string desc, IoNodeAddress dest = null, string id = null)
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
                var pingRequest = new Ping
                {
                    DstAddr = dest.Ip,
                    NetworkId = parm_network_id,
                    Version = parm_protocol_version,
                    SrcAddr = "0.0.0.0",
                    SrcPort = (uint) CcCollective.Services.CcRecord.Endpoints[CcService.Keys.peering].Port,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                };

                var reqBuf = pingRequest.ToByteString();

                if (reqBuf?.IsEmpty ?? true)
                    return false;

                // Is this a routed request?
                if (Proxy)
                {
                    //send
                    IoQueue<IoZeroMatcher<ByteString>.IoChallenge>.IoZNode challenge;
                    if ((challenge = await _pingRequest.ChallengeAsync(RemoteAddress.IpPort, reqBuf)
                        .FastPath().ConfigureAwait(Zc)) == null) 
                        return false;

                    var sent = await SendMessageAsync(data: reqBuf, type: CcDiscoveries.MessageTypes.Ping).FastPath().ConfigureAwait(Zc);
                    if (sent > 0)
                    {
#if DEBUG

                        try
                        {
                            _logger.Debug($"-/> {nameof(Ping)}({sent})[payload = {reqBuf.Span.PayloadSig()}, hash = {challenge.Value.Hash}]: sent [[{desc}]] {Description}");
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                            throw;
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

                    await _pingRequest.RemoveAsync(challenge).FastPath().ConfigureAwait(Zc);

                    if (Collected)
                        _logger.Error($"-/> {nameof(Ping)}: [FAILED], {Description}");

                    return false;
                }
                else //The destination state was undefined, this is local
                {
                    IoQueue<IoZeroMatcher<ByteString>.IoChallenge>.IoZNode challenge;
                    if ((challenge = await _pingRequest.ChallengeAsync(dest.IpPort, reqBuf).FastPath().ConfigureAwait(Zc)) == null)
                        return false;

                    var sent = await SendMessageAsync(reqBuf, CcDiscoveries.MessageTypes.Ping, dest)
                        .FastPath().ConfigureAwait(Zc);

                    if (sent > 0)
                    {
#if DEBUG
                        _logger.Debug($"-/> {nameof(Ping)}({sent})[{reqBuf.Span.PayloadSig()}]: sent [[{desc}]], dest = {dest}, {Description}");
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

                    await _pingRequest.RemoveAsync(challenge).FastPath().ConfigureAwait(Zc);
                    if(Collected)
                        _logger.Trace($"-/> {nameof(Ping)}:({desc}) [FAILED], {Description}");
                    return false;
                }
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger.Debug(e,$"ERROR z = {Zeroed()}, dest = {dest}, source = {MessageService}, _discoveryRequest = {_discoveryRequest.Count}");
            }

            return false;
        }

        private long _lastScan;

        private long LastScan => Volatile.Read(ref _lastScan);
        /// <summary>
        /// Sends a discovery request
        /// </summary>
        /// <returns>Task</returns>
        public async ValueTask<bool> SendDiscoveryRequestAsync()
        {
            try
            {
                if (!Assimilating)
                {
                    _logger.Trace($"{nameof(SendDiscoveryRequestAsync)}: [ABORTED], {Description}, s = {State}, a = {Assimilating}");
                    return false;
                }

                //rate limit
                //if (LastScan.Elapsed() < parm_max_network_latency_ms / 1000)
                //    return false;
                
                var discoveryRequest = new DiscoveryRequest
                {
                     Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                };

                var reqBuf = discoveryRequest.ToByteString();

                IoQueue<IoZeroMatcher<ByteString>.IoChallenge>.IoZNode challenge;
                if ((challenge = await _discoveryRequest.ChallengeAsync(RemoteAddress.IpPort, reqBuf)
                    .FastPath().ConfigureAwait(Zc)) == null)
                {
                    return false;
                }

                _lastScan = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                var sent = await SendMessageAsync(reqBuf,CcDiscoveries.MessageTypes.DiscoveryRequest, RemoteAddress).FastPath().ConfigureAwait(Zc);
                if (sent > 0)
                {
                    _logger.Trace($"-/> {nameof(DiscoveryRequest)}({sent}){reqBuf.Memory.PayloadSig()}: Sent, {Description}");

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

                await _discoveryRequest.RemoveAsync(challenge).FastPath().ConfigureAwait(Zc);
                if (Collected)
                    _logger.Debug($"-/> {nameof(DiscoveryRequest)}: [FAILED], {Description} ");
            }
            catch when (Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Debug(e,$"{nameof(DiscoveryRequest)}: [ERROR] z = {Zeroed()}, state = {State}, dest = {RemoteAddress}, source = {MessageService}, _discoveryRequest = {_discoveryRequest.Count}");
            }

            return false;
        }

        /// <summary>
        /// Sends a peer request
        /// </summary>
        /// <returns>Task</returns>
        public async ValueTask<bool> SendPeerRequestAsync()
        {
            if (IsDroneConnected || CcCollective.ZeroDrone || _currState.Value == AdjunctState.Connected)
            {
                if (Collected && !CcCollective.ZeroDrone)
                    _logger.Warn($"{nameof(SendPeerRequestAsync)}: [ABORTED], {Description}, s = {State}, a = {Assimilating}, p = {IsDroneConnected}");
                return false;
            }

            if (_currState.Value > AdjunctState.Unverified)
            {
                AdjunctState oldState;

                var stateIsValid = (oldState = CompareAndEnterState(AdjunctState.Peering, AdjunctState.Verified,
                    overrideHung: parm_max_network_latency_ms * 2)) == AdjunctState.Verified;

                if (!stateIsValid)
                {
                    if (oldState != AdjunctState.Peering)
                        _logger.Warn($"{nameof(SendPeerRequestAsync)} - {Description}: Invalid state, {oldState}, age = {_currState.EnterTime.ElapsedMs()}ms. Wanted {nameof(AdjunctState.Verified)} - [RACE OK!] ");
                    return false;
                }
                
                if(oldState != AdjunctState.Verified)
                {
                    _logger.Warn($"{nameof(SendPeerRequestAsync)} - {Description}: Invalid state, {oldState}, age = {_currState.EnterTime.ElapsedMs()}ms. Wanted {nameof(AdjunctState.Verified)} - [PUSH OK?] ");
                }
            }

            //oneshot
            await ZeroAsync(async (@this) =>
            {
                try
                {
                    //TODO prod:
                    await Task.Delay(parm_max_network_latency_ms / 16 + 1, AsyncTasks.Token).ConfigureAwait(Zc);
                    //await Task.Yield();

                    var peerRequest = new PeeringRequest
                    {
                        Salt = new Salt
                        { ExpTime = (ulong)DateTimeOffset.UtcNow.AddHours(2).ToUnixTimeSeconds(), Bytes = GetSalt },
                        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    };

                    var peerRequestRaw = peerRequest.ToByteString();

                    IoQueue<IoZeroMatcher<ByteString>.IoChallenge>.IoZNode challenge;
                    if ((challenge = await _peerRequest.ChallengeAsync(RemoteAddress.IpPort, peerRequestRaw).FastPath().ConfigureAwait(Zc)) == null)
                    {
                        return;
                    }

                    var sent = await SendMessageAsync(peerRequestRaw, CcDiscoveries.MessageTypes.PeeringRequest).FastPath().ConfigureAwait(Zc);
                    if (sent > 0)
                    {
                        
                        Interlocked.Increment(ref _peerRequestsSentCount);
                        _logger.Trace($"-/> {nameof(PeeringRequest)}({sent})[{peerRequestRaw.Memory.PayloadSig()}]: Sent, {Description}");
                        
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

                    await _peerRequest.RemoveAsync(challenge).FastPath().ConfigureAwait(Zc);

                    if (Collected)
                        _logger.Debug($"-/> {nameof(PeeringRequest)}: [FAILED], {Description}, {MetaDesc}");
                }
                catch when (Zeroed()) { }
                catch (Exception e) when (!Zeroed())
                {
                    _logger.Debug(e, $"{nameof(PeeringRequest)}: [FAILED], {Description}, {MetaDesc}");
                }
            }, this, TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(Zc);

            return true;
        }

        /// <summary>
        /// Tell peer to drop us when things go wrong. (why or when? cause it wont reconnect otherwise. This is a bug)
        /// </summary>
        /// <returns></returns>
        internal async ValueTask<bool> SendPeerDropAsync(IoNodeAddress dest = null)
        {
            try
            {
                dest ??= RemoteAddress;

                var dropRequest = new PeeringDrop
                {
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                };

                var sent = await SendMessageAsync(dropRequest.ToByteString(), CcDiscoveries.MessageTypes.PeeringDrop, dest)
                    .FastPath().ConfigureAwait(Zc);

#if DEBUG
                _logger.Trace(
                    (sent) > 0
                        ? $"-/> {nameof(PeeringDrop)}({sent}): Sent, {Description}"
                        : $"-/> {nameof(PeeringDrop)}: [FAILED], {Description}, {MetaDesc}");
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
                        $"{nameof(SendPeerDropAsync)}: [ERROR], {Description}, s = {State}, a = {Assimilating}, p = {IsDroneConnected}, d = {dest}, s = {MessageService}");
            }

            return false;
        }

        //TODO complexity
        /// <summary>
        /// Attaches a gossip peer to this neighbor
        /// </summary>
        /// <param name="ccDrone">The peer</param>
        /// <param name="direction"></param>
        public async ValueTask<bool> AttachPeerAsync(CcDrone ccDrone, Heading direction)
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

                _logger.Trace($"{nameof(AttachPeerAsync)}: [WON] {_drone?.Description}");
                
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
                    await SendDiscoveryRequestAsync().FastPath().ConfigureAwait(Zc);

                return true;
            }
            catch when (!Zeroed()){}
            catch (Exception e) when(Zeroed())
            {
                _logger.Trace(e, Description);
            }

            //if ((oldState = CompareAndEnterState(AdjunctState.Verified, AdjunctState.Peering)) != AdjunctState.Peering)
            //    _logger.Warn($"{nameof(AttachPeerAsync)} - {Description}:5 Invalid state, {oldState}. Wanted {nameof(AdjunctState.Peering)}");

            return false;
        }

        /// <summary>
        /// If a connection was ever made
        /// </summary>
        public bool WasAttached { get; protected internal set; }

        /// <summary>
        /// Detaches a peer from this neighbor
        /// </summary>
        public async ValueTask DetachPeerAsync()
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
                        //var backOff = @this.parm_max_network_latency_ms / 2 +
                        //              @this._random.Next(@this.parm_max_network_latency_ms * 2);
                        var backOff = @this.parm_max_network_latency_ms / 2 + @this._random.Next(@this.parm_max_network_latency_ms);
                        //var s = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        await Task.Delay(backOff, @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);

                        if (@this.State == AdjunctState.Verified)
                        {
                            await @this.SendPingAsync("SYN-BRG").FastPath().ConfigureAwait(@this.Zc);
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
                _logger.Error(e, $"{nameof(DetachPeerAsync)}: ");
            }
            finally
            {
                severedDrone.Zero(this);
                if(WasAttached)
                    await SendPeerDropAsync().FastPath().ConfigureAwait(Zc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AdjunctState ResetState(AdjunctState state)
        {
            Interlocked.Exchange(ref _direction, 0);
            _peerRequestsRecvCount = _peerRequestsSentCount = 0;

            _currState.Set((int)state);

            return _currState.Value;
        }

#if DEBUG
        [MethodImpl(MethodImplOptions.Synchronized)]
#endif
        public AdjunctState CompareAndEnterState(AdjunctState state, AdjunctState cmp, bool compare = true, int overrideHung = 0)
        {
            var oldValue = _currState.Value;
//#if DEBUG
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
                        _peerRequestsRecvCount = _peerRequestsSentCount = 0;
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
            catch (Exception e)
            {
                _logger.Error(e);
            }
//#else
//            if (overrideHung > 0)
//            {
//                var hung = _currState.EnterTime.ElapsedMs() > overrideHung;

//                if (hung)
//                {
//                    Interlocked.Exchange(ref _direction, 0);
//                    _peerRequestsRecvCount = _peerRequestsSentCount = 0;
//                }

//                compare = compare && !hung;
//            }
            
//            if (compare)
//            {
//                return _currState.CompareAndEnterState((int)state, (int)cmp);
//            }
//            else
//            {
//                _currState.Set((int)state);
                
//            }
//#endif
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