//#define LOSS
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
#if DEBUG
using System.Runtime.InteropServices;
#endif
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
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
using zero.core.misc;
using zero.core.models.protobuffer;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
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
                false,
                false
            )
        {
            _logger = LogManager.GetCurrentClassLogger();

            //TODO tuning
            _pingRequest = new IoZeroMatcher<ByteString>(nameof(_pingRequest), Source.ZeroConcurrencyLevel(), parm_max_network_latency * 2, CcCollective.MaxAdjuncts * 100);
            _peerRequest = new IoZeroMatcher<ByteString>(nameof(_peerRequest), Source.ZeroConcurrencyLevel(), parm_max_network_latency * 2, CcCollective.MaxAdjuncts * 100);
            _discoveryRequest = new IoZeroMatcher<ByteString>(nameof(_discoveryRequest), (int)(CcCollective.MaxAdjuncts * parm_max_discovery_peers + 1), parm_max_network_latency * 10, CcCollective.MaxAdjuncts * 100);

            if (extraData != null)
            {
                
                var (item1, item2, item3) = (Tuple<CcDesignation, CcService, IPEndPoint>) extraData;
                Designation = item1;
                Services = services ?? item2;
                RemoteAddress = IoNodeAddress.CreateFromEndpoint("udp", item3);
                Key = MakeId(Designation, RemoteAddress);
                Source = new IoUdpClient<CcProtocMessage<Packet, CcDiscoveryBatch>>($"UDP Proxy ~> {Description}", MessageService, RemoteAddress.IpEndPoint);
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
                ZeroAsync(RoboAsync, this,TaskCreationOptions.LongRunning | TaskCreationOptions.PreferFairness | TaskCreationOptions.DenyChildAttach).AsTask().GetAwaiter();
            }
            else
            {
                CompareAndEnterState(AdjunctState.Local, AdjunctState.Undefined);
                _routingTable = new ConcurrentDictionary<string, CcAdjunct>();
            }
            _viral = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
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
                    var targetDelay = @this._random.Next(@this.CcCollective.parm_mean_pat_delay / 5 * 1000) + @this.CcCollective.parm_mean_pat_delay / 4 * 1000;
                    var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    await Task.Delay(targetDelay, @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);
                    if(@this.Zeroed())
                        break;

                    if (ts.ElapsedMs() > targetDelay * 1.25 && ts.ElapsedMsToSec() > 0)
                    {
                        @this._logger.Warn($"{@this.Description}: Popdog is slow!!!, {ts.ElapsedMs() / 1000.0:0.0}s");
                    }

                    try
                    {
                        //@this._logger.Debug($"R> {@this.Description}");
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
            ZeroState = 2,
            Local = 3,
            Unverified = 4,
            Verified = 5,
            Peering = 6,
            Connected = 7,
            Sentinel = 8,
        }

        /// <summary>
        /// The current state
        /// </summary>
        private volatile IoStateTransition<AdjunctState> _currState = new()
        {
            FinalState = (int)AdjunctState.FinalState
        };

        /// <summary>
        /// logger
        /// </summary>
        private Logger _logger;

        /// <summary>
        /// Description
        /// </summary>
        private string _description;

        public override string Description
        {
            get
            {
                try
                {
                    if(Proxy)
                        return _description = $"`adjunct ({(Verified ? "+v" : "-v")},{(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{PeerRequestsSentCount}:{PeerRequestsRecvCount}] local: ||{MessageService.IoNetSocket.LocalAddress} ~> {MessageService.IoNetSocket.RemoteAddress}||, [{Hub?.Designation.IdString()}, {Designation.IdString()}], uptime = {TimeSpan.FromSeconds(Uptime.ElapsedMsToSec())}'";
                    else
                        return _description = $"`hub ({(Verified ? "+v" : "-v")},{(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{PeerRequestsSentCount}:{PeerRequestsRecvCount}] local: ||{MessageService.IoNetSocket.LocalAddress} ~> {MessageService.IoNetSocket.RemoteAddress}||, [{Hub?.Designation?.IdString()}, {Designation.IdString()}], uptime = {TimeSpan.FromSeconds(Uptime.ElapsedMsToSec())}'";
                }
                catch (Exception)
                {
                    if(Proxy)
                        return _description?? $"`adjunct({(Verified ? "+v" : "-v")},{(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{PeerRequestsSentCount}:{PeerRequestsRecvCount}] local: ||{MessageService?.IoNetSocket?.LocalAddress} ~> {MessageService?.IoNetSocket?.RemoteAddress}||,' [{Hub?.Designation?.IdString()}, {Designation?.IdString()}], uptime = {TimeSpan.FromSeconds(Uptime.ElapsedMsToSec())}'";    
                    else
                        return _description = $"`hub ({(Verified ? "+v" : "-v")},{(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({Priority}):{PeerRequestsSentCount}:{PeerRequestsRecvCount}] local: ||{MessageService?.IoNetSocket?.LocalAddress} ~> {MessageService?.IoNetSocket?.RemoteAddress}||, [{Hub?.Designation?.IdString()}, {Designation?.IdString()}], uptime = {TimeSpan.FromSeconds(Uptime.ElapsedMsToSec())}'";
                }
            }
        }

        /// <summary>
        /// return extra information about the state
        /// </summary>
        public string MetaDesc => $"{(Zeroed()?"Zeroed!!!,":"")} d={Direction}, s={State}, v={Verified}, a={Assimilating}, da={IsDroneAttached}, cc={IsDroneConnected}, g={IsGossiping}, arb={IsArbitrating}, s={MessageService.IsOperational}";

        /// <summary>
        /// Random number generator
        /// </summary>
        private readonly Random _random = new((int)DateTimeOffset.Now.Ticks);

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

        private volatile uint _peerRequestsRecvCount;
        /// <summary>
        /// Indicates whether we have successfully established a connection before
        /// </summary>
        protected uint PeerRequestsRecvCount => _peerRequestsRecvCount;

        private volatile uint _peerRequestsSentCount;
        /// <summary>
        /// Number of peer requests
        /// </summary>
        public uint PeerRequestsSentCount => _peerRequestsSentCount;

        /// <summary>
        /// Node broadcast priority. 
        /// </summary>
        public long Priority => PeerRequestsSentCount + PeerRequestsRecvCount;

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

        /// <summary>
        /// Maintains viral checkpoints
        /// </summary>
        private long _viral;

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
        public int parm_max_network_latency = 60000;
#else
        public int parm_max_network_latency = 2000;
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
        public int parm_zombie_max_connection_attempts = 100;

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
        public int parm_min_spare_bays = 0;

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
        /// How viral the collective is
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public uint parm_virality = 60;

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
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
            
#if SAFE_RELEASE
            _logger = null;
            _routingTable = null;
            _pingRequest = null;
            _drone = null;
            _protocolConduit = null;
            StateTransitionHistory = null;
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
                            _logger.Trace($"ROUTER: cull failed,wanted = [{Designation?.PkString()}], got [{currentRoute.Designation.PkString()}, serial1 = {SerialNr} vs {currentRoute.SerialNr}], {Description}");
                        }
                    }

                    
                }
                catch
                {
                    // ignored
                }

                if (Assimilated && Direction != Heading.Undefined && WasAttached)
                    _logger.Info($"- `{(Assimilated ? "apex" : "sub")} {Direction}: {Description}, from: {ZeroedFrom?.Description}");

                await DetachPeerAsync().FastPath().ConfigureAwait(Zc);
                
                //swarm 
                await ZeroAsync(async @this =>
                {
                    await Task.Delay(parm_max_network_latency * 2, AsyncTasks.Token).ConfigureAwait(Zc);
                    await @this.Router.SendPingAsync(RemoteAddress).FastPath().ConfigureAwait(Zc);
                }, this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach | TaskCreationOptions.PreferFairness);
            }
            else
            {
                _logger.Info($"~> {Description}, from: {ZeroedFrom?.Description}");
            }

            SetState(AdjunctState.ZeroState);

            await _pingRequest.ZeroAsync(this).FastPath().ConfigureAwait(Zc);
            await _peerRequest.ZeroAsync(this).FastPath().ConfigureAwait(Zc);
            await _discoveryRequest.ZeroAsync(this).FastPath().ConfigureAwait(Zc);
            
#if DEBUG
            Array.Clear(StateTransitionHistory, 0, StateTransitionHistory.Length);
#endif
        }

        /// <summary>
        /// Ensure robotic diagnostics
        /// </summary>
        public async ValueTask EnsureRoboticsAsync()
        {
            try
            {
                //Reset hung peering attempts
                CompareAndEnterState(AdjunctState.Verified, AdjunctState.Peering);

                //send PAT
                if (await SendPingAsync().FastPath().ConfigureAwait(Zc))
                {
#if DEBUG
                    _logger.Trace($"-/> {nameof(EnsureRoboticsAsync)}: PAT to = {Description}");
#endif
                }

                else if (Collected)
                    _logger.Error($"-/> {nameof(SendPingAsync)}: PAT Send [FAILED], {Description}, {MetaDesc}");
            }
            catch when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"{nameof(EnsureRoboticsAsync)} - {Description}: Send PAT failed!");
            }
            
            var threshold = IsDroneConnected ? CcCollective.parm_mean_pat_delay * 2 : CcCollective.parm_mean_pat_delay;
            //Watchdog failure
            if (SecondsSincePat > threshold)
            {
                _logger.Trace($"w {nameof(EnsureRoboticsAsync)} - {Description}, s = {SecondsSincePat} >> {CcCollective.parm_mean_pat_delay}, {MetaDesc}");
                await ZeroAsync(new IoNanoprobe($"-wd: l = {SecondsSincePat}s ago, uptime = {TimeSpan.FromMilliseconds(Uptime.ElapsedMs()).TotalHours:0.00}h")).FastPath().ConfigureAwait(Zc);
            }
        }
        
        /// <summary>
        /// Start processors for this neighbor
        /// </summary>
        /// <returns></returns>
        public override async Task BlockOnReplicateAsync()
        {
            try
            {
                if (!Proxy)
                {
                    //Discoveries
                    await ZeroOptionAsync(static async state =>
                    {
                        var (@this, assimilateAsync) = state;
                        var processDiscoveriesAsync = @this.ProcessDiscoveriesAsync();
                        
                    }, ValueTuple.Create<CcAdjunct,Func<Task>>(this, base.BlockOnReplicateAsync), TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(Zc);

                    //UDP traffic
                    await ZeroOptionAsync(static async state =>
                    {
                        var (@this, assimilateAsync) = state;
                        await assimilateAsync();
                    }, ValueTuple.Create<CcAdjunct, Func<Task>>(this, base.BlockOnReplicateAsync), TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(Zc);
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
            //Validate request
            if (!Assimilating && !IsDroneConnected && State < AdjunctState.Verified)
            {
                _logger.Fatal($"{Description}: Connect aborted, wrong state: {MetaDesc}, ");
                return false;
            }

            if (CcCollective.Neighbors.TryGetValue(Key, out var existingNeighbor))
            {
                if (existingNeighbor.Source.IsOperational)
                {
                    _logger.Warn("Drone was connected...");
                    return false;
                }
                await existingNeighbor.ZeroAsync(new IoNanoprobe("Dropped because reconnect?")).FastPath().ConfigureAwait(Zc);
            }
            
            await ZeroAsync(static async @this =>
            {
                //Attempt the connection, race to win
                if (!await @this.CcCollective.ConnectToDroneAsync(@this).FastPath().ConfigureAwait(@this.Zc))
                    @this._logger.Trace($"{@this.Description}: Leashing adjunct failed!");

            }, this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(Zc);
            
            return true;
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
                    await msgBatch.ReturnToHeapAsync().FastPath();
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
                                        @this._produceTaskPool[i] = @this._protocolConduit.ProduceAsync();
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
                    
                    },this, TaskCreationOptions.AttachedToParent | TaskCreationOptions.DenyChildAttach);

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
                                    try
                                    {
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
                                                                currentRoute = (CcAdjunct)@this.Hub.Neighbors.Values.FirstOrDefault(n => ((CcAdjunct)n).Proxy && ((CcAdjunct)n).RemoteAddress.Key == discoveryBatch.RemoteEndPoint);
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
                                                                        @this._logger.Trace($"Added new route: {@this.Description}");
#endif
                                                                    }
                                                                }
                                                            }
                                                            else
                                                            {
                                                                //validate
                                                                for (var j = 2; j < 4; j++)
                                                                {
                                                                    for (var i = j; i < currentRoute!.Designation.PublicKey.Length; i += i)
                                                                    {
                                                                        if (currentRoute.Designation.PublicKey[i] != packet.PublicKey[i])
                                                                        {
                                                                            @this._logger.Warn($"{nameof(@this.Router)}: Dropped incoming {currentRoute.RemoteAddress}/{currentRoute.Designation.PkString()} ~> {discoveryBatch.RemoteEndPoint}/{Base58.Bitcoin.Encode(packet.PublicKey.Span)}");
                                                                            await currentRoute.ZeroAsync(@this).FastPath().ConfigureAwait(@this.Zc);
                                                                            currentRoute = null;
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        catch when(@this.Zeroed()){return;}
                                                        catch (Exception e) when(!@this.Zeroed())
                                                        {
                                                            @this._logger.Error(e, @this.Description);
                                                            return;
                                                        }

                                                        currentRoute ??= @this.Hub.Router;

                                                        var extraData = IPEndPoint.Parse(discoveryBatch.RemoteEndPoint);//TODO get rid of this

                                                        switch ((CcDiscoveries.MessageTypes) packet.Type)
                                                        {
                                                            case CcDiscoveries.MessageTypes.Ping:
                                                                await currentRoute.ProcessAsync((Ping)message, extraData, packet);
                                                                break;
                                                            case CcDiscoveries.MessageTypes.Pong:
                                                                await currentRoute.ProcessAsync((Pong)message, extraData, packet);
                                                                break;
                                                            case CcDiscoveries.MessageTypes.DiscoveryRequest:
                                                                await currentRoute.ProcessAsync((DiscoveryRequest)message, extraData, packet);
                                                                break;
                                                            case CcDiscoveries.MessageTypes.DiscoveryResponse:
                                                                await currentRoute.ProcessAsync((DiscoveryResponse) message, extraData, packet);
                                                                break;
                                                            case CcDiscoveries.MessageTypes.PeeringRequest:
                                                                await currentRoute
                                                                    .ProcessAsync((PeeringRequest) message, extraData,
                                                                        packet);
                                                                break;
                                                            case CcDiscoveries.MessageTypes.PeeringResponse:
                                                                await currentRoute
                                                                    .ProcessAsync((PeeringResponse) message, extraData, packet);
                                                                break;
                                                            case CcDiscoveries.MessageTypes.PeeringDrop:
                                                                await currentRoute
                                                                    .ProcessAsync((PeeringDrop) message, extraData, packet);
                                                                break;
                                                        }
                                                    }
                                                    catch when(@this.Zeroed()){}
                                                    catch (Exception e) when (!@this.Zeroed())
                                                    {
                                                        @this._logger?.Error(e, $"{message.GetType().Name} [FAILED]: l = {packet.Data.Length}, {@this.Key}");
                                                    }
                                                }, @this);
                                            }
                                            finally
                                            {
                                                if (batchJob != null && batchJob.State != IoJobMeta.JobState.Consumed)
                                                    batchJob.State = IoJobMeta.JobState.ConsumeErr;
                                            }
                                        }, @this);
                                    }
                                    catch when(!@this.Zeroed()){}
                                    catch (Exception e)when(!@this.Zeroed())
                                    {
                                        @this._logger?.Error(e, $"Consumption failed for {@this.Description}");
                                        break;
                                    }
                                }

                                if (!await @this._consumeTaskPool[^1].FastPath())
                                    break;
                            }
                        }
                        catch when(@this.Zeroed() || @this.Source.Zeroed() || @this._protocolConduit?.Source == null) {}
                        catch (Exception e) when (!@this.Zeroed() && !@this.Source.Zeroed() && @this._protocolConduit?.Source != null)
                        {
                            @this._logger?.Error(e, $"{@this.Description}");
                        }
                    }, this, TaskCreationOptions.AttachedToParent | TaskCreationOptions.PreferFairness );
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
            if (!Assimilating || request.Timestamp.ElapsedMs() > parm_max_network_latency * 2)
            {
                if(Proxy)
                    _logger.Trace($"{(Proxy ? "V>" : "X>")}{nameof(PeeringDrop)}: Ignoring {request.Timestamp.ElapsedDelta()}s old/invalid request");
                return;
            }
            
            //PAT
            LastPat = 0;

            if (IsDroneAttached)
            {
                _logger.Trace($"{nameof(PeeringDrop)}: {Direction} Peer = {_drone.Key}: {Description}, {MetaDesc}");
                await DetachPeerAsync().FastPath().ConfigureAwait(Zc);
            }
            else
            {
                //Hup
                if (Direction == Heading.Undefined && CcCollective.IngressCount < CcCollective.parm_max_inbound)
                    await SendPingAsync().FastPath().ConfigureAwait(Zc);
            }
        }

        /// <summary>
        /// Peering Request message from client
        /// </summary>
        /// <param name="request">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(PeeringRequest request, object extraData, Packet packet)
        {
            //Drop old requests
            if (request.Timestamp.ElapsedMs() > parm_max_network_latency * 2)
            {
                _logger.Trace(
                    $"{nameof(PeeringRequest)}: Dropped!, {(Verified ? "verified" : "un-verified")}, age = {request.Timestamp.ElapsedDelta()}");
                return;
            }

            Interlocked.Increment(ref _peerRequestsRecvCount);

            //fail fast
            int sent;
            if (!Assimilating)
            {
                //send reject so that the sender's state can be fixed
                var reject = new PeeringResponse
                {
                    ReqHash = UnsafeByteOperations.UnsafeWrap(
                        CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                    Status = false
                };

                var remote = IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData);
                if ((sent = await Router.SendMessageAsync(reject.ToByteString(), CcDiscoveries.MessageTypes.PeeringResponse, remote).FastPath().ConfigureAwait(Zc)) > 0)
                {
                    _logger.Trace($"-/> {nameof(PeeringResponse)}({sent}): Sent {(reject.Status ? "ACCEPT" : "REJECT")}, {Description}");
                }
                else
                    _logger.Debug($"<\\- {nameof(PeeringRequest)}({sent}): [FAILED], {Description}, {MetaDesc}");

                //DMZ-syn
                if (!Collected) return;
                
                //We syn here (Instead of in process ping) to force the other party to do some work (prepare to connect) before we do work (verify).
                if (await Router
                    .SendPingAsync(remote, CcDesignation.FromPubKey(packet.PublicKey.Memory).IdString())
                    .FastPath().ConfigureAwait(Zc))
                    _logger.Trace($"-/> {nameof(PeeringRequest)}: [[DMZ/SYN]] => {extraData}");
                
                return;
            }

            //Only one at a time

            if(State != AdjunctState.Verified)
                return;

            //PAT
            LastPat = 0;

            var peeringResponse = new PeeringResponse
            {
                ReqHash = UnsafeByteOperations.UnsafeWrap(
                    CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                Status = CcCollective.IngressCount < CcCollective.parm_max_inbound & _direction == 0
            };

            if ((sent= await SendMessageAsync(peeringResponse.ToByteString(),
                CcDiscoveries.MessageTypes.PeeringResponse).FastPath().ConfigureAwait(Zc)) > 0)
            {
                if (peeringResponse.Status)
                {
                    AdjunctState oldState;
                    if ((oldState = CompareAndEnterState(AdjunctState.Peering, AdjunctState.Verified)) != AdjunctState.Verified)
                    {
                        _logger.Warn($"{Description}: Invalid state, {oldState}. Wanted {nameof(AdjunctState.Verified)}");
                        peeringResponse.Status = false;
                    }
                }

                var response = $"{(peeringResponse.Status ? "accept" : "reject")}";
                _logger.Trace($"-/> {nameof(PeeringResponse)}({sent}): Sent {response}, {Description}");

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
                _logger.Debug($"<\\- {nameof(PeeringRequest)}: [FAILED], {Description}, {MetaDesc}");
        }

        /// <summary>
        /// Peer response message from client
        /// </summary>
        /// <param name="response">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(PeeringResponse response, object extraData, Packet packet)
        {
            //Validate
            if (!Assimilating)
            {
                return;
            }

            var peerRequest = await _peerRequest.ResponseAsync(extraData.ToString(), response.ReqHash).FastPath().ConfigureAwait(Zc);
            if (!peerRequest)
            {
                if (Collected)
                    _logger.Debug(
                        $"<\\- {nameof(PeeringResponse)}{response.ToByteArray().PayloadSig()}: No-Hash {extraData}, {RemoteAddress}, r = {response.ReqHash.Memory.HashSig()}, _peerRequest = {_peerRequest.Count}");
                return;
            }
            
            //drop old requests
            //if (peerRequest.TimestampMs.ElapsedMs() > parm_max_network_latency * 2)
            //    return;

            //Validated
            _logger.Trace($"<\\- {nameof(PeeringResponse)}: Accepted = {response.Status}, {Description}");

            //PAT
            LastPat = 0;

            switch (response.Status)
            {
                //Race for 
                case true when _direction == 0:
                {
                    
                    //await Task.Delay(@this._random.Next(@this.parm_max_network_latency) + @this.parm_max_network_latency/2, @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);
                    var connectionTime = Stopwatch.StartNew();
                    if (!await ConnectAsync().FastPath().ConfigureAwait(Zc))
                    {
                        AdjunctState oldState;
                        if ((oldState = CompareAndEnterState(AdjunctState.Verified, AdjunctState.Peering)) != AdjunctState.Peering)
                            _logger.Warn($"{nameof(PeeringResponse)} - {Description}: Invalid state, {oldState}. Wanted {nameof(AdjunctState.Peering)}");
                        
                        _logger.Trace($"{nameof(PeeringResponse)}: FAILED to connect to {Description}");
                        return;
                    }
                    break;
                }
                //at least probe
                case false when Hub.Neighbors.Count <= CcCollective.MaxAdjuncts:
                {

                    AdjunctState oldState;
                    if ((oldState = CompareAndEnterState(AdjunctState.Verified, AdjunctState.Peering)) != AdjunctState.Peering)
                        _logger.Warn($"{nameof(PeeringResponse)} - {Description}: Invalid state, {oldState}. Wanted {nameof(AdjunctState.Peering)}");

                    if (_peerRequestsSentCount > 1)
                    {
                        await ZeroAsync(static async @this =>
                        {
                            if (!await @this.SendDiscoveryRequestAsync().FastPath().ConfigureAwait(@this.Zc))
                                @this._logger.Debug($"{nameof(PeeringResponse)} - {@this.Description}: {nameof(SendDiscoveryRequestAsync)} did not execute...");

                        }, this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach | TaskCreationOptions.PreferFairness).FastPath().ConfigureAwait(Zc);
                    }

                    break;
                }
                case false:
                {
                    AdjunctState oldState;
                    if ((oldState = CompareAndEnterState(AdjunctState.Verified, AdjunctState.Peering)) != AdjunctState.Peering)
                        _logger.Warn($"{nameof(PeeringResponse)}{Description}: Invalid state, {oldState}. Wanted {nameof(AdjunctState.Peering)}");
                    
                    break;
                }
            }
        }

        public static long ConnectionTime;
        public static long ConnectionCount;

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
                    _logger.Debug(
                        $"{nameof(DiscoveryResponse)}{response.ToByteArray().PayloadSig()}: Reject, rq = {response.ReqHash.Memory.HashSig()}, {response.Peers.Count} > {parm_max_discovery_peers}? from {MakeId(CcDesignation.FromPubKey(packet.PublicKey.Memory), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData))}, RemoteAddress = {RemoteAddress}, c = {_discoveryRequest.Count}, matched[{extraData}]");
                return;
            }

            //if (discoveryRequest.TimestampMs.ElapsedMs() > parm_max_network_latency * 2)
            //{
            //    return;
            //}

            var count = 0;

            _logger.Trace(
                $"<\\- {nameof(DiscoveryResponse)}: Received {response.Peers.Count} potentials from {Description}");

            //PAT
            LastPat = 0;

            foreach (var responsePeer in response.Peers)
            {
                //take only one
                if (Hub.Neighbors.Count > CcCollective.MaxAdjuncts && count > parm_min_spare_bays)
                    break;

                //Any services attached?
                if (responsePeer.Services?.Map == null || responsePeer.Services.Map.Count == 0)
                {
                    _logger.Trace(
                        $"<\\- {nameof(DiscoveryResponse)}: Invalid services recieved!, map = {responsePeer.Services?.Map}, count = {responsePeer.Services?.Map?.Count ?? -1}");
                    continue;
                }

                //ignore strange services
                if (responsePeer.Services.Map.Count > parm_max_services)
                    continue;

                //Never add ourselves (by NAT)
                if (responsePeer.Services.Map.ContainsKey(CcService.Keys.peering.ToString()) &&
                    responsePeer.Ip == CcCollective.ExtAddress.Ip &&
                    CcCollective.ExtAddress.Port == responsePeer.Services.Map[CcService.Keys.peering.ToString()].Port)
                    continue;

                //Never add ourselves (by ID)
                if (responsePeer.PublicKey.SequenceEqual(CcCollective.CcId.PublicKey))
                    continue;

                //Don't add already known neighbors
                var id = CcDesignation.FromPubKey(responsePeer.PublicKey.Memory);
                if (Hub.Neighbors.Values.Any(n => ((CcAdjunct) n).Designation?.Equals(id) ?? false))
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
                if (services == null || services.CcRecord.Endpoints.Count == 0)
                    continue;

                await ZeroAsync(static async state =>
                {
                    var (@this, newRemoteEp, id, services) = state;
                    await Task.Delay(@this._random.Next(@this.parm_max_network_latency) + @this.parm_max_network_latency, @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);

                    if (!await @this.CollectAsync(newRemoteEp, id, services).FastPath().ConfigureAwait(@this.Zc))
                    {
#if DEBUG
                        @this._logger.Error($"{@this.Description}: Collecting {newRemoteEp.Address} failed!");
#else
                        @this._logger.Debug($"{@this.Description}: Collecting {newRemoteEp.Address} failed!");
#endif
                    }
                        
                }, ValueTuple.Create(this,newRemoteEp, id, services), TaskCreationOptions.LongRunning|TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(Zc);
                
                count++;
            }
            
        }
        
        /// <summary>
        /// Collect another drone into the collective
        /// </summary>
        /// <param name="newRemoteEp">The location</param>
        /// <param name="id">The adjunctId</param>
        /// <param name="services">The adjunct services</param>
        /// <returns>A task, true if successful, false otherwise</returns>
        private async ValueTask<bool> CollectAsync(IPEndPoint newRemoteEp, CcDesignation id, CcService services)
        {
            if (newRemoteEp != null && newRemoteEp.Equals(NatAddress?.IpEndPoint))
            {
                _logger.Fatal($"x {Description}");
                return false;
            }
            
            CcAdjunct newAdjunct = null;

            //var source = new IoUdpClient<CcProtocMessage<Packet, CcDiscoveryBatch>>($"UDP Proxy ~> {Description}",MessageService, newRemoteEp);
            newAdjunct = (CcAdjunct) Hub.MallocNeighbor(Hub, MessageService, Tuple.Create(id, services, newRemoteEp));

            if (!Zeroed() && await Hub.ZeroAtomicAsync(static async (s, state, ___) =>
            {
                var (@this, newAdjunct) = state;
                try
                {
                    if (@this.Hub.Neighbors.Count + 1 > @this.CcCollective.MaxAdjuncts)
                    {
                        //drop something
                        var bad = @this.Hub.Neighbors.Values.Where(n =>
                            ((CcAdjunct) n).Proxy &&
                            ((CcAdjunct) n).Direction == Heading.Undefined &&
                            ((CcAdjunct) n).Uptime.ElapsedMs() > @this.parm_max_network_latency * 2 &&
                            ((CcAdjunct) n).State < AdjunctState.Verified)
                            .OrderByDescending(n => ((CcAdjunct)n).Uptime.ElapsedMs());

                        var good = @this.Hub.Neighbors.Values.Where(n =>
                                ((CcAdjunct)n).Proxy &&
                                ((CcAdjunct)n).Direction == Heading.Undefined &&
                                ((CcAdjunct)n).Uptime.ElapsedMs() > @this.parm_max_network_latency * 2 &&
                                ((CcAdjunct)n).State < AdjunctState.Peering &&
                                ((CcAdjunct)n).TotalPats > @this.parm_min_pats_before_shuffle)
                            .OrderByDescending(n => ((CcAdjunct)n).Priority)
                            .ThenByDescending(n => ((CcAdjunct)n).Uptime.ElapsedMs());

                        var badList = bad.ToList();
                        if (badList.Any())
                        {
                            var dropped = badList.FirstOrDefault();
                            if (dropped != default) 
                            {
                                await ((CcAdjunct)dropped).ZeroAsync(new IoNanoprobe("got collected")).FastPath().ConfigureAwait(@this.Zc);
                                @this._logger.Debug($"~ {dropped.Description}");
                            }
                        }
                        else //try harder 
                        {
                            var dropped = good.FirstOrDefault();
                            if (dropped != default && ((CcAdjunct)dropped).State < AdjunctState.Peering)
                            {
                                await ((CcAdjunct)dropped).ZeroAsync(new IoNanoprobe("Assimilated!")).FastPath().ConfigureAwait(@this.Zc);
                                @this._logger.Info($"@ {dropped.Description}");
                            }
                        }
                    }
                    
                    //Transfer?
                    if (!@this.Hub.Neighbors.TryAdd(newAdjunct.Key, newAdjunct))
                    {
                        await newAdjunct.ZeroAsync(@this).FastPath().ConfigureAwait(@this.Zc);
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
            }
                , ValueTuple.Create(this, newAdjunct)).FastPath().ConfigureAwait(Zc))
            {
                //setup conduits to messages
                newAdjunct.ExtGossipAddress = ExtGossipAddress;

                var sub = await newAdjunct.ZeroSubAsync(static async (from, state) =>
                {
                    var (@this, newAdjunct) = state;
                    try
                    {
                        if (@this.Hub.Neighbors.TryRemove(newAdjunct.Key, out var n))
                        {
                            await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                            {
                                EventType = AutoPeerEventType.RemoveAdjunct,
                                Adjunct = new Adjunct
                                {
                                    CollectiveId = @this.CcCollective.Hub.Router.Designation.IdString(),
                                    Id = newAdjunct.Designation.IdString(),
                                }
                            }).FastPath().ConfigureAwait(@this.Zc);

                            if (newAdjunct != n)
                            {
                                @this._logger.Fatal($"{@this.Description}: Removing incorrect id = {n.Key}, wanted = {newAdjunct.Key}");
                            }
                            //n.ZeroedFrom ??= @from;
                            if (((CcAdjunct) n).Assimilated)
                            {
                                @this._logger.Debug($"% {n.Description} - from: {@from?.Description}");
                            }
                        }

                        //MessageService.WhiteList(__newNeighbor.RemoteAddress.Port);
                    }
                    catch
                    {
                        // ignored
                    }

                    return true;
                },ValueTuple.Create(this,newAdjunct)).FastPath().ConfigureAwait(Zc);

                if (sub == null)
                {
                    _logger.Fatal($"Could not subscribe to zero: {Description}");
                    return false;
                }
                
                _logger.Debug($"# {newAdjunct.Description}");

                //emit event
                await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                {
                    EventType = AutoPeerEventType.AddAdjunct,
                    Adjunct = new Adjunct
                    {
                        Id = newAdjunct.Designation.IdString(),
                        CollectiveId = CcCollective.Hub.Router.Designation.IdString()
                    }
                }).FastPath().ConfigureAwait(Zc);

                //Start processing
                await ZeroAsync(static async state =>
                {
                    var (@this, newAdjunct) = state;
                    await @this.CcCollective.Hub.BlockOnAssimilateAsync(newAdjunct).FastPath().ConfigureAwait(@this.Zc);
                }, ValueTuple.Create(this, newAdjunct), TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(false);

                
                if (!await newAdjunct.SendPingAsync().FastPath().ConfigureAwait(Zc))
                {
                    _logger.Debug($"{newAdjunct.Description}: Unable to send ping!!!");
                    return false;
                }

                return true;
            }
            else
            {
                if (newAdjunct != null)
                    await newAdjunct.ZeroAsync(new IoNanoprobe("CollectAsync")).FastPath().ConfigureAwait(Zc);
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
        private async ValueTask ProcessAsync(DiscoveryRequest request, object extraData, Packet packet)
        {
            if (!Assimilating || request.Timestamp.CurrentMsDelta() > parm_max_network_latency * 2)
            {
                if (Assimilating)
                {
                    _logger.Trace(
                        $"<\\- {nameof(DiscoveryRequest)}: [ABORTED] {!Assimilating}, age = {request.Timestamp.CurrentMsDelta():0.0}ms/{parm_max_network_latency * 2:0.0}ms, {Description}, {MetaDesc}");
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
            var certified = Hub.Neighbors.Values.Where(n =>
                    ((CcAdjunct) n).Assimilating && n != this)
                .OrderBy(n => (int) ((CcAdjunct) n).Priority)
                .ThenBy(n => ((CcAdjunct)n).Uptime.ElapsedMs()).ToList();
            foreach (var ioNeighbor in certified.Take((int)parm_max_discovery_peers))
            {
                discoveryResponse.Peers.Add(new Peer
                {
                    PublicKey = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(((CcAdjunct) ioNeighbor).Designation.PublicKey)),
                    Services = ((CcAdjunct) ioNeighbor).ServiceMap,
                    Ip = ((CcAdjunct) ioNeighbor).RemoteAddress.Ip
                });
                count++;
            }

            
            int sent;
            if ((sent = await SendMessageAsync(discoveryResponse.ToByteString(),
                    CcDiscoveries.MessageTypes.DiscoveryResponse)
                .FastPath().ConfigureAwait(Zc)) > 0)
            {
                _logger.Trace($"-/> {nameof(DiscoveryResponse)}({sent}): Sent {count} discoveries to {Description}");

                //Emit message event
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
            var remoteEp = (IPEndPoint) extraData;
            
            //Drop old API calls
            if(ping.NetworkId != parm_network_id || ping.Version != parm_protocol_version)
                return;

            //Drop old messages
            if (ping.Timestamp.ElapsedMs() > parm_max_network_latency * 2) //TODO params
            {
                _logger.Error($"<\\- {nameof(Ping)}: [WARN] Dropped stale, age = {ping.Timestamp.ElapsedMs()}ms");
                return;
            }

            Pong pong;
            if (_serviceMapLocal == null)
            {
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
                    Services =_serviceMapLocal = new ServiceMap
                    {
                        Map =
                        {
                            {
                                CcService.Keys.peering.ToString(),
                                new NetworkAddress {Network = "udp", Port = (uint) peeringAddress.Port}
                            },
                            {
                                CcService.Keys.gossip.ToString(),
                                new NetworkAddress {Network = "tcp", Port = (uint) gossipAddress.Port}
                            },
                            {
                                CcService.Keys.fpc.ToString(),
                                new NetworkAddress {Network = "tcp", Port = (uint) fpcAddress.Port}
                            }
                        }
                    }
                };    
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
            
            var toAddress = IoNodeAddress.CreateFromEndpoint("udp", remoteEp);
            //PROCESS DMZ/SYNs
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
                int sent;
                if ((sent = await SendMessageAsync(pong.ToByteString(), CcDiscoveries.MessageTypes.Pong, toAddress)
                    .FastPath().ConfigureAwait(Zc)) > 0)
                {
#if DEBUG
                    _logger.Trace($"-/> {nameof(Pong)}({sent})[{pong.ToByteString().Memory.PayloadSig()} ~ {pong.ReqHash.Memory.HashSig()}]: Sent [[SYN-ACK]], [{MessageService.IoNetSocket.LocalAddress} ~> {toAddress}]");
#endif

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
                    if ((sent = await SendMessageAsync(pong.ToByteString(), CcDiscoveries.MessageTypes.Pong, toAddress)
                        .FastPath().ConfigureAwait(Zc)) > 0)
                    {
#if DEBUG
                        _logger.Trace($"-/> {nameof(Pong)}({sent})[{pong.ToByteString().Memory.PayloadSig()} ~ {pong.ReqHash.Memory.HashSig()}]: Sent [[SYN-ACK]], [{MessageService.IoNetSocket.LocalAddress} ~> {toAddress}]");
#endif
                    }
                }
            }
            else //PROCESS ACK
            {
                LastPat = 0;

                var sent = 0;
                if ((sent = await SendMessageAsync(data: pong.ToByteString(), type: CcDiscoveries.MessageTypes.Pong)
                    .FastPath().ConfigureAwait(Zc)) > 0)
                {
#if DEBUG
                    if (IsDroneConnected)
                        _logger.Trace($"-/> {nameof(Pong)}({sent})[{pong.ToByteString().Memory.PayloadSig()} ~ {pong.ReqHash.Memory.HashSig()}]: Sent [[KEEPALIVE]], {Description}");
                    else
                        _logger.Trace($"-/> {nameof(Pong)}({sent})[{pong.ToByteString().Memory.PayloadSig()} ~ {pong.ReqHash.Memory.HashSig()}]: Sent [[ACK SYN]], {Description}");
#endif

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
                        _logger.Error($"-/> {nameof(Ping)}: [FAILED] Send [[ACK SYN/KEEPALIVE]], to = {Description}");
                }
            }
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
                if (Collected)
                {
                    _logger.Error($"<\\- {nameof(Pong)} {packet.Data.Memory.PayloadSig()}: SEC! {pong.ReqHash.Memory.HashSig()}, hash = {MemoryMarshal.Read<long>(packet.Data.ToByteArray())}, d = {_pingRequest.Count}, t = {TotalPats},  " +
                                  $"PK={Designation.PkString()} != {Base58.Bitcoin.Encode(packet.PublicKey.Span)} (proxy = {Proxy}),  ssp = {SecondsSincePat}, d = {(AttachTimestamp > 0 ? (AttachTimestamp - LastPat).ToString() : "N/A")}, v = {Verified}, s = {extraData}, {Description}");
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
                    await staleNeighbor.ZeroAsync(new IoNanoprobe($"{nameof(staleNeighbor)}")).FastPath().ConfigureAwait(Zc);
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
                        if (!await @this.CollectAsync(fromAddress.IpEndPoint, idCheck, remoteServices).FastPath()
                            .ConfigureAwait(@this.Zc))
                        {
#if DEBUG
                            @this._logger.Error($"{@this.Description}: Collecting {fromAddress.IpEndPoint} failed!");
#else
                            @this._logger.Debug($"{@this.Description}: Collecting {fromAddress.IpEndPoint} failed!");
#endif
                        }
                    }, ValueTuple.Create(this, fromAddress, remoteServices, idCheck), TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(Zc);
                }
            }
            else if (!Verified) //Process ACK SYN
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

                AdjunctState oldState;
                if ((oldState = CompareAndEnterState(AdjunctState.Verified, AdjunctState.Unverified)) != AdjunctState.Unverified)
                {
                    _logger.Warn($"{nameof(Pong)} - {Description}: Invalid state, {oldState}. Wanted {nameof(AdjunctState.Unverified)}");
                    return;
                }
                Thread.MemoryBarrier();
                Verified = true;

#if DEBUG
                _logger.Trace($"<\\- {nameof(Pong)}: <<ACK SYN>>: {Description}");
#endif

                if (!await SendPeerRequestAsync().FastPath().ConfigureAwait(Zc))
                {
#if DEBUG
                    _logger.Trace($"-/> {nameof(SendPeerRequestAsync)}: FAILED to send, {Description}");
#endif
                }
                    
            }
            else 
            {
                //PAT
                LastPat = 0;

                if (Volatile.Read(ref _viral).ElapsedMsToSec() >= parm_virality)
                {
                    //ensure egress
                    if (Direction == Heading.Undefined && CcCollective.EgressCount < CcCollective.parm_max_outbound && PeerRequestsRecvCount < parm_zombie_max_connection_attempts)
                    {
                        if (!await SendPeerRequestAsync().FastPath().ConfigureAwait(Zc))
                            _logger.Trace($"<\\- {nameof(SendPeerRequestAsync)} (acksyn-fast): [FAILED] Send Peer request, {Description}");
                        else
                        {
#if DEBUG
                            _logger.Trace($"-/> {nameof(Pong)} (acksyn-fast): Send Peer REQUEST), to = {Description}");
#endif
                        }
                    }

                    //ensure ingress
                    if (Direction == Heading.Undefined && CcCollective.IngressCount < CcCollective.parm_max_inbound && PeerRequestsSentCount < parm_zombie_max_connection_attempts)
                    {
                        if (!await SendPeerDropAsync().FastPath().ConfigureAwait(Zc))
                            _logger.Trace($"<\\- {nameof(SendPeerDropAsync)}(acksyn-rst): [FAILED ]Send Peer HUP");
                        else
                        {
#if DEBUG
                            _logger.Trace($"-/> {nameof(Pong)}(acksyn-rst): Send Peer HUP to = {Description}");
#endif
                        }
                    }

                    Volatile.Write(ref _viral, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                }
            }
        }

        /// <summary>
        /// Sends a universal ping packet
        /// </summary>
        /// <param name="dest">The destination address</param>
        /// <param name="id">Optional Id of the node pinged</param>
        /// <returns>Task</returns>
        public async ValueTask<bool> SendPingAsync(IoNodeAddress dest = null, string id = null)
        {
            try
            {
                //Check for teardown
                if (!Collected)
                    return false;

                dest ??= RemoteAddress;

                //Create the ping request
                var pingRequest = new Ping
                {
                    DstAddr = dest.IpEndPoint.Address.ToString(),
                    NetworkId = parm_network_id,
                    Version = parm_protocol_version,
                    SrcAddr = "0.0.0.0",
                    SrcPort = (uint) CcCollective.Services.CcRecord.Endpoints[CcService.Keys.peering].Port,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                };

                var reqBuf = pingRequest.ToByteString();

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
                        _logger.Trace($"-/> {nameof(Ping)}({sent})[{reqBuf.Span.PayloadSig()}, {challenge.Value.Payload.Memory.PayloadSig()}]: {Description}");
#endif

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
                    var router = Hub.Router;

                    IoQueue<IoZeroMatcher<ByteString>.IoChallenge>.IoZNode challenge;
                    if ((challenge = await router._pingRequest.ChallengeAsync(dest.IpPort, reqBuf).FastPath().ConfigureAwait(Zc)) == null)
                        return false;

                    var sent = await SendMessageAsync(reqBuf, CcDiscoveries.MessageTypes.Ping, dest)
                        .FastPath().ConfigureAwait(Zc);

                    if (sent > 0)
                    {
#if DEBUG
                        _logger.Trace($"-/> {nameof(Ping)}({sent})[{reqBuf.Span.PayloadSig()}]: dest = {dest}, {Description}");
#endif
                        await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = id,
                                Type = "ping"
                            }
                        }).FastPath().ConfigureAwait(Zc);

                        return true;
                    }

                    await router._pingRequest.RemoveAsync(challenge).FastPath().ConfigureAwait(Zc);
                    if(Collected)
                        _logger.Trace($"-/> {nameof(Ping)}:(X) [FAILED], {Description}");
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

        private long _lastScan = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 1000;

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
                    _logger.Error($"{nameof(SendDiscoveryRequestAsync)}: [ABORTED], {Description}, s = {State}, a = {Assimilating}");
                    return false;
                }

                //rate limit
                //if (LastScan.Elapsed() < parm_max_network_latency / 1000)
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
                    Interlocked.Increment(ref _peerRequestsSentCount);
                    _logger.Trace(
                        $"-/> {nameof(DiscoveryRequest)}({sent}){reqBuf.Memory.PayloadSig()}: Sent, {Description}");

                    //Emit message event
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
            try
            {
                if (!Assimilating || IsDroneConnected)
                {
                    if (Collected)
                        _logger.Warn(
                            $"{nameof(SendPeerRequestAsync)}: [ABORTED], {Description}, s = {State}, a = {Assimilating}, p = {IsDroneConnected}");
                    return false;
                }

                AdjunctState oldState;
                if ((oldState = CompareAndEnterState(AdjunctState.Peering, AdjunctState.Verified)) != AdjunctState.Verified)
                {
                    _logger.Warn($"{nameof(SendPeerRequestAsync)} - {Description}: Invalid state, {oldState}. Wanted {nameof(AdjunctState.Verified)}");
                    return false;
                }
                
                var peerRequest = new PeeringRequest
                {
                    Salt = new Salt
                        {ExpTime = (ulong) DateTimeOffset.UtcNow.AddHours(2).ToUnixTimeSeconds(), Bytes = GetSalt},
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                };

                var peerRequestRaw = peerRequest.ToByteString();

                IoQueue<IoZeroMatcher<ByteString>.IoChallenge>.IoZNode challenge;
                if ((challenge = await _peerRequest.ChallengeAsync(RemoteAddress.IpPort, peerRequestRaw).FastPath().ConfigureAwait(Zc)) == null)
                {
                    return false;
                }
                
                var sent = await SendMessageAsync(peerRequestRaw, CcDiscoveries.MessageTypes.PeeringRequest).FastPath().ConfigureAwait(Zc);
                if (sent > 0)
                {
                    Interlocked.Increment(ref _peerRequestsSentCount);
                    _logger.Trace(
                        $"-/> {nameof(PeeringRequest)}({sent})[{peerRequestRaw.Memory.PayloadSig()}]: Sent, {Description}");

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

                    return true;
                }

                await _peerRequest.RemoveAsync(challenge).FastPath().ConfigureAwait(Zc);

                if (Collected)
                    _logger.Debug($"-/> {nameof(PeeringRequest)}: [FAILED], {Description}, {MetaDesc}");
            }
            catch when(Zeroed()){}
            catch (Exception e)when (!Zeroed())
            {
                _logger.Debug(e, $"{nameof(PeeringRequest)}: [FAILED], {Description}, {MetaDesc}");
            }
            
            return false;
        }

        /// <summary>
        /// Tell peer to drop us when things go wrong. (why or when? cause it wont reconnect otherwise. This is a bug)
        /// </summary>
        /// <returns></returns>
        private async ValueTask<bool> SendPeerDropAsync(IoNodeAddress dest = null)
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
            AdjunctState oldState;
            try
            {
                //raced?
                if (IsDroneAttached)
                {
                    if ((oldState = CompareAndEnterState(AdjunctState.Verified, (AdjunctState)_currState.Value)) != AdjunctState.Peering)
                        _logger.Warn($"{nameof(AttachPeerAsync)} - {Description}:1 Invalid state, {oldState}. Wanted {nameof(AdjunctState.Peering)}");
                    
                    return false;
                }

                if (!await ZeroAtomicAsync( static (_, state, _) =>
                {
                    var (@this, ioCcDrone, direction) = state;

                    //Guarantee hard cap on allowed drones here, other implemented caps are soft caps. This is the only one that matters
                    if (direction == Heading.Ingress && @this.CcCollective.IngressCount >= @this.CcCollective.parm_max_inbound || 
                        direction == Heading.Egress && @this.CcCollective.EgressCount >= @this.CcCollective.parm_max_outbound)
                    {
                        AdjunctState oldState;
                        if ((oldState = @this.CompareAndEnterState(AdjunctState.Verified, AdjunctState.Peering)) != AdjunctState.Peering)
                            @this._logger.Warn($"{nameof(AttachPeerAsync)} - {@this.Description}:2 Invalid state, {oldState}. Wanted {nameof(AdjunctState.Peering)}");

                        return new ValueTask<bool>(false);
                    }

                    //Race for direction
                    if (Interlocked.CompareExchange(ref @this._direction, (int) direction, (int) Heading.Undefined) !=
                        (int) Heading.Undefined)
                    {
                        @this._logger.Warn(
                            $"oz: race for {direction} lost {ioCcDrone.Description}, current = {@this.Direction}, {@this._drone?.Description}");

                        AdjunctState oldState;
                        if ((oldState = @this.CompareAndEnterState(AdjunctState.Verified, AdjunctState.Peering)) != AdjunctState.Peering)
                            @this._logger.Warn($"{nameof(AttachPeerAsync)} - {@this.Description}:3 Invalid state, {oldState}. Wanted {nameof(AdjunctState.Peering)}");

                        return new ValueTask<bool>(false);
                    }
                    
                    @this._drone = ioCcDrone ?? throw new ArgumentNullException($"{nameof(ioCcDrone)}");
                    
                    @this.WasAttached = true;

                    return new ValueTask<bool>(@this.CompareAndEnterState(AdjunctState.Connected, AdjunctState.Peering) == AdjunctState.Peering);
                }, ValueTuple.Create(this, ccDrone, direction)))
                {
                    return false;
                }

                _logger.Trace($"{nameof(AttachPeerAsync)}: [WON] {_drone?.Description}");
                
                Assimilated = true;
                AttachTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                //emit event
                await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                {
                    EventType = AutoPeerEventType.AddDrone,
                    Drone = new Drone
                    {
                        CollectiveId = CcCollective.Hub.Router.Designation.IdString(),
                        Adjunct = ccDrone.Adjunct.Designation.IdString(),
                        Direction = ccDrone.Adjunct.Direction.ToString(),
                    }
                }).FastPath().ConfigureAwait(Zc);

                await SendDiscoveryRequestAsync().FastPath().ConfigureAwait(Zc);

                return true;
            }
            catch when (!Zeroed()){}
            catch (Exception e) when(Zeroed())
            {
                _logger.Trace(e, Description);
            }

            if ((oldState = CompareAndEnterState(AdjunctState.Verified, AdjunctState.Peering)) != AdjunctState.Peering)
                _logger.Warn($"{nameof(AttachPeerAsync)} - {Description}:5 Invalid state, {oldState}. Wanted {nameof(AdjunctState.Peering)}");
            return false;
        }

        /// <summary>
        /// If a connection was ever made
        /// </summary>
        public bool WasAttached { get; protected set; }

        /// <summary>
        /// Detaches a peer from this neighbor
        /// </summary>
        public async ValueTask DetachPeerAsync()
        {
            var severedDrone = Interlocked.CompareExchange(ref _drone, null, _drone);

            if(severedDrone == null)
                return;

            var direction = Direction;

            await severedDrone.ZeroAsync(this).FastPath().ConfigureAwait(Zc);
            
            Interlocked.Exchange(ref _direction, 0);
            AttachTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            Interlocked.Exchange(ref _totalPats, 0);
            _peerRequestsRecvCount = _peerRequestsSentCount = 0;

            //send drop request
            await SendPeerDropAsync().FastPath().ConfigureAwait(Zc);

            if (direction == Heading.Ingress && CcCollective.IngressCount < CcCollective.parm_max_inbound)
            {
                //back off for a while... Try to re-establish a link 
                await ZeroAsync(static async @this =>
                {
                    await Task.Delay(@this.parm_max_network_latency + @this._random.Next(@this.parm_max_network_latency), @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);
                    await @this.SendPingAsync().FastPath().ConfigureAwait(@this.Zc);
                }, this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach | TaskCreationOptions.PreferFairness).FastPath().ConfigureAwait(Zc);
            }


            //emit event
            await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
            {
                EventType = AutoPeerEventType.RemoveDrone,
                Drone = new Drone
                {   
                    CollectiveId = CcCollective.Hub.Router.Designation.IdString(),
                    Adjunct = Designation.IdString()
                }
            }).FastPath().ConfigureAwait(Zc);

            SetState(AdjunctState.Verified);
        }


        /// <summary>
        /// The state transition history, sourced from <see  cref="IoZero{TJob}"/>
        /// </summary>
#if DEBUG
        public IoStateTransition<AdjunctState>[] StateTransitionHistory =
            new IoStateTransition<AdjunctState>[Enum.GetNames(typeof(AdjunctState))
                .Length]; //TODO what should this size be?
#else
        public IoStateTransition<IoJobMeta.JobState>[] StateTransitionHistory;
#endif


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AdjunctState SetState(AdjunctState state)
        {
            return CompareAndEnterState(state, default, compare: false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AdjunctState CompareAndEnterState(AdjunctState state, AdjunctState cmp, bool compare = true)
        {
            
#if DEBUG
            var nextState = new IoStateTransition<AdjunctState>((int)state);
            var matched = AdjunctState.Sentinel;
            if (compare)
            {
                if((matched = (AdjunctState)nextState.CompareAndEnterState((int)state, (int)cmp, _currState)) != cmp)
                    return AdjunctState.Sentinel;
            }

            var prevState = _currState;
            _currState = _currState.Exit(nextState);

            if (prevState != null)
            {
                if (StateTransitionHistory[prevState!.Value] != null)
                {
                    var c = StateTransitionHistory[prevState.Value];

                    while (c.Repeat != null)
                        c = c.Repeat;

                    c.Repeat = prevState;
                }
                else
                    StateTransitionHistory[prevState.Value] = prevState;
            }

            return matched;
#else
            if (compare)
            {
                return (AdjunctState)_currState.CompareAndEnterState((int)state, (int)cmp, _currState);
            }
            else
            {
                _currState.Set((int)state);
                return AdjunctState.Sentinel;
            }
#endif
        }

        /// <summary>
        /// Print state history
        /// </summary>
        void PrintStateHistory()
        {
            var c = StateTransitionHistory[0];
            var i = 0;
            while (c != null)
            {
                Console.WriteLine($"{i++} ~> {(AdjunctState)c.Value}");
                c = c.Next;
            }
        }

        /// <summary>
        /// Gets and sets the state of the work
        /// </summary>
        public AdjunctState State
        {
            get => (AdjunctState)_currState.Value;
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