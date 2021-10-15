//#define LOSS
using System;
using System.Buffers;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
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
using zero.core.models.protobuffer.sources;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using Logger = NLog.Logger;

namespace zero.cocoon.autopeer
{
    /// <summary>
    /// Processes (UDP) discovery messages from the collective.
    /// </summary>
    public class CcAdjunct : IoNeighbor<CcProtocMessage<Packet, CcDiscoveryMessage>>
    {
        public CcAdjunct(CcHub node, IoNetClient<CcProtocMessage<Packet, CcDiscoveryMessage>> ioNetClient,
            object extraData = null, CcService services = null)
            : base
            (
                node,
                ioNetClient,
                _ => new CcDiscoveries("adjunct RX", $"{ioNetClient.Key}", ioNetClient), false
            )
        {
            _logger = LogManager.GetCurrentClassLogger();

            //TODO tuning
            _pingRequest = new IoZeroMatcher<ByteString>(nameof(_pingRequest), Source.ZeroConcurrencyLevel(), parm_max_network_latency * 20, CcCollective.MaxAdjuncts * 10);
            _peerRequest = new IoZeroMatcher<ByteString>(nameof(_peerRequest), Source.ZeroConcurrencyLevel(), parm_max_network_latency * 20, CcCollective.MaxAdjuncts * 10);
            _discoveryRequest = new IoZeroMatcher<ByteString>(nameof(_discoveryRequest), (int)(CcCollective.MaxAdjuncts * parm_max_discovery_peers + 1), parm_max_network_latency * 10, CcCollective.parm_max_adjunct);

            if (extraData != null)
            {
                var (item1, item2, item3) = (Tuple<CcDesignation, CcService, IPEndPoint>) extraData;
                Designation = item1;
                Services = services ?? item2;
                RemoteAddress = IoNodeAddress.CreateFromEndpoint("udp", item3);
                Key = MakeId(Designation, RemoteAddress);
            }
            else
            {
                Designation = CcCollective.CcId;
                Services = services ?? node.Services;
                Key = MakeId(Designation, CcCollective.ExtAddress);
            }

            if (Proxy)
            {
                State = AdjunctState.Unverified;
                var z = ZeroAsync(static async @this =>
                {
                    try
                    {
                        while (!@this.Zeroed())
                        {
                            var patTime = @this.IsDroneConnected ? @this.CcCollective.parm_mean_pat_delay * 2 : @this.CcCollective.parm_mean_pat_delay;
                            var targetDelay = @this._random.Next(patTime / 2 * 1000) + patTime * 1000 / 4;
                        
                            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                            await Task.Delay(targetDelay, @this.AsyncTasks.Token).ConfigureAwait(false);

                            if (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - ts > targetDelay * 1.1)
                            {
                                @this._logger.Warn($"{@this.Description}: Popdog is slow!!!, {(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - ts)/1000.0:0.0}s");
                            }

                            try
                            {
                                await @this.WatchdogAsync().FastPath().ConfigureAwait(false);
                            }
                            catch (Exception) when (@this.Zeroed()) { }
                            catch (Exception e) when (!@this.Zeroed())
                            {
                                if (@this.Collected)
                                    @this._logger.Fatal(e, $"{@this.Description}: Watchdog down!");
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
                    
                }, this,TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach | TaskCreationOptions.PreferFairness);
            }
            else
            {
                State = AdjunctState.Local;
                _routingTable = new CcAdjunct[IPEndPoint.MaxPort];
            }
        }

        public enum AdjunctState
        {
            Undefined = 0,
            FinalState = 1,
            ZeroState = 2,
            Zombie = 3,
            Local = 4,
            Unverified = 5,
            Verified = 6,
            Disconnected = 7,
            Standby = 8,
            Peering = 9,
            Reconnecting = 10,
            Connecting = 11,
            Connected = 12,
        }

        /// <summary>
        /// The current state
        /// </summary>
        private volatile IoStateTransition<AdjunctState> _currState = new()
        {
            FinalState = AdjunctState.FinalState
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
                        return _description = $"`adjunct ({(Verified ? "+v" : "-v")},{(WasAttached ? "C!" : "dc")})[{TotalPats}:{Priority}:{PeerRequests}:{PeeringAttempts}] local: {MessageService.IoNetSocket.LocalAddress} - {MessageService.IoNetSocket.RemoteAddress}, [{Hub?.Designation.IdString()}, {Designation.IdString()}]'";
                    else
                        return _description = $"`hub ({(Verified ? "+v" : "-v")},{(WasAttached ? "C!" : "dc")})[{TotalPats}:{Priority}:{PeerRequests}:{PeeringAttempts}] local: {MessageService.IoNetSocket.LocalAddress} - {MessageService.IoNetSocket.RemoteAddress}, [{Hub?.Designation?.IdString()}, {Designation.IdString()}]'";
                }
                catch (Exception)
                {
                    if(Proxy)
                        return _description?? $"`adjunct({(Verified ? "+v" : "-v")},{(WasAttached ? "C!" : "dc")})[{TotalPats}:{Priority}:{PeerRequests}:{PeeringAttempts}] local: {MessageService?.IoNetSocket?.LocalAddress} - {MessageService?.IoNetSocket?.RemoteAddress},' [{Hub?.Designation?.IdString()}, {Designation?.IdString()}]'";    
                    else
                        return _description = $"`hub ({(Verified ? "+v" : "-v")},{(WasAttached ? "C!" : "dc")})[{TotalPats}:{Priority}:{PeerRequests}:{PeeringAttempts}] local: {MessageService?.IoNetSocket?.LocalAddress} - {MessageService?.IoNetSocket?.RemoteAddress}, [{Hub?.Designation?.IdString()}, {Designation?.IdString()}]'";
                }
            }
        }

        /// <summary>
        /// return extra information about the state
        /// </summary>
        public string MetaDesc =>
            $"(d= {Direction}, s= {State}, v= {Verified}, a= {Assimilating}, att= {IsDroneAttached}, c= {IsDroneConnected}, r= {PeeringAttempts}, g= {IsGossiping}, arb= {IsArbitrating}, o= {MessageService.IsOperational}, w= {TotalPats})";

        /// <summary>
        /// Random number generator
        /// </summary>
        private readonly Random _random = new Random((int) DateTimeOffset.Now.Ticks);

        /// <summary>
        /// Discovery services
        /// </summary>
        protected internal CcHub Hub => (CcHub)Node;

        /// <summary>
        /// Source
        /// </summary>
        protected IoNetClient<CcProtocMessage<Packet, CcDiscoveryMessage>> MessageService => (IoNetClient<CcProtocMessage<Packet, CcDiscoveryMessage>>) Source;

        /// <summary>
        /// The udp routing table 
        /// </summary>
        private CcAdjunct[] _routingTable;

        /// <summary>
        /// Occupied routing slot
        /// </summary>
        private int _routingIndex;

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

        /// <summary>
        /// Indicates whether we have successfully established a connection before
        /// </summary>
        protected volatile uint PeeringAttempts;

        /// <summary>
        /// Number of peer requests
        /// </summary>
        public uint PeerRequests { get; protected set; }

        /// <summary>
        /// Node broadcast priority. 
        /// </summary>
        public long Priority =>
            Enum.GetNames(typeof(Heading)).Length - (long) Direction - 1 + PeerRequests - PeeringAttempts > 1 ? 1 : 0;

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
            [Description(" Egress")]
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
        private IoConduit<CcProtocBatch<Packet, CcDiscoveryMessage>> _protocolConduit;

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
            set => Interlocked.Exchange(ref _lastPat, value);
        }

        /// <summary>
        /// Seconds since last adjunct pat
        /// </summary>
        public long TotalPats
        {
            get => Interlocked.Read(ref _totalPats);
            set => Interlocked.Exchange(ref _totalPats, value);
        }

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
        /// Message heap
        /// </summary>
        public ArrayPool<CcDiscoveryMessage> ArrayPoolProxy { get; protected set; }
        
        /// <summary>
        /// Producer tasks
        /// </summary>
        private ValueTask<bool>[] _produceTaskPool;

        /// <summary>
        /// Consumer tasks
        /// </summary>
        private ValueTask<bool>[] _consumeTaskPool;

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
        public int parm_max_network_latency = 2000;

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
        public int parm_min_pats_before_shuffle = 3;

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
            ArrayPoolProxy = null;
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
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(false);

            if (Proxy)
            {
                try
                {
                    //Remove from routing table
                    if(_routingIndex > 0 && Interlocked.CompareExchange(ref Router._routingTable[_routingIndex], null,this) != this)
                    {
                        _logger.Warn($"Router reset [FAILED], value is {Router._routingTable[_routingIndex]}: {Description}");
                    }
                }
                catch
                {
                    // ignored
                }

                await DetachPeerAsync().FastPath().ConfigureAwait(false);
                
                await ZeroAsync(async @this =>
                {
                    await Task.Delay(parm_max_network_latency, AsyncTasks.Token);
                    await @this.Router.SendPingAsync(RemoteAddress).FastPath().ConfigureAwait(false);
                }, this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach | TaskCreationOptions.PreferFairness);

                if (Assimilated && Direction != Heading.Undefined && WasAttached)
                    _logger.Info($"- `{(Assimilated ? "apex" : "sub")} {Direction}: {Description}, from: {ZeroedFrom?.Description}");
            }
            else
            {
                _logger.Info($"~> {Description}, from: {ZeroedFrom?.Description}");
            }

            State = AdjunctState.ZeroState;
            
            await _pingRequest.ZeroAsync(this).FastPath().ConfigureAwait(false);
            await _peerRequest.ZeroAsync(this).FastPath().ConfigureAwait(false);
            await _discoveryRequest.ZeroAsync(this).FastPath().ConfigureAwait(false);
            
#if DEBUG
            Array.Clear(StateTransitionHistory, 0, StateTransitionHistory.Length);
#endif
        }

        /// <summary>
        /// Test mode
        /// </summary>
        private static uint _dropOne = 1;

        /// <summary>
        /// Ensures that the peer is running
        /// </summary>
        public async ValueTask WatchdogAsync()
        {
            // Verify request
            if (!Assimilating)
                return;
            
            try
            {
                await ZeroAsync(static async @this =>
                {
                    if (await @this.SendPingAsync().FastPath().ConfigureAwait(false))
                    {
                        @this._logger.Trace($"-/> {nameof(WatchdogAsync)}: PAT to = {@this.Description}");
                    }
                    else
                    {
                        if (@this.Collected)
                            @this._logger.Error($"-/> {nameof(SendPingAsync)}: PAT Send [FAILED], {@this.Description}, {@this.MetaDesc}");
                    }
                    
                    //Are we limping?
                    if (@this.Hub.Neighbors.Count <= 2)
                        await @this.CcCollective.DeepScanAsync().FastPath().ConfigureAwait(false);
                    
                }, this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach | TaskCreationOptions.PreferFairness).FastPath().ConfigureAwait(false);
            }
            catch when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"{Description}");
            }
            
            var threshold = IsDroneConnected ? CcCollective.parm_mean_pat_delay * 2 : CcCollective.parm_mean_pat_delay;
            threshold *= 2;
            //Watchdog failure
            if (SecondsSincePat > threshold || _dropOne == 0)
            {
                _dropOne = 1;

                if (TotalPats > 1)
                    _logger.Debug($"w {Description}");
                else
                    _logger.Trace($"w {Description}, s = {SecondsSincePat} >> {CcCollective.parm_mean_pat_delay}, {MetaDesc}");

                await ZeroAsync(static async collective =>
                {
                    await collective.DeepScanAsync().FastPath().ConfigureAwait(false);
                },CcCollective, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(false);
                
                await ZeroAsync(new IoNanoprobe($"-wd: l = {SecondsSincePat}s ago, uptime = {TimeSpan.FromMilliseconds(Uptime.ElapsedMs()).TotalHours:0.00}h")).FastPath().ConfigureAwait(false);
            }
        }
        
        /// <summary>
        /// Start processors for this neighbor
        /// </summary>
        /// <returns></returns>
        public override async Task AssimilateAsync()
        {
            try
            {
                if (Proxy)
                {
                    await ProcessMessagesAsync().ConfigureAwait(false);
                }
                else
                {
                    await ZeroAsync(static async @this =>
                    {
                        await @this.ProcessMessagesAsync().ConfigureAwait(false);
                    }, this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach /*CONFIRMED TUNE*/ | TaskCreationOptions.PreferFairness);
                
                    await base.AssimilateAsync().ConfigureAwait(false);
                }
            }
            catch(Exception e)
            {
                _logger.Error(e, $"{Description}: {nameof(AssimilateAsync)} Failed!");
            }
        }

        /// <summary>
        /// Connect to an adjunct, turning it into a drone for use
        /// </summary>
        /// <returns>True on success, false otherwise</returns>
        protected async ValueTask<bool> ConnectAsync()
        {
            //Validate request
            if (!Assimilating && !IsDroneConnected && State < AdjunctState.Verified && State >= AdjunctState.Standby)
            {
                _logger.Fatal($"{Description}: Connect aborted, wrong state: {MetaDesc}, ");
                return false;
            }

            State = Assimilated ? AdjunctState.Reconnecting : AdjunctState.Connecting;

            if (CcCollective.Neighbors.TryGetValue(Key, out var existingNeighbor))
            {
                if (existingNeighbor.Source.IsOperational)
                {
                    _logger.Warn("Drone was connected...");
                    return false;
                }
                await existingNeighbor.ZeroAsync(new IoNanoprobe("Dropped because reconnect?")).FastPath().ConfigureAwait(false);
            }
            
            await ZeroAsync(static async @this =>
            {
                //Attempt the connection, race to win
                if (!await @this.CcCollective.ConnectToDroneAsync(@this).FastPath().ConfigureAwait(false))
                    @this._logger.Trace($"{@this.Description}: Leashing adjunct failed!");

            }, this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach | TaskCreationOptions.PreferFairness).FastPath().ConfigureAwait(false);
            
            return true;
        }

        /// <summary>
        /// Processes protocol messages
        /// </summary>
        /// <param name="msg">The consumer that need processing</param>
        /// <param name="msgArbiter">The arbiter</param>
        /// <param name="processCallback">The process callback</param>
        /// <param name="nanite"></param>
        /// <returns>Task</returns>
        private async ValueTask ProcessMsgBatchAsync<T>(IoSink<CcProtocBatch<Packet, CcDiscoveryMessage>> msg,
            IoConduit<CcProtocBatch<Packet, CcDiscoveryMessage>> msgArbiter,
            Func<CcDiscoveryMessage, IoConduit<CcProtocBatch<Packet, CcDiscoveryMessage>>, T, ValueTask>
                processCallback, T nanite)
        {
            if (msg == null)
                return;

            //var stopwatch = Stopwatch.StartNew();
            CcDiscoveryMessage[] protocolMsgs = default;
            try
            {
                protocolMsgs = ((CcProtocBatch<Packet, CcDiscoveryMessage>) msg).Get();
                foreach (var message in protocolMsgs)
                {
                    if (message == default)
                        break;

                    try
                    {
                        await processCallback(message, msgArbiter, nanite).FastPath().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        if (Collected)
                            _logger.Debug(e, $"Processing protocol failed for {Description}: ");
                    }
                }
                
                msg.State = IoJobMeta.JobState.Consumed;

                //stopwatch.Stop();
                //_logger.Trace($"{(Proxy ? "V>" : "X>")} Processed `{protocolMsgs.Count}' consumer: t = `{stopwatch.ElapsedMilliseconds:D}', `{protocolMsgs.Count * 1000 / (stopwatch.ElapsedMilliseconds + 1):D} t/s'");
            }
            catch (Exception e)
            {
                if (Collected)
                    _logger.Debug(e, "Error processing message batch");
            }
            finally
            {
                if(protocolMsgs != null)
                    ArrayPoolProxy?.Return(protocolMsgs);
            }
        }
        
        /// <summary>
        /// Processes protocol messages
        /// </summary>
        /// <returns></returns>
        private async Task ProcessMessagesAsync()
        {
            try
            {
                //ensure the channel
                do
                {
                    if (_protocolConduit == null)
                    {
                        _protocolConduit = MessageService.GetConduit<CcProtocBatch<Packet, CcDiscoveryMessage>>(nameof(CcAdjunct));
                        if (_protocolConduit != null)
                        {
                            ArrayPoolProxy = ((CcProtocBatchSource<Packet, CcDiscoveryMessage>)_protocolConduit.Source).ArrayPool;
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
                        ArrayPoolProxy = ((CcProtocBatchSource<Packet, CcDiscoveryMessage>)_protocolConduit.Source).ArrayPool;
                        _produceTaskPool = new ValueTask<bool>[_protocolConduit.ZeroConcurrencyLevel()];
                        _consumeTaskPool = new ValueTask<bool>[_protocolConduit.ZeroConcurrencyLevel()];
                    }
                    
                    await Task.Delay(parm_conduit_spin_up_wait_time++, AsyncTasks.Token).ConfigureAwait(false);
                } while (_protocolConduit == null && !Zeroed());

            
                //The producer
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
                                    return;
                                }
                            }

                            for (var i = 0; i < @this._produceTaskPool.Length; i++)
                            {
                                if (!await @this._produceTaskPool[i].FastPath().ConfigureAwait(false))
                                    return;
                            }
                        }
                    }
                    catch when (@this.Zeroed() || @this.Source.Zeroed() || @this._protocolConduit?.Source == null) { }
                    catch (Exception e) when (!@this.Zeroed())
                    {
                        @this._logger?.Error(e, $"{@this.Description}");
                    }
                    
                },this, TaskCreationOptions.LongRunning | TaskCreationOptions.PreferFairness/*CONFIRMED TUNE*/);

                var consumer = ZeroOptionAsync(static async @this  =>
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
                                    @this._consumeTaskPool[i] = @this._protocolConduit.ConsumeAsync(static async (msg, @this) =>
                                    {
                                        try
                                        {
                                            await @this.ProcessMsgBatchAsync(msg, @this._protocolConduit,
                                                static async (msgBatch, forward, @this) =>
                                                {
                                                    IMessage message = default;
                                                    Packet packet = default;
                                                    try
                                                    {
                                                        message = msgBatch.EmbeddedMsg;
                                                        packet = msgBatch.Message;
                                                        
                                                        var extraData = IPEndPoint.Parse(msgBatch.RemoteEndPoint);
                                                        var routed = @this.Router._routingTable[extraData.Port] != null;

                                                        CcAdjunct ccNeighbor = null;
                                                        try
                                                        {
                                                            ccNeighbor =
                                                                @this.Router._routingTable[extraData.Port] ??
                                                                (CcAdjunct) @this.Hub.Neighbors.Values.FirstOrDefault(n =>
                                                                    ((IoNetClient<CcProtocMessage<Packet,
                                                                            CcDiscoveryMessage>>)
                                                                        ((CcAdjunct) n).Source).IoNetSocket
                                                                    .RemoteNodeAddress
                                                                    .Port == extraData.Port);
                                                        }
                                                        catch (Exception e)
                                                        {
                                                            if (!@this.Zeroed())
                                                                @this._logger.Trace(e, @this.Description);
                                                            return;
                                                        }

                                                        @this.Router._routingTable[extraData.Port] ??=
                                                            ccNeighbor;


                                                        ccNeighbor ??= @this.Hub.Router;

                                                        if (!routed && ccNeighbor.Proxy)
                                                            ccNeighbor._routingIndex = extraData.Port;

                                                        switch ((CcDiscoveries.MessageTypes) packet.Type)
                                                        {
                                                            case CcDiscoveries.MessageTypes.Ping:
                                                                await ccNeighbor.ProcessAsync((Ping) message, extraData,
                                                                        packet)
                                                                    .FastPath().ConfigureAwait(false);
                                                                break;
                                                            case CcDiscoveries.MessageTypes.Pong:
                                                                await ccNeighbor.ProcessAsync((Pong) message, extraData,
                                                                        packet)
                                                                    .FastPath().ConfigureAwait(false);
                                                                break;
                                                            case CcDiscoveries.MessageTypes.DiscoveryRequest:
                                                                await ccNeighbor
                                                                    .ProcessAsync((DiscoveryRequest) message, extraData,
                                                                        packet)
                                                                    .FastPath().ConfigureAwait(false);
                                                                break;
                                                            case CcDiscoveries.MessageTypes.DiscoveryResponse:
                                                                await ccNeighbor.ProcessAsync((DiscoveryResponse) message,
                                                                        extraData, packet)
                                                                    .FastPath().ConfigureAwait(false);
                                                                break;
                                                            case CcDiscoveries.MessageTypes.PeeringRequest:
                                                                await ccNeighbor
                                                                    .ProcessAsync((PeeringRequest) message, extraData,
                                                                        packet)
                                                                    .FastPath().ConfigureAwait(false);
                                                                break;
                                                            case CcDiscoveries.MessageTypes.PeeringResponse:
                                                                await ccNeighbor
                                                                    .ProcessAsync((PeeringResponse) message, extraData,
                                                                        packet)
                                                                    .FastPath().ConfigureAwait(false);
                                                                break;
                                                            case CcDiscoveries.MessageTypes.PeeringDrop:
                                                                await ccNeighbor
                                                                    .ProcessAsync((PeeringDrop) message, extraData, packet)
                                                                    .FastPath().ConfigureAwait(false);
                                                                break;
                                                        }
                                                    }
                                                    catch when(@this.Zeroed()){}
                                                    catch (Exception e) when (!@this.Zeroed())
                                                    {
                                                        @this._logger?.Debug(e, $"{message.GetType().Name} [FAILED]: l = {packet.Data.Length}, {@this.Key}");
                                                    }
                                                    finally
                                                    {
                                                        if(msgBatch !=  null)
                                                            await msgBatch.HeapRef.ReturnAsync(msgBatch).FastPath().ConfigureAwait(false);
                                                    }
                                                }, @this).FastPath().ConfigureAwait(false);
                                        }
                                        finally
                                        {
                                            if (msg != null && msg.State != IoJobMeta.JobState.Consumed)
                                                msg.State = IoJobMeta.JobState.ConsumeErr;
                                        }
                                    }, @this);
                                }
                                catch (Exception e)
                                {
                                    @this._logger?.Error(e, $"Consumption failed for {@this.Description}");
                                    return;
                                }
                            }

                            for (var i = 0; i < @this._consumeTaskPool.Length; i++)
                            {
                                if (!await @this._consumeTaskPool[i].FastPath().ConfigureAwait(false))
                                    return;
                            }
                        }
                    }
                    catch when(@this.Zeroed() || @this.Source.Zeroed() || @this._protocolConduit?.Source == null) {}
                    catch (Exception e) when (!@this.Zeroed())
                    {
                        @this._logger?.Error(e, $"{@this.Description}");
                    }
                }, this, TaskCreationOptions.LongRunning /*CONFIRMED TUNE*/| TaskCreationOptions.DenyChildAttach | TaskCreationOptions.PreferFairness);

                await Task.WhenAll(producer.AsTask(), consumer.AsTask());
            }
            catch when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger?.Debug(e, $"Error processing {Description}");
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
            LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            if(!IsDroneAttached)
                return;
            
            _logger.Trace($"{nameof(PeeringDrop)}: {Direction} Peer = {_drone.Key}: {Description}, {MetaDesc}");

            await DetachPeerAsync().FastPath().ConfigureAwait(false);
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

            PeerRequests++;

            //fail fast
            int sent;
            if (!Assimilating)
            {
                //var dmzSyn = Task.Factory.StartNew(async () =>
                {
                    //send reject so that the sender's state can be fixed
                    var reject = new PeeringResponse
                    {
                        ReqHash = UnsafeByteOperations.UnsafeWrap(
                            CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                        Status = false
                    };

                    var remote = IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData);
                    if ((sent = await Router.SendMessageAsync(reject.ToByteString(), CcDiscoveries.MessageTypes.PeeringResponse, remote).FastPath().ConfigureAwait(false)) > 0)
                    {
                        _logger.Trace(
                            $"-/> {nameof(PeeringResponse)}({sent}): Sent {(reject.Status ? "ACCEPT" : "REJECT")}, {Description}");
                    }
                    else
                        _logger.Debug($"<\\- {nameof(PeeringRequest)}({sent}): [FAILED], {Description}, {MetaDesc}");

                    //DMZ-syn
                    if (!Collected) return;
                    
                    //We syn here (Instead of in process ping) to force the other party to do some work (this) before we do work (verify).
                    if (await Router
                        .SendPingAsync(remote, CcDesignation.FromPubKey(packet.PublicKey.Memory).IdString())
                        .FastPath().ConfigureAwait(false))
                        _logger.Trace($"{nameof(PeeringRequest)}: DMZ/SYN => {extraData}");
                }
                //}, TaskCreationOptions.LongRunning);
                
                return;
            }

            //PAT
            LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            
            var peeringResponse = new PeeringResponse
            {
                ReqHash = UnsafeByteOperations.UnsafeWrap(
                    CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                Status = CcCollective.IngressConnections < CcCollective.parm_max_inbound & _direction == 0
            };

            if ((sent= await SendMessageAsync(peeringResponse.ToByteString(),
                CcDiscoveries.MessageTypes.PeeringResponse).FastPath().ConfigureAwait(false)) > 0)
            {
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
                }).FastPath().ConfigureAwait(false);
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

            var peerRequest = await _peerRequest.ResponseAsync(extraData.ToString(), response.ReqHash).FastPath().ConfigureAwait(false);
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
            LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            
            switch (response.Status)
            {
                //Race for 
                case true when _direction == 0:
                {
                    await ZeroOptionAsync(static async @this =>
                    {
                        await Task.Delay(@this._random.Next(@this.parm_max_network_latency) + @this.parm_max_network_latency/2, @this.AsyncTasks.Token).ConfigureAwait(false);
                        var connectionTime = Stopwatch.StartNew();
                        if (!await @this.ConnectAsync().FastPath().ConfigureAwait(false))
                        {
                            @this._logger.Trace($"{nameof(PeeringResponse)}: FAILED to connect to {@this.Description}");
                            @this.State = AdjunctState.Standby;
                            return;
                        }

                        Interlocked.Add(ref ConnectionTime, connectionTime.ElapsedMilliseconds);
                        Interlocked.Increment(ref ConnectionCount);
                    },this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(false);
                    break;
                }
                //at least probe
                case false when Hub.Neighbors.Count < CcCollective.MaxAdjuncts:
                {
                    await ZeroAsync(static async @this =>
                    {
                        if (!await @this.SendDiscoveryRequestAsync().FastPath().ConfigureAwait(false))
                            @this._logger.Debug($"{@this.Description}: {nameof(SendDiscoveryRequestAsync)} did not execute...");
                        
                    }, this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach | TaskCreationOptions.PreferFairness).FastPath().ConfigureAwait(false);
                    
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
                        .FastPath().ConfigureAwait(false);
                    
                    sent = await MessageService.IoNetSocket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint)
                        .FastPath().ConfigureAwait(false);
                }
                else //nominal
                {
                    sent = await MessageService.IoNetSocket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint)
                        .FastPath().ConfigureAwait(false);
                }

                return sent;
#else
                return await MessageService.IoNetSocket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint).FastPath().ConfigureAwait(false);
#endif
                
#if DEBUG
                //await sent.OverBoostAsync().FastPath().ConfigureAwait(false);
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
            var discoveryRequest = await _discoveryRequest.ResponseAsync(extraData.ToString(), response.ReqHash).FastPath().ConfigureAwait(false);

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
            LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            foreach (var responsePeer in response.Peers)
            {
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

                // var assimilate = Task.Factory.StartNew(async c =>
                // {
                //     await Task.Delay(parm_max_network_latency * (int)c + (int)c * 1000).ConfigureAwait(false);
                //     await CollectAsync(newRemoteEp, id, services);
                // },++count, TaskCreationOptions.LongRunning);

                await ZeroAsync(static async state =>
                {
                    var (@this, newRemoteEp, id, services) = state;
                    if (!await @this.CollectAsync(newRemoteEp, id, services).FastPath().ConfigureAwait(false))
                        @this._logger.Error($"{@this.Description}: Collecting {newRemoteEp.Address} failed!");
                }, ValueTuple.Create(this,newRemoteEp, id, services), TaskCreationOptions.LongRunning|TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(false);
                
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
        private async ValueTask<bool> CollectAsync(IPEndPoint newRemoteEp, CcDesignation id, CcService services, bool synAck = false)
        {
            if (newRemoteEp != null && newRemoteEp.Equals(NatAddress?.IpEndPoint))
            {
                _logger.Fatal($"x {Description}");
                return false;
            }
            
            CcAdjunct newAdjunct = null;

            var source = new IoUdpClient<CcProtocMessage<Packet, CcDiscoveryMessage>>($"UDP Proxy ~> {Description}",MessageService, newRemoteEp);
            newAdjunct = (CcAdjunct) Hub.MallocNeighbor(Hub, source, Tuple.Create(id, services, newRemoteEp));

            if (!Zeroed() && await Hub.ZeroAtomicAsync(static async (s, state, ___) =>
            {
                var (@this, newAdjunct, synAck) = state;
                try
                {
                    if (@this.Hub.Neighbors.Count > @this.CcCollective.MaxAdjuncts)
                    {
                        //drop something
                        var bad = @this.Hub.Neighbors.Values.Where(n =>
                            ((CcAdjunct) n).Proxy &&
                            !((CcAdjunct)n).IsDroneConnected &&
                            ((CcAdjunct) n).Direction == Heading.Undefined &&
                            ((CcAdjunct) n).Uptime.ElapsedMs() > @this.parm_max_network_latency * 2 &&
                            ((CcAdjunct) n).State < AdjunctState.Verified);

                        var good = @this.Hub.Neighbors.Values.Where(n =>
                                ((CcAdjunct)n).Proxy &&
                                !((CcAdjunct)n).IsDroneConnected &&
                                ((CcAdjunct)n).Direction == Heading.Undefined &&
                                ((CcAdjunct)n).Uptime.ElapsedMs() > @this.parm_max_network_latency * 2 &&
                                ((CcAdjunct)n).State < AdjunctState.Peering &&
                                ((CcAdjunct)n)._totalPats > @this.parm_min_pats_before_shuffle &&
                                ((CcAdjunct)n).PeerRequests > 0)
                            .OrderBy(n => ((CcAdjunct)n).Priority);

                        var badList = bad.ToList();
                        if (badList.Count > 0)
                        {
                            var dropped = badList.Skip(@this._random.Next(badList.Count)).FirstOrDefault();
                            if (dropped != default) 
                            {
                                await ((CcAdjunct)dropped).ZeroAsync(new IoNanoprobe("got collected")).FastPath().ConfigureAwait(false);
                                @this._logger.Info($"~ {dropped.Description}");
                            }
                        }
                        else if (synAck) //try harder when this comes from a synack 
                        {
                            var goodList = good.TakeWhile(dropped=>((CcAdjunct)dropped).State < AdjunctState.Peering).ToList();
                            var dropped = goodList.Skip(@this._random.Next(goodList.Count)).FirstOrDefault();
                            if (dropped != default && ((CcAdjunct)dropped).State < AdjunctState.Peering)
                            {
                                await ((CcAdjunct)dropped).ZeroAsync(new IoNanoprobe("Assimilated!")).FastPath().ConfigureAwait(false);
                                @this._logger.Info($"@ {dropped.Description}");
                            }
                        }
                    }

                    //Transfer?
                    if (!@this.Hub.Neighbors.TryAdd(newAdjunct.Key, newAdjunct))
                    {
                        await newAdjunct.ZeroAsync(@this).FastPath().ConfigureAwait(false);
                        return false;    
                    }
                    return true;
                }
                catch (Exception e)
                {
                    if(!@this.Zeroed())
                        @this._logger.Error(e, $"{@this.Description??"N/A"}");
                }

                return false;
            }
                , ValueTuple.Create(this, newAdjunct, synAck)).FastPath().ConfigureAwait(false))
            {
                //setup conduits to messages
                newAdjunct.MessageService.SetConduit(nameof(CcAdjunct), MessageService.GetConduit<CcProtocBatch<Packet, CcDiscoveryMessage>>(nameof(CcAdjunct)));
                newAdjunct.ExtGossipAddress = ExtGossipAddress; 
                newAdjunct.State = AdjunctState.Unverified;
                newAdjunct.Verified = false;

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
                            }).FastPath().ConfigureAwait(false);

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
                },ValueTuple.Create(this,newAdjunct)).FastPath().ConfigureAwait(false);

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
                }).FastPath().ConfigureAwait(false);

                
                await CcCollective.Hub.AssimilateAsync(newAdjunct).FastPath().ConfigureAwait(false);
                
                if (!await newAdjunct.SendPingAsync().FastPath().ConfigureAwait(false))
                {
                    _logger.Debug($"{newAdjunct.Description}: Unable to send ping!!!");
                    return false;
                }

                return true;
            }
            else
            {
                if (newAdjunct != null)
                    await newAdjunct.ZeroAsync(new IoNanoprobe("CollectAsync")).FastPath().ConfigureAwait(false);
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
            if (!Assimilating || request.Timestamp.ElapsedMs() > parm_max_network_latency * 2)
            {
                if (Collected)
                    _logger.Trace(
                        $"<\\- {nameof(DiscoveryRequest)}: [ABORTED], age = {request.Timestamp.ElapsedDelta()}, {Description}, {MetaDesc}");
                return;
            }

            //PAT
            LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            
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
                .OrderByDescending(n => (int) ((CcAdjunct) n).Priority).ToList();
            foreach (var ioNeighbor in certified.TakeWhile(ioNeighbor => count != parm_max_discovery_peers))
            {
                discoveryResponse.Peers.Add(new Peer
                {
                    PublicKey = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(((CcAdjunct) ioNeighbor).Designation.PublicKey)),
                    Services = ((CcAdjunct) ioNeighbor).ServiceMap,
                    Ip = ((CcAdjunct) ioNeighbor).RemoteAddress.Ip
                });
                count++;
            }

            //var discResponseTask = Task.Factory.StartNew(async () =>
            {
                int sent;
                if ((sent = await SendMessageAsync(discoveryResponse.ToByteString(),
                        CcDiscoveries.MessageTypes.DiscoveryResponse)
                    .FastPath().ConfigureAwait(false)) > 0)
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
                    }).FastPath().ConfigureAwait(false);
                }
                else
                {
                    if (Collected)
                        _logger.Debug($"<\\- {nameof(DiscoveryRequest)}({sent}): [FAILED], {Description}, {MetaDesc}");
                }
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
                _logger.Error($"<\\- {(Proxy ? "V>" : "X>")}{nameof(Ping)}: [WARN] Dropped stale, age = {ping.Timestamp.ElapsedSec()}s");
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
                    .FastPath().ConfigureAwait(false)) > 0)
                {
                    _logger.Trace($"-/> {nameof(Pong)}({sent})[{pong.ToByteString().Memory.PayloadSig()} ~ {pong.ReqHash.Memory.HashSig()}]: Sent SYN-ACK, {MessageService.IoNetSocket.LocalAddress} ~> {toAddress}");

                    await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.SendProtoMsg,
                        Msg = new ProtoMsg
                        {
                            CollectiveId = Hub.Router.Designation.IdString(),
                            Id = Base58.Bitcoin.Encode(packet.PublicKey.Span[..8].ToArray()),
                            Type = "pong"
                        }
                    }).FastPath().ConfigureAwait(false);
                }

                if (toProxyAddress != null && CcCollective.UdpTunnelSupport && toAddress.Ip != toProxyAddress.Ip)
                {
                    if ((sent = await SendMessageAsync(pong.ToByteString(), CcDiscoveries.MessageTypes.Pong, toAddress)
                        .FastPath().ConfigureAwait(false)) > 0)
                    {
                        _logger.Trace($"-/> {nameof(Pong)}({sent})[{pong.ToByteString().Memory.PayloadSig()} ~ {pong.ReqHash.Memory.HashSig()}]: Sent SYN-ACK, {MessageService.IoNetSocket.LocalAddress} ~> {toAddress}");
                    }
                }
            }
            else //PROCESS ACK
            {
                LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                var sent = 0;
                if ((sent = await SendMessageAsync(data: pong.ToByteString(), type: CcDiscoveries.MessageTypes.Pong)
                    .FastPath().ConfigureAwait(false)) > 0)
                {
                    if (IsDroneConnected)
                        _logger.Trace($"-/> {nameof(Pong)}({sent})[{pong.ToByteString().Memory.PayloadSig()} ~ {pong.ReqHash.Memory.HashSig()}]: Sent KEEPALIVE, {Description}");
                    else
                        _logger.Trace($"-/> {nameof(Pong)}({sent})[{pong.ToByteString().Memory.PayloadSig()} ~ {pong.ReqHash.Memory.HashSig()}]: Sent ACK SYN, {Description}");

                    await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.SendProtoMsg,
                        Msg = new ProtoMsg
                        {
                            CollectiveId = Hub.Router.Designation.IdString(),
                            Id = Designation.IdString(),
                            Type = "pong"
                        }
                    }).FastPath().ConfigureAwait(false);
                }
                else
                {
                    if (Collected)
                        _logger.Debug($"<\\- {nameof(Ping)}: [FAILED] Sent ACK SYN/KEEPALIVE, to = {Description}");
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
            var pingRequest = await _pingRequest.ResponseAsync(extraData.ToString(), pong.ReqHash).FastPath().ConfigureAwait(false);
            
            if (!pingRequest && Proxy)
            {
                pingRequest = await Hub.Router._pingRequest.ResponseAsync(extraData.ToString(), pong.ReqHash).FastPath().ConfigureAwait(false);
            }

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
            
            //drop old matches
            //if (pingRequest.TimestampMs.ElapsedMs() > parm_max_network_latency * 2)
            //    return;
            
            //Process SYN-ACK
            if (!Proxy)
            {
                var idCheck = CcDesignation.FromPubKey(packet.PublicKey.Memory);
                var fromAddress = IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData);
                var keyStr = MakeId(idCheck, fromAddress);

                // remove stale adjunctPKs
                var staleId = Hub.Neighbors
                    .Where(kv => ((CcAdjunct) kv.Value).Proxy)
                    .Where(kv => ((CcAdjunct) kv.Value).RemoteAddress.Port == ((IPEndPoint) extraData).Port)
                    .Where(kv => kv.Value.Key.Contains(idCheck.PkString()))
                    .Select(kv => kv.Value.Key).FirstOrDefault();

                if (!string.IsNullOrEmpty(staleId) &&
                    Hub.Neighbors.TryRemove(staleId, out var staleNeighbor))
                {
                    _logger.Warn(
                        $"Removing stale adjunct {staleNeighbor.Key}:{((CcAdjunct) staleNeighbor).RemoteAddress.Port} ==> {keyStr}:{((IPEndPoint) extraData).Port}");
                    await staleNeighbor.ZeroAsync(new IoNanoprobe($"{nameof(staleNeighbor)}")).FastPath().ConfigureAwait(false);
                }

                var remoteServices = new CcService();
                foreach (var key in pong.Services.Map.Keys.ToList())
                    remoteServices.CcRecord.Endpoints.TryAdd(Enum.Parse<CcService.Keys>(key),
                        IoNodeAddress.Create(
                            $"{pong.Services.Map[key].Network}://{((IPEndPoint) extraData).Address}:{pong.Services.Map[key].Port}"));

                await ZeroAsync(static async state =>
                {
                    var (@this, fromAddress, remoteServices, idCheck) = state;
                    if (!await @this.CollectAsync(fromAddress.IpEndPoint, idCheck, remoteServices, true).FastPath().ConfigureAwait(false))
                        @this._logger.Error($"{@this.Description}: Collecting {fromAddress.IpEndPoint} failed!");
                }, ValueTuple.Create(this, fromAddress, remoteServices, idCheck), TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(false);

            }
            else if (!Verified) //Process ACK SYN
            {
                State = AdjunctState.Verified;

                //PAT
                LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                
                //TODO: vector?
                //set ext address as seen by neighbor
                ExtGossipAddress =
                    IoNodeAddress.Create(
                        $"tcp://{pong.DstAddr}:{CcCollective.Services.CcRecord.Endpoints[CcService.Keys.gossip].Port}");

                NatAddress =
                    IoNodeAddress.Create(
                        $"udp://{pong.DstAddr}:{CcCollective.Services.CcRecord.Endpoints[CcService.Keys.peering].Port}");

                Verified = true;

                _logger.Trace($"<\\- {nameof(Pong)}: ACK SYN: {Description}");
                
                //we need this peer request even if we are full. Verification needs this signal
                //if (CcCollective.EgressConnections < CcCollective.parm_max_outbound)
                {
                    //_logger.Trace($"(acksyn): {(CcCollective.EgressConnections < CcCollective.parm_max_outbound ? "Send Peer REQUEST" : "Withheld Peer REQUEST")}, to = {Description}, from nat = {ExtGossipAddress}");
                    //var peerRequestTask = Task.Factory.StartNew(async () =>
                    //{
                    //    await Task.Delay(_random.Next(parm_max_network_latency), AsyncTasks.Token).ConfigureAwait(false);
                    //    var s = SendPeerRequestAsync();
                    //}, AsyncTasks.Token, TaskCreationOptions.LongRunning, TaskScheduler.Current);                  
                    if (!await SendPeerRequestAsync().FastPath().ConfigureAwait(false))
                    {
                        _logger.Error($"{nameof(SendDiscoveryRequestAsync)}: Failed!");
                    }
                }
            }
            else //sometimes drones we know prod us for connections
            {
                //PAT
                LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                
                _logger.Trace($"<\\- {nameof(Pong)}: {Description}");
                Interlocked.Increment(ref _totalPats);

                if (Direction == Heading.Undefined && CcCollective.EgressConnections < CcCollective.parm_max_outbound && PeeringAttempts < parm_zombie_max_connection_attempts)
                {
                    _logger.Trace($"<\\- {nameof(Pong)}(acksyn-fast): {(CcCollective.EgressConnections < CcCollective.parm_max_outbound ? "Send Peer REQUEST" : "Withheld Peer REQUEST")}, to = {Description}, from nat = {ExtGossipAddress}");
                    await SendPeerRequestAsync().FastPath().ConfigureAwait(false);
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
                        .FastPath().ConfigureAwait(false)) == null) 
                        return false;

                    var sent = await SendMessageAsync(data: reqBuf, type: CcDiscoveries.MessageTypes.Ping).FastPath().ConfigureAwait(false);

                    if (sent > 0)
                    {
                        _logger.Trace($"-/> {nameof(Ping)}({sent})[{reqBuf.Span.PayloadSig()}, {challenge.Value.Payload.Memory.PayloadSig()}]: {Description}");

                        await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = Designation.IdString(),
                                Type = "ping"
                            }
                        }).FastPath().ConfigureAwait(false);

                        return true;
                    }

                    await _pingRequest.RemoveAsync(challenge).FastPath().ConfigureAwait(false);

                    if (Collected)
                        _logger.Error($"-/> {nameof(Ping)}: [FAILED], {Description}");
                    return false;
                }
                else //The destination state was undefined, this is local
                {
                    var router = Hub.Router;

                    IoQueue<IoZeroMatcher<ByteString>.IoChallenge>.IoZNode challenge;
                    if ((challenge = await router._pingRequest.ChallengeAsync(dest.IpPort, reqBuf).FastPath().ConfigureAwait(false)) == null)
                        return false;

                    var sent = await SendMessageAsync(reqBuf, CcDiscoveries.MessageTypes.Ping, dest)
                        .FastPath().ConfigureAwait(false);

                    if (sent > 0)
                    {
                        _logger.Trace($"-/> {nameof(SendPingAsync)}({sent})[{reqBuf.Span.PayloadSig()}]: dest = {dest}, {Description}");

                        await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = id,
                                Type = "ping"
                            }
                        }).FastPath().ConfigureAwait(false);

                        return true;
                    }

                    await router._pingRequest.RemoveAsync(challenge).FastPath().ConfigureAwait(false);
                    if (!router.Zeroed() && !router.MessageService.Zeroed())
                        _logger.Error($"-/> {nameof(SendPingAsync)}:(X) [FAILED], {Description}");
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
        /// <summary>
        /// Sends a discovery request
        /// </summary>
        /// <returns>Task</returns>
        public async ValueTask<bool> SendDiscoveryRequestAsync()
        {
            try
            {
                if (!Assimilating || State < AdjunctState.Verified)
                {
                    _logger.Error($"{nameof(SendDiscoveryRequestAsync)}: [ABORTED], {Description}, s = {State}, a = {Assimilating}");
                    return false;
                }

                //rate limit
                if (_lastScan.Elapsed() < parm_max_network_latency / 1000)
                    return false;
                
                var discoveryRequest = new DiscoveryRequest
                {
                     Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                };

                var reqBuf = discoveryRequest.ToByteString();

                IoQueue<IoZeroMatcher<ByteString>.IoChallenge>.IoZNode challenge;
                if ((challenge = await _discoveryRequest.ChallengeAsync(RemoteAddress.IpPort, reqBuf)
                    .FastPath().ConfigureAwait(false)) == null)
                {
                    return false;
                }

                _lastScan = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                var sent = await SendMessageAsync(reqBuf,CcDiscoveries.MessageTypes.DiscoveryRequest, RemoteAddress).FastPath().ConfigureAwait(false);
                if (sent > 0)
                {
                    _logger.Trace(
                        $"-/> {nameof(SendDiscoveryRequestAsync)}({sent}){reqBuf.Memory.PayloadSig()}: Sent, {Description}");

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
                    }).FastPath().ConfigureAwait(false);

                    return true;
                }

                await _discoveryRequest.RemoveAsync(challenge).FastPath().ConfigureAwait(false);
                if (Collected)
                    _logger.Debug($"-/> {nameof(SendDiscoveryRequestAsync)}: [FAILED], {Description} ");
            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e, Description);
            }
            catch (ObjectDisposedException e)
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
            catch (Exception e)
            {
                if (Collected)
                    _logger.Debug(e,
                        $"{nameof(SendDiscoveryRequestAsync)}: [ERROR] z = {Zeroed()}, state = {State}, dest = {RemoteAddress}, source = {MessageService}, _discoveryRequest = {_discoveryRequest.Count}");
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

                //assimilate count
                Interlocked.Increment(ref PeeringAttempts);

                var peerRequest = new PeeringRequest
                {
                    Salt = new Salt
                        {ExpTime = (ulong) DateTimeOffset.UtcNow.AddHours(2).ToUnixTimeSeconds(), Bytes = GetSalt},
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                };

                var peerRequestRaw = peerRequest.ToByteString();

                IoQueue<IoZeroMatcher<ByteString>.IoChallenge>.IoZNode challenge;
                if ((challenge = await _peerRequest.ChallengeAsync(RemoteAddress.IpPort, peerRequestRaw).FastPath().ConfigureAwait(false)) == null)
                {
                    return false;
                }
                
                var sent = await SendMessageAsync(peerRequestRaw, CcDiscoveries.MessageTypes.PeeringRequest).FastPath().ConfigureAwait(false);
                if (sent > 0)
                {
                    _logger.Trace(
                        $"-/> {nameof(SendPeerRequestAsync)}({sent})[{peerRequestRaw.Memory.PayloadSig()}]: Sent, {Description}");

                    await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.SendProtoMsg,
                        Msg = new ProtoMsg
                        {
                            CollectiveId = Hub.Router.Designation.IdString(),
                            Id = Designation.IdString(),
                            Type = "peer request"
                        }
                    }).FastPath().ConfigureAwait(false);

                    return true;
                }

                await _peerRequest.RemoveAsync(challenge).FastPath().ConfigureAwait(false);

                if (Collected)
                    _logger.Debug($"-/> {nameof(SendPeerRequestAsync)}: [FAILED], {Description}, {MetaDesc}");
            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e, Description);
            }
            catch (ObjectDisposedException e)
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
            catch (Exception e)
            {
                _logger.Debug(e, $"{nameof(SendPeerRequestAsync)}: [FAILED], {Description}, {MetaDesc}");
            }
            
            return false;
        }

        /// <summary>
        /// Tell peer to drop us when things go wrong. (why or when? cause it wont reconnect otherwise. This is a bug)
        /// </summary>
        /// <returns></returns>
        private async ValueTask SendPeerDropAsync(IoNodeAddress dest = null)
        {
            try
            {
                dest ??= RemoteAddress;

                var dropRequest = new PeeringDrop
                {
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                };

                int sent;
                _logger.Trace(
                    (sent = await SendMessageAsync(dropRequest.ToByteString(),CcDiscoveries.MessageTypes.PeeringDrop, dest).FastPath().ConfigureAwait(false)) > 0
                        ? $"-/> {nameof(PeeringDrop)}({sent}): Sent, {Description}"
                        : $"-/> {nameof(PeeringDrop)}: [FAILED], {Description}, {MetaDesc}");

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
                    }).FastPath().ConfigureAwait(false);
                }
            }
            catch (NullReferenceException e) when(!Zeroed())
            {
                _logger.Trace(e, Description);
            }
            catch (ObjectDisposedException e) when(!Zeroed())
            {
                _logger.Trace(e, Description);
            }
            catch (TaskCanceledException e) when(!Zeroed())
            {
                _logger.Trace(e, Description);
            }
            catch (OperationCanceledException e) when(!Zeroed())
            {
                _logger.Trace(e, Description);
            }
            catch (Exception) when(Zeroed()){}
            catch (Exception e)
            {
                if (Collected)
                    _logger.Debug(e,
                        $"{nameof(SendPeerDropAsync)}: [ERROR], {Description}, s = {State}, a = {Assimilating}, p = {IsDroneConnected}, d = {dest}, s = {MessageService}");
            }
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
                    return false;

                if (!await ZeroAtomicAsync( static (_, state, _) =>
                {
                    var (@this, ioCcDrone, direction) = state;

                    //Guarantee hard cap on allowed drones here, other implemented caps are soft caps. This is the only one that matters
                    if (direction == Heading.Ingress && @this.CcCollective.IngressConnections >= @this.CcCollective.parm_max_inbound || 
                        direction == Heading.Egress && @this.CcCollective.EgressConnections >= @this.CcCollective.parm_max_outbound)
                    {
                        return new ValueTask<bool>(false);
                    }

                    //Race for direction
                    if (Interlocked.CompareExchange(ref @this._direction, (int) direction, (int) Heading.Undefined) !=
                        (int) Heading.Undefined)
                    {
                        @this._logger.Warn(
                            $"oz: race for {direction} lost {ioCcDrone.Description}, current = {@this.Direction}, {@this._drone?.Description}");
                        return new ValueTask<bool>(false);
                    }
                    
                    @this._drone = ioCcDrone ?? throw new ArgumentNullException($"{nameof(ioCcDrone)}");
                    @this.State = AdjunctState.Connected;
                    @this.WasAttached = true;

                    return new ValueTask<bool>(true);
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
                }).FastPath().ConfigureAwait(false);

                if (!await SendDiscoveryRequestAsync().FastPath().ConfigureAwait(false))
                    _logger.Debug($"{Description}: {nameof(SendDiscoveryRequestAsync)} did not execute...");
                

                return true;
            }
            catch (Exception e)
            {
                _logger.Trace(e, Description);
                return false;
            }
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
            CcDrone latch = null;
            var state = ValueTuple.Create(this, latch);
            
            if (!await ZeroAtomicAsync(static (s, state, d) =>
            {
                state.Item2 = ((CcAdjunct) s)._drone;
                ((CcAdjunct)s)._drone = null;

                return ValueTask.FromResult(state.Item2 != null);
            },state).FastPath().ConfigureAwait(false))
            {
                return;
            }

            latch = state.Item2;

            var direction = Direction;
            _logger.Warn($"{(Assimilated ? "Distinct" : "Common")} {Direction} peer detaching: s = {State}, a = {Assimilating}, p = {IsDroneConnected}, {latch?.Description ?? Description}");
            
            await latch!.ZeroAsync(this).FastPath().ConfigureAwait(false);
            
            Interlocked.Exchange(ref _direction, 0);
            AttachTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            TotalPats = 0;
            PeeringAttempts = 0;
            State = AdjunctState.Disconnected;

            //send drop request
            await SendPeerDropAsync().FastPath().ConfigureAwait(false);

            
            if(direction == Heading.Ingress && CcCollective.IngressConnections < CcCollective.parm_max_inbound)
            //back off for a while... Try to re-establish a link 
            await ZeroAsync(static async @this =>
            {
                await Task.Delay(@this.parm_max_network_latency + @this._random.Next(@this.parm_max_network_latency), @this.AsyncTasks.Token).ConfigureAwait(false);
                await @this.SendPingAsync().FastPath().ConfigureAwait(false);
            },this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach | TaskCreationOptions.PreferFairness).FastPath().ConfigureAwait(false);

            //emit event
            
            await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
            {
                EventType = AutoPeerEventType.RemoveDrone,
                Drone = new Drone
                {   
                    CollectiveId = CcCollective.Hub.Router.Designation.IdString(),
                    Adjunct = Designation.IdString()
                }
            }).FastPath().ConfigureAwait(false);

            if(direction == Heading.Egress && CcCollective.EgressConnections < CcCollective.parm_max_outbound)
            {
                await ZeroAsync(static async @this =>
                {
                    await @this.SendPeerRequestAsync().FastPath().ConfigureAwait(false);
                },this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach | TaskCreationOptions.PreferFairness).FastPath().ConfigureAwait(false);    
            }
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

        /// <summary>
        /// Gets and sets the state of the work
        /// </summary>
        public AdjunctState State
        {
            get => _currState.Value;
            set
            {
#if DEBUG
                var nextState = new IoStateTransition<AdjunctState>();
                nextState.Enter(value, _currState);

                var prevState = _currState;
                _currState = nextState;

                if (StateTransitionHistory[(int)prevState.Value] != null)
                {
                    var c = StateTransitionHistory[(int)prevState.Value];
                    while (c.Repeat != null)
                        c = c.Repeat;
                    StateTransitionHistory[(int) prevState.Value].Repeat = prevState;
                }
                else
                    StateTransitionHistory[(int) prevState.Value] = prevState;
#else
                _currState.Enter(value);
#endif
                //terminate
                if (value == AdjunctState.ZeroState)
                {
                    State = AdjunctState.FinalState;
                }
            }
        }
    }
}