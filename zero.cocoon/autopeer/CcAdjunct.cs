//#define LOSS
using System;
using System.Buffers;
using System.Diagnostics;
using System.Linq;
using System.Net;
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
using Logger = NLog.Logger;

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
                userData => new CcDiscoveries("adjunct RX", $"{ioNetClient.Key}", ioNetClient),
                ioNetClient.ConcurrencyLevel,
                ioNetClient.ConcurrencyLevel
            )
        {
            _logger = LogManager.GetCurrentClassLogger();

            _pingRequest = new IoZeroMatcher<ByteString>(nameof(_pingRequest), Source.ConcurrencyLevel * 2 + 1, parm_max_network_latency, CcCollective.parm_max_inbound);
            _peerRequest = new IoZeroMatcher<ByteString>(nameof(_peerRequest), Source.ConcurrencyLevel * 2 + 1, parm_max_network_latency, CcCollective.parm_max_inbound);
            _discoveryRequest = new IoZeroMatcher<ByteString>(nameof(_discoveryRequest), Source.ConcurrencyLevel * 2 + CcCollective.MaxAdjuncts * parm_max_discovery_peers + 1, parm_max_network_latency, CcCollective.parm_max_inbound);

            if (extraData != null)
            {
                var extra = (Tuple<CcDesignation, CcService, IPEndPoint>) extraData;
                Designation = extra.Item1;
                Services = services ?? extra.Item2;
                RemoteAddress = IoNodeAddress.CreateFromEndpoint("udp", extra.Item3);
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
                var task = Task.Factory.StartNew(async () =>
                {
                    while (!Zeroed())
                    {
                        await WatchdogAsync().ConfigureAwait(false);
                        await Task.Delay(_random.Next(CcCollective.parm_mean_pat_delay / 2) * 1000 + CcCollective.parm_mean_pat_delay / 8 * 1000,AsyncTasks.Token).ConfigureAwait(false);
                    }
                }, AsyncTasks.Token, TaskCreationOptions.LongRunning | TaskCreationOptions.PreferFairness, TaskScheduler.Default);
            }
            else
            {
                State = AdjunctState.Local;
                _routingTable = new CcAdjunct[IPEndPoint.MaxPort];
            }
        }

        public enum AdjunctState
        {
            Undefined,
            FinalState,
            ZeroState,
            Zombie,
            Local,
            Unverified,
            Verified,
            Disconnected,
            Standby,
            Peering,
            Reconnecting,
            Connecting,
            Connected,
        }

        /// <summary>
        /// The current state
        /// </summary>
        private volatile IoStateTransition<AdjunctState> _currState = new IoStateTransition<AdjunctState>()
        {
            FinalState = AdjunctState.FinalState
        };

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Description
        /// </summary>
        private string _description;

        /// <summary>
        /// rate limit desc gens
        /// </summary>
        private long _lastDescGen = (DateTimeOffset.UtcNow + TimeSpan.FromDays(1)).Millisecond;

        public override string Description
        {
            get
            {
                if (_lastDescGen.ElapsedMsDelta() > 100 && _description != null)
                    return _description;

                _lastDescGen = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                try
                {
                    return _description = $"`adjunct({(Verified ? "+v" : "-v")},{(Assimilated ? "C" : "dc")})[{(Proxy?TotalPats.ToString().PadLeft(3):"  0")}:{Priority}] local: {MessageService.IoNetSocket.LocalAddress} - {MessageService.IoNetSocket.RemoteAddress}, [{Designation.IdString()}]'";
                }
                catch (Exception e)
                {
                    if (Collected)
                        _logger.Debug(e, Description);
                    return _description?? $"`adjunct({(Verified ? "+v" : "-v")},{(Assimilated ? "C" : "dc")})[{(Proxy?TotalPats.ToString().PadLeft(3):"  0")}:{Priority}] local: {MessageService?.IoNetSocket?.LocalAddress} - {MessageService?.IoNetSocket?.RemoteAddress}, [{Designation.IdString()}]'";;
                }
            }
        }

        /// <summary>
        /// return extra information about the state
        /// </summary>
        public string MetaDesc =>
            $"(d = {Direction}, s = {State}, v = {Verified}, a = {Assimilating}, att = {IsDroneAttached}, c = {IsDroneConnected}, r = {PeeringAttempts}, g = {IsGossiping}, arb = {IsArbitrating}, o = {MessageService.IsOperational}, w = {TotalPats})";

        /// <summary>
        /// Random number generator
        /// </summary>
        private readonly Random _random = new Random((int) DateTimeOffset.Now.Ticks);

        /// <summary>
        /// Discovery services
        /// </summary>
        protected CcHub Hub => (CcHub) Node;

        /// <summary>
        /// Source
        /// </summary>
        protected IoNetClient<CcProtocMessage<Packet, CcDiscoveryBatch>> MessageService => (IoNetClient<CcProtocMessage<Packet, CcDiscoveryBatch>>) Source;

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
        public bool IsDroneConnected => IsDroneAttached && (_drone?.IoSource?.IsOperational ?? false) && State == AdjunctState.Connected && Direction != Heading.Undefined;

        /// <summary>
        /// Is this the local listener
        /// </summary>
        public bool IsLocal => !Proxy;

        /// <summary>
        /// If the adjunct is working 
        /// </summary>
        public bool Assimilating => !Zeroed() && Verified && Proxy;

        /// <summary>
        /// Whether the node, peer and adjunct are nominal
        /// </summary>
        public bool IsGossiping => Assimilating && IsDroneConnected;

        /// <summary>
        /// Looks for a zombie peer
        /// </summary>
        public bool PolledZombie => Direction != Heading.Undefined && !(Assimilating && IsDroneConnected);

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
        public IoNodeAddress RemoteAddress { get; protected set; }

        /// <summary>
        /// Whether this adjunctcontains verified remote client connection information
        /// </summary>
        public bool Proxy => (RemoteAddress != null);

        /// <summary>
        /// The our IP as seen by neighbor
        /// </summary>
        public IoNodeAddress ExtGossipAddress { get; protected set; }
        
        /// <summary>
        /// The our discovery service IP as seen by neighbor
        /// </summary>
        public IoNodeAddress NATAddress { get; protected set; }
        
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
        public bool Ingress => Direction == Heading.Ingress && IsDroneConnected;

        /// <summary>
        /// outbound
        /// </summary>
        public bool Egress => Direction == Heading.Egress && IsDroneConnected;

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
        private IoConduit<CcProtocBatch<Packet, CcDiscoveryBatch>> _protocolConduit;

        /// <summary>
        /// Seconds since pat
        /// </summary>
        private long _lastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        /// <summary>
        /// Total pats
        /// </summary>
        private long _totalPats;

        /// <summary>
        /// Seconds since last adjunctpat
        /// </summary>
        protected long LastPat
        {
            get => Interlocked.Read(ref _lastPat);
            set => Interlocked.Exchange(ref _lastPat, value);
        }

        /// <summary>
        /// Seconds since last adjunctpat
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
                if (_curSalt == null || _curSaltStamp.Elapsed() > parm_salt_ttl)
                {
                    using var rand = new RNGCryptoServiceProvider();
                    _curSalt = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(new byte[parm_salt_length]));
                    rand.GetNonZeroBytes(_curSalt.ToByteArray());
                    _curSaltStamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                }

                return _curSalt;
            }
        }

        /// <summary>
        /// Message heap
        /// </summary>
        public ArrayPool<CcDiscoveryBatch> ArrayPoolProxy { get; protected set; }

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
        /// The adjunctservices
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
        public int parm_max_discovery_peers = 6;

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
        public int parm_min_pats_before_shuffle = 6;

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
        /// Handle to peer zero sub
        /// </summary>
        private IoZeroSub _zeroSub;

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
                foreach (var service in Services.CcRecord.Endpoints)
                {
                    if (service.Value != null && service.Value.Validated)
                        mapping.Map.Add(service.Key.ToString(),
                            new NetworkAddress
                            {
                                Network = $"{service.Value.Protocol().ToString().ToLower()}",
                                Port = (uint) service.Value.Port
                            });
                    else
                    {
                        _logger.Warn($"Invalid endpoints found ({service.Value?.ValidationErrorString})");
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
            _routingTable = null;
            _pingRequest = null;
            _drone = null;
            _protocolConduit = null;
            _zeroSub = default;
            ArrayPoolProxy = null;
            StateTransitionHistory = null;
            _peerRequest = null;
            _discoveryRequest = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
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

                if (IsDroneConnected)
                {
                    await Router.SendPeerDropAsync(RemoteAddress).ConfigureAwait(false);
                }
            }
            else
            {
                await _protocolConduit.ZeroAsync(this).ConfigureAwait(false);
            }
            
            State = AdjunctState.ZeroState;

            if (Assimilated && Direction != Heading.Undefined)
                _logger.Info($"- `{(Assimilated ? "Distinct" : "Common")} {Direction}: {Description}, from: {ZeroedFrom?.Description}");

            await DetachPeerAsync().ConfigureAwait(false);

            await _pingRequest.DumpAsync(Router._pingRequest).ConfigureAwait(false);
            await _pingRequest.ZeroAsync(this).ConfigureAwait(false);
            await _peerRequest.ZeroAsync(this).ConfigureAwait(false);
            await _discoveryRequest.ZeroAsync(this).ConfigureAwait(false);
#if DEBUG
            Array.Clear(StateTransitionHistory, 0, StateTransitionHistory.Length);
#endif
            await base.ZeroManagedAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Test mode
        /// </summary>
        private static uint _dropOne = 1;

        /// <summary>
        /// Ensures that the peer is running
        /// </summary>
        public async Task WatchdogAsync()
        {
            // Verify request
            if (!Assimilating)
            {
                return;
            }
            
            //Moderate requests if we ensured at least once
            //if (SecondsSincePat < parm_zombie_max_ttl / 2)
            //{
            //    return;
            //}
            
            //Are we limping?
            if (!Proxy && Hub.Neighbors.Count <= 3)
            {
                await CcCollective.BootAsync().ConfigureAwait(false);
                return;
            }

            //Watchdog failure
            //if (SecondsSincePat > CcNode.parm_mean_pat_delay * 400 || (_dropOne == 0 && _random.Next(0) == 0 && Interlocked.CompareExchange(ref _dropOne, 1, 0) == 0) )
            if (SecondsSincePat > CcCollective.parm_mean_pat_delay || _dropOne == 0)
            {
                _dropOne = 1;
                var reconnect = this.Direction == Heading.Egress;
                var address = RemoteAddress;

                if (TotalPats > 1)
                    _logger.Debug($"w {Description}");
                else
                    _logger.Trace($"w {Description}, s = {SecondsSincePat} >> {CcCollective.parm_mean_pat_delay}, {MetaDesc}");
                
                await ZeroAsync(new IoNanoprobe($"-wd: l = {SecondsSincePat}s ago...")).ConfigureAwait(false);
                
                return;
            }

            if (await SendPingAsync().ConfigureAwait(false))
            {
                _logger.Trace($"-/> {nameof(WatchdogAsync)}: PAT to = {Description}");
            }
            else
            {
                if (Collected && !(_pingRequest.Peek(RemoteAddress.IpPort) || Router._pingRequest.Peek(RemoteAddress.IpPort)))
                    _logger.Debug($"-/> {nameof(SendPingAsync)}: PAT Send [FAILED], {Description}, {MetaDesc}");
            }
        }

        /// <summary>
        /// Start processors for this neighbor
        /// </summary>
        /// <returns></returns>
        public override async Task AssimilateAsync()
        {
            var processingAsync = base.AssimilateAsync();
            var protocol = Task.Factory.StartNew(o => ProcessAsync(),TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach).Unwrap();
            //var protocol = ProcessAsync();

            try
            {
                await Task.WhenAll(processingAsync, protocol).ConfigureAwait(false);

                if (processingAsync.IsFaulted)
                {
                    _logger.Fatal(processingAsync.Exception, "Ajunct processing returned with errors!");
                    await ZeroAsync(new IoNanoprobe($"processingAsync.IsFaulted")).ConfigureAwait(false);
                }

                if (protocol.IsFaulted)
                {
                    _logger.Fatal(protocol.Exception, "Protocol processing returned with errors!");

                    await ZeroAsync(new IoNanoprobe($"protocol.IsFaulted")).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                if (Collected)
                    _logger.Debug(e, Description);
            }
        }

        /// <summary>
        /// Connect to adjunctforming a peer connection
        /// </summary>
        /// <returns></returns>
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
                if(IsDroneConnected)
                    _logger.Fatal("Drone was connected...");
                //await existingNeighbor.ZeroAsync(new IoNanoprobe("Dropped because reconnect?")).ConfigureAwait(false);
                return false;
            }

            //Attempt the connection, race to win
            if (await CcCollective.ConnectToDroneAsync(this).ConfigureAwait(false))
            {
                _logger.Trace($"Connected to {Description}");
                return true;
            }

            _logger.Trace($"{nameof(CcCollective.ConnectToDroneAsync)}: [LOST], {Description}, {MetaDesc}");
            State = AdjunctState.Standby;
            return false;
        }

        /// <summary>
        /// Processes protocol messages
        /// </summary>
        /// <param name="msg">The consumer that need processing</param>
        /// <param name="msgArbiter">The arbiter</param>
        /// <param name="processCallback">The process callback</param>
        /// <param name="zeroClosure"></param>
        /// <returns>Task</returns>
        private async ValueTask ProcessMsgBatchAsync(IoSink<CcProtocBatch<Packet, CcDiscoveryBatch>> msg,
            IoConduit<CcProtocBatch<Packet, CcDiscoveryBatch>> msgArbiter,
            Func<CcDiscoveryBatch, IoConduit<CcProtocBatch<Packet, CcDiscoveryBatch>>, IIoZero, Task>
                processCallback, IIoZero zeroClosure)
        {
            if (msg == null)
                return;

            //var stopwatch = Stopwatch.StartNew();
            try
            {
                var protocolMsgs = ((CcProtocBatch<Packet, CcDiscoveryBatch>) msg).Batch;

                foreach (var message in protocolMsgs)
                {
                    if (message == default)
                        break;

                    try
                    {
                        await processCallback(message, msgArbiter, zeroClosure).ConfigureAwait(false);
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
                ((CcProtocBatch<Packet, CcDiscoveryBatch>) msg).Batch[0] = default;
                ArrayPoolProxy?.Return(((CcProtocBatch<Packet, CcDiscoveryBatch>)msg).Batch);
            }
        }
        
        /// <summary>
        /// Processes protocol messageS
        /// </summary>
        /// <returns></returns>
        private async Task ProcessAsync()
        {
            _protocolConduit ??= MessageService.GetConduit<CcProtocBatch<Packet, CcDiscoveryBatch>>(nameof(CcAdjunct));

            _logger.Debug($"$ {Description}");

            ValueTask<bool>[] channelProduceTasks = null;
            ValueTask<bool>[] channelTasks = null;
            try
            {
                while (!Zeroed())
                {
                    //Get the conduit
                    if (_protocolConduit == null)
                    {
                        _logger.Trace($"Waiting for {Description} stream to spin up...");
                        _protocolConduit = await MessageService.AttachConduitAsync<CcProtocBatch<Packet, CcDiscoveryBatch>>(nameof(CcAdjunct)).ConfigureAwait(false);
                        if (_protocolConduit != null)
                            ArrayPoolProxy = ((CcProtocBatchSource<Packet,CcDiscoveryBatch>)_protocolConduit.Source).ArrayPool;
                        else
                        {
                            await Task.Delay(2000, AsyncTasks.Token).ConfigureAwait(false); //TODO config
                        }

                        continue;
                    }
                    else if (channelTasks == null)
                    {
                        channelProduceTasks = new ValueTask<bool>[_protocolConduit.ProducerCount];
                        channelTasks = new ValueTask<bool>[_protocolConduit.ConsumerCount];
                    }

                    //produce
                    for (int i = 0; i < _protocolConduit.ProducerCount; i++)
                    {
                        if (AsyncTasks.IsCancellationRequested || !_protocolConduit.Source.IsOperational)
                            break;

                        channelProduceTasks[i] = _protocolConduit.ProduceAsync();
                    }
                    
                    //Consume
                    for (int i = 0; i < _protocolConduit.ConsumerCount; i++)
                    {
                        if (AsyncTasks.IsCancellationRequested || !_protocolConduit.Source.IsOperational)
                            break;

                        channelTasks[i] = _protocolConduit.ConsumeAsync(async (msg, ioZero) =>
                        {
                            var _this = (CcAdjunct) ioZero;
                            try
                            {
                                await _this.ProcessMsgBatchAsync(msg, _this._protocolConduit,
                                    async (msgBatch, forward, iioZero) =>
                                    {
                                        var __this = (CcAdjunct) iioZero;
                                        var message = msgBatch.EmbeddedMsg;
                                        var extraData = msgBatch.UserData;
                                        var packet = msgBatch.Message;
                                        try
                                        {
                                            var routed = __this.Router._routingTable[((IPEndPoint) extraData).Port] != null;

                                            CcAdjunct ccNeighbor  = null;
                                            try
                                            {
                                                ccNeighbor = __this.Router._routingTable[((IPEndPoint)extraData).Port] ??
                                                             (CcAdjunct)__this.Hub.Neighbors.Values.FirstOrDefault(n => ((IoNetClient<CcProtocMessage<Packet, CcDiscoveryBatch>>)((CcAdjunct)n).Source).IoNetSocket.RemoteNodeAddress.Port == ((IPEndPoint)extraData).Port);
                                            }
                                            catch (Exception e)
                                            {
                                                _logger.Trace(e, Description);
                                                return;
                                            }

                                            __this.Router._routingTable[((IPEndPoint) extraData).Port] ??= ccNeighbor;
                                            

                                            ccNeighbor ??= __this.Hub.Router;

                                            if(!routed && ccNeighbor.Proxy)
                                                ccNeighbor._routingIndex = ((IPEndPoint)extraData).Port;

                                            switch ((CcDiscoveries.MessageTypes)packet.Type)
                                            {
                                                case CcDiscoveries.MessageTypes.Ping:
                                                    await ccNeighbor.ProcessAsync((Ping) message, extraData, packet)
                                                        .ConfigureAwait(false);
                                                    break;
                                                case CcDiscoveries.MessageTypes.Pong:
                                                    await ccNeighbor.ProcessAsync((Pong) message, extraData, packet)
                                                        .ConfigureAwait(false);
                                                    break;
                                                case CcDiscoveries.MessageTypes.DiscoveryRequest:
                                                    await ccNeighbor
                                                        .ProcessAsync((DiscoveryRequest) message, extraData, packet)
                                                        .ConfigureAwait(false);
                                                    break;
                                                case CcDiscoveries.MessageTypes.DiscoveryResponse:
                                                    await ccNeighbor.ProcessAsync((DiscoveryResponse) message,
                                                        extraData, packet).ConfigureAwait(false);
                                                    break;
                                                case CcDiscoveries.MessageTypes.PeeringRequest:
                                                    await ccNeighbor
                                                        .ProcessAsync((PeeringRequest) message, extraData, packet)
                                                        .ConfigureAwait(false);
                                                    break;
                                                case CcDiscoveries.MessageTypes.PeeringResponse:
                                                    await ccNeighbor
                                                        .ProcessAsync((PeeringResponse) message, extraData, packet)
                                                        .ConfigureAwait(false);
                                                    break;
                                                case CcDiscoveries.MessageTypes.PeeringDrop:
                                                    await ccNeighbor
                                                        .ProcessAsync((PeeringDrop) message, extraData, packet)
                                                        .ConfigureAwait(false);
                                                    break;
                                            }
                                        }
                                        catch (TaskCanceledException e)
                                        {
                                            __this._logger.Trace(e, __this.Description);
                                        }
                                        catch (OperationCanceledException e)
                                        {
                                            __this._logger.Trace(e, __this.Description);
                                        }
                                        catch (NullReferenceException e)
                                        {
                                            __this._logger.Trace(e, __this.Description);
                                        }
                                        catch (ObjectDisposedException e)
                                        {
                                            __this._logger.Trace(e, __this.Description);
                                        }
                                        catch (Exception e)
                                        {
                                            __this._logger.Debug(e,
                                                $"{message.GetType().Name} [FAILED]: l = {packet.Data.Length}, {__this.Key}");
                                        }
                                    }, _this).ConfigureAwait(false);
                            }
                            finally
                            {
                                if (msg != null && msg.State != IoJobMeta.JobState.Consumed)
                                    msg.State = IoJobMeta.JobState.ConsumeErr;
                            }
                        }, this);
                    }

                    for (var i = 0; i < channelProduceTasks.Length; i++)
                    {
                        if (!await channelProduceTasks[i].ConfigureAwait(false))
                            return;
                    }

                    for (var i = 0; i < channelTasks.Length; i++)
                    {
                        if (!await channelTasks[i].ConfigureAwait(false))
                            return;
                    }
                }
            }
            catch (TaskCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (OperationCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (NullReferenceException e)
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
                    _logger.Debug(e, $"Error processing {Description}");
            }

            _logger.Debug($"Stopped processing msgs from {Description}");
        }

        /// <summary>
        /// Peer drop request
        /// </summary>
        /// <param name="request">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(PeeringDrop request, object extraData, Packet packet)
        {
            if (!Assimilating || request.Timestamp.ElapsedDelta() > parm_max_network_latency/1000 * 2)
            {
                if(Proxy)
                    _logger.Trace($"{(Proxy ? "V>" : "X>")}{nameof(PeeringDrop)}: Ignoring {request.Timestamp.ElapsedDelta()}s old/invalid request");
                return;
            }

            //only verified nodes get to drop
            if (!Verified || _drone == null)
                return;

            _logger.Trace($"{nameof(PeeringDrop)}: {Direction} Peer = {_drone.Key}: {Description}, {MetaDesc}");

            try
            {
                await _drone.ZeroAsync(this).ConfigureAwait(false);
            }
            catch
            {
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
            //fail fast
            if (!Assimilating)
            {
                //send reject so that the sender's state can be fixed
                var reject = new PeeringResponse
                {
                    ReqHash = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray()))),
                    Status = false
                };

                var remote = IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData);                    
                if (await Router.SendMessageAsync(reject.ToByteString(), remote,
                    CcDiscoveries.MessageTypes.PeeringResponse).ConfigureAwait(false) > 0)
                {
                    _logger.Trace($"-/> {nameof(PeeringResponse)}: Sent {(reject.Status ? "ACCEPT" : "REJECT")}, {Description}");
                }
                else
                    _logger.Debug($"<\\- {nameof(PeeringRequest)}: [FAILED], {Description}, {MetaDesc}");
                
                //DMZ-syn
                if (Collected)
                {
                    //We syn here (Instead of in process ping) to force the other party to do some work (this) before we do work (verify).
                    if (await Router.SendPingAsync(remote, CcDesignation.FromPubKey(packet.PublicKey.Span).IdString())
                        .ConfigureAwait(false)) 
                        _logger.Trace($"{nameof(PeeringRequest)}: DMZ/SYN => {extraData}");
                }
                
                return;
            }

            //Drop old requests
            if (request.Timestamp.ElapsedDelta() > parm_max_network_latency / 1000 * 2)
            {
                _logger.Trace(
                    $"{nameof(PeeringRequest)}: Dropped!, {(Verified ? "verified" : "un-verified")}, age = {request.Timestamp.ElapsedDelta()}");
                return;
            }

            PeerRequests++;

            PeeringResponse peeringResponse = peeringResponse = new PeeringResponse
            {
                ReqHash = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray()))),
                Status = CcCollective.IngressConnections < CcCollective.parm_max_inbound & _direction == 0
            };
            
            if (await SendMessageAsync(data: peeringResponse.ToByteString(),
                type: CcDiscoveries.MessageTypes.PeeringResponse).ConfigureAwait(false) > 0)
            {
                var response = $"{(peeringResponse.Status ? "accept" : "reject")}";
                _logger.Trace(
                    $"-/> {nameof(PeeringResponse)}: Sent {response}, {Description}");

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
            
            var peerRequest = await _peerRequest.ResponseAsync(extraData.ToString(), response.ReqHash).ConfigureAwait(false);
            if (peerRequest?.Key == null)
            {
                if (Collected)
                    _logger.Debug(
                        $"<\\- {nameof(PeeringResponse)}{response.ToByteArray().PayloadSig()}: No-Hash {extraData}, {RemoteAddress}, r = {response.ReqHash.Memory.HashSig()}, _peerRequest = {_peerRequest.Count}");
                return;
            }
            
            //drop old requests
            if (peerRequest.TimestampMs.ElapsedMs() > parm_max_network_latency * 2)
                return;

            //Validated
            _logger.Trace($"<\\- {nameof(PeeringResponse)}: Accepted = {response.Status}, {Description}");

            //Race for 
            if (response.Status && _direction == 0)
            {
                var backoffTask = Task.Factory.StartNew(async () =>
                {
                    await Task.Delay(_random.Next(parm_max_network_latency) + parm_max_network_latency/2).ConfigureAwait(false);
                    var connectionTime = Stopwatch.StartNew();
                    if (!await ConnectAsync().ConfigureAwait(false))
                    {
                        _logger.Trace($"<\\- {nameof(PeeringResponse)}: [LOST] Connect to {Description}, {MetaDesc}");
                    }
                    else
                    {
                        await ZeroAtomicAsync((s, u, d) =>
                        {
                            var _this = (CcAdjunct)s;
                            if (_this.State == AdjunctState.Peering)
                                _this.State = AdjunctState.Standby;
                            return ValueTask.FromResult(true);
                        }).ConfigureAwait(false);
                    }

                    Interlocked.Add(ref ConnectionTime, connectionTime.ElapsedMilliseconds);
                    Interlocked.Increment(ref ConnectionCount);
                }, TaskCreationOptions.LongRunning);
            }
            else if (!response.Status && Hub.Neighbors.Count < CcCollective.MaxAdjuncts) //at least probe
            {
                var t = SendDiscoveryRequestAsync();
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
        private async ValueTask<int> SendMessageAsync(ByteString data, IoNodeAddress dest = null,
            CcDiscoveries.MessageTypes type = CcDiscoveries.MessageTypes.Undefined)
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
                    PublicKey = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcCollective.CcId.PublicKey)),
                    Type = (uint) type
                };

                packet.Signature =
                    UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcCollective.CcId.Sign(packet.Data!.Memory.AsArray(), 0, packet.Data.Length)));
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
                        .ConfigureAwait(false);
                    
                    sent = await MessageService.IoNetSocket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint)
                        .ConfigureAwait(false);
                }
                else //nominal
                {
                    sent = await MessageService.IoNetSocket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint)
                        .ConfigureAwait(false);
                }

                return sent;
#else
                return await MessageService.IoNetSocket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint)
                    .ConfigureAwait(false);
#endif
                
#if DEBUG
                //await sent.OverBoostAsync().ConfigureAwait(false);
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
            var discoveryRequest = await _discoveryRequest.ResponseAsync(extraData.ToString(), response.ReqHash).ConfigureAwait(false);

            if (discoveryRequest?.Key == null || !Assimilating || response.Peers.Count > parm_max_discovery_peers)
            {
                if (Proxy && Collected && response.Peers.Count <= parm_max_discovery_peers)
                    _logger.Debug(
                        $"{nameof(DiscoveryResponse)}{response.ToByteArray().PayloadSig()}: Reject, rq = {response.ReqHash.Memory.HashSig()}, {response.Peers.Count} > {parm_max_discovery_peers}? from {MakeId(CcDesignation.FromPubKey(packet.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData))}, RemoteAddress = {RemoteAddress}, request = {_discoveryRequest.Count}, matched[{extraData}] = {discoveryRequest.Payload != null}");
                return;
            }

            if (discoveryRequest.TimestampMs.ElapsedMs() > parm_max_network_latency * 2)
            {
                return;
            }

            var count = 0;

            _logger.Trace(
                $"<\\- {nameof(DiscoveryResponse)}: Received {response.Peers.Count} potentials from {Description}");

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
                var id = CcDesignation.FromPubKey(responsePeer.PublicKey.Span);
                if (Hub.Neighbors.Values.Any(n => ((CcAdjunct) n).Designation?.Equals(id) ?? false))
                    continue;

                var services = new CcService {CcRecord = new CcRecord()};
                var newRemoteEp = new IPEndPoint(IPAddress.Parse(responsePeer.Ip),
                    (int) responsePeer.Services.Map[CcService.Keys.peering.ToString()].Port);

                foreach (var kv in responsePeer.Services.Map)
                {
                    services.CcRecord.Endpoints.TryAdd(Enum.Parse<CcService.Keys>(kv.Key),
                        IoNodeAddress.Create($"{kv.Value.Network}://{responsePeer.Ip}:{kv.Value.Port}"));
                }

                //sanity check
                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (services == null || services.CcRecord.Endpoints.Count == 0)
                    continue;

                var assimilate = Task.Factory.StartNew(async c =>
                {
                    await Task.Delay(parm_max_network_latency * (int)c + (int)c * 1000).ConfigureAwait(false);
                    await CollectAsync(newRemoteEp, id, services);
                },++count, TaskCreationOptions.LongRunning);
                
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
            if (newRemoteEp != null && newRemoteEp.Equals(NATAddress?.IpEndPoint))
            {
                _logger.Fatal($"x {Description}");
                return false;
            }
            
            CcAdjunct newAdjunct = null;

            var source = new IoUdpClient<CcProtocMessage<Packet, CcDiscoveryBatch>>(MessageService, newRemoteEp);
            newAdjunct = (CcAdjunct) Hub.MallocNeighbor(Hub, source,
                Tuple.Create(id, services, newRemoteEp));

            if (await Hub.ZeroAtomicAsync(async (s, u, ___) =>
            {
                var t = (ValueTuple<CcAdjunct, CcAdjunct, bool>) u;
                var _this = t.Item1;
                var __newNeighbor = t.Item2;
                var __synAck = t.Item3;

                if (_this.Hub.Neighbors.Count > _this.CcCollective.MaxAdjuncts)
                {
                    //drop something
                    var q = _this.Hub.Neighbors.Values.Where(n =>
                        ((CcAdjunct) n).State < AdjunctState.Local || 
                        ((CcAdjunct) n).Proxy &&
                        ((CcAdjunct) n).Direction == Heading.Undefined &&
                        ((CcAdjunct) n).State < AdjunctState.Peering &&
                        ((CcAdjunct) n).Uptime.TickMs() > _this.parm_max_network_latency * 2);
                    
                    var assimilated = q.Where(n =>
                            ((CcAdjunct) n).Assimilating &&
                            ((CcAdjunct) n)._totalPats > _this.parm_min_pats_before_shuffle)
                        .OrderBy(n => ((CcAdjunct) n).Priority).FirstOrDefault();
                    
                    //try harder when this comes from a synack 
                    if (__synAck && assimilated == null && __newNeighbor.Designation.PublicKey[_this._random.Next(0, CcDesignation.PubKeyLen) - 1] < byte.MaxValue/2)
                    {
                        var selection = q.ToList();
                        if(selection.Count > 0)
                            assimilated = selection[Math.Max(_this._random.Next(selection.Count) - 1, 0)];
                    }

                    if (assimilated != null && ((CcAdjunct) assimilated).State < AdjunctState.Peering)
                    {
                        //Drop assimilated neighbors
                        _this._logger.Debug($"~ {assimilated.Description}");
                        await ((CcAdjunct) assimilated).ZeroAsync(new IoNanoprobe("Assimilated!")).ConfigureAwait(false);
                    }
                    else if(__synAck)
                    {
                        if(_this._logger.IsDebugEnabled)
                            _this._logger.Warn($"@ {_this.Description}");
                    }
                }

                //Transfer?
                if (_this.Hub.Neighbors.Count - 1 < _this.CcCollective.MaxAdjuncts)
                {
                    if (_this.Hub.Neighbors.TryAdd(__newNeighbor.Key, __newNeighbor))
                    {
                        //avoid races
                        //if (_this.Hub.Neighbors.Count - 1 < _this.CcCollective.MaxAdjuncts)
                        {
                            Hub.ZeroOnCascade(newAdjunct);
                            return true;
                        }

                        //we raced
                        //_this.Hub.Neighbors.TryRemove(__newNeighbor.Key, out _);
                    }

                    return false;
                }
                else
                    return false;
            }, ValueTuple.Create(this, newAdjunct, synAck)).ConfigureAwait(false))
            {
                //setup conduits to messages
                newAdjunct.MessageService.SetConduit(nameof(CcAdjunct), MessageService.GetConduit<CcProtocBatch<Packet, CcDiscoveryBatch>>(nameof(CcAdjunct)));
                newAdjunct.ExtGossipAddress = ExtGossipAddress; 
                newAdjunct.State = AdjunctState.Unverified;
                newAdjunct.Verified = false;

                var sub = newAdjunct.ZeroEvent(from =>
                {
                    try
                    {
                        if (Hub.Neighbors.TryRemove(newAdjunct.Key, out var n))
                        {
                            AutoPeeringEventService.AddEvent(new AutoPeerEvent
                            {
                                EventType = AutoPeerEventType.RemoveAdjunct,
                                Adjunct = new Adjunct()
                                {
                                    CollectiveId = CcCollective.Hub.Router.Designation.IdString(),
                                    Id = newAdjunct.Designation.IdString(),
                                }
                            });

                            //n.ZeroedFrom ??= @from;
                            if (((CcAdjunct) n).Assimilated)
                            {
                                _logger.Info($"% {Description} - from: {@from?.Description}");
                            }
                        }

                        //MessageService.WhiteList(__newNeighbor.RemoteAddress.Port);
                    }
                    catch
                    {
                        // ignored
                    }

                    return ValueTask.CompletedTask;
                });

                if (sub == null)
                {
                    _logger.Fatal($"Could not subscribe to zero: {Description}");
                    return false;
                }
                
                _logger.Debug($"# {newAdjunct.Description}");

                //emit event
                AutoPeeringEventService.AddEvent(new AutoPeerEvent
                {
                    EventType = AutoPeerEventType.AddAdjunct,
                    Adjunct = new Adjunct
                    {
                        Id = newAdjunct.Designation.IdString(),
                        CollectiveId = CcCollective.Hub.Router.Designation.IdString()
                    }
                });

                return await newAdjunct.SendPingAsync().ConfigureAwait(false);
            }
            else
            {
                if (newAdjunct != null)
                    await newAdjunct.ZeroAsync(new IoNanoprobe("CollectAsync")).ConfigureAwait(false);
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
            if (!Assimilating || request.Timestamp.ElapsedDelta() > parm_max_network_latency / 1000 * 2)
            {
                if (Collected)
                    _logger.Trace(
                        $"<\\- {nameof(DiscoveryRequest)}: [ABORTED], age = {request.Timestamp.ElapsedDelta()}, {Description}, {MetaDesc}");
                return;
            }

            var discoveryResponse = new DiscoveryResponse
            {
                ReqHash = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray()))),
            };

            var count = 0;
            // var certified = Hub.Neighbors.Values.Where(n =>
            //         ((CcNeighbor) n).Verified && n != this && !((CcNeighbor) n).IsLocal)
            //     .OrderByDescending(n => (int) ((CcNeighbor) n).Priority).ToList();
            var certified = Hub.Neighbors.Values.Where(n =>
                    ((CcAdjunct) n).Assimilating && n != this)
                .OrderByDescending(n => (int) ((CcAdjunct) n).Priority).ToList();
            foreach (var ioNeighbor in certified)
            {
                if (count == parm_max_discovery_peers)
                    break;

                discoveryResponse.Peers.Add(new Peer
                {
                    PublicKey = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(((CcAdjunct) ioNeighbor).Designation.PublicKey)),
                    Services = ((CcAdjunct) ioNeighbor).ServiceMap,
                    Ip = ((CcAdjunct) ioNeighbor).RemoteAddress.Ip
                });
                count++;
            }

            if (await SendMessageAsync(discoveryResponse.ToByteString(),
                    RemoteAddress, CcDiscoveries.MessageTypes.DiscoveryResponse)
                .ConfigureAwait(false) > 0)
            {
                _logger.Trace($"-/> {nameof(DiscoveryResponse)}: Sent {count} discoveries to {Description}");

                //Emit message event
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
                if (Collected)
                    _logger.Debug($"<\\- {nameof(DiscoveryRequest)}: [FAILED], {Description}, {MetaDesc}");
            }
        }

        private ServiceMap _serviceMapLocal;
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
            if (ping.Timestamp.ElapsedDelta() > parm_max_network_latency/1000 * 2) //TODO params
            {
                _logger.Trace($"<\\- {(Proxy ? "V>" : "X>")}{nameof(Ping)}: [WARN] Dropped stale, age = {ping.Timestamp.Elapsed()}s");
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
                    ReqHash = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcDesignation.Sha256.ComputeHash(ping.ToByteArray()))),
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
                    //ReqHash = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                    ReqHash = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(CcDesignation.Sha256.ComputeHash(ping.ToByteArray()))),
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
                if (await SendMessageAsync(pong.ToByteString(), toAddress, CcDiscoveries.MessageTypes.Pong)
                    .ConfigureAwait(false) > 0)
                {
                    _logger.Trace($"-/> {nameof(Pong)}: Sent SYN-ACK, to = {toAddress}");

                    AutoPeeringEventService.AddEvent(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.SendProtoMsg,
                        Msg = new ProtoMsg
                        {
                            CollectiveId = Hub.Router.Designation.IdString(),
                            Id = Base58.Bitcoin.Encode(packet.PublicKey.Span.Slice(0, 8).ToArray()),
                            Type = "pong"
                        }
                    });
                }

                if (CcCollective.UdpTunnelSupport && toAddress.Ip != toProxyAddress.Ip)
                {
                    await SendMessageAsync(pong.ToByteString(), toAddress, CcDiscoveries.MessageTypes.Pong)
                        .ConfigureAwait(false);

                }
                    
            }
            else //PROCESS ACK
            {
                if (await SendMessageAsync(data: pong.ToByteString(), type: CcDiscoveries.MessageTypes.Pong).ConfigureAwait(false) > 0)
                {
                    if (IsDroneConnected)
                        _logger.Trace($"-/> {nameof(Pong)}: Sent KEEPALIVE, to = {Description}");
                    else
                        _logger.Trace($"-/> {nameof(Pong)}: Sent ACK SYN, to = {Description}");

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
            var pingRequest = await _pingRequest.ResponseAsync(extraData.ToString(), pong.ReqHash).ConfigureAwait(false);
            
            if (pingRequest?.Key == null && Proxy)
            {
                pingRequest = await Hub.Router._pingRequest.ResponseAsync(extraData.ToString(), pong.ReqHash).ConfigureAwait(false);
            }

            if (pingRequest?.Key == null)
            {
                if (Collected)
                    _logger.Trace(
                        $"<\\- {nameof(Pong)}{pong.ToByteArray().PayloadSig()}: No-Hash!, rh = {pong.ReqHash.Memory.HashSig()}, tp = {TotalPats},  ssp = {SecondsSincePat}, d = {(AttachTimestamp > 0 ? (AttachTimestamp - LastPat).ToString() : "N/A")}, v = {Verified}, s = {extraData}, {Description}");
                return;
            }
            
            LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            Interlocked.Increment(ref _totalPats);
            
            //drop old matches
            if (pingRequest.TimestampMs.ElapsedMs() > parm_max_network_latency * 2)
                return;
            
            //Process SYN-ACK
            if (!Proxy)
            {
                var idCheck = CcDesignation.FromPubKey(packet.PublicKey.Span);
                var fromAddr = IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData);
                var keyStr = MakeId(idCheck, fromAddr);

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
                    await staleNeighbor.ZeroAsync(new IoNanoprobe($"{nameof(staleNeighbor)}")).ConfigureAwait(false);
                }

                var remoteServices = new CcService();
                foreach (var key in pong.Services.Map.Keys.ToList())
                    remoteServices.CcRecord.Endpoints.TryAdd(Enum.Parse<CcService.Keys>(key),
                        IoNodeAddress.Create(
                            $"{pong.Services.Map[key].Network}://{((IPEndPoint) extraData).Address}:{pong.Services.Map[key].Port}"));

                await CollectAsync(fromAddr.IpEndPoint, idCheck, remoteServices, true)
                    .ConfigureAwait(false);
            }
            else if (!Verified) //Process ACK SYN
            {
                State = AdjunctState.Verified;

                //TODO: vector?
                //set ext address as seen by neighbor
                ExtGossipAddress =
                    IoNodeAddress.Create(
                        $"tcp://{pong.DstAddr}:{CcCollective.Services.CcRecord.Endpoints[CcService.Keys.gossip].Port}");

                NATAddress =
                    IoNodeAddress.Create(
                        $"udp://{pong.DstAddr}:{CcCollective.Services.CcRecord.Endpoints[CcService.Keys.peering].Port}");

                Verified = true;

                _logger.Trace($"<\\- {nameof(Pong)}: ACK SYN: {Description}");


                //we need this peer request even if we are full. Verification needs this signal
                //if (CcCollective.EgressConnections < CcCollective.parm_max_outbound)
                {
                    _logger.Trace($"{(Proxy ? "V>" : "X>")}(acksyn): {(CcCollective.EgressConnections < CcCollective.parm_max_outbound ? "Send Peer REQUEST" : "Withheld Peer REQUEST")}, to = {Description}, from nat = {ExtGossipAddress}");
                    await SendPeerRequestAsync().ConfigureAwait(false);
                }
            }
            else //sometimes drones we know prod us for connections
            {
                if (Direction == Heading.Undefined && CcCollective.TotalConnections < CcCollective.MaxDrones && PeeringAttempts < parm_zombie_max_connection_attempts)
                {
                    _logger.Trace(
                        $"<\\- {nameof(Pong)}(acksyn-fast): {(CcCollective.EgressConnections < CcCollective.parm_max_outbound ? "Send Peer REQUEST" : "Withheld Peer REQUEST")}, to = {Description}, from nat = {ExtGossipAddress}");
                    await SendPeerRequestAsync().ConfigureAwait(false);
                }
                else
                {
                    _logger.Trace($"<\\- {nameof(Pong)}: {Description}");
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
                    // Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / parm_max_time_error *
                    //     parm_max_time_error + parm_max_time_error / 2
                     Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                };

                var reqBuf = pingRequest.ToByteString();

                // Is this a routed request?
                if (Proxy)
                {
                    //send
                    if (!await _pingRequest.ChallengeAsync(RemoteAddress.IpPort, reqBuf)
                        .ConfigureAwait(false))
                        return false;
                    var sent = await SendMessageAsync(data: reqBuf, type: CcDiscoveries.MessageTypes.Ping)
                        .ConfigureAwait(false);
                    if (sent > 0)
                    {
                        _logger.Trace($"-/> {nameof(Ping)}: Sent {sent} bytes, {Description}");

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
                    else
                    {
                        if (Collected)
                            _logger.Error($"-/> {nameof(Ping)}: [FAILED], {Description}");
                        return false;
                    }
                }
                else //The destination state was undefined, this is local
                {
                    var router = Hub.Router;

                    if (!await router._pingRequest.ChallengeAsync(dest.IpPort, reqBuf)
                        .ConfigureAwait(false))
                        return false;

                    var sent = await SendMessageAsync(reqBuf, dest, CcDiscoveries.MessageTypes.Ping)
                        .ConfigureAwait(false);
                    if (sent > 0)
                    {
                        _logger.Trace($"-/> {nameof(SendPingAsync)}:(X) {sent},{Description}");

                        AutoPeeringEventService.AddEvent(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = id,
                                Type = "ping"
                            }
                        });

                        return true;
                    }
                    else
                    {
                        if (!router.Zeroed() && !router.MessageService.Zeroed())
                            _logger.Error($"-/> {nameof(SendPingAsync)}:(X) [FAILED], {Description}");
                        return false;
                    }
                }
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
                        $"ERROR z = {Zeroed()}, dest = {dest}, source = {MessageService}, _discoveryRequest = {_discoveryRequest.Count}");
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
                    _logger.Debug(
                        $"{nameof(SendDiscoveryRequestAsync)}: [ABORTED], {Description}, s = {State}, a = {Assimilating}");
                    return false;
                }

                //rate limit
                if (_lastScan.Elapsed() < parm_max_network_latency / 1000)
                    await Task.Delay(parm_max_network_latency + _random.Next(parm_max_network_latency * 10)).ConfigureAwait(false);
                
                var discoveryRequest = new DiscoveryRequest
                {
                     Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                };

                var reqBuf = discoveryRequest.ToByteString();
                if (!await _discoveryRequest.ChallengeAsync(RemoteAddress.IpPort, reqBuf)
                    .ConfigureAwait(false))
                {
                    return false;
                }

                _lastScan = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                var sent = await SendMessageAsync(reqBuf, RemoteAddress,
                    CcDiscoveries.MessageTypes.DiscoveryRequest).ConfigureAwait(false);
                if (sent > 0)
                {
                    _logger.Trace(
                        $"-/> {nameof(SendDiscoveryRequestAsync)}{reqBuf.Memory.PayloadSig()}: Sent {sent}, {Description}");

                    //Emit message event
                    AutoPeeringEventService.AddEvent(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.SendProtoMsg,
                        Msg = new ProtoMsg
                        {
                            CollectiveId = Hub.Router.Designation.IdString(),
                            Id = Designation.IdString(),
                            Type = "discovery request"
                        }
                    });

                    return true;
                }
                else
                {
                    if (Collected)
                        _logger.Debug($"-/> {nameof(SendDiscoveryRequestAsync)}: [FAILED], {Description} ");
                }
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
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                };

                var reqBuf = peerRequest.ToByteString();
                if (!await _peerRequest.ChallengeAsync(RemoteAddress.IpPort, reqBuf)
                    .ConfigureAwait(false))
                {
                    return false;
                }
                
                var sent = await SendMessageAsync(reqBuf, null,
                    CcDiscoveries.MessageTypes.PeeringRequest).ConfigureAwait(false);
                if (sent > 0)
                {
                    _logger.Trace(
                        $"-/> {nameof(SendPeerRequestAsync)}{reqBuf.Memory.PayloadSig()}: Sent {sent}, {Description}");

                    AutoPeeringEventService.AddEvent(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.SendProtoMsg,
                        Msg = new ProtoMsg
                        {
                            CollectiveId = Hub.Router.Designation.IdString(),
                            Id = Designation.IdString(),
                            Type = "peer request"
                        }
                    });

                    return true;
                }
                else
                {
                    if (Collected)
                        _logger.Debug($"-/> {nameof(SendPeerRequestAsync)}: [FAILED], {Description}, {MetaDesc}");
                }
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
                    // Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / parm_max_time_error *
                    //     parm_max_time_error + parm_max_time_error / 2
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                };

                var sent = 0;
                _logger.Trace(
                    (sent = await SendMessageAsync(dropRequest.ToByteString(), dest,
                        CcDiscoveries.MessageTypes.PeeringDrop).ConfigureAwait(false)) > 0
                        ? $"-/> {nameof(PeeringDrop)}: Sent {sent}, {Description}"
                        : $"-/> {nameof(SendPeerDropAsync)}: [FAILED], {Description}, {MetaDesc}");

                if (sent > 0)
                {
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
                }
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
                if (!await ZeroAtomicAsync(async (s, u, d) =>
                {
                    var _this = (CcAdjunct) s;
                    var t = (ValueTuple<CcDrone, Heading>) u;
                    var __ioCcDrone = t.Item1;
                    var __direction = t.Item2;

                    //Guarantee hard cap on allowed drones here, other implemented caps are soft caps. This is the only one that matters
                    if (__ioCcDrone.Adjunct.CcCollective.TotalConnections >= __ioCcDrone.Adjunct.CcCollective.MaxDrones)
                    {
                        return false;
                    }

                    //Race for direction
                    if (Interlocked.CompareExchange(ref _this._direction, (int) __direction, (int) Heading.Undefined) !=
                        (int) Heading.Undefined)
                    {
                        _this._logger.Warn(
                            $"oz: race for {__direction} lost {__ioCcDrone.Description}, current = {_this.Direction}, {_this._drone?.Description}");
                        return false;
                    }
                    
                    _this._drone = __ioCcDrone ?? throw new ArgumentNullException($"{nameof(__ioCcDrone)}");
                    _this.State = AdjunctState.Connected;
                    return await CcCollective.ZeroAtomicAsync((ioNanite, _, disposing) => new ValueTask<bool>(((CcCollective)ioNanite).TotalConnections <= ((CcCollective)ioNanite).MaxDrones), force:true).ConfigureAwait(false);
                }, ValueTuple.Create(ccDrone, direction)))
                {
                    return false;
                }

                _logger.Trace($"{nameof(AttachPeerAsync)}: [WON] {_drone?.Description}");
                
                Assimilated = true;
                AttachTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                _zeroSub = ZeroEvent(async sender =>
                {
                    try
                    {
                        if (_drone != null)
                            await _drone.ZeroAsync(this).ConfigureAwait(false);
                    }
                    catch
                    {
                        // ignored
                    }
                });

                //emit event
                AutoPeeringEventService.AddEvent(new AutoPeerEvent
                {
                    EventType = AutoPeerEventType.AddDrone,
                    Drone = new Drone
                    {
                        CollectiveId = CcCollective.Hub.Router.Designation.IdString(),
                        Adjunct = ccDrone.Adjunct.Designation.IdString(),
                        Direction = ccDrone.Adjunct.Direction.ToString(),
                    }
                });

                var t = SendDiscoveryRequestAsync();

                return true;
            }
            catch (Exception e)
            {
                _logger.Trace(e, Description);
                return false;
            }
        }

        /// <summary>
        /// Detaches a peer from this neighbor
        /// </summary>
        public async ValueTask DetachPeerAsync()
        {
            CcDrone latch = null;
            if (!await ZeroAtomicAsync((s, u, d) =>
            {
                var _this = (CcAdjunct) s;
                latch = ((CcAdjunct) s)._drone;
                ((CcAdjunct)s)._drone = null;

                return ValueTask.FromResult(latch != null);
            }).ConfigureAwait(false))
            {
                return;
            }

            //send drop request
            await SendPeerDropAsync().ConfigureAwait(false);
            
            _logger.Trace($"{(Assimilated ? "Distinct" : "Common")} {Direction} peer detaching: s = {State}, a = {Assimilating}, p = {IsDroneConnected}, {latch?.Description ?? Description}");

            //Detach zeroed
            Unsubscribe(_zeroSub);
            _zeroSub = default;

            await latch.ZeroAsync(this).ConfigureAwait(false);
            
            Interlocked.Exchange(ref _direction, 0);
            AttachTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            TotalPats = 0;
            PeeringAttempts = 0;
            State = AdjunctState.Disconnected;

            //back off for a while... Try to re-establish a link 
            var t = Task.Run(async () =>
            {
                await Task.Delay(parm_max_network_latency).ConfigureAwait(false);
                await SendPingAsync().ConfigureAwait(false);
            });

            //emit event
            
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

                nextState.Enter(value);
                _currState.Exit(nextState);

                var prevState = _currState;
                _currState = nextState;
                if (StateTransitionHistory[(int) prevState.Value] != null)
                {
                    StateTransitionHistory[(int) prevState.Value].Repeat = prevState;
                }
                else
                {
                    StateTransitionHistory[(int) prevState.Value] = prevState;
                }
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