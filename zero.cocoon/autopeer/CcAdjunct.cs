﻿//#define LOSS
using System;
using System.Buffers;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using Proto;
using zero.cocoon.identity;
using zero.cocoon.models;
using zero.cocoon.models.services;
using zero.cocoon.models.sources;
using zero.core.conf;
using zero.core.core;
using zero.core.misc;
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
    public class CcAdjunct : IoNeighbor<CcSubspaceMessage>
    {
        public CcAdjunct(CcHub node, IoNetClient<CcSubspaceMessage> ioNetClient,
            object extraData = null, CcService services = null)
            : base
            (
                node,
                ioNetClient,
                userData => new CcSubspaceMessage("adjunct RX", $"{ioNetClient.Key}", ioNetClient),
                ioNetClient.ConcurrencyLevel,
                ioNetClient.ConcurrencyLevel
            )
        {
            _logger = LogManager.GetCurrentClassLogger();

            _pingRequest = new IoZeroMatcher<ByteString>(nameof(_pingRequest), Source.ConcurrencyLevel, parm_max_network_latency, CcNode.parm_max_inbound);
            _peerRequest = new IoZeroMatcher<ByteString>(nameof(_peerRequest), Source.ConcurrencyLevel, parm_max_network_latency, CcNode.parm_max_inbound);
            _discoveryRequest = new IoZeroMatcher<ByteString>(nameof(_discoveryRequest), Source.ConcurrencyLevel, parm_max_network_latency, CcNode.parm_max_inbound);

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
                Designation = CcNode.CcId;
                Services = services ?? ((CcHub) node).Services;
                Key = MakeId(Designation, CcNode.ExtAddress);
            }

            if (Proxy)
            {
                State = AdjunctState.Unverified;
                var task = Task.Factory.StartNew(async () =>
                {
                    while (!Zeroed())
                    {
                        await WatchdogAsync().ConfigureAwait(false);
                        await Task.Delay(_random.Next(parm_zombie_max_ttl / 2) * 1000 + parm_zombie_max_ttl / 4 * 1000,AsyncTasks.Token).ConfigureAwait(false);
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
                if (_lastDescGen.ElapsedMsDelta() > 10000 && _description != null)
                    return _description;
                
                _lastDescGen = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                try
                {
                    return _description = $"`adjunct({(Verified ? "+v" : "-v")},{(ConnectedAtLeastOnce ? "C" : "dc")})[{(Proxy?TotalPats.ToString().PadLeft(3):"  0")}:{Priority}] local: {MessageService.IoNetSocket.LocalAddress} - {MessageService.IoNetSocket.RemoteAddress}, [{Designation.IdString()}]'";
                }
                catch (Exception e)
                {
                    if (Collected)
                        _logger.Debug(e, Description);
                    return _description?? $"`adjunct({(Verified ? "+v" : "-v")},{(ConnectedAtLeastOnce ? "C" : "dc")})[{(Proxy?TotalPats.ToString().PadLeft(3):"  0")}:{Priority}] local: {MessageService?.IoNetSocket?.LocalAddress} - {MessageService?.IoNetSocket?.RemoteAddress}, [{Designation.IdString()}]'";;
                }
            }
        }

        /// <summary>
        /// return extra information about the state
        /// </summary>
        public string MetaDesc =>
            $"(d = {Direction}, s = {State}, v = {Verified}, a = {Assimilated}, att = {IsPeerAttached}, c = {IsPeerConnected}, r = {PeeringAttempts}, g = {IsGossiping}, arb = {IsArbitrating}, o = {MessageService.IsOperational}, w = {TotalPats})";

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
        protected IoNetClient<CcSubspaceMessage> MessageService => (IoNetClient<CcSubspaceMessage>) Source;

        /// <summary>
        /// The udp routing table 
        /// </summary>
        private CcAdjunct[] _routingTable;

        /// <summary>
        /// The gossip peer associated with this neighbor
        /// </summary>
        private volatile CcDrone _drone;

        /// <summary>
        /// Whether The peer is attached
        /// </summary>
        public bool IsPeerAttached => _drone != null;

        /// <summary>
        /// Whether the peer is nominal
        /// </summary>
        public bool IsPeerConnected => IsPeerAttached && (_drone?.IoSource?.IsOperational ?? false) && State == AdjunctState.Connected;

        /// <summary>
        /// Is this the local listener
        /// </summary>
        public bool IsLocal => !Proxy;

        /// <summary>
        /// Is autopeering
        /// </summary>
        public bool Assimilated => !Zeroed() && Verified && Proxy;

        /// <summary>
        /// Whether the node, peer and adjunctare nominal
        /// </summary>
        public bool IsGossiping => Assimilated && IsPeerConnected;

        /// <summary>
        /// Looks for a zombie peer
        /// </summary>
        public bool PolledZombie => Direction != Heading.Undefined && !(Assimilated && IsPeerConnected);

        /// <summary>
        /// Indicates whether we have successfully established a connection before
        /// </summary>
        public volatile bool ConnectedAtLeastOnce;

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
        /// The adjunctaddress
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
        /// Tcp Readahead
        /// </summary>
        public const int TcpReadAhead = 1;

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
        public bool Inbound => Direction == Heading.Ingress && IsPeerConnected; //TODO

        /// <summary>
        /// outbound
        /// </summary>
        public bool Outbound => Direction == Heading.Egress && IsPeerConnected;

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
        /// The node that this adjunctbelongs to
        /// </summary>
        public CcNode CcNode => Hub.CcNode;

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
        private IoConduit<CcProtocolMessage> _protocolConduit;

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
        /// Holds unrouted ping requests
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
        //private ByteString GetSalt => _curSalt = ByteString.CopyFrom(CcIdentity.Sha256.ComputeHash(Encoding.ASCII.GetBytes((DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 120 * 60).ToString())), 0, parm_salt_length);

        private ByteString GetSalt
        {
            get
            {
                if (_curSalt == null || _curSaltStamp.Elapsed() > parm_salt_ttl)
                {
                    using var rand = new RNGCryptoServiceProvider();
                    _curSalt = ByteString.CopyFrom(new byte[parm_salt_length]);
                    rand.GetNonZeroBytes(_curSalt.ToByteArray());
                    _curSaltStamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                }

                return _curSalt;
            }
        }

        /// <summary>
        /// heap
        /// </summary>
        public ArrayPool<ValueTuple<IIoZero, IMessage, object, Packet>> ArrayPoolProxy { get; protected set; }

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
        /// Maximum number of services supported
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_time_error = 10;

        /// <summary>
        /// Average time allowed between pats
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_zombie_max_ttl = 120;

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
        /// Handle to peer zero sub
        /// </summary>
        private IoZeroSub _zeroSub;

        /// <summary>
        /// A service map helper
        /// </summary>
        private ServiceMap ServiceMap
        {
            get
            {
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

                return mapping;
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
                    Interlocked.CompareExchange(ref Router._routingTable[((IoNetClient<CcSubspaceMessage>) IoSource).IoNetSocket.RemoteNodeAddress.Port], null,this);
                }
                catch
                {
                    // ignored
                }

                await SendPeerDropAsync().ConfigureAwait(false);
            }
            
            State = AdjunctState.ZeroState;

            if (ConnectedAtLeastOnce && Direction != Heading.Undefined)
                _logger.Info($"- `{(ConnectedAtLeastOnce ? "Distinct" : "Common")} {Direction}: {Description}, from: {ZeroedFrom?.Description}");

            await DetachPeerAsync(_drone, true).ConfigureAwait(false);

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
        /// Ensures that the peer is running
        /// </summary>
        public async Task WatchdogAsync()
        {
            // Verify request
            if (!Assimilated)
            {
                return;
            }
            
            //Moderate requests if we ensured at least once
            if (SecondsSincePat < parm_zombie_max_ttl / 2)
            {
                return;
            }
            
            //Are we limping?
            if (!Proxy && Hub.Neighbors.Count <= 3)
            {
                await CcNode.BootAsync().ConfigureAwait(false);
            }

            //Watchdog failure
            if (SecondsSincePat > parm_zombie_max_ttl * 4)
            {
                var reconnect = this.Direction == Heading.Egress;
                var address = RemoteAddress;

                if (TotalPats > 1)
                    _logger.Debug($"w {Description}");
                else
                    _logger.Trace($"w {Description}, s = {SecondsSincePat} >> {parm_zombie_max_ttl * 2}, {MetaDesc}");
                
                await ZeroAsync(new IoNanoprobe($"-wd: l = {SecondsSincePat}s ago...")).ConfigureAwait(false);
                
                if (reconnect)
                    await Router.SendPingAsync(address).ConfigureAwait(false);

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
            if (!Assimilated && !IsPeerAttached)
            {
#if DEBUG
                if (IsPeerAttached || State < NeighborState.Disconnected)
                    _logger.Fatal($"Incorrect state, {MetaDesc}, {Description}");
#endif

                _logger.Debug($"Connection aborted, {MetaDesc}, {Description}");
                return false;
            }

            State = ConnectedAtLeastOnce ? AdjunctState.Reconnecting : AdjunctState.Connecting;

            //Attempt the connection, race to win
            if (await CcNode.ConnectToPeerAsync(this).ConfigureAwait(false))
            {
                _logger.Trace($"Connected to {Description}");
                return true;
            }

            _logger.Trace($"{nameof(CcNode.ConnectToPeerAsync)}: [LOST], {Description}, {MetaDesc}");
            return false;
        }

        /// <summary>
        /// Processes protocol messages
        /// </summary>
        /// <param name="msg">The consumer that need processing</param>
        /// <param name="msgArbiter">The arbiter</param>
        /// <param name="processCallback">The process callback</param>
        /// <param name="zeroClosure"></param>
        /// <returns></returns>
        private async Task ProcessMsgBatchAsync(IoSink<CcProtocolMessage> msg,
            IoConduit<CcProtocolMessage> msgArbiter,
            Func<ValueTuple<IIoZero, IMessage, object, Packet>, IoConduit<CcProtocolMessage>, IIoZero, Task>
                processCallback, IIoZero zeroClosure)
        {
            if (msg == null)
                return;

            //var stopwatch = Stopwatch.StartNew();
            try
            {
                var protocolMsgs = ((CcProtocolMessage) msg).Batch;

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
                ((CcProtocolMessage) msg).Batch[0] = default;
                ArrayPoolProxy?.Return(((CcProtocolMessage) msg).Batch);
            }
        }
        
        /// <summary>
        /// Processes a protocol message
        /// </summary>
        /// <returns></returns>
        private async Task ProcessAsync()
        {
            _protocolConduit ??= MessageService.GetChannel<CcProtocolMessage>(nameof(CcAdjunct));

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
                        _protocolConduit = MessageService.EnsureChannel<CcProtocolMessage>(nameof(CcAdjunct));
                        if (_protocolConduit != null)
                            ArrayPoolProxy = ((CcProtocolBuffer) _protocolConduit.Source).ArrayPoolProxy;
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
                                        var (_, message, extraData, packet) = msgBatch;
                                        try
                                        {
                                            var ccNeighbor = __this.Router._routingTable[((IPEndPoint) extraData).Port] ?? 
                                                             (CcAdjunct) __this.Hub.Neighbors.Values.FirstOrDefault(n => ((IoNetClient<CcSubspaceMessage>)((CcAdjunct)n).Source).IoNetSocket.RemoteNodeAddress.Port == ((IPEndPoint)extraData).Port);
                                            
                                            __this.Router._routingTable[((IPEndPoint) extraData).Port] ??= ccNeighbor;
                                            
                                            ccNeighbor ??= __this.Hub.Router;

                                            switch ((CcSubspaceMessage.MessageTypes)packet.Type)
                                            {
                                                case CcSubspaceMessage.MessageTypes.Ping:
                                                    await ccNeighbor.ProcessAsync((Ping) message, extraData, packet)
                                                        .ConfigureAwait(false);
                                                    break;
                                                case CcSubspaceMessage.MessageTypes.Pong:
                                                    await ccNeighbor.ProcessAsync((Pong) message, extraData, packet)
                                                        .ConfigureAwait(false);
                                                    break;
                                                case CcSubspaceMessage.MessageTypes.DiscoveryRequest:
                                                    await ccNeighbor
                                                        .ProcessAsync((DiscoveryRequest) message, extraData, packet)
                                                        .ConfigureAwait(false);
                                                    break;
                                                case CcSubspaceMessage.MessageTypes.DiscoveryResponse:
                                                    await ccNeighbor.ProcessAsync((DiscoveryResponse) message,
                                                        extraData, packet).ConfigureAwait(false);
                                                    break;
                                                case CcSubspaceMessage.MessageTypes.PeeringRequest:
                                                    await ccNeighbor
                                                        .ProcessAsync((PeeringRequest) message, extraData, packet)
                                                        .ConfigureAwait(false);
                                                    break;
                                                case CcSubspaceMessage.MessageTypes.PeeringResponse:
                                                    await ccNeighbor
                                                        .ProcessAsync((PeeringResponse) message, extraData, packet)
                                                        .ConfigureAwait(false);
                                                    break;
                                                case CcSubspaceMessage.MessageTypes.PeeringDrop:
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
            var diff = 0;
            if (!Assimilated || request.Timestamp.ElapsedDelta() > parm_max_network_latency/1000 * 2)
            {
                _logger.Trace(
                    $"{(Proxy ? "V>" : "X>")}{nameof(PeeringDrop)}: Ignoring {diff}s old/invalid request, error = ({diff})");
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
            if (!Assimilated)
            {
                //send reject so that the sender's state can be fixed
                var reject = new PeeringResponse
                {
                    ReqHash = ByteString.CopyFrom(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                    Status = false
                };

                var remote = IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData);                    
                if (await Router.SendMessageAsync(reject.ToByteString(), remote,
                    CcSubspaceMessage.MessageTypes.PeeringResponse).ConfigureAwait(false) > 0)
                {
                    _logger.Trace($"-/> {nameof(PeeringResponse)}: Sent {(reject.Status ? "ACCEPT" : "REJECT")}, {Description}");
                }
                else
                    _logger.Debug($"<\\- {nameof(PeeringRequest)}: [FAILED], {Description}, {MetaDesc}");
                
                //DMZ-syn
                if (Collected)
                {
                    //We syn here (Instead of in process ping) to force the other party to do some work (this) before we do work (verify).
                    if (await Router.SendPingAsync(remote)
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
                ReqHash = ByteString.CopyFrom(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                Status = CcNode.IngressConnections < CcNode.parm_max_inbound & _direction == 0
            };
            
            if (await SendMessageAsync(data: peeringResponse.ToByteString(),
                type: CcSubspaceMessage.MessageTypes.PeeringResponse).ConfigureAwait(false) > 0)
            {
                _logger.Trace(
                    $"-/> {nameof(PeeringResponse)}: Sent {(peeringResponse.Status ? "ACCEPT" : "REJECT")}, {Description}");
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
            if (!Assimilated)
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

            await ZeroAtomicAsync((s, u, d) =>
            {
                var _this = (CcAdjunct) s;
                if (_this.State == AdjunctState.Peering)
                    _this.State = AdjunctState.Standby;
                return ValueTask.FromResult(true);
            }).ConfigureAwait(false);

            //Race for 
            if (response.Status && _direction == 0)
            {
                if (!await ConnectAsync().ConfigureAwait(false))
                {
                    _logger.Trace($"<\\- {nameof(PeeringResponse)}: [LOST] Connect to {Description}, {MetaDesc}");
                }
                else
                {
                    await ZeroAtomicAsync((s, u, d) =>
                    {
                        var _this = (CcAdjunct) s;
                        if (_this.State == AdjunctState.Peering)
                            _this.State = AdjunctState.Standby;
                        return ValueTask.FromResult(true);
                    }).ConfigureAwait(false);
                }
            } //Were we inbound?
        }

        /// <summary>
        /// Sends a message to the neighbor
        /// </summary>
        /// <param name="data">The message data</param>
        /// <param name="dest">The destination address</param>
        /// <param name="type">The message type</param>
        /// <returns></returns>
        private async ValueTask<int> SendMessageAsync(ByteString data, IoNodeAddress dest = null,
            CcSubspaceMessage.MessageTypes type = CcSubspaceMessage.MessageTypes.Undefined)
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
                    PublicKey = ByteString.CopyFrom(CcNode.CcId.PublicKey),
                    Type = (uint) type
                };

                packet.Signature =
                    ByteString.CopyFrom(CcNode.CcId.Sign(packet.Data!.Memory.AsArray(), 0, packet.Data.Length));
                var msgRaw = packet.ToByteArray();


//simulate byzantine failure.                
#if LOSS
                var sent = 0;
                var loss = 27;
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
#else
                var sent = await MessageService.IoNetSocket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint)
                    .ConfigureAwait(false);
#endif
                
#if DEBUG
                //await sent.OverBoostAsync().ConfigureAwait(false);
                _logger.Trace(
                    $"=/> {Enum.GetName(typeof(CcPeerMessage.MessageTypes), packet.Type)} {MessageService.IoNetSocket.LocalAddress} /> {dest.IpEndPoint}>>{data.Memory.PayloadSig()}: s = {sent}");
#endif
                return sent;
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

            if (discoveryRequest?.Key == null || !Assimilated || response.Peers.Count > parm_max_discovery_peers)
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
                if (Hub.Neighbors.Count > CcNode.MaxNeighbors && count > parm_min_spare_bays)
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
                    responsePeer.Ip == CcNode.ExtAddress.Ip &&
                    CcNode.ExtAddress.Port == responsePeer.Services.Map[CcService.Keys.peering.ToString()].Port)
                    continue;

                //Never add ourselves (by ID)
                if (responsePeer.PublicKey.SequenceEqual(CcNode.CcId.PublicKey))
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
                    await Task.Delay(parm_max_network_latency * (int) c).ConfigureAwait(false);
                    await CollectAsync(newRemoteEp, id, services);
                },++count, TaskCreationOptions.LongRunning);
                
            }
        }
        
        /// <summary>
        /// Collect another drone into the fold
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

            var source = new IoUdpClient<CcSubspaceMessage>(MessageService, newRemoteEp);
            newAdjunct = (CcAdjunct) Hub.MallocNeighbor(Hub, source,
                Tuple.Create(id, services, newRemoteEp));

            if (await Hub.ZeroAtomicAsync(async (s, u, ___) =>
            {
                var t = (ValueTuple<CcAdjunct, CcAdjunct, bool>) u;
                var _this = t.Item1;
                var __newNeighbor = t.Item2;
                var __synAck = t.Item3;

                if (_this.Hub.Neighbors.Count > _this.CcNode.MaxNeighbors)
                {
                    //drop something
                    var q = _this.Hub.Neighbors.Values.Where(n =>
                        ((CcAdjunct) n).State < AdjunctState.Local || 
                        ((CcAdjunct) n).Proxy &&
                        ((CcAdjunct) n).Direction == Heading.Undefined &&
                        ((CcAdjunct) n).State < AdjunctState.Peering &&
                        ((CcAdjunct) n).Uptime.TickMs() > _this.parm_max_network_latency * 2);
                    
                    var assimilated = q.Where(n =>
                            ((CcAdjunct) n).Assimilated &&
                            ((CcAdjunct) n)._totalPats > _this.parm_min_pats_before_shuffle)
                        .OrderBy(n => ((CcAdjunct) n).Priority).FirstOrDefault();
                    
                    //try harder when this comes from a synack 
                    if (__synAck && assimilated == null && __newNeighbor.Designation.PublicKey[_this._random.Next(byte.MaxValue)] < byte.MaxValue/2)
                    {
                        var selection = q.ToList();
                        if(selection.Count > 0)
                            assimilated = selection[Math.Max(_this._random.Next(selection.Count) - 1, 0)];
                    }

                    if (assimilated != null && ((CcAdjunct) assimilated).State < AdjunctState.Peering)
                    {
                        //Drop assimilated neighbors
                        _this._logger.Debug($"~ {assimilated.Description}");
                        await ((CcAdjunct) assimilated).ZeroAsync(new IoNanoprobe("Assimilated")).ConfigureAwait(false);
                    }
                    else if(__synAck)
                    {
                        if(_this._logger.IsDebugEnabled)
                            _this._logger.Warn($"@ {_this.Description}");
                    }
                }

                //Transfer?
                if (_this.Hub.Neighbors.Count <= _this.CcNode.MaxNeighbors)
                    return _this.Hub.Neighbors.TryAdd(__newNeighbor.Key, __newNeighbor);
                else
                    return false;
            }, ValueTuple.Create(this, newAdjunct, synAck)).ConfigureAwait(false))
            {
                //setup conduits to messages
                newAdjunct.MessageService.SetChannel(nameof(CcAdjunct),
                    MessageService.GetChannel<CcProtocolMessage>(nameof(CcAdjunct)));
                newAdjunct.ExtGossipAddress = ExtGossipAddress; 
                newAdjunct.State = AdjunctState.Unverified;
                newAdjunct.Verified = false;

                Hub.ZeroOnCascade(newAdjunct);
                
                var sub = newAdjunct.ZeroEvent(_ =>
                {
                    try
                    {
                        if (Hub.Neighbors.TryRemove(newAdjunct.Key, out var n))
                        {
                            if(((CcAdjunct)n).ConnectedAtLeastOnce)
                                _logger.Info( $"% {Description}");
                        }

                        //MessageService.WhiteList(__newNeighbor.RemoteAddress.Port);
                    }
                    catch
                    {
                        // ignored
                    }

                    return Task.CompletedTask;
                });

                if (sub == null)
                {
                    _logger.Fatal($"Could not subscribe to zero: {Description}");
                    return false;
                }
                
                _logger.Debug($"# {newAdjunct.Description}");
                return await newAdjunct.SendPingAsync().ConfigureAwait(false);
            }
            else
            {
                if (newAdjunct != null)
                    await newAdjunct.ZeroAsync(new IoNanoprobe("AssimilateNeighborAsync")).ConfigureAwait(false);
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
            if (!Assimilated || request.Timestamp.ElapsedDelta() > parm_max_network_latency / 1000 * 2)
            {
                if (Collected)
                    _logger.Trace(
                        $"<\\- {nameof(DiscoveryRequest)}: [ABORTED], age = {request.Timestamp.ElapsedDelta()}, {Description}, {MetaDesc}");
                return;
            }

            var discoveryResponse = new DiscoveryResponse
            {
                ReqHash = ByteString.CopyFrom(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
            };

            var count = 0;
            // var certified = Hub.Neighbors.Values.Where(n =>
            //         ((CcNeighbor) n).Verified && n != this && !((CcNeighbor) n).IsLocal)
            //     .OrderByDescending(n => (int) ((CcNeighbor) n).Priority).ToList();
            var certified = Hub.Neighbors.Values.Where(n =>
                    ((CcAdjunct) n).Assimilated && n != this)
                .OrderByDescending(n => (int) ((CcAdjunct) n).Priority).ToList();
            foreach (var ioNeighbor in certified)
            {
                if (count == parm_max_discovery_peers)
                    break;

                discoveryResponse.Peers.Add(new Peer
                {
                    PublicKey = ByteString.CopyFrom(((CcAdjunct) ioNeighbor).Designation.PublicKey),
                    Services = ((CcAdjunct) ioNeighbor).ServiceMap,
                    Ip = ((CcAdjunct) ioNeighbor).RemoteAddress.Ip
                });
                count++;
            }

            if (await SendMessageAsync(discoveryResponse.ToByteString(),
                    RemoteAddress, CcSubspaceMessage.MessageTypes.DiscoveryResponse)
                .ConfigureAwait(false) > 0)
            {
                _logger.Trace($"-/> {nameof(DiscoveryResponse)}: Sent {count} discoveries to {Description}");
            }
            else
            {
                if (Collected)
                    _logger.Debug($"<\\- {nameof(DiscoveryRequest)}: [FAILED], {Description}, {MetaDesc}");
            }
        }

        /// <summary>
        /// Ping message
        /// </summary>
        /// <param name="ping">The ping packet</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(Ping ping, object extraData, Packet packet)
        {
            var remoteEp = (IPEndPoint) extraData;
            
            if (ping.Timestamp.ElapsedDelta() > parm_max_network_latency/1000 * 2) //TODO params
            {
                _logger.Trace($"<\\- {(Proxy ? "V>" : "X>")}{nameof(Ping)}: [WARN] Dropped stale, age = {ping.Timestamp.Elapsed()}s");
                return;
            }

            if (!Proxy && ((IPEndPoint) extraData).Equals(ExtGossipAddress?.IpEndPoint))
            {
                _logger.Fatal($"<\\- {(Proxy ? "V>" : "X>")}{nameof(Ping)}: Dropping ping from self: {extraData}");
                return;
            }

            //TODO optimize
            var gossipAddress =
                Hub.Services.CcRecord.Endpoints[CcService.Keys.gossip];
            var peeringAddress =
                Hub.Services.CcRecord.Endpoints[CcService.Keys.peering];
            var fpcAddress =
                Hub.Services.CcRecord.Endpoints[CcService.Keys.fpc];

            var pong = new Pong
            {
                ReqHash = ByteString.CopyFrom(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                DstAddr = $"{remoteEp.Address}", //TODO, add port somehow
                Services = new ServiceMap
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

            var toAddress = IoNodeAddress.Create($"udp://{remoteEp.Address}:{ping.SrcPort}");
            //PROCESS DMZ/SYN
            if (!Proxy)
            {
                IoNodeAddress toProxyAddress = null;

                if (CcNode.UdpTunnelSupport)
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
                if (await SendMessageAsync(pong.ToByteString(), toAddress, CcSubspaceMessage.MessageTypes.Pong)
                    .ConfigureAwait(false) > 0)
                {
                    _logger.Trace($"-/> {nameof(Pong)}: Sent SYN-ACK, to = {toAddress}");
                }

                if (CcNode.UdpTunnelSupport && toAddress.Ip != toProxyAddress.Ip)
                    await SendMessageAsync(pong.ToByteString(), toAddress, CcSubspaceMessage.MessageTypes.Pong)
                        .ConfigureAwait(false);
            }
            else //PROCESS ACK
            {
                if ((await SendMessageAsync(data: pong.ToByteString(), type: CcSubspaceMessage.MessageTypes.Pong)
                    
                    .ConfigureAwait(false)) > 0)
                {
                    if (IsPeerConnected)
                        _logger.Trace($"-/> {nameof(Pong)}: Sent KEEPALIVE, to = {Description}");
                    else
                        _logger.Trace($"-/> {nameof(Pong)}: Sent ACK SYN, to = {Description}");
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
                        $"tcp://{pong.DstAddr}:{CcNode.Services.CcRecord.Endpoints[CcService.Keys.gossip].Port}");

                NATAddress =
                    IoNodeAddress.Create(
                        $"udp://{pong.DstAddr}:{CcNode.Services.CcRecord.Endpoints[CcService.Keys.peering].Port}");

                Verified = true;

                _logger.Trace($"<\\- {nameof(Pong)}: ACK SYN: {Description}");

                //if (CcNode.EgressConnections < CcNode.parm_max_outbound)
                {
                    _logger.Trace(
                        $"{(Proxy ? "V>" : "X>")}(acksyn): {(CcNode.EgressConnections < CcNode.parm_max_outbound ? "Send Peer REQUEST" : "Withheld Peer REQUEST")}, to = {Description}, from nat = {ExtGossipAddress}");
                    await SendPeerRequestAsync().ConfigureAwait(false);
                }
            }
            else
            {
                _logger.Trace($"<\\- {nameof(Pong)}: {Description}");
            }
        }

        /// <summary>
        /// Sends a universal ping packet
        /// </summary>
        /// <param name="dest">The destination address</param>
        /// <returns>Task</returns>
        public async ValueTask<bool> SendPingAsync(IoNodeAddress dest = null)
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
                    NetworkId = 8,
                    Version = 0,
                    SrcAddr = "0.0.0.0",
                    SrcPort = (uint) CcNode.Services.CcRecord.Endpoints[CcService.Keys.peering].Port,
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
                    var sent = await SendMessageAsync(data: reqBuf, type: CcSubspaceMessage.MessageTypes.Ping)
                        .ConfigureAwait(false);
                    if (sent > 0)
                    {
                        _logger.Trace($"-/> {nameof(Ping)}: Sent {sent} bytes, {Description}");
                        return true;
                    }
                    else
                    {
                        if (Collected)
                            _logger.Debug($"-/> {nameof(Ping)}: [FAILED], {Description}");
                        return false;
                    }
                }
                else //The destination state was undefined, this is local
                {
                    var router = Hub.Router;

                    if (!await router._pingRequest.ChallengeAsync(dest.IpPort, reqBuf)
                        .ConfigureAwait(false))
                        return false;

                    var sent = await SendMessageAsync(reqBuf, dest, CcSubspaceMessage.MessageTypes.Ping)
                        .ConfigureAwait(false);
                    if (sent > 0)
                    {
                        _logger.Trace($"-/> {nameof(SendPingAsync)}:(X) {sent},{Description}");
                        return true;
                    }
                    else
                    {
                        if (!router.Zeroed() && !router.MessageService.Zeroed())
                            _logger.Debug($"-/> {nameof(SendPingAsync)}:(X) [FAILED], {Description}");
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
                if (!Assimilated || State < AdjunctState.Verified)
                {
                    _logger.Debug(
                        $"{nameof(SendDiscoveryRequestAsync)}: [ABORTED], {Description}, s = {State}, a = {Assimilated}");
                    return false;
                }

                var discoveryRequest = new DiscoveryRequest
                {
                    // Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / parm_max_time_error *
                    //     parm_max_time_error + parm_max_time_error / 2
                     Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                };

                var reqBuf = discoveryRequest.ToByteString();
                if (!await _discoveryRequest.ChallengeAsync(RemoteAddress.IpPort, reqBuf)
                    .ConfigureAwait(false))
                {
                    return false;
                }

                //rate limit
                if (_lastScan.Elapsed() < CcNode.parm_mean_pat_delay * 4)
                    return false;

                _lastScan = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                var sent = await SendMessageAsync(reqBuf, RemoteAddress,
                    CcSubspaceMessage.MessageTypes.DiscoveryRequest).ConfigureAwait(false);
                if (sent > 0)
                {
                    _logger.Trace(
                        $"-/> {nameof(SendDiscoveryRequestAsync)}{reqBuf.Memory.PayloadSig()}: Sent {sent}, {Description}");
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
                //atomic peer request
                if (await ZeroAtomicAsync((s, u, d) =>
                {
                    var _this = (CcAdjunct) s;
                    if (_this.State > AdjunctState.Peering)
                        return ValueTask.FromResult(true);

                    if (_this.State == AdjunctState.Peering)
                    {
                        _this.State = AdjunctState.Standby;
                        return ValueTask.FromResult(true);
                    }

                    return ValueTask.FromResult(false);
                }).ConfigureAwait(false))
                {
                    return false;
                }


                if (!Assimilated || IsPeerConnected)
                {
                    if (Collected)
                        _logger.Warn(
                            $"{nameof(SendPeerRequestAsync)}: [ABORTED], {Description}, s = {State}, a = {Assimilated}, p = {IsPeerConnected}");
                    return false;
                }

                //assimilate count
                Interlocked.Increment(ref PeeringAttempts);

                var peerRequest = new PeeringRequest
                {
                    Salt = new Salt
                        {ExpTime = (ulong) DateTimeOffset.UtcNow.AddHours(2).ToUnixTimeSeconds(), Bytes = GetSalt},
                    // Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / parm_max_time_error *
                    //     parm_max_time_error + parm_max_time_error / 2
                     Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                };

                var reqBuf = peerRequest.ToByteString();
                if (!await _peerRequest.ChallengeAsync(RemoteAddress.IpPort, reqBuf)
                    .ConfigureAwait(false))
                {
                    return false;
                }


                var sent = await SendMessageAsync(reqBuf, RemoteAddress,
                    CcSubspaceMessage.MessageTypes.PeeringRequest).ConfigureAwait(false);
                if (sent > 0)
                {
                    _logger.Trace(
                        $"-/> {nameof(SendPeerRequestAsync)}{reqBuf.Memory.PayloadSig()}: Sent {sent}, {Description}");
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
                if (!Assimilated)
                {
                    _logger.Trace($"{nameof(SendPeerDropAsync)}: [ABORTED], {Description}, {MetaDesc}");
                    return;
                }

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
                        CcSubspaceMessage.MessageTypes.PeeringDrop).ConfigureAwait(false)) > 0
                        ? $"-/> {nameof(PeeringDrop)}: Sent {sent}, {Description}"
                        : $"-/> {nameof(SendPeerDropAsync)}: [FAILED], {Description}, {MetaDesc}");
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
                        $"{nameof(SendPeerDropAsync)}: [ERROR], {Description}, s = {State}, a = {Assimilated}, p = {IsPeerConnected}, d = {dest}, s = {MessageService}");
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
                await ZeroAtomicAsync((s, u, d) =>
                {
                    var _this = (CcAdjunct) s;
                    var t = (ValueTuple<CcDrone, Heading>) u;
                    var __ioCcPeer = t.Item1;
                    var __direciton = t.Item2;

                    //Race for direction
                    if (Interlocked.CompareExchange(ref _this._direction, (int) __direciton, (int) Heading.Undefined) !=
                        (int) Heading.Undefined)
                    {
                        _this._logger.Warn(
                            $"oz: race for {__direciton} lost {__ioCcPeer.Description}, current = {_this.Direction}, {_this._drone?.Description}");
                        return ValueTask.FromResult(false);
                    }

                    _this._drone = __ioCcPeer ?? throw new ArgumentNullException($"{nameof(__ioCcPeer)}");
                    _this.State = AdjunctState.Connected;
                    return ValueTask.FromResult(true);
                }, ValueTuple.Create(ccDrone, direction));

                _logger.Trace($"{nameof(AttachPeerAsync)}: [WON] {_drone?.Description}");

                
                ConnectedAtLeastOnce = true;
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

                await SendDiscoveryRequestAsync().ConfigureAwait(false);

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
        public async ValueTask DetachPeerAsync(CcDrone dc, bool force = false)
        {
            var parms = ValueTuple.Create(dc, force);
            if (!await ZeroAtomicAsync((s, u, d) =>
            {
                var _this = (CcAdjunct) s;
                var t = (ValueTuple<CcDrone, bool>) u;
                if (_this._drone == null && !t.Item2)
                    return ValueTask.FromResult(false);

                if (_this._drone != t.Item1)
                    return ValueTask.FromResult(false);

                //peer = _this._peer;
                t.Item1 = _this._drone;

                _this._drone = null;
                return ValueTask.FromResult(true);
            }, parms).ConfigureAwait(false))
            {
                return;
            }

            var peer = parms.Item1;

            if (peer != dc)
                throw new ApplicationException("peer == dc");

            //send drop request
            await SendPeerDropAsync().ConfigureAwait(false);

            _logger.Trace(
                $"{(ConnectedAtLeastOnce ? "Useful" : "Useless")} {Direction} peer detaching: s = {State}, a = {Assimilated}, p = {IsPeerConnected}, {peer?.Description ?? Description}");

            //Detach zeroed
            Unsubscribe(_zeroSub);
            _zeroSub = default;

            if (peer != null)
            {
                await peer.DetachNeighborAsync().ConfigureAwait(false);
                await peer.ZeroAsync(this).ConfigureAwait(false);
            }

            Interlocked.Exchange(ref _direction, 0);
            ExtGossipAddress = null;
            NATAddress = null;
            AttachTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            TotalPats = 0;
            PeeringAttempts = 0;
            State = AdjunctState.Disconnected;
        }


        /// <summary>
        /// The state transition history, sourced from <see  cref="IoZero{TJob}"/>
        /// </summary>
#if DEBUG
        public IoStateTransition<NeighborState>[] StateTransitionHistory =
            new IoStateTransition<NeighborState>[Enum.GetNames(typeof(NeighborState))
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
                var nextState = new IoStateTransition<NeighborState>();

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