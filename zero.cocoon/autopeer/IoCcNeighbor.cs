using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
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
    public class IoCcNeighbor : IoNeighbor<IoCcPeerMessage>
    {
        public IoCcNeighbor(IoNode<IoCcPeerMessage> node, IoNetClient<IoCcPeerMessage> ioNetClient, object extraData = null, IoCcService services = null) : base(node, ioNetClient, userData => new IoCcPeerMessage("peer rx", $"{ioNetClient.AddressString}", ioNetClient), 2,2)
        {
            _logger = LogManager.GetCurrentClassLogger();

            NeighborDiscoveryNode = (IoCcNeighborDiscovery)node;

            if (extraData != null)
            {
                var extra = (Tuple<IoCcIdentity, IoCcService, IPEndPoint>) extraData;
                Identity = extra.Item1;
                Services = services ?? extra.Item2;
                RemoteAddress = IoNodeAddress.CreateFromEndpoint("udp", extra.Item3);
            }
            else
            {
                Identity = CcNode.CcId;
                Services = services ?? ((IoCcNeighborDiscovery)node).Services;
            }
            
            if (RoutedRequest)
            {
                State = NeighborState.Unverified;
                var task = Task.Factory.StartNew(async () =>
                {
                    while (!Zeroed())
                    {
                        await Task.Delay(_random.Next(parm_zombie_max_ttl / 2) * 1000 + parm_zombie_max_ttl / 4 * 1000, AsyncTokenProxy.Token).ConfigureAwait(false);
                        await EnsurePeerAsync().ConfigureAwait(false);
                    }
                }, AsyncTokenProxy.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            }
            else
            {
                State = NeighborState.Local;
            }
        }

        public enum NeighborState
        {
            Undefined,
            FinalState,
            ZeroState,
            Zombie,
            Local,
            Unverified,
            Verified,
            Standby,
            Disconnected,
            Reconnecting,
            Connecting,
            Connected,
        }

        /// <summary>
        /// The current state
        /// </summary>
#if DEBUG

        private IoStateTransition<NeighborState> _currState = new IoStateTransition<NeighborState>()
        {
            FinalState = NeighborState.FinalState
        };
#else
        private volatile IoStateTransition<NeighborState> _currState = new IoStateTransition<NeighborState>()
        {
            FinalState = NeighborState.FinalState
        };

#endif

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Description
        /// </summary>
        private string _description;
        public override string Description
        {
            get
            {
                //if (_description != null)
                //    return _description;
                try
                {
                    return _description =
                        $"`neighbor({(RoutedRequest ? "R" : "L")},{(ConnectedAtLeastOnce ? "C" : "D")})[{TotalPats}] {Id}:{RemoteAddress?.Port} <-> {IoSource?.Key}'";
                }
                catch (NullReferenceException) { }
                catch (Exception e)
                {
                    _logger.Error(e, Description);
                }

                return _description;
            }
        }

        public string MetaDesc => $"(d = {Direction}, s = {State}, v = {Verified}, a = {IsAutopeering}, att = {IsPeerAttached}, c = {IsPeerConnected}, r = {ConnectionAttempts}, g = {IsGossiping}, arb = {IsArbitrating}, o = {Source.IsOperational}, w = {TotalPats})";

        /// <summary>
        /// Random number generator
        /// </summary>
        private readonly Random _random = new Random((int) DateTimeOffset.Now.Ticks);

        /// <summary>
        /// Discovery services
        /// </summary>
        protected IoCcNeighborDiscovery NeighborDiscoveryNode;

        /// <summary>
        /// The gossip peer associated with this neighbor
        /// </summary>
        private volatile IoCcPeer _peer;

        /// <summary>
        /// Whether The peer is attached
        /// </summary>
        public bool IsPeerAttached => _peer != null;

        /// <summary>
        /// Whether the peer is nominal
        /// </summary>
        public bool IsPeerConnected => IsPeerAttached && _peer.IoSource.IsOperational;

        /// <summary>
        /// Is this the local listener
        /// </summary>
        public bool IsLocal => !RoutedRequest;

        /// <summary>
        /// Is autopeering
        /// </summary>
        public bool IsAutopeering => !Zeroed() && Verified && RoutedRequest;

        /// <summary>
        /// Whether the node, peer and neighbor are nominal
        /// </summary>
        public bool IsGossiping => IsAutopeering && IsPeerConnected;

        /// <summary>
        /// Looks for a zombie peer
        /// </summary>
        public bool PolledZombie => Direction != Kind.Undefined && !(IsAutopeering && IsPeerConnected && ConnectedAtLeastOnce);

        /// <summary>
        /// Indicates whether we have successfully established a connection before
        /// </summary>
        protected volatile bool ConnectedAtLeastOnce;

        /// <summary>
        /// Indicates whether we have successfully established a connection before
        /// </summary>
        protected volatile int ConnectionAttempts;

        //uptime
        private long _uptime;
        public long PeerUptime
        {
            get => Interlocked.Read(ref _uptime);
            private set => Interlocked.Exchange(ref _uptime, value);
        }

        /// <summary>
        /// The neighbor address
        /// </summary>
        public IoNodeAddress RemoteAddress { get; protected set; }

        /// <summary>
        /// Whether this neighbor contains verified remote client connection information
        /// </summary>
        public bool RoutedRequest => (RemoteAddress != null);

        /// <summary>
        /// The our IP as seen by neighbor
        /// </summary>
        public IoNodeAddress ExtGossipAddress { get; protected set; }

        /// <summary>
        /// Tcp Readahead
        /// </summary>
        public const int TcpReadAhead = 1;

        /// <summary>
        /// The node identity
        /// </summary>
        public IoCcIdentity Identity { get; protected set; }

        /// <summary>
        /// Whether the node has been verified
        /// </summary>
        public bool Verified { get; protected set; }

        private volatile int _direction;
        /// <summary>
        /// Who contacted who?
        /// </summary>
        //public Kind Direction { get; protected set; } = Kind.Undefined;
        public Kind Direction => (Kind) _direction;

        /// <summary>
        /// inbound
        /// </summary>
        public bool Inbound => Direction == Kind.Inbound && Verified; //TODO

        /// <summary>
        /// outbound
        /// </summary>
        public bool Outbound => Direction == Kind.OutBound && Verified;

        /// <summary>
        /// Who contacted who?
        /// </summary>
        public enum Kind
        {
            Undefined,
            Inbound,
            OutBound
        }

        /// <summary>
        /// The node that this neighbor belongs to
        /// </summary>
        public IoCcNode CcNode => NeighborDiscoveryNode?.CcNode;

        /// <summary>
        /// Receives protocol messages from here
        /// </summary>
        private IoChannel<IoCcProtocolMessage> _protocolChannel;

        /// <summary>
        /// Seconds since pat
        /// </summary>
        private long _lastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        /// <summary>
        /// Total pats
        /// </summary>
        private long _totalPats;

        /// <summary>
        /// Seconds since last neighbor pat
        /// </summary>
        protected long LastPat
        {
            get => Interlocked.Read(ref _lastPat);
            set => Interlocked.Exchange(ref _lastPat, value);
        }

        /// <summary>
        /// Seconds since last neighbor pat
        /// </summary>
        protected long TotalPats
        {
            get => Interlocked.Read(ref _totalPats);
            set => Interlocked.Exchange(ref _totalPats, value);
        }

        /// <summary>
        /// Seconds since valid
        /// </summary>
        public long SecondsSincePat => LastPat.Elapsed();

        /// <summary>
        /// loss
        /// </summary>
        private long _patMatcher;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private volatile ByteString _pingRequest;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private volatile ByteString _discoveryRequest;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private volatile ByteString _peerRequest;

        /// <summary>
        /// Holds unrouted ping requests
        /// </summary>
        private ConcurrentDictionary<string, ByteString> _pingRequests = new ConcurrentDictionary<string, ByteString>();

        /// <summary>
        /// Match ping request
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        private (ByteString pingReq,string key, bool matched) MatchPingRequest(string key)
        {
            var match = _pingRequests.TryGetValue(key, out var ping) ? (ping, key, true) : (default(ByteString), default(string), false);
            return PopMatch(match.Item1, match.Item2, match.Item3);
        }

        /// <summary>
        /// Pop a match off the stack
        /// </summary>
        /// <param name="ping"></param>
        /// <param name="key"></param>
        /// <param name="match"></param>
        private (ByteString pingReq, string key, bool matched) PopMatch(ByteString ping, string key, bool match)
        {
            return _pingRequests.TryRemove(key ?? "", out _) ? (_: ping,key, match) : (null, null, false);
        }

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
        //private ByteString GetSalt => _curSalt = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(Encoding.ASCII.GetBytes((DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 120 * 60).ToString())), 0, parm_salt_length);

        private ByteString GetSalt
        {
            get
            {
                if (_curSalt == null || DateTimeOffset.UtcNow.ToUnixTimeSeconds() - _curSaltStamp > parm_ping_timeout)
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
        public ArrayPool<Tuple<IMessage, object, Packet>> ArrayPoolProxy { get; protected set; }

        /// <summary>
        /// Create an CcId string
        /// </summary>
        /// <param name="identity">The crypto identity</param>
        /// <param name="address">The transport identity</param>
        /// <returns></returns>
        public static string MakeId(IoCcIdentity identity, IoNodeAddress address)
        {
            return $"{identity.IdString()}|{identity.PkString()}@{address.Ip}";
        }

        /// <summary>
        /// The CcId
        /// </summary>
        public sealed override string Id => $"{MakeId(Identity, RemoteAddress ?? ((IoNetClient<IoCcPeerMessage>)Source).ListeningAddress)}";

        /// <summary>
        /// The neighbor services
        /// </summary>
        public IoCcService Services { get; protected set; }

        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_salt_length = 20;

        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_salt_ttl = 2 * 60 * 60;

        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_ping_timeout = 10000;

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
        public int parm_max_time_error = 60;

        /// <summary>
        /// Average time allowed between pats
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_zombie_max_ttl = 240;

        /// <summary>
        /// Maximum number of services supported
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_zombie_max_connection_attempts = 3;

        /// <summary>
        /// Handle to neighbor zero sub
        /// </summary>
        private IoZeroSub _neighborZeroSub;

        /// <summary>
        /// A service map helper
        /// </summary>
        private ServiceMap ServiceMap
        {
            get
            {
                var mapping = new ServiceMap();
                foreach (var service in Services.IoCcRecord.Endpoints)
                {
                    if (service.Value != null && service.Value.Validated)
                        mapping.Map.Add(service.Key.ToString(), new NetworkAddress { Network = $"{service.Value.Protocol().ToString().ToLower()}", Port = (uint)service.Value.Port });
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
            //_pingRequestBarrier.Dispose();
            base.ZeroUnmanaged();

            //_pingRequestBarrier = null;
#if SAFE_RELEASE
            _pingRequests = null;
            _peer = null;
            NeighborDiscoveryNode = null;
            _protocolChannel = null;
            _neighborZeroSub = default;
            ArrayPoolProxy = null;
            StateTransitionHistory = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            _pingRequests.Clear();
            Array.Clear(StateTransitionHistory, 0, StateTransitionHistory.Length);
            await DetachPeerAsync().ConfigureAwait(false);
            await base.ZeroManagedAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures that the peer is running
        /// </summary>
        public async Task EnsurePeerAsync()
        {
            //Are we limping?
            if (!RoutedRequest && Node.Neighbors.Count < 3)
            {
                //Try to bootstrap again
                foreach (var ioNodeAddress in CcNode.BootstrapAddress)
                {
                    _logger.Trace($"Boostrapping: {ioNodeAddress} from {Description}");
                    await ((IoCcNeighborDiscovery)Node).LocalNeighbor.SendPingAsync(ioNodeAddress).ConfigureAwait(false);
                    return;
                }
            }

            // Verify request
            if (!IsAutopeering)
            {
                _logger.Debug($"{nameof(EnsurePeerAsync)}: [ABORTED], {Description}, {MetaDesc}");
                return;
            }
            
            //Moderate requests if we ensured at least once
            if (SecondsSincePat < parm_zombie_max_ttl / 2)
            {
                return;
            }

            //Watchdog failure
            if (SecondsSincePat > parm_zombie_max_ttl * 2)
            {
                var reconnect = this.Direction == Kind.OutBound;
                var address = RemoteAddress;

                _logger.Error($"{nameof(EnsurePeerAsync)}: Watchdog failure! s = {SecondsSincePat} >> {parm_zombie_max_ttl * 2}, {Description}, {MetaDesc}");

                await ZeroAsync(this).ConfigureAwait(false);

                if (reconnect)
                    await SendPingAsync(address).ConfigureAwait(false);

                return;
            }

            if (await SendPingAsync().ConfigureAwait(false))
            {
                _logger.Trace($">>{nameof(EnsurePeerAsync)}: PAT to = {Description}");
            }
            else
            {
                _logger.Fatal($">>{nameof(SendPingAsync)}: PAT Send [FAILED], {Description}, {MetaDesc}");
            }
        }

        /// <summary>
        /// Start processors for this neighbor
        /// </summary>
        /// <param name="spawnProducer">Spawns a source thread</param>
        /// <returns></returns>
        public override async Task AssimilateAsync()
        {
            var processingAsync = base.AssimilateAsync();
            var protocol = ProcessAsync();

            try
            {
                await Task.WhenAll(processingAsync, protocol).ConfigureAwait(false);

                if (processingAsync.IsFaulted)
                {
                    _logger.Fatal(processingAsync.Exception, "Neighbor processing returned with errors!");

                    await ZeroAsync(this).ConfigureAwait(false);

                }

                if (protocol.IsFaulted)
                {
                    _logger.Fatal(protocol.Exception, "Protocol processing returned with errors!");

                    await ZeroAsync(this).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                _logger.Error(e,Description);
            }
        }

        /// <summary>
        /// Connect to neighbor forming a peer connection
        /// </summary>
        /// <returns></returns>
        protected async ValueTask<bool> ConnectAsync()
        {
            //Validate request
            if (!IsAutopeering && !IsPeerAttached || State == NeighborState.Connected)
            {
#if DEBUG
                if(IsPeerAttached || State < NeighborState.Disconnected )
                    _logger.Fatal($"Incorrect state, {MetaDesc}, {Description}");
#endif

                _logger.Debug($"Connection aborted, {MetaDesc}, {Description}");
                return false;
            }

            Interlocked.Increment(ref ConnectionAttempts);
            State = ConnectedAtLeastOnce ? NeighborState.Reconnecting : NeighborState.Connecting;

            //Attempt the connection, race to win
            if (await CcNode.ConnectToPeerAsync(this).ConfigureAwait(false))
            {
                State = NeighborState.Connected;
                _logger.Debug($"Connected to {Description}");

                //Send discovery request
                await SendDiscoveryRequestAsync().ConfigureAwait(false);
                return true;
            }
            else
            {
                _logger.Debug($"{nameof(CcNode.ConnectToPeerAsync)}: [FAILED], {Description}, {MetaDesc}");
            }

            State = NeighborState.Disconnected;
            return false;
        }

        /// <summary>
        /// Probes for neighhor
        /// </summary>
        protected async Task ProbeAsync(IoNodeAddress dest = null)
        {
            dest ??= RemoteAddress;

            await SendPingAsync(dest).ConfigureAwait(false);
        }

        /// <summary>
        /// Processes protocol messages
        /// </summary>
        /// <param name="msg">The consumer that need processing</param>
        /// <param name="msgArbiter">The arbiter</param>
        /// <param name="processCallback">The process callback</param>
        /// <param name="zeroClosure"></param>
        /// <returns></returns>
        private async Task ProcessMsgBatchAsync(IoLoad<IoCcProtocolMessage> msg,
            IoChannel<IoCcProtocolMessage> msgArbiter,
            Func<Tuple<IMessage, object, Packet>, IoChannel<IoCcProtocolMessage>, IIoZero, Task> processCallback, IIoZero zeroClosure)
        {
            if (msg == null)
                return;

            var stopwatch = Stopwatch.StartNew();

            try
            {
                var protocolMsgs = ((IoCcProtocolMessage) msg).Batch;

                foreach (var message in protocolMsgs)
                {
                    if (message == null)
                        break;

                    try
                    {
                        await processCallback(message, msgArbiter, zeroClosure).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e, $"Processing protocol failed for {Description}: ");
                    }
                }
                
                msg.State = IoJobMeta.JobState.Consumed;

                stopwatch.Stop();
                //_logger.Trace($"{(RoutedRequest ? "V>" : "X>")} Processed `{protocolMsgs.Count}' consumer: t = `{stopwatch.ElapsedMilliseconds:D}', `{protocolMsgs.Count * 1000 / (stopwatch.ElapsedMilliseconds + 1):D} t/s'");
            }
            catch (Exception e)
            {
                _logger.Error(e, "Error processing message batch");
            }
            finally
            {
                ArrayPoolProxy?.Return(((IoCcProtocolMessage)msg).Batch);
            }
        }

        /// <summary>
        /// Processes a protocol message
        /// </summary>
        /// <returns></returns>
        private async Task ProcessAsync()
        {
            _protocolChannel ??= Source.GetChannel<IoCcProtocolMessage>(nameof(IoCcNeighbor));

            _logger.Debug($"Processing {Description}");

            Task[] channelTasks = null;
            try
            {
                while (!Zeroed())
                {
                    if (_protocolChannel == null)
                    {
                        _logger.Debug($"Waiting for {Description} stream to spin up...");
                        _protocolChannel = Source.EnsureChannel<IoCcProtocolMessage>(nameof(IoCcNeighbor));
                        if(_protocolChannel != null)
                            ArrayPoolProxy = ((IoCcProtocolBuffer) _protocolChannel.Source).ArrayPoolProxy;
                        else
                        {
                            await Task.Delay(2000, AsyncTokenProxy.Token).ConfigureAwait(false);//TODO config
                        }
                        continue;
                    }
                    else if( channelTasks == null )
                    {
                        channelTasks = new Task[_protocolChannel.ConsumerCount];
                    }

                    for (int i = 0; i < _protocolChannel.ProducerCount; i++)
                    {
                        if (!await _protocolChannel.ProduceAsync().ConfigureAwait(false))
                            continue;
                    }
                    

                    for (int i = 0; i < _protocolChannel.ConsumerCount; i++)
                    {
                        channelTasks[i] = _protocolChannel.ConsumeAsync(async (msg, ioZero) =>
                        {
                            var _this = (IoCcNeighbor) ioZero;
                            try
                            {
                                await _this.ProcessMsgBatchAsync(msg, _this._protocolChannel, async (msg, forward, iioZero) =>
                                {
                                    var __this = (IoCcNeighbor) iioZero;
                                    var (message, extraData, packet) = msg;
                                    try
                                    {
                                        IoCcNeighbor ccNeighbor;
                                        //TODO optimize
                                        __this.Node.Neighbors.TryGetValue(
                                            MakeId(IoCcIdentity.FromPubKey(packet.PublicKey.Span),
                                                IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData)),
                                            out var n);
                                        if (n == null)
                                            ccNeighbor = ((IoCcNeighborDiscovery)__this.Node).LocalNeighbor;
                                        else
                                            ccNeighbor = (IoCcNeighbor) n;

                                        switch (message.GetType().Name)
                                        {
                                            case nameof(Ping):
                                                await ccNeighbor.ProcessAsync((Ping) message, extraData, packet).ConfigureAwait(false);
                                                break;
                                            case nameof(Pong):
                                                await ccNeighbor.ProcessAsync((Pong) message, extraData, packet).ConfigureAwait(false);
                                                break;
                                            case nameof(DiscoveryRequest):
                                                await ccNeighbor.ProcessAsync((DiscoveryRequest) message, extraData, packet).ConfigureAwait(false);
                                                break;
                                            case nameof(DiscoveryResponse):
                                                await ccNeighbor.ProcessAsync((DiscoveryResponse) message, extraData, packet).ConfigureAwait(false);
                                                break;
                                            case nameof(PeeringRequest):
                                                await ccNeighbor.ProcessAsync((PeeringRequest) message, extraData, packet).ConfigureAwait(false);
                                                break;
                                            case nameof(PeeringResponse):
                                                await ccNeighbor.ProcessAsync((PeeringResponse) message, extraData, packet).ConfigureAwait(false);
                                                break;
                                            case nameof(PeeringDrop):
                                                await ccNeighbor.ProcessAsync((PeeringDrop) message, extraData, packet).ConfigureAwait(false);
                                                break;
                                        }
                                    }
                                    catch (TaskCanceledException e) { __this._logger.Trace(e, Description); }
                                    catch (OperationCanceledException e) { __this._logger.Trace(e, Description); }
                                    catch (NullReferenceException e) { __this._logger.Trace(e, Description); }
                                    catch (ObjectDisposedException e) { __this._logger.Trace(e, Description); }
                                    catch (Exception e)
                                    {
                                        __this._logger.Error(e, $"{message.GetType().Name} [FAILED]: l = {packet.Data.Length}, {Id}");
                                    }
                                }, _this).ConfigureAwait(false);
                            }
                            finally
                            {
                                if (msg != null && msg.State != IoJobMeta.JobState.Consumed)
                                    msg.State = IoJobMeta.JobState.ConsumeErr;
                            }
                        }, this).AsTask();

                        if (!_protocolChannel.Source.IsOperational)
                            break;
                    }

                    await Task.WhenAll(channelTasks).ConfigureAwait(false);
                }
            }
            catch (TaskCanceledException e){_logger.Trace(e,Description );}
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            catch (NullReferenceException e) { _logger.Trace(e, Description); }
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (Exception e)
            {
                _logger.Error(e,$"Error processing {Description}");
            }

            _logger.Debug($"Stopped processing msgs from {Description}");
        }

        /// <summary>
        /// Peer drop request
        /// </summary>
        /// <param name="request">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>

        private async Task ProcessAsync(PeeringDrop request, object extraData, Packet packet)
        {
            var diff = 0;
            if (!RoutedRequest || (diff = Math.Abs((int)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Timestamp))) > parm_max_time_error * 2)
            {
                _logger.Trace($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringDrop)}: Ignoring {diff}s old/invalid request, error = ({diff})");
                return;
            }

            //only verified nodes get to drop
            if (!Verified || _peer == null)
                return;

            _logger.Debug($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringDrop)}: {Direction} Peer= {_peer?.Id ?? "null"}");

            try
            {
                await _peer.ZeroAsync(this).ConfigureAwait(false);
            }
            catch { }
        }

        /// <summary>
        /// Peering Request message from client
        /// </summary>
        /// <param name="request">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task ProcessAsync(PeeringRequest request, object extraData, Packet packet)
        {
            if (!IsAutopeering || request.Timestamp.UtDelta() > parm_max_time_error * 2)
            {
                if (!RoutedRequest)
                {
                    //We syn here (Instead of in process ping) to force the other party to do some work (this) before we do work (verify).
                    await SendPingAsync(IoNodeAddress.CreateFromEndpoint("udp://", (IPEndPoint) extraData)).ConfigureAwait(false);
                    _logger.Trace($"{nameof(PeeringRequest)}: DMZ/SYN => {extraData}");
                    return;
                }
                else
                {
                    _logger.Trace($"{nameof(PeeringRequest)}: Dropped!, {(Verified ? "verified" : "un-verified")}, age = {Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Timestamp)}");
                }
                return;
            }

            PeeringResponse peeringResponse = peeringResponse = new PeeringResponse
            {
                ReqHash = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                Status = CcNode.InboundCount < CcNode.parm_max_inbound
            };

            var wasInbound = Direction == Kind.Inbound;

            if (Direction == Kind.Undefined && peeringResponse.Status)
            {
                await SendDiscoveryRequestAsync().ConfigureAwait(false);
            }
            else if (Direction == Kind.OutBound) //If it is outbound say no
            {
                _logger.Trace($"<<{nameof(PeeringRequest)}: Peering request {Kind.Inbound} Rejected: {Description} is already {Kind.OutBound}, {MetaDesc}");
                peeringResponse.Status = false;
            }
            else if (wasInbound)
            {
                await EnsureZombieAsync().ConfigureAwait(false);
            }
            
            if((await SendMessageAsync( data: peeringResponse.ToByteString(), type: IoCcPeerMessage.MessageTypes.PeeringResponse).ConfigureAwait(false)).sent > 0)
                _logger.Trace($">>{nameof(PeeringResponse)}: Sent {(peeringResponse.Status?"ACCEPT":"REJECT")}, {Description}");
            else
                _logger.Debug($"<<{nameof(PeeringRequest)}: [FAILED], {Description}, {MetaDesc}");
        }

        /// <summary>
        /// Peer response message from client
        /// </summary>
        /// <param name="response">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task ProcessAsync(PeeringResponse response, object extraData, Packet packet)
        {
            //Validate
            var request = _peerRequest;
            if (!IsAutopeering || request == null)
            {
                _logger.Debug($"<<{nameof(PeeringResponse)}: Unexpected {extraData}, {RemoteAddress}");
                return;
            }

            var hash = IoCcIdentity.Sha256.ComputeHash(request.Memory.AsArray());
            if (!response.ReqHash.SequenceEqual(hash))
            {
                _logger.Debug($"<<{nameof(PeeringResponse)}: Request mash: {Description} <-> {extraData}, {Convert.ToBase64String(hash)} - {Convert.ToBase64String(response.ReqHash.Memory.AsArray())}");
                return;
            }
            _peerRequest = null;

            //Validated
            _logger.Debug($"<<{nameof(PeeringResponse)}: Accepted = {response.Status}, {Description}");

            var alreadyOutbound = Direction == Kind.OutBound;
            //Race for 
            if (Direction == Kind.Undefined && response.Status)
            {
                if (!await ConnectAsync().ConfigureAwait(false))
                {
                    _logger.Debug($"<<{nameof(PeeringResponse)}: [FAILED] Connect to {Description}, {MetaDesc}");
                }
            }//Were we inbound?
            else if (Direction == Kind.Inbound)
            {
                _logger.Debug($"<<{nameof(PeeringResponse)}: {nameof(Kind.OutBound)} request dropped, {nameof(Kind.Inbound)} received");
                //TODO can we do better?
            }
            else if (alreadyOutbound)//We were already outbound
            {
                await EnsureZombieAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Checks for zombie peers and compensates
        /// </summary>
        /// <returns>Task</returns>
        private async Task EnsureZombieAsync()
        {
            if(!IsAutopeering)
                return;
            
            var direction = Direction;
            //Did the outbound peer gossip connection fail silently?
            if (PolledZombie)
            {
                _logger.Warn($"{nameof(EnsureZombieAsync)}: Found zombie {Description}, {MetaDesc}");
                await _peer.ZeroAsync(this).ConfigureAwait(false);

                //Was it outbound?
                if(direction == Kind.OutBound)
                {
                    //Do we have outbound capacity?
                    if (ConnectionAttempts < parm_zombie_max_connection_attempts)
                    {
                        //Send a peer request
                        if (await SendPeerRequestAsync().ConfigureAwait(false))
                        {
                            State = NeighborState.Standby;
                        }
                        else //failed to request, something is wrong give up
                        {
                            State = NeighborState.Zombie;
                            await ZeroAsync(this).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        //Give up
                        State = NeighborState.Zombie;
                        await ZeroAsync(this).ConfigureAwait(false);
                    }
                }
                else if( CcNode.InboundCount < CcNode.parm_max_inbound )
                {
                    State = NeighborState.Standby;

                    if (await SendPingAsync().ConfigureAwait(false))
                        _logger.Debug($"{nameof(EnsureZombieAsync)}: Attempting to wake zombie at {Description}");
                    else //Something is wrong, give up
                    {
                        State = NeighborState.Zombie;
                        await ZeroAsync(this).ConfigureAwait(false);
                    }
                }
                else
                {
                    State = NeighborState.Standby;
                }
            } 
            else if (Direction == Kind.OutBound && !IsPeerAttached && ConnectionAttempts > 0) //If we have attempted a connect
            {
                State = NeighborState.Standby;
                if (await ConnectAsync().ConfigureAwait(false))
                    _logger.Debug($"{nameof(EnsureZombieAsync)}: Re-/>connected to {Description}");
                else
                {
                    State = NeighborState.Standby;
                    _logger.Debug($"{nameof(EnsureZombieAsync)}: Re-/>connect rejected, {MetaDesc}, {Description}");
                }
            }
            else if(Direction == Kind.Inbound && !IsPeerAttached) // are we waiting for a connect?
            {
                _logger.Debug($"{nameof(EnsureZombieAsync)}: Waiting for {Direction} connection from {Description}, {MetaDesc}");
                if (await SendPingAsync().ConfigureAwait(false))
                {
                    _logger.Debug($"{nameof(EnsureZombieAsync)}: Poke {Description}, {MetaDesc}");
                }
            }
        }

        /// <summary>
        /// Sends a message to the neighbor
        /// </summary>
        /// <param name="dest">The destination address</param>
        /// <param name="data">The message data</param>
        /// <param name="type">The message type</param>
        /// <returns></returns>
        private async Task<(int sent, Packet responsePacket)> SendMessageAsync(IoNodeAddress dest = null, ByteString data = null, IoCcPeerMessage.MessageTypes type = IoCcPeerMessage.MessageTypes.Undefined)
        {
            try
            {
                if (Zeroed())
                    return (0, null);
            
                dest ??= RemoteAddress;

                var packet = new Packet
                {
                    Data = data,
                    PublicKey = ByteString.CopyFrom(CcNode.CcId.PublicKey),
                    Type = (uint)type
                };

                packet.Signature = ByteString.CopyFrom(CcNode.CcId.Sign(packet!.Data!.Memory.AsArray(), 0, packet.Data.Length));

                var msgRaw = packet.ToByteArray();

                var sentTask = ((IoUdpClient<IoCcPeerMessage>) Source).Socket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint);
                if (!sentTask.IsCompletedSuccessfully)
                    await sentTask.ConfigureAwait(false);
                    
                _logger.Trace($"/{dest.IpEndPoint.Port}>>{Enum.GetName(typeof(IoCcPeerMessage.MessageTypes), packet.Type)}({GetHashCode()}): Sent {sentTask.Result} bytes to {(RoutedRequest?$"{Identity.IdString()}":"")}@{dest.IpEndPoint}");
                return (sentTask.Result, packet);
            }
            catch (NullReferenceException e) { _logger.Trace(e, Description);}
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (Exception e)
            {
                _logger.Error(e, $"Failed to send message {Description}");
            }
            
            return (0, null);
        }

        /// <summary>
        /// Discovery response message
        /// </summary>
        /// <param name="response">The response</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task ProcessAsync(DiscoveryResponse response, object extraData, Packet packet)
        {

            var discoveryRequest = _discoveryRequest;

            if (discoveryRequest == null ||  !RoutedRequest || response.Peers.Count > parm_max_discovery_peers)
            {
                _logger.Debug($"{(RoutedRequest?"V>":"X>")}{nameof(DiscoveryResponse)}: Reject, count = ({response.Peers.Count}) from {MakeId(IoCcIdentity.FromPubKey(packet.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint)extraData))}, RemoteAddress = {RemoteAddress}, request = {_discoveryRequest}");
                return;
            }

            if (!response.ReqHash.SequenceEqual(IoCcIdentity.Sha256.ComputeHash(discoveryRequest.Memory.AsArray())))
            {
                _logger.Debug($"<<{nameof(DiscoveryResponse)}: Reject invalid hash {Description}");
                return;
            }

            _discoveryRequest = null;

            var count = 0;

            _logger.Trace($"<<{nameof(DiscoveryResponse)}: Received {response.Peers.Count} potentials from {Description}");
            foreach (var responsePeer in response.Peers)
            {
                //max neighbor check
                if (Node.Neighbors.Count > CcNode.MaxClients)
                    break;

                //Any services attached?
                if (responsePeer.Services?.Map == null || responsePeer.Services.Map.Count == 0)
                {
                    _logger.Trace($"<<{nameof(DiscoveryResponse)}: Invalid services recieved!, map = {responsePeer.Services?.Map}, count = {responsePeer.Services?.Map?.Count??-1}");
                    continue;
                }
                    

                //ignore strange services
                if (responsePeer.Services.Map.Count > parm_max_services)
                    continue;

                //Never add ourselves (by NAT)
                if (responsePeer.Services.Map.ContainsKey(IoCcService.Keys.peering.ToString()) &&
                    responsePeer.Ip == ExtGossipAddress.Ip &&
                    CcNode.ExtAddress.Port == responsePeer.Services.Map[IoCcService.Keys.peering.ToString()].Port)
                    continue;

                //Never add ourselves (by ID)
                if(responsePeer.PublicKey.SequenceEqual(CcNode.CcId.PublicKey))
                   continue;

                //Don't add already known neighbors
                var id = IoCcIdentity.FromPubKey(responsePeer.PublicKey.Span);
                if(Node.Neighbors.Values.Any(n=>n.Id.Contains(id.PkString())))
                    continue;
                
                var services = new IoCcService {IoCcRecord = new IoCcRecord()};
                var newRemoteEp = new IPEndPoint(IPAddress.Parse(responsePeer.Ip), (int)responsePeer.Services.Map[IoCcService.Keys.peering.ToString()].Port);

                //Validate services received
                if (responsePeer.Services.Map.Count <= parm_max_services)
                {
                    foreach (var kv in responsePeer.Services.Map)
                    {
                        services.IoCcRecord.Endpoints.TryAdd(Enum.Parse<IoCcService.Keys>(kv.Key), IoNodeAddress.Create($"{kv.Value.Network}://{responsePeer.Ip}:{kv.Value.Port}"));
                    }
                }
                else //Services not valid
                {
                    _logger.Debug($"<<{nameof(DiscoveryResponse)}: Max service supported {parm_max_services}, got {responsePeer.Services.Map.Count}");
                    services = null;
                    break;
                }

                //sanity check
                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if(services == null || services.IoCcRecord.Endpoints.Count == 0)
                    continue;

                //create neighbor
                IoCcNeighbor newNeighbor = null;
                if( await Node.ZeroEnsureAsync(async s =>
                {
                    newNeighbor = (IoCcNeighbor) Node.MallocNeighbor(Node, (IoNetClient<IoCcPeerMessage>) Source, Tuple.Create(id, services, newRemoteEp));

                    //Transfer?
                    if (!Node.Neighbors.TryAdd(newNeighbor.Id, newNeighbor)) return false;

                    _logger.Debug($"Discovered new neighbor: {newNeighbor.Description}");

                    Node.ZeroOnCascade(newNeighbor); //TODO: Maybe remove? Use the one that floods through source?

                    return await newNeighbor.ZeroEnsureAsync(s =>
                    {
                        var sub = newNeighbor.ZeroEvent(source =>
                        {
                            try
                            {
                                if (Node.Neighbors.TryRemove(newNeighbor.Id, out var n))
                                {
                                    _logger.Trace($"{nameof(DiscoveryResponse)}: Removed {n.Description} from {Description}");
                                }
                            }
                            catch
                            {
                                // ignored
                            }
                            return Task.CompletedTask;
                        });
                        return Task.FromResult(true);
                    });
                }).ConfigureAwait(false))
                {
                    if (!newNeighbor.Verified)
                    {
                        _logger.Trace($"{nameof(DiscoveryResponse)}: DMZ/SYN, to = {newNeighbor.Description}");
                        await newNeighbor.SendPingAsync().ConfigureAwait(false);
                        count++;
                    }
                }
                else
                {
                    await newNeighbor.ZeroAsync(this).ConfigureAwait(false);
                }
            }

            if(Node.Neighbors.Count < CcNode.MaxClients && count > 0)
                _logger.Trace($"{nameof(DiscoveryResponse)}: Scanned {count}/{response.Peers.Count} discoveries from {Description} ...");

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="extraData"></param>
        /// <param name="packet"></param>
        /// <returns></returns>
        private async Task ProcessAsync(DiscoveryRequest request, object extraData, Packet packet)
        {
            if (!IsAutopeering || request.Timestamp.UtDelta() > parm_max_time_error * 2)
            {
                _logger.Trace($"<<{nameof(DiscoveryRequest)}: [ABORTED], age = {request.Timestamp.UtDelta()}, {Description}, {MetaDesc}");
                return;
            }

            var discoveryResponse = new DiscoveryResponse
            {
                ReqHash = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
            };

            var count = 0;
            var certified = NeighborDiscoveryNode.Neighbors.Values.Where(n => ((IoCcNeighbor) n).Verified && n != this)
                .OrderBy(n => (int) ((IoCcNeighbor) n).Direction).ToList();
            foreach (var ioNeighbor in certified)
            {
                if(count == parm_max_discovery_peers)
                    break;
                
                discoveryResponse.Peers.Add(new Peer
                {
                    PublicKey = ByteString.CopyFrom(((IoCcNeighbor)ioNeighbor).Identity.PublicKey),
                    Services = ((IoCcNeighbor)ioNeighbor).ServiceMap,
                    Ip = ((IoCcNeighbor)ioNeighbor).RemoteAddress.Ip
                });
                count++;
            }

            if ((await SendMessageAsync(RemoteAddress, discoveryResponse.ToByteString(),
                IoCcPeerMessage.MessageTypes.DiscoveryResponse).ConfigureAwait(false)).sent > 0)
            {
                _logger.Trace($">>{nameof(DiscoveryResponse)}: Sent {count} discoveries to {Description}");
            }
            else
            {
                _logger.Error($"<<{nameof(DiscoveryRequest)}: [FAILED], {Description}, {MetaDesc}");
            }
        }

        /// <summary>
        /// Ping message
        /// </summary>
        /// <param name="ping">The ping packet</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task ProcessAsync(Ping ping, object extraData, Packet packet)
        {
            var remoteEp = (IPEndPoint)extraData;
            var age = Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - ping.Timestamp);
            if (age > parm_max_time_error * 2 ) //TODO params
            {
                _logger.Trace($"<<{(RoutedRequest?"V>":"X>")}{nameof(Ping)}: [WARN] Dropped stale, age = {age}s");
                return;
            }

            if (!RoutedRequest && ((IPEndPoint) extraData).Equals(ExtGossipAddress?.IpEndPoint))
            {
                _logger.Fatal($"<<{(RoutedRequest?"V>":"X>")}{nameof(Ping)}: Dropping ping from self: {extraData}");
                return;
            }

            //TODO optimize
            var gossipAddress = ((IoCcNeighborDiscovery)Node).Services.IoCcRecord.Endpoints[IoCcService.Keys.gossip];
            var peeringAddress = ((IoCcNeighborDiscovery)Node).Services.IoCcRecord.Endpoints[IoCcService.Keys.peering];
            var fpcAddress = ((IoCcNeighborDiscovery)Node).Services.IoCcRecord.Endpoints[IoCcService.Keys.fpc];

            var pong = new Pong
            {
                ReqHash = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                DstAddr = $"{remoteEp.Address}",//TODO, add port somehow
                Services = new ServiceMap
                {
                    Map =
                    {
                        {IoCcService.Keys.peering.ToString(), new NetworkAddress {Network = "udp", Port = (uint)peeringAddress.Port}},
                        {IoCcService.Keys.gossip.ToString(), new NetworkAddress {Network = "tcp", Port = (uint)gossipAddress.Port}},
                        {IoCcService.Keys.fpc.ToString(), new NetworkAddress {Network = "tcp", Port = (uint)fpcAddress.Port}}
                    }
                }
            };

            IoNodeAddress toAddress = IoNodeAddress.Create($"udp://{remoteEp.Address}:{ping.SrcPort}");
            var id = MakeId(IoCcIdentity.FromPubKey(packet.PublicKey.Span), toAddress);

            //PROCESS DMZ/SYN
            if (!RoutedRequest)
            {
                IoNodeAddress toProxyAddress = null;

                if (CcNode.UdpTunnelSupport)
                {
                    if (ping.SrcAddr != "0.0.0.0" && remoteEp.Address.ToString() != ping.SrcAddr)
                    {
                        toProxyAddress = IoNodeAddress.Create($"udp://{ping.SrcAddr}:{ping.SrcPort}");
                        _logger.Trace($"<<{nameof(Ping)}: static peer address received: {toProxyAddress}, source detected = udp://{remoteEp}");
                    }
                    else
                    {
                        toProxyAddress = IoNodeAddress.CreateFromEndpoint("udp", remoteEp);
                        _logger.Trace($"<<{nameof(Ping)}: automatic peer address detected: {toProxyAddress}, source declared = udp://{ping.SrcAddr}:{ping.SrcPort}");
                    }
                }

                //SEND SYN-ACK
                if ((await SendMessageAsync(toAddress, pong.ToByteString(), IoCcPeerMessage.MessageTypes.Pong)
                    .ConfigureAwait(false)).sent > 0)
                {
                    _logger.Trace($">>{nameof(Pong)}: Sent SYN-ACK, to = {id}:{ping.SrcPort}");
                }

                if (CcNode.UdpTunnelSupport && toAddress.Ip != toProxyAddress.Ip)
                    await SendMessageAsync(toAddress, pong.ToByteString(), IoCcPeerMessage.MessageTypes.Pong).ConfigureAwait(false);
            }
            else//PROCESS ACK
            {
                if ((await SendMessageAsync(data: pong.ToByteString(), type: IoCcPeerMessage.MessageTypes.Pong)
                    .ConfigureAwait(false)).sent > 0)
                {
                    if(IsPeerConnected)
                        _logger.Trace($">>{nameof(Pong)}: Sent KEEPALIVE, to = {Description}");
                    else
                        _logger.Trace($">>{nameof(Pong)}: Sent ACK SYN, to = {Description}");
                }
                else
                {
                    _logger.Error($"<<{nameof(Ping)}: [FAILED] Sent ACK SYN/KEEPALIVE, to = {Description}");
                }
            }
        }

        /// <summary>
        /// Pong message
        /// </summary>
        /// <param name="pong">The Pong packet</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task ProcessAsync(Pong pong, object extraData, Packet packet)
        {
            ByteString pingRequest;

            if (RoutedRequest)
                pingRequest = _pingRequest ?? ((IoCcNeighborDiscovery) Node).LocalNeighbor
                    .MatchPingRequest(Convert.ToBase64String(pong.ReqHash.Memory.AsArray())).pingReq;
            else
                pingRequest = ((IoCcNeighborDiscovery) Node).LocalNeighbor.MatchPingRequest(Convert.ToBase64String(pong.ReqHash.Memory.AsArray())).pingReq;

            if (pingRequest == null)
            {
                if (RoutedRequest)
                {
                    if(TotalPats > 0)
                        _logger.Debug($"<<{nameof(Pong)}({GetHashCode()}):  Unexpected!, pats = {TotalPats},  ssp = {SecondsSincePat}, l = {Interlocked.Read(ref _patMatcher)}, d = {(PeerUptime>0? (PeerUptime - LastPat).ToString():"N/A")}, v = {Verified}, id = {Description}");
                }
                else { } //ignore

                return;
            }

            var hash = IoCcIdentity.Sha256.ComputeHash(pingRequest.Memory.AsArray());

            if (!pong.ReqHash.SequenceEqual(hash))
            {
                _logger.Debug(!RoutedRequest
                    ? $"{(RoutedRequest?"V>":"X>")}{nameof(Pong)}: Invalid request {MakeId(IoCcIdentity.FromPubKey(packet.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData))}"
                    : $"{(RoutedRequest?"V>":"X>")}{nameof(Pong)}: Invalid hash {extraData} <=> {pingRequest}, {Convert.ToBase64String(hash)} - {Convert.ToBase64String(pong.ReqHash.Memory.AsArray())}");

                return;
            }

            if (!RoutedRequest)
            {
                //This is hacky to clean old requests
                if (_pingRequests.Count > CcNode.MaxClients * 2)
                {
                    foreach (var request in _pingRequests)
                    {
                        var req = Ping.Parser.ParseFrom(request.Value);
                        if (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - req.Timestamp > parm_ping_timeout)
                        {
                            _logger.Debug($"{nameof(Pong)}: dropping ping not received: {req.DstAddr} ({_pingRequests.Count})");
                        }
                    }
                }
            }
            else
            {
                _pingRequest = null;
                //_logger.Fatal($"UNSET({GetHashCode()}) {Description} ");
            }

            LastPat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            Interlocked.Increment(ref _totalPats);
            
            //Process SYN-ACK
            if (!RoutedRequest)
            {
                var idCheck = IoCcIdentity.FromPubKey(packet.PublicKey.Span);
                var keyStr = MakeId(idCheck, IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint)extraData));

                // remove stale neighbor PKs
                var staleId = Node.Neighbors
                    .Where(kv => ((IoCcNeighbor)kv.Value).RoutedRequest)
                    .Where(kv => ((IoCcNeighbor)kv.Value).RemoteAddress.Port == ((IPEndPoint) extraData).Port)
                    .Where(kv => kv.Value.Id.Contains(idCheck.PkString()))
                    .Select(kv => kv.Value.Id).FirstOrDefault();

                if(!string.IsNullOrEmpty(staleId) && Node.Neighbors.TryRemove(staleId, out var staleNeighbor)) 
                {
                    _logger.Warn($"Removing stale neighbor {staleNeighbor.Id}:{((IoCcNeighbor)staleNeighbor).RemoteAddress.Port} ==> {keyStr}:{((IPEndPoint)extraData).Port}");
                    await staleNeighbor.ZeroAsync(this).ConfigureAwait(false);
                }

                IoNeighbor<IoCcPeerMessage> oldNeighbor = null;
                //Do we have capacity for one more new neighbor?
                if (Node.Neighbors.Count <= CcNode.MaxClients * 2 && !Node.Neighbors.TryGetValue(keyStr, out oldNeighbor))
                {
                    var remoteServices = new IoCcService();
                    foreach (var key in pong.Services.Map.Keys.ToList())
                        remoteServices.IoCcRecord.Endpoints.TryAdd(Enum.Parse<IoCcService.Keys>(key), IoNodeAddress.Create($"{pong.Services.Map[key].Network}://{((IPEndPoint)extraData).Address}:{pong.Services.Map[key].Port}"));

                    var newNeighbor = (IoCcNeighbor)Node.MallocNeighbor(Node, (IoNetClient<IoCcPeerMessage>)Source, Tuple.Create(idCheck, remoteServices, (IPEndPoint)extraData));

                    //Add new neighbor
                    if(await Node.ZeroEnsureAsync(async s =>
                    {
                        //transfer?
                        if (!newNeighbor.RemoteAddress.IpEndPoint.Address.Equals(((IPEndPoint) extraData).Address) ||
                            newNeighbor.RemoteAddress.IpEndPoint.Port != ((IPEndPoint) extraData).Port ||
                            !Node.Neighbors.TryAdd(keyStr, newNeighbor)) return false;

                        // Handle zero
                        Node.ZeroOnCascade(newNeighbor);

                        return await newNeighbor.ZeroEnsureAsync(s =>
                        {
                            var id = newNeighbor.Id;
                            var sub = newNeighbor.ZeroEvent(source =>
                            {
                                try
                                {
                                    if (Node.Neighbors.TryRemove(id, out var n))
                                    {
                                        _logger.Trace($"Removed {n.Description} from {Description}");
                                    }
                                } catch { }

                                return Task.CompletedTask;
                            });
                            return Task.FromResult(true);
                        });

                    }).ConfigureAwait(false))
                    {
                        //SEND ACK
                        await newNeighbor.SendPingAsync().ConfigureAwait(false);
                    }
                    else
                    {
                        _logger.Warn($"<<{nameof(Pong)}: Unable to transfer neighbor {keyStr} ownershit, dropped");
                        await newNeighbor.ZeroAsync(this).ConfigureAwait(false);
                    }
                }
                else if(oldNeighbor != null)
                {
                    //throw new ApplicationException($"Neighbor UDP router failed! BUG!");
                }
            }
            else if (!Verified) //Process ACK SYN
            {
                State = NeighborState.Verified;

                //set ext address as seen by neighbor
                ExtGossipAddress ??= IoNodeAddress.Create($"tcp://{pong.DstAddr}:{CcNode.Services.IoCcRecord.Endpoints[IoCcService.Keys.gossip].Port}");

                Verified = true;

                _logger.Trace($"<<{nameof(Pong)}: ACK SYN: {Description}");

                if (CcNode.OutboundCount < CcNode.parm_max_outbound)
                {
                    _logger.Trace($"{(RoutedRequest ? "V>" : "X>")}(acksyn): {(CcNode.OutboundCount < CcNode.parm_max_outbound ? "Send Peer REQUEST" : "Withheld Peer REQUEST")}, to = {Description}, from nat = {ExtGossipAddress}");
                    await SendPeerRequestAsync().ConfigureAwait(false);
                }
            }
            else
            {
                Interlocked.Decrement(ref _patMatcher);
                _logger.Trace($"<<{nameof(Pong)}: {Description}");   
            }
        }

        /// <summary>
        /// Sends a universal ping packet
        /// </summary>
        /// <param name="dest">The destination address</param>
        /// <returns>Task</returns>
        public async Task<bool> SendPingAsync(IoNodeAddress dest = null)
        {
            try
            {
                //Check for teardown
                if(Zeroed())
                    return false;

                dest ??= RemoteAddress;

                //Create the ping request
                var pingRequest = new Ping
                {
                    DstAddr = dest.IpEndPoint.Address.ToString(),
                    NetworkId = 6,
                    Version = 0,
                    SrcAddr = "0.0.0.0", //TODO auto/manual option here
                    SrcPort = (uint)CcNode.Services.IoCcRecord.Endpoints[IoCcService.Keys.peering].Port,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / parm_max_time_error * parm_max_time_error + parm_max_time_error / 2 
                };

                // Is this a routed request?
                if (RoutedRequest)
                {
                    // send
                    var (sent, ping) = await SendMessageAsync(data: pingRequest.ToByteString(), type: IoCcPeerMessage.MessageTypes.Ping);
                    if (sent > 0)
                    {
                        _pingRequest = ping.Data;
                        Interlocked.Increment(ref _patMatcher);
                        _logger.Trace($">>{nameof(Ping)}: Sent {sent} bytes, {Description}");
                        return true;
                    }
                    else
                    {
                        _logger.Debug($">>{nameof(Ping)}: [FAILED], {Description}");
                        return false;
                    }
                }
                else //The destination state was undefined, this is local
                {
                    //Try to route it...
                    IoCcNeighbor ccNeighbor = null;
                    foreach (var neighbor in Node.Neighbors.Values)
                    {
                        // Skip local
                        if (!((IoCcNeighbor)neighbor).RoutedRequest)
                            continue;

                        //Only route if verified 
                        if (!((IoCcNeighbor)neighbor).Verified)
                            continue;

                        //Find match
                        if (((IoCcNeighbor)neighbor).RemoteAddress.Equals(dest))
                            ccNeighbor = (IoCcNeighbor)neighbor;
                    }

                    //If no match we take local
                    ccNeighbor ??= ((IoCcNeighborDiscovery)Node).LocalNeighbor;

                    if (ccNeighbor.RoutedRequest)
                    {
                        return await ccNeighbor.SendPingAsync().ConfigureAwait(false);
                    }
                    else
                    {
                        var (sent, ping) = await SendMessageAsync(dest, pingRequest.ToByteString(), IoCcPeerMessage.MessageTypes.Ping).ConfigureAwait(false);
                        if (sent > 0)
                        {
                            ccNeighbor._pingRequests.TryAdd(Convert.ToBase64String(IoCcIdentity.Sha256.ComputeHash(ping.Data.Memory.AsArray())), ping.Data);
                            _logger.Trace($">>{nameof(SendPingAsync)}:(X) Sent {sent}, {Description}");
                            return true;
                        }
                        else
                        {
                            _logger.Debug($">>{nameof(SendPingAsync)}:(X) [FAILED], {Description}");
                            return false;
                        }
                    }
                }
            }
            catch (NullReferenceException e) { _logger.Trace(e, Description); }
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            catch (Exception e)
            {
                _logger.Error(e, $"ERROR z = {Zeroed()}, dest = {dest}, source = {Source}, _discoveryRequest = {_discoveryRequest}");
            }

            return false;
        }

        /// <summary>
        /// Sends a discovery request
        /// </summary>
        /// <param name="dest">The destination address</param>
        /// <returns>Task</returns>
        public async Task<bool> SendDiscoveryRequestAsync(IoNodeAddress dest = null)
        {
            try
            {
                if (!IsAutopeering || State < NeighborState.Verified)
                {
                    _logger.Warn($"{nameof(SendDiscoveryRequestAsync)}: [ABORTED], {Description}, s = {State}, a = {IsAutopeering}");
                    return false;
                }
                
                dest ??= RemoteAddress;
                var discoveryRequest = new DiscoveryRequest
                {
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / parm_max_time_error * parm_max_time_error + parm_max_time_error / 2
                };

                var (sent, discovery) = await SendMessageAsync(dest, discoveryRequest.ToByteString(), IoCcPeerMessage.MessageTypes.DiscoveryRequest).ConfigureAwait(false);
                if (sent > 0)
                {
                    _discoveryRequest = discovery.Data;
                    _logger.Trace($">>{nameof(SendDiscoveryRequestAsync)}: Sent {sent}, {Description}");
                }
                else
                    _logger.Error($">>{nameof(SendDiscoveryRequestAsync)}: [FAILED], {Description} ");
            }
            catch (NullReferenceException e) { _logger.Trace(e, Description); }
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            catch (Exception e)
            {
                _logger.Error(e, $"{nameof(SendDiscoveryRequestAsync)}: [ERROR] z = {Zeroed()}, state = {State}, dest = {dest}, source = {Source}, _discoveryRequest = {_discoveryRequest}");
            }

            return false;
        }

        /// <summary>
        /// Sends a peer request
        /// </summary>
        /// <returns>Task</returns>
        public async Task<bool> SendPeerRequestAsync(IoNodeAddress dest = null)
        {
            try
            {
                if (!IsAutopeering || IsPeerConnected)
                {
                    _logger.Warn($"{nameof(SendPeerRequestAsync)}: [ABORTED], {Description}, s = {State}, a = {IsAutopeering}, p = {IsPeerConnected}");
                    return false;
                }
                
                dest ??= RemoteAddress;
                var peerRequest = new PeeringRequest
                {
                    Salt = new Salt { ExpTime = (ulong)DateTimeOffset.UtcNow.AddHours(2).ToUnixTimeSeconds(), Bytes = GetSalt },
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / parm_max_time_error * parm_max_time_error + parm_max_time_error / 2
                };

                var (sent,peerReqPacket) = await SendMessageAsync(dest, peerRequest.ToByteString(), IoCcPeerMessage.MessageTypes.PeeringRequest).ConfigureAwait(false);
                if (sent > 0)
                {
                    _peerRequest = peerReqPacket.Data;
                    _logger.Trace($">>{nameof(SendPeerRequestAsync)}: Sent {sent}, {Description}");
                    return true;
                }
                else
                    _logger.Debug($">>{nameof(SendPeerRequestAsync)}: [FAILED], {Description}, {MetaDesc}");
            }
            catch (NullReferenceException e){_logger.Trace(e, Description);}
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            catch (Exception e)
            {
                _logger.Error(e,$"{nameof(SendPeerRequestAsync)}: [FAILED], {Description}, {MetaDesc}");
            }

            return false;
        }

        /// <summary>
        /// Tell peer to drop us when things go wrong. (why or when? cause it wont reconnect otherwise. This is a bug)
        /// </summary>
        /// <returns></returns>
        private async Task SendPeerDropAsync(IoNodeAddress dest = null)
        {
            try
            {
                if (!IsAutopeering)
                {
                    _logger.Warn($"{nameof(SendPeerDropAsync)}: [ABORTED], {Description}, {MetaDesc}");
                    return;
                }

                dest ??= RemoteAddress;

                var dropRequest = new PeeringDrop
                {
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / parm_max_time_error * parm_max_time_error + parm_max_time_error / 2
                };

                var sent = 0;
                _logger.Trace((sent = (await SendMessageAsync(dest, dropRequest.ToByteString(),
                        IoCcPeerMessage.MessageTypes.PeeringDrop)
                    .ConfigureAwait(false)).sent) > 0
                    ? $">>{nameof(PeeringDrop)}: Sent {sent}, {Description}"
                    : $">>{nameof(SendPeerDropAsync)}: [FAILED], {Description}, {MetaDesc}");
            }
            catch (NullReferenceException e) { _logger.Trace(e, Description); }
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            catch (Exception e)
            {
                _logger.Error(e, $"{nameof(SendPeerDropAsync)}: [ERROR], {Description}, s = {State}, a = {IsAutopeering}, p = {IsPeerConnected}, d = {dest}, s = {Source}");
            }
        }

        //TODO complexity
        /// <summary>
        /// Attaches a gossip peer to this neighbor
        /// </summary>
        /// <param name="ioCcPeer">The peer</param>
        /// <param name="direction"></param>
        public bool AttachPeer(IoCcPeer ioCcPeer, Kind direction)
        {
            lock (this)
            {
                //Race for direction
                if (_peer != null || Interlocked.CompareExchange(ref _direction, (int)direction, (int)Kind.Undefined) != (int)Kind.Undefined)
                {
                    _logger.Warn($"oz: {_peer?.Description} already won {Direction}, race for {direction} lost");
                    return false;
                }
                
                _peer = ioCcPeer ?? throw new ArgumentNullException($"{nameof(ioCcPeer)}");
            }

            _logger.Debug($"{nameof(AttachPeer)}: [WON] {_peer.Description}");

            State = NeighborState.Connected;
            ConnectedAtLeastOnce = true;
            PeerUptime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            _neighborZeroSub = ZeroEvent(async sender =>
            {
                try
                {
                    await _peer.ZeroAsync(this).ConfigureAwait(false);
                }
                catch { }
            });

            return true;
        }

        /// <summary>
        /// Detaches a peer from this neighbor
        /// </summary>
        public async Task DetachPeerAsync(bool force = false)
        {
            var peer = _peer;

            lock (this)
            {
                if (_peer == null && !force)
                    return;
                _peer = null;
            }

            _logger.Trace($"{(ConnectedAtLeastOnce ? "Useful" : "Useless")} {Direction} peer detaching: s = {State}, a = {IsAutopeering}, p = {IsPeerConnected}, {peer?.Description??Description}");

            await DetachPeerAsync().ConfigureAwait(false);

            //Detach zeroed
            Unsubscribe(_neighborZeroSub);
            _neighborZeroSub = default;

            if(peer != null)
                await peer.DetachNeighborAsync().ConfigureAwait(false);

            Interlocked.Exchange(ref _direction, 0);
            ExtGossipAddress = null;
            PeerUptime = 0;
            TotalPats = 0;
            ConnectionAttempts = 0;
            State = NeighborState.Disconnected;
        }


        /// <summary>
        /// The state transition history, sourced from <see  cref="IoZero{TJob}"/>
        /// </summary>
#if DEBUG
        public IoStateTransition<NeighborState>[] StateTransitionHistory = new IoStateTransition<NeighborState>[Enum.GetNames(typeof(NeighborState)).Length];//TODO what should this size be?
#else
        public IoStateTransition<IoJobMeta.JobState>[] StateTransitionHistory;
#endif

        /// <summary>
        /// Gets and sets the state of the work
        /// </summary>
        public NeighborState State
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
                    StateTransitionHistory[(int)prevState.Value].Repeat = prevState;
                }
                else
                {
                    StateTransitionHistory[(int)prevState.Value] = prevState;
                }
#else
                _currState.Enter(value);
#endif
                //terminate
                if (value == NeighborState.ZeroState)
                {
                    State = NeighborState.FinalState;
                }
            }
        }
    }
}
