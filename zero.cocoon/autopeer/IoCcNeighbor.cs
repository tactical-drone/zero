using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.Collections;
using NLog;
using Proto;
using zero.cocoon.identity;
using zero.cocoon.models;
using zero.cocoon.models.services;
using zero.core.conf;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.misc;
using Logger = NLog.Logger;

namespace zero.cocoon.autopeer
{
    public class IoCcNeighbor : IoNeighbor<IoCcPeerMessage>
    {
        public IoCcNeighbor(IoNode<IoCcPeerMessage> node, IoNetClient<IoCcPeerMessage> ioNetClient, object extraData = null, IoCcService services = null) : base(node, ioNetClient, userData => new IoCcPeerMessage("peer rx", $"{ioNetClient.AddressString}", ioNetClient))
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
                Task.Factory.StartNew(async () =>
                {
                    while (!Zeroed())
                    {
                        await Task.Delay(_random.Next(parm_zombie_max_ttl / 2) * 1000 + parm_zombie_max_ttl / 4 * 1000, AsyncTasks.Token).ConfigureAwait(false);
                        await EnsurePeerAsync().ConfigureAwait(false);
                    }
                }, TaskCreationOptions.LongRunning);
            }
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        private string _description;
        public override string Description
        {
            get
            {
                if (_description != null)
                    return _description;
                return _description = $"{(RoutedRequest?"R":"L")} {Identity.IdString()}:{((IoNetClient<IoCcPeerMessage>)Source).RemoteAddress?.Port}";
            }
        }

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
        protected volatile IoCcPeer Peer;

        /// <summary>
        /// Indicates whether we have successfully established a connection before
        /// </summary>
        protected volatile bool PeerConnectedAtLeastOnce;

        //uptime
        private long _uptime = 0;
        public long Uptime
        {
            get => Interlocked.Read(ref _uptime);
            set => Interlocked.Exchange(ref _uptime, value);
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

        /// <summary>
        /// Who contacted who?
        /// </summary>
        public Kind Direction { get; protected set; } = Kind.Undefined;

        /// <summary>
        /// inbound
        /// </summary>
        public bool Inbound => Direction == Kind.Inbound && Verified;

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
        /// Only one un-routed ping request allowed at a time. 
        /// </summary>
        private SemaphoreSlim _pingRequestBarrier = new SemaphoreSlim(1);

        /// <summary>
        /// Seconds since valid
        /// </summary>
        private long _keepAliveSec;

        /// <summary>
        /// Seconds since last neighbor pat
        /// </summary>
        protected long KeepAliveSec
        {
            get => Interlocked.Read(ref _keepAliveSec);
            set => Interlocked.Exchange(ref _keepAliveSec, value);
        }

        /// <summary>
        /// Seconds since valid
        /// </summary>
        private long _keepAlives;

        /// <summary>
        /// Seconds since last neighbor pat
        /// </summary>
        protected long KeepAlives
        {
            get => Interlocked.Read(ref _keepAlives);
            set => Interlocked.Exchange(ref _keepAlives, value);
        }

        /// <summary>
        /// Seconds since valid
        /// </summary>
        public long LastKeepAliveReceived => DateTimeOffset.UtcNow.ToUnixTimeSeconds() - KeepAliveSec;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private volatile Ping _pingRequest;
        
        /// <summary>
        /// Used to Match requests
        /// </summary>
        private volatile DiscoveryRequest _discoveryRequest;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private volatile PeeringRequest _peerRequest;

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
        public override string Id => $"{MakeId(Identity, RemoteAddress ?? ((IoNetClient<IoCcPeerMessage>)Source).ListeningAddress)}";

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
        /// Maximum number of services supported
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_zombie_max_ttl = 240;

        /// <summary>
        /// Handle to peer zero sub
        /// </summary>
        private Func<IIoZeroable, Task> _peerZeroSub;

        /// <summary>
        /// Handle to neighbor zero sub
        /// </summary>
        private Func<IIoZeroable, Task> _neighborZeroSub;

        /// <summary>
        /// Number of connection attempts
        /// </summary>
        private long _connectionAttempts;

        /// <summary>
        /// A servicemap helper
        /// </summary>
        protected ServiceMap ServiceMap
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
        protected override void ZeroUnmanaged()
        {
            _pingRequestBarrier.Dispose();
            base.ZeroUnmanaged();

            _pingRequestBarrier = null;
            Peer = null;
            NeighborDiscoveryNode = null;
            _protocolChannel = null;
            _neighborZeroSub = null;
            _peerZeroSub = null;
        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override void ZeroManaged()
        {
            DetachPeer();
            base.ZeroManaged();
        }


        /// <summary>
        /// Ensures that the peer is running
        /// </summary>
        public async Task EnsurePeerAsync()
        {
            if(!RoutedRequest || KeepAliveSec > 0 && LastKeepAliveReceived < parm_zombie_max_ttl / 2)
                return;

            if (KeepAliveSec > 0 && LastKeepAliveReceived > parm_zombie_max_ttl * 2)
            {
                _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} Zeroing zombie neighbor {Id}");
                await Zero(this);
            }

            await SendPingAsync();

            if (PeerConnectedAtLeastOnce && Direction != Kind.Inbound && (Peer == null || !(Peer?.Source?.IsOperational??false)))
            {
                _logger.Info($"{(RoutedRequest ? "V>" : "X>")} RE-/Requesting to peer with neighbor {Id}:{RemoteAddress.Port}...");
                DetachPeer(true);
                await SendPingAsync();
            }
            else if (Peer == null && Direction == Kind.Inbound) //was there a race?
            {
                DetachPeer(true);
                await SendPingAsync();
            }
        }

        /// <summary>
        /// Start processors for this neighbor
        /// </summary>
        /// <param name="spawnProducer">Spawns a source thread</param>
        /// <returns></returns>
        public override async Task SpawnProcessingAsync(bool spawnProducer = true)
        {
            var processingAsync = base.SpawnProcessingAsync(spawnProducer);
            var protocol = ProcessAsync();

            if (!RoutedRequest && CcNode.BootstrapAddress != null)
            {
                _logger.Info($"{(RoutedRequest ? "V>" : "X>")} {Id} Boostrapping from {CcNode.BootstrapAddress}");
                await SendPingAsync(CcNode.BootstrapAddress);
            }

            await Task.WhenAll(processingAsync, protocol).ConfigureAwait(false);

            if (processingAsync.IsFaulted)
            {
                _logger.Fatal(processingAsync.Exception, "Neighbor processing returned with errors!");
#pragma warning disable 4014
                Zero(this);
#pragma warning restore 4014
            }

            if (protocol.IsFaulted)
            {
                _logger.Fatal(protocol.Exception, "Protocol processing returned with errors!");
#pragma warning disable 4014
                Zero(this);
#pragma warning restore 4014
            }
        }

        /// <summary>
        /// Processes protocol messages
        /// </summary>
        /// <param name="consumer">The consumer that need processing</param>
        /// <param name="msgArbiter">The arbiter</param>
        /// <param name="processCallback">The process callback</param>
        /// <returns></returns>
        private async Task ProcessMsgBatchAsync(IoLoad<IoCcProtocolMessage> consumer,
            IoChannel<IoCcProtocolMessage> msgArbiter,
            Func<Tuple<IMessage, object, Packet>, IoChannel<IoCcProtocolMessage>, Task> processCallback)
        {
            if (consumer == null)
                return;

            var stopwatch = Stopwatch.StartNew();

            try
            {
                var protocolMsgs = ((IoCcProtocolMessage)consumer).Messages;

                //_logger.Trace($"{consumer.TraceDescription} Processing `{protocolMsgs.Count}' protocol messages");
                foreach (var message in protocolMsgs)
                {
                    try
                    {
                        await processCallback(message, msgArbiter).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e, $"{consumer.TraceDescription} Processing protocol failed: ");
                    }
                }

                consumer.ProcessState = IoJob<IoCcProtocolMessage>.State.Consumed;

                stopwatch.Stop();
                //_logger.Trace($"{(RoutedRequest ? "V>" : "X>")} Processed `{protocolMsgs.Count}' consumer: t = `{stopwatch.ElapsedMilliseconds:D}', `{protocolMsgs.Count * 1000 / (stopwatch.ElapsedMilliseconds + 1):D} t/s'");
            }
            finally
            {
                ((IoCcProtocolMessage)consumer).Messages = null;
            }
        }

        /// <summary>
        /// Processes a protocol message
        /// </summary>
        /// <returns></returns>
        private async Task ProcessAsync()
        {
            if (_protocolChannel == null)
                _protocolChannel = Source.GetChannel<IoCcProtocolMessage>(nameof(IoCcNeighbor));

            _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} Processing peer msgs: `{Description}'");
            while (!Zeroed())
            {
                if (_protocolChannel == null)
                {
                    _logger.Warn($"{(RoutedRequest ? "V>" : "X>")} Waiting for `{Description}' stream to spin up...");
                    _protocolChannel = Source.AttachProducer<IoCcProtocolMessage>(nameof(IoCcNeighbor));
                    await Task.Delay(2000);//TODO config
                    continue;
                }

                await _protocolChannel.ConsumeAsync(async batch =>
                {
                    try
                    {
                        await ProcessMsgBatchAsync(batch, _protocolChannel, (msg, forward) =>
                        {
#pragma warning disable 4014
                            try
                            {
                                IoCcNeighbor ccNeighbor = null;
                                //TODO optimize
                                Node.Neighbors.TryGetValue(
                                    MakeId(IoCcIdentity.FromPubKey(msg.Item3.PublicKey.Span),
                                        IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) msg.Item2)), out var n);
                                if (n == null)
                                    ccNeighbor = this;
                                else
                                    ccNeighbor = (IoCcNeighbor) n;

                                switch (msg.Item1.GetType().Name)
                                {
                                    case nameof(Ping):
                                        ccNeighbor.Process((Ping) msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(Pong):
                                        ccNeighbor.Process((Pong) msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(DiscoveryRequest):
                                        ccNeighbor.Process((DiscoveryRequest) msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(DiscoveryResponse):
                                        ccNeighbor.Process((DiscoveryResponse) msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(PeeringRequest):
                                        ccNeighbor.Process((PeeringRequest) msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(PeeringResponse):
                                        ccNeighbor.Process((PeeringResponse) msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(PeeringDrop):
                                        ccNeighbor.Process((PeeringDrop) msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                }
                            }
                            catch (Exception e)
                            {
                                _logger.Error(e, "Unable to process protocol message");
                            }
#pragma warning restore 4014
                            return Task.CompletedTask;
                        }); //dont'.ConfigureAwait(false);
                    }
                    finally
                    {
                        if (batch != null && batch.ProcessState != IoJob<IoCcProtocolMessage>.State.Consumed)
                            batch.ProcessState = IoJob<IoCcProtocolMessage>.State.ConsumeErr;
                    }
                });//don't ConfigureAwait(false);

                if (!_protocolChannel.Source.IsOperational)
                    break;
            }

            _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} Stopped processing peer msgs: `{Description}'");
        }

        /// <summary>
        /// Peer drop request
        /// </summary>
        /// <param name="request">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task Process(PeeringDrop request, object extraData, Packet packet)
        {
            var diff = 0;
            if (!RoutedRequest || (diff = Math.Abs((int)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Timestamp))) > parm_max_time_error * 2)
            {
                _logger.Trace($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringDrop)}: Ignoring {diff}s old/invalid request, error = ({diff})");
                return;
            }

            //only verified nodes get to drop
            if (!Verified || Peer == null)
                return;

            _logger.Debug($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringDrop)}: {Direction} Peer= {Peer?.Id ?? "null"}");
            Peer?.Zero(this);
            
            //Attempt reconnect
            //await EnsurePeerAsync();
        }

        /// <summary>
        /// Peering Request message from client
        /// </summary>
        /// <param name="request">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task Process(PeeringRequest request, object extraData, Packet packet)
        {
            if (!RoutedRequest || !Verified)
            {
                _logger.Trace($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringRequest)}({(RoutedRequest?"R":"L")}): Dropped request, not verified! ({DateTimeOffset.FromUnixTimeSeconds(request.Timestamp)})");
                return;
            }

            if (Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Timestamp) > parm_max_time_error * 2)
            {
                _logger.Trace($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringRequest)}({(RoutedRequest ? "R" : "L")}): Dropped request, stale! error = ({Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Timestamp)})");
                return;
            }

            var peeringResponse = new PeeringResponse
            {
                ReqHash = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(packet.Data.ToByteArray())),
                Status = Direction == Kind.Inbound || CcNode.InboundCount < CcNode.parm_max_inbound
            };
            var wasInbound = false;

            lock (this)
            {
                //If not selected, select inbound
                if (Direction == Kind.Undefined)
                {
                    Direction = peeringResponse.Status? Kind.Inbound : Direction;
                }
                else if (Direction == Kind.OutBound) //If it is outbound say no
                {
                    _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} Peering {Kind.Inbound} Rejected: {Id} is already {Kind.OutBound}");
                    peeringResponse.Status = false;
                }
                else if (Direction == Kind.Inbound)
                {
                    wasInbound = true;
                }
            }

            //Discovery request
            if (!wasInbound)
            {
                _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} {Kind.Inbound} peering request {(peeringResponse.Status ? "[ACCEPTED]" : "[REJECTED]")}({(CcNode.InboundCount < CcNode.parm_max_inbound?"Open":"FULL")}), currently {Direction}: {Id}");

                if( Direction == Kind.Inbound)
                    await SendDiscoveryRequestAsync();
            }
            else
            {
                if (Peer != null && PeerConnectedAtLeastOnce && (!Peer.Source.IsOperational ))// || !Peer.IsArbitrating)) //TODO
                {
                    _logger.Warn($"{(RoutedRequest ? "V>" : "X>")} Found zombie {Direction} peer({(PeerConnectedAtLeastOnce ? "C" : "DC")}) {Id}, Operational = {Peer?.Source?.IsOperational}, Arbitrating = {Peer?.IsArbitrating}");
                    Peer?.Zero(this);
                }
                else if (Peer == null)
                {
                    _logger.Warn($"{(RoutedRequest ? "V>" : "X>")} Peering Re-/Authorized... {Direction} ({(PeerConnectedAtLeastOnce ? "C" : "DC")}), {Id}");
                }
            }

            _logger.Trace($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringResponse)}: Sent Allow Status = {peeringResponse.Status}, Capacity = {-CcNode.Neighbors.Count + CcNode.MaxClients}");
            await SendMessage(RemoteAddress, peeringResponse.ToByteString(), IoCcPeerMessage.MessageTypes.PeeringResponse);
        }

        /// <summary>
        /// Peer response message from client
        /// </summary>
        /// <param name="response">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task Process(PeeringResponse response, object extraData, Packet packet)
        {
            var request = Volatile.Read(ref _peerRequest); 
            if (!RoutedRequest || request == null)
            {
                _logger.Trace($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringResponse)}: Unexpected response from {extraData}, {RemoteAddress}");
                return;
            }

            var hash = IoCcIdentity.Sha256.ComputeHash(request.ToByteArray());

            if (!response.ReqHash.SequenceEqual(hash))
            {
                if (RemoteAddress == null)
                    _logger.Debug($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringResponse)}: Got invalid response from {MakeId(IoCcIdentity.FromPubKey(packet.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint)extraData))}");
                else
                    _logger.Debug($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringResponse)}: Got invalid response hash from {extraData}, age = {DateTimeOffset.UtcNow.ToUnixTimeSeconds() - _peerRequest.Timestamp}s, {Convert.ToBase64String(hash)} - {Convert.ToBase64String(response.ReqHash.ToByteArray())}");

                return;
            }

            _peerRequest = null;

            _logger.Trace($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringResponse)}: Got status = {response.Status}");

            var alreadyOutbound = false;
            lock (this)
            {
                if (Direction == Kind.Undefined)
                    Direction = response.Status? Kind.OutBound : Direction;
                else if (Direction == Kind.Inbound)
                {
                    _logger.Debug($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringResponse)}: {nameof(Kind.OutBound)} request dropped, {nameof(Kind.Inbound)} received");
                }
                else if(Direction == Kind.OutBound)
                {
                    alreadyOutbound = true;
                }
            }

            if (!alreadyOutbound)
            {
                //if(response.Status && Direction == Kind.OutBound)
                //    _logger.Info($"{Kind.OutBound} peering request {(response.Status?"[ACCEPTED]":"[REJECTED]")}, currently {Direction}: {Id}");
                //else
                _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} {Kind.OutBound} peering request {(response.Status ? "[ACCEPTED]" : "[REJECTED]")}, currently {Direction}: {Id}");

                if (Direction == Kind.OutBound)
                {
                    if(await CcNode.ConnectToPeer(this))
                        await SendDiscoveryRequestAsync();
                    Interlocked.Increment(ref _connectionAttempts);
                }
            }
            else
            {
                //_logger.Fatal($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringResponse)}: Not expected, already {nameof(Kind.OutBound)}");
                if (Peer != null && PeerConnectedAtLeastOnce && (!Peer.Source.IsOperational ))// || !Peer.IsArbitrating))
                {
                    _logger.Warn($"{(RoutedRequest ? "V>" : "X>")} Found zombie {Direction} peer({(PeerConnectedAtLeastOnce ? "C" : "DC")}) {Id}, Operational = {Peer?.Source?.IsOperational}, Arbitrating = {Peer?.IsArbitrating}");
                    Peer?.Zero(this);
                }
                else if (Peer == null && Interlocked.Read(ref _connectionAttempts) > 0)
                {
                    _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} Peering reconnecting attempts {Interlocked.Read(ref _connectionAttempts)}... {Direction} ({(PeerConnectedAtLeastOnce ? "C" : "DC")}), {Id}");
                    if (await CcNode.ConnectToPeer(this))
                        await SendDiscoveryRequestAsync();
                    else
                    {
                        Interlocked.Increment(ref _connectionAttempts);
                    }
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
        private async Task<(int sent, Packet responsePacket)> SendMessage(IoNodeAddress dest = null, ByteString data = null, IoCcPeerMessage.MessageTypes type = IoCcPeerMessage.MessageTypes.Undefined)
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

                packet.Signature = ByteString.CopyFrom(CcNode.CcId.Sign(packet.Data.ToByteArray(), 0, packet.Data.Length));

                var msgRaw = packet.ToByteArray();

                
                    var sent = await ((IoUdpClient<IoCcPeerMessage>) Source).Socket.SendAsync(msgRaw, 0, msgRaw.Length,
                        dest.IpEndPoint);
                    _logger.Trace($"{(RoutedRequest ? "V>" : "X>")} {Enum.GetName(typeof(IoCcPeerMessage.MessageTypes), packet.Type)}: Sent {sent} bytes to {(RoutedRequest?$"{Identity.IdString()}":$"")}@{dest.IpEndPoint}");
                    return (sent, packet);
            }
            catch (NullReferenceException) { }
            catch (TaskCanceledException){ }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
            catch (Exception e)
            {
                _logger.Error(e, $"Failed to send message {Id}");
            }
            
            return (0, null);
        }

        /// <summary>
        /// Discovery response message
        /// </summary>
        /// <param name="response">The response</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task Process(DiscoveryResponse response, object extraData, Packet packet)
        {
            var discoveryRequest = Volatile.Read(ref _discoveryRequest);
            if (discoveryRequest == null || !RoutedRequest || response.Peers.Count > parm_max_discovery_peers)
            {
                _logger.Debug($"{(RoutedRequest?"V>":"X>")}{nameof(DiscoveryResponse)}: Dropped! Got unexpected response count = ({response.Peers.Count}) from {MakeId(IoCcIdentity.FromPubKey(packet.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint)extraData))}, RemoteAddress = {RemoteAddress}, request = {_discoveryRequest}");
                return;
            }

            if (!response.ReqHash.SequenceEqual(IoCcIdentity.Sha256.ComputeHash(discoveryRequest.ToByteArray())))
            {
                _logger.Debug($"{(RoutedRequest?"V>":"X>")}{nameof(DiscoveryResponse)}: Got request hash from {Id}");
                return;
            }

            _discoveryRequest = null;

            var count = 0;
            foreach (var responsePeer in response.Peers)
            {
                //max neighbor check
                if (Node.Neighbors.Count > CcNode.MaxClients)
                    break;

                //Any services attached?
                if (responsePeer.Services == null)
                    continue;

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

                var services = new IoCcService {IoCcRecord = new IoCcRecord()};
                var newRemoteEp = new IPEndPoint(IPAddress.Parse(responsePeer.Ip), (int)responsePeer.Services.Map[IoCcService.Keys.peering.ToString()].Port);

                if (responsePeer.Services.Map.Count <= parm_max_services)
                {
                    foreach (var kv in responsePeer.Services.Map)
                    {
                        services.IoCcRecord.Endpoints.TryAdd(Enum.Parse<IoCcService.Keys>(kv.Key), IoNodeAddress.Create($"{kv.Value.Network}://{responsePeer.Ip}:{kv.Value.Port}"));
                    }
                }
                else
                {
                    _logger.Debug($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringResponse)}: Max service supported {parm_max_services}, got {responsePeer.Services.Map.Count}");
                    services = null;
                    break;
                }

                //sanity check
                if(services == null || services.IoCcRecord.Endpoints.Count == 0)
                    continue;

                //create neighbor
                var newNeighbor = (IoCcNeighbor)Node.MallocNeighbor(Node, (IoNetClient<IoCcPeerMessage>)Source, Tuple.Create(IoCcIdentity.FromPubKey(responsePeer.PublicKey.Span), services, newRemoteEp));
                if (Node.Neighbors.TryAdd(newNeighbor.Id, newNeighbor))
                {
                    Node.ZeroOnCascade(newNeighbor);//TODO: Maybe remove? Use the one that floods through source?
                    var task = Task.Factory.StartNew(async () =>
                    {
                        await Task.Delay(parm_ping_timeout * count++);
                        if (!newNeighbor.Verified)
                        {
                            _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} Suggested: {newNeighbor.Id} from {Id}");
                            await newNeighbor.EnsurePeerAsync();
                        }
                    }, TaskCreationOptions.LongRunning);
                }
                else
                {
                   await newNeighbor.Zero(this);
                }
            }

            if(Node.Neighbors.Count < CcNode.MaxClients)
                _logger.Debug($"{(RoutedRequest ? "V>" : "X>")}{nameof(PeeringResponse)}: Scanned {count}/{response.Peers.Count} neighbors...");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="extraData"></param>
        /// <param name="packet"></param>
        /// <returns></returns>
        private async Task Process(DiscoveryRequest request, object extraData, Packet packet)
        {
            if (!RoutedRequest || Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Timestamp) > parm_max_time_error * 2)
            {
                _logger.Trace($"{(RoutedRequest?"V>":"X>")}{nameof(DiscoveryRequest)}: Dropped request, not verified! error = {Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Timestamp)}, ({DateTimeOffset.FromUnixTimeSeconds(request.Timestamp)})");
                return;
            }

            //TODO accuracy param
            if (Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Timestamp) > parm_max_time_error * 2)
            {
                _logger.Trace($"{(RoutedRequest?"V>":"X>")}{nameof(DiscoveryRequest)}: Dropped stale request, error = ({Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Timestamp)})");
                return;
            }

            var discoveryResponse = new DiscoveryResponse
            {
                ReqHash = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(packet.Data.ToByteArray())),
                //Peers = { new Peer { PublicKey = ByteString.CopyFrom(CcNode.CcId.PublicKey), Ip = ExtGossipAddress.Ip, Services = new ServiceMap { Map = { new Dictionary<string, NetworkAddress> { { IoCcService.Keys.peering.ToString(), new NetworkAddress { Network = "tcp", Port = (uint)CcNode.ExtAddress.Port } } } } } } }
            };

            int count = 0;
            foreach (var ioNeighbor in NeighborDiscoveryNode.Neighbors.Where(n=>((IoCcNeighbor)n.Value).Verified))
            {
                if(count == parm_max_discovery_peers)
                    break;
                
                if (ioNeighbor.Value == this || !((IoCcNeighbor)ioNeighbor.Value).RoutedRequest)
                    continue;

                if (((IoCcNeighbor) ioNeighbor.Value).RemoteAddress.Equals(CcNode.ExtAddress))
                {
                    _logger.Fatal($"Found us {((IoCcNeighbor)ioNeighbor.Value).RemoteAddress} in neighbors");
                    continue;
                }

                discoveryResponse.Peers.Add(new Peer
                {
                    PublicKey = ByteString.CopyFrom(((IoCcNeighbor)ioNeighbor.Value).Identity.PublicKey),
                    Services = ((IoCcNeighbor)ioNeighbor.Value).ServiceMap,
                    Ip = ((IoCcNeighbor)ioNeighbor.Value).RemoteAddress.Ip
                });
                count++;
            }

            await SendMessage(RemoteAddress, discoveryResponse.ToByteString(), IoCcPeerMessage.MessageTypes.DiscoveryResponse);
        }

        /// <summary>
        /// Ping message
        /// </summary>
        /// <param name="ping">The ping packet</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task Process(Ping ping, object extraData, Packet packet)
        {
            var remoteEp = (IPEndPoint)extraData;
            var age = Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - ping.Timestamp);
            if (age > parm_max_time_error * 2 ) //TODO params
            {
                _logger.Trace($"{(RoutedRequest?"V>":"X>")}{nameof(Ping)}: Dropped stale, age = {age}s");
                return;
            }

            //TODO optimize
            var gossipAddress = ((IoCcNeighborDiscovery)Node).Services.IoCcRecord.Endpoints[IoCcService.Keys.gossip];
            var peeringAddress = ((IoCcNeighborDiscovery)Node).Services.IoCcRecord.Endpoints[IoCcService.Keys.peering];
            var fpcAddress = ((IoCcNeighborDiscovery)Node).Services.IoCcRecord.Endpoints[IoCcService.Keys.fpc];

            var pong = new Pong
            {
                ReqHash = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(packet.Data.ToByteArray())),
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

            IoNodeAddress toProxyAddress = null;
            IoNodeAddress toAddress = IoNodeAddress.Create($"udp://{remoteEp.Address}:{ping.SrcPort}");
            var id = MakeId(IoCcIdentity.FromPubKey(packet.PublicKey.Span), toAddress);

            if (RoutedRequest)
            {
                await SendMessage(data: pong.ToByteString(), type: IoCcPeerMessage.MessageTypes.Pong);

                Verified = true;

                //set ext address as seen by neighbor
                ExtGossipAddress ??= IoNodeAddress.Create($"tcp://{ping.DstAddr}:{CcNode.Services.IoCcRecord.Endpoints[IoCcService.Keys.gossip].Port}");

                if (Peer == null && PeerConnectedAtLeastOnce && CcNode.Neighbors.Count <= CcNode.MaxClients) //TODO 
                {
                    _logger.Info($"RE-/Verified peer {Id}, Peering = {CcNode.Neighbors.Count <= CcNode.MaxClients}, NAT = {ExtGossipAddress}");
                    await SendPeerRequestAsync();
                }
            }
            else//new neighbor 
            {
                if (ping.SrcAddr != "0.0.0.0" && remoteEp.Address.ToString() != ping.SrcAddr)
                {
                    toProxyAddress = IoNodeAddress.Create($"udp://{ping.SrcAddr}:{ping.SrcPort}");
                    _logger.Trace($"static peer address received: {toProxyAddress}, source detected = udp://{remoteEp}");
                }
                else
                {
                    toProxyAddress = IoNodeAddress.CreateFromEndpoint("udp", remoteEp);
                    _logger.Trace($"automatic peer address detected: {toProxyAddress}, source declared = udp://{ping.SrcAddr}:{ping.SrcPort}");
                }

                await SendMessage(toAddress, pong.ToByteString(), IoCcPeerMessage.MessageTypes.Pong);

                if (toAddress.Ip != toProxyAddress.Ip)
                    await SendMessage(toAddress, pong.ToByteString(), IoCcPeerMessage.MessageTypes.Pong);

                _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} Probing new destination Id = {id}:{ping.SrcPort}, to = {toAddress}");
                await SendPingAsync(toAddress);

                if (CcNode.UdpTunnelSupport && toProxyAddress.Ip != toAddress.Ip)
                    await SendPingAsync(toProxyAddress);
            }
        }

        /// <summary>
        /// Pong message
        /// </summary>
        /// <param name="pong">The Pong packet</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task Process(Pong pong, object extraData, Packet packet)
        {
            if (!RoutedRequest)
                _pingRequestBarrier.Release();

            var pingRequest = Volatile.Read(ref _pingRequest);
            if (pingRequest == null)
            {
                if (RoutedRequest)
                {
                    if(KeepAlives > 0)
                        _logger.Debug($"{(RoutedRequest?"V>":"X>")}{nameof(Pong)}({GetHashCode()}):  Unexpected!, t = {KeepAlives},  s = {LastKeepAliveReceived}, d = {(Uptime>0? (Uptime - KeepAliveSec).ToString():"N/A")}, v = {Verified}, id = {Id}:{RemoteAddress.Port}");
                    //Node.Neighbors.Where(kv=> ((IoCcNeighbor)kv.Value).RoutedRequest).ToList().ForEach(kv=>_logger.Fatal($"{kv.Value.Id}:{((IoCcNeighbor)kv.Value).RemoteAddress.Port}"));
                }
                else { } //ignore

                return;
            }
            
            var hash = IoCcIdentity.Sha256.ComputeHash(pingRequest.ToByteArray());

            if (!pong.ReqHash.SequenceEqual(hash))
            {
                _logger.Debug(!RoutedRequest
                    ? $"{(RoutedRequest?"V>":"X>")}{nameof(Pong)}: Got invalid request from {MakeId(IoCcIdentity.FromPubKey(packet.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData))}"
                    : $"{(RoutedRequest?"V>":"X>")}{nameof(Pong)}: Got invalid request hash from {extraData} <=> {pingRequest}, {Convert.ToBase64String(hash)} - {Convert.ToBase64String(pong.ReqHash.ToByteArray())}");

                return;
            }

            _pingRequest = null;

            KeepAliveSec = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            Interlocked.Increment(ref _keepAlives);

            //Unknown source IP
            if (!RoutedRequest)
            {
                var idCheck = IoCcIdentity.FromPubKey(packet.PublicKey.Span);
                var keyStr = MakeId(idCheck, IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint)extraData));

                // remove stale neighbor PKs
                var staleId = Node.Neighbors
                    .Where(kv => ((IoCcNeighbor)kv.Value).RoutedRequest)
                    .Where(kv => ((IoCcNeighbor)kv.Value).RemoteAddress.Port == ((IPEndPoint) extraData).Port)
                    .Where(kv => !kv.Value.Source.Key.Contains(idCheck.PkString()))
                    .Select(kv => kv.Value.Id).FirstOrDefault();

                if(!string.IsNullOrEmpty(staleId) && Node.Neighbors.TryRemove(staleId, out var staleNeighbor)) 
                {
                    _logger.Warn($"Removing stale neighbor {staleNeighbor.Id}:{((IoUdpClient<IoCcPeerMessage>)staleNeighbor.Source).Socket.RemotePort}");
                    _logger.Warn($"Replaced stale neighbor {keyStr}:{((IPEndPoint)extraData).Port}");
#pragma warning disable 4014
                    staleNeighbor.Zero(this);
#pragma warning restore 4014
                }

                if (Node.Neighbors.Count <= CcNode.MaxClients * 2 && !Node.Neighbors.TryGetValue(keyStr, out _))
                {
                    var remoteServices = new IoCcService();
                    foreach (var key in pong.Services.Map.Keys.ToList())
                        remoteServices.IoCcRecord.Endpoints.TryAdd(Enum.Parse<IoCcService.Keys>(key), IoNodeAddress.Create($"{pong.Services.Map[key].Network}://{((IPEndPoint)extraData).Address}:{pong.Services.Map[key].Port}"));

                    var newNeighbor = (IoCcNeighbor)Node.MallocNeighbor(Node, (IoNetClient<IoCcPeerMessage>)Source, Tuple.Create(idCheck, remoteServices, (IPEndPoint)extraData));

                    if (newNeighbor.RemoteAddress.IpEndPoint.Address.Equals(((IPEndPoint)extraData).Address) &&
                        newNeighbor.RemoteAddress.IpEndPoint.Port == ((IPEndPoint)extraData).Port &&
                        Node.Neighbors.TryAdd(keyStr, newNeighbor))
                    {
                        var id = newNeighbor.Id;
                        var port = ((IPEndPoint) extraData).Port;
                        newNeighbor.ZeroEvent(zeroable =>
                        {
                            try
                            {
                                if (Node.Neighbors.TryRemove(id, out var n))
                                    _logger.Info($"{(PeerConnectedAtLeastOnce?"Useful":"Useless")} neighbor dropped {n.Id}:{port} from node {Description}");
                            }
                            catch { }

                            return Task.CompletedTask;
                        });
                        await newNeighbor.EnsurePeerAsync();
                    }
                    else
                    {
                        _logger.Warn($"{(RoutedRequest ? "V>" : "X>")} Create new neighbor {keyStr} skipped!");
                    }
                }
                else if(Node.Neighbors.Count <= CcNode.MaxClients * 2)
                {
                    throw new ApplicationException($"Neighbor UDP router failed! BUG!");
                }
            }
            else if (!Verified) //We have seen this IP before
            {
                Verified = true;
                //set ext address as seen by neighbor
                ExtGossipAddress ??= IoNodeAddress.Create($"tcp://{pong.DstAddr}:{CcNode.Services.IoCcRecord.Endpoints[IoCcService.Keys.gossip].Port}");

                if (CcNode.Neighbors.Count <= CcNode.MaxClients) //TODO 
                {
                    _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} Discovered/Verified peer {Id}:{RemoteAddress.Port}, {(CcNode.OutboundCount < CcNode.parm_max_outbound?"Connecting...":"Saved")}, NAT = {ExtGossipAddress}");
                    await SendPeerRequestAsync();
                }
            }
            else if (Verified)
            {
                //Just check everything is cool
                if (Peer != null && (/*!Peer.IsArbitrating ||*/ !Peer.Source.IsOperational) && Direction != Kind.Undefined)
                {
                    _logger.Warn($"Found zombie {Peer?.Neighbor?.Direction} Peer, closing: {Peer?.Id}");
#pragma warning disable 4014
                    Peer?.Zero(this);
#pragma warning restore 4014
                }
                else if(Peer == null && (PeerConnectedAtLeastOnce || Direction != Kind.Inbound)) //TODO remove
                {
                    //DetachPeer(true);
                    await SendPeerRequestAsync();
                }
                else if(Peer == null && Direction == Kind.Inbound && !PeerConnectedAtLeastOnce) //race
                {
                    await SendPingAsync();
                }
            }
        }

        /// <summary>
        /// Sends a ping packet
        /// </summary>
        /// <param name="dest">The destination address</param>
        /// <returns>Task</returns>
        public async Task SendPingAsync(IoNodeAddress dest = null)
        {
            try
            {
                if(Zeroed())
                    return;

                dest ??= RemoteAddress;

                var pingRequest = new Ping
                {
                    DstAddr = dest.IpEndPoint.Address.ToString(),
                    NetworkId = 6,
                    Version = 0,
                    SrcAddr = "0.0.0.0", //TODO auto/manual option here
                    SrcPort = (uint)CcNode.Services.IoCcRecord.Endpoints[IoCcService.Keys.peering].Port,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / parm_max_time_error * parm_max_time_error + parm_max_time_error / 2 
                };

                if (RoutedRequest)
                {
                    await SendMessage(dest, pingRequest.ToByteString(), IoCcPeerMessage.MessageTypes.Ping).ContinueWith(
                        r =>
                        {
                            if(r.IsCompletedSuccessfully)
                                _pingRequest = pingRequest;
                        });
                }
                else
                {
                    IoCcNeighbor ccNeighbor = null;
                    foreach (var neighbor in Node.Neighbors.Values)
                    {
                        if (!((IoCcNeighbor)neighbor).RoutedRequest)
                            continue;

                        if (!((IoCcNeighbor)neighbor).Verified)
                            continue;

                        if (((IoCcNeighbor)neighbor).RemoteAddress.Equals(dest))
                            ccNeighbor = (IoCcNeighbor)neighbor;
                    }

                    ccNeighbor ??= this;

                    if (!ccNeighbor.RoutedRequest)
                    {
                        if (!await _pingRequestBarrier.WaitAsync(parm_ping_timeout/4))
                        {
                            var task = Task.Factory.StartNew(async () =>
                            {
                                var sw = Stopwatch.StartNew();
                                if (!await _pingRequestBarrier.WaitAsync( _random.Next(parm_ping_timeout * 2) + parm_ping_timeout * _pingRequestBarrier.CurrentCount).ConfigureAwait(false))
                                {
                                    sw.Stop();
                                    _logger.Debug($"{(RoutedRequest?"V>":"X>")} {nameof(Ping)}:Probe {ccNeighbor.RemoteAddress ?? dest}, timed out! waited = {sw.ElapsedMilliseconds}ms, blokers = {_pingRequestBarrier.CurrentCount}");
                                }

                                //Try again
                                await ccNeighbor.SendMessage(dest, pingRequest.ToByteString(), IoCcPeerMessage.MessageTypes.Ping).ContinueWith(
                                    r =>
                                    {
                                        if (r.IsCompletedSuccessfully)
                                            ccNeighbor._pingRequest = pingRequest;
                                    }).ConfigureAwait(false);

                            }, TaskCreationOptions.LongRunning).ConfigureAwait(false);
                            return;
                        }

                        await ccNeighbor.SendMessage(dest, pingRequest.ToByteString(), IoCcPeerMessage.MessageTypes.Ping).ContinueWith(
                            r =>
                            {
                                if (r.IsCompletedSuccessfully)
                                    ccNeighbor._pingRequest = pingRequest;
                            });
                        return;
                    }

                    await ccNeighbor.SendPingAsync();
                }
            }
            catch (NullReferenceException) { }
            catch (ObjectDisposedException) { }
            catch (TaskCanceledException) { }
            catch (OperationCanceledException) { }
            catch (Exception e)
            {
                _logger.Debug(e, $"ERROR z = {Zeroed()}, dest = {dest}, source = {Source}, _discoveryRequest = {_discoveryRequest}");
            }
        }

        /// <summary>
        /// Sends a discovery request
        /// </summary>
        /// <param name="dest">The destination address</param>
        /// <returns>Task</returns>
        public async Task SendDiscoveryRequestAsync(IoNodeAddress dest = null)
        {
            try
            {
                if(Zeroed() && !RoutedRequest && !Verified)
                    return;

                dest ??= RemoteAddress;
                var discoveryRequest = new DiscoveryRequest
                {
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / parm_max_time_error * parm_max_time_error + parm_max_time_error / 2
                };

                await SendMessage(dest, discoveryRequest.ToByteString(), IoCcPeerMessage.MessageTypes.DiscoveryRequest).ContinueWith(
                    r =>
                    {
                        if (r.IsCompletedSuccessfully)
                        {
                            _discoveryRequest = discoveryRequest;
                        }
                    });
            }
            catch (NullReferenceException) { }
            catch (ObjectDisposedException) { }
            catch (TaskCanceledException) { }
            catch (OperationCanceledException) { }
            catch (Exception e)
            {
                _logger.Debug(e, $"ERROR z = {Zeroed()}, dest = {dest}, source = {Source}, _discoveryRequest = {_discoveryRequest}");
            }
        }

        /// <summary>
        /// Sends a peer request
        /// </summary>
        /// <returns>Task</returns>
        public async Task SendPeerRequestAsync(IoNodeAddress dest = null)
        {
            try
            {
                if(Zeroed() && !RoutedRequest || CcNode.OutboundCount >= CcNode.parm_max_outbound || !Verified)
                    return;

                dest ??= RemoteAddress;
                PeeringRequest peerRequest = new PeeringRequest
                {
                    Salt = new Salt { ExpTime = (ulong)DateTimeOffset.UtcNow.AddHours(2).ToUnixTimeSeconds(), Bytes = GetSalt },
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / parm_max_time_error * parm_max_time_error + parm_max_time_error / 2
                };

                await SendMessage(dest, peerRequest.ToByteString(), IoCcPeerMessage.MessageTypes.PeeringRequest).ContinueWith(
                    r =>
                    {
                        if (r.IsCompletedSuccessfully)
                        {
                            _peerRequest = peerRequest;
                        }
                    });
            }
            catch (NullReferenceException){}
            catch (ObjectDisposedException){}
            catch (TaskCanceledException){}
            catch (OperationCanceledException){}
            catch (Exception e)
            {
                _logger.Debug(e, $"ERROR z = {Zeroed()}, dest = {dest}, source = {Source}, request = {_peerRequest}");
            }
        }

        /// <summary>
        /// Tell peer to drop us when things go wrong. (why or when? cause it wont reconnect otherwise. This is a bug)
        /// </summary>
        /// <returns></returns>
        private async Task SendPeerDropAsync(IoNodeAddress dest = null)
        {
            try
            {
                if(Zeroed() || !RoutedRequest)
                    return;

                dest ??= RemoteAddress;

                var dropRequest = new PeeringDrop
                {
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / parm_max_time_error * parm_max_time_error + parm_max_time_error / 2
                };

                await SendMessage(dest, dropRequest.ToByteString(), IoCcPeerMessage.MessageTypes.PeeringDrop);
            }
            catch (NullReferenceException) { }
            catch (ObjectDisposedException) { }
            catch (TaskCanceledException) { }
            catch (OperationCanceledException) { }
            catch (Exception e)
            {
                _logger.Debug(e, $"ERROR z = {Zeroed()}, dest = {dest}, source = {Source}");
            }
        }

        //TODO complexity
        /// <summary>
        /// Attaches a gossip peer to this neighbor
        /// </summary>
        /// <param name="ioCcPeer">The peer</param>
        public bool AttachPeer(IoCcPeer ioCcPeer)
        {
            lock (this)
            {
                if (Peer == ioCcPeer || Peer != null)
                {
                    _logger.Fatal($"Peer id = {Peer?.Id} already attached!");
                    return false;
                }
                
                Peer = ioCcPeer ?? throw new ArgumentNullException($"{(RoutedRequest?"V>":"X>")}{nameof(ioCcPeer)}");
            }
            
            _logger.Debug($"{(RoutedRequest ? "V>" : "X>")}{GetType().Name}: Attached to peer {Peer.Description}");

            //if(Peer.IsArbitrating && Peer.Source.IsOperational)
            PeerConnectedAtLeastOnce = true;
            Uptime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();


            //ioCcPeer.AttachNeighbor(this);

            _peerZeroSub = Peer.ZeroEvent(async sender =>
            {
                DetachPeer();
                await SendPeerDropAsync();
            });

            _neighborZeroSub = ZeroEvent(sender =>
            {
                return Peer?.Zero(this);
            });

            return true;
        }

        /// <summary>
        /// Detaches a peer from this neighbor
        /// </summary>
        public void DetachPeer(bool force = false)
        {
            IoCcPeer peer = Peer;

            lock (this)
            {
                if (Peer == null && !force)
                    return;
                Peer = null;
            }
            
            if(_peerZeroSub != null)
                peer?.Unsubscribe(_peerZeroSub);
            _peerZeroSub = null;
            if(_neighborZeroSub != null)
                Unsubscribe(_neighborZeroSub);
            _neighborZeroSub = null;
            peer?.DetachNeighbor();
            Direction = Kind.Undefined;
            Verified = false;
            ExtGossipAddress = null;
            KeepAliveSec = 0;
            Uptime = 0;
            KeepAlives = 0;

            _logger.Info($"{(PeerConnectedAtLeastOnce?"Useful":"Useless")} peer detached, {Id} ({peer?.Source?.Key})");
        }


        /// <summary>
        /// Whether this neighbor is peered
        /// </summary>
        /// <returns>True if peered</returns>
        public bool Peered()
        {
            return Peer != null;
        }
    }
}
