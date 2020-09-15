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
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using Logger = NLog.Logger;

namespace zero.cocoon.autopeer
{
    public class IoCcNeighbor : IoNeighbor<IoCcPeerMessage>
    {
        public IoCcNeighbor(IoNode<IoCcPeerMessage> node, IoNetClient<IoCcPeerMessage> ioNetClient, object extraData = null, IoCcService services = null) : base(node, ioNetClient, userData => new IoCcPeerMessage("peer rx", $"{ioNetClient.AddressString}", ioNetClient), 16,16)
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
                var task = Task.Factory.StartNew(async () =>
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
                        $"`neighbor({(RoutedRequest ? "R" : "L")},{(PeerConnectedAtLeastOnce ? "C" : "D")})[{_keepAlives}:0000] {Id}:{RemoteAddress?.Port} <-> {IoSource?.Key}'";
                }
                catch (NullReferenceException) { }
                catch (Exception e)
                {
                    _logger.Error(e, Description);
                }

                return _description;
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

        /// <summary>
        /// Indicates whether we have successfully established a connection before
        /// </summary>
        protected volatile int PeerConnectionAttempts = 0;

        //uptime
        private long _uptime = 0;
        public long PeerUptime
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

        private volatile int _direction;
        /// <summary>
        /// Who contacted who?
        /// </summary>
        //public Kind Direction { get; protected set; } = Kind.Undefined;
        public Kind Direction => (Kind) _direction;

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
        //private SemaphoreSlim _pingRequestBarrier = new SemaphoreSlim(1);

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
        /// loss
        /// </summary>
        private long _keepAliveLoss;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private volatile Ping _pingRequest;

        /// <summary>
        /// Holds unrouted ping requests
        /// </summary>
        private ConcurrentDictionary<string, Ping> _pingRequests = new ConcurrentDictionary<string, Ping>();

        /// <summary>
        /// Match ping request
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        private (Ping pingReq,string key, bool matched) MatchPingRequest(string key)
        {
            var match = _pingRequests.TryGetValue(key, out var ping) ? (ping, key, true) : (default(Ping), default(string), false);
            return PopMatch(match.Item1, match.Item2, match.Item3);
        }

        /// <summary>
        /// Pop a match off the stack
        /// </summary>
        /// <param name="ping"></param>
        /// <param name="key"></param>
        /// <param name="match"></param>
        private (Ping pingReq, string key, bool matched) PopMatch(Ping ping, string key, bool match)
        {
            return _pingRequests.TryRemove(key ?? "", out _) ? (_: ping,key, match) : (null, null, false);
        }

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
        public int parm_max_discovery_peers = 12;

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
        /// Maximum number of services supported
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_zombie_max_connection_attempts = 3;

        /// <summary>
        /// Handle to peer zero sub
        /// </summary>
        private ZeroSub _peerZeroSub;

        /// <summary>
        /// Handle to neighbor zero sub
        /// </summary>
        private ZeroSub _neighborZeroSub;

        /// <summary>
        /// Number of connection attempts
        /// </summary>
        private long _connectionAttempts;

        /// <summary>
        /// A service map helper
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
            //_pingRequestBarrier.Dispose();
            base.ZeroUnmanaged();

            //_pingRequestBarrier = null;
            _pingRequests = null;
            Peer = null;
            NeighborDiscoveryNode = null;
            _protocolChannel = null;
            _neighborZeroSub = null;
            _peerZeroSub = null;
            ArrayPoolProxy = null;
        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override Task ZeroManagedAsync()
        {
            _pingRequests.Clear();
            DetachPeer();
            return base.ZeroManagedAsync();
        }


        /// <summary>
        /// Ensures that the peer is running
        /// </summary>
        /// <param name="force">Ignore keepalive check</param>
        public async Task EnsurePeerAsync(bool force = false)
        {
            if (!force)
            {
                if (Zeroed() || CcNode.Zeroed() || CcNode.DiscoveryService.Zeroed() || KeepAliveSec > 0 && LastKeepAliveReceived < parm_zombie_max_ttl / 2)
                    return;

                if (!RoutedRequest && Node.Neighbors.Count < 3)
                {
                    foreach (var ioNodeAddress in CcNode.BootstrapAddress)
                    {
                        _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} {Description} Boostrapping from {CcNode.BootstrapAddress}");
                        await ((IoCcNeighborDiscovery)Node).LocalNeighbor.SendPingAsync(ioNodeAddress).ConfigureAwait(false);
                        return;
                    }
                }

                if (KeepAliveSec > 0 && LastKeepAliveReceived > parm_zombie_max_ttl * 2)
                {
                    bool reconnect = this.Direction == Kind.OutBound;
                    var address = RemoteAddress;
                    await ZeroAsync(this).ConfigureAwait(false);
                    if (reconnect)
                    {
                        _logger.Warn($"{(RoutedRequest ? "V>" : "X>")} Zeroing zombie neighbor, reconnecting... {Description}");
                        await SendPingAsync(RemoteAddress).ConfigureAwait(false);
                    }
                    else
                        _logger.Warn($"{(RoutedRequest ? "V>" : "X>")} Zeroing zombie neighbor {Description}");

                    return;
                }
            }

            await SendPingAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Start processors for this neighbor
        /// </summary>
        /// <param name="spawnProducer">Spawns a source thread</param>
        /// <returns></returns>
        public override async Task SpawnProcessingAsync(bool spawnProducer = true)
        {
            var processingAsync = base.SpawnProcessingAsync(spawnProducer);
            var protocol = ProcessAsync().ContinueWith(t =>
            {
                if(t.IsFaulted)
                    _logger.Error(t.Exception, $"Processing protocol msgs for {Description} failed");
            });

            if (!RoutedRequest && CcNode.BootstrapAddress != null)
            {
                foreach (var ioNodeAddress in CcNode.BootstrapAddress)
                {
                    _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} {Description} Boostrapping from {ioNodeAddress}");
                    await SendPingAsync(ioNodeAddress).ConfigureAwait(false);
                }
            }

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
                var protocolMsgs = ((IoCcProtocolMessage) consumer).Messages;

                foreach (var message in protocolMsgs)
                {
                    if (message == null)
                        break;

                    try
                    {
                        await processCallback(message, msgArbiter).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e, $"Processing protocol failed for {Description}: ");
                    }
                }
                
                //Array.Clear(protocolMsgs, 0, count);

                consumer.State = IoJobMeta.JobState.Consumed;

                stopwatch.Stop();
                //_logger.Trace($"{(RoutedRequest ? "V>" : "X>")} Processed `{protocolMsgs.Count}' consumer: t = `{stopwatch.ElapsedMilliseconds:D}', `{protocolMsgs.Count * 1000 / (stopwatch.ElapsedMilliseconds + 1):D} t/s'");
            }
            catch (Exception e)
            {
                _logger.Error(e, "Error processing message batch");
            }
            finally
            {
                //ArrayPoolProxy.Return(((IoCcProtocolMessage)consumer).Messages, true);
                ArrayPoolProxy.Return(((IoCcProtocolMessage)consumer).Messages);
                ((IoCcProtocolMessage) consumer).Messages = null;
            }
        }

        /// <summary>
        /// Processes a protocol message
        /// </summary>
        /// <returns></returns>
        private async Task ProcessAsync()
        {
            _protocolChannel ??= Source.GetChannel<IoCcProtocolMessage>(nameof(IoCcNeighbor));

            _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} Processing peer msgs: `{Description}'");

            Task[] channelTasks = null;
            try
            {
                while (!Zeroed())
                {
                    if (_protocolChannel == null)
                    {
                        _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} Waiting for {Description} stream to spin up...");
                        _protocolChannel = Source.AttachProducer<IoCcProtocolMessage>(nameof(IoCcNeighbor));
                        ArrayPoolProxy = ((IoCcProtocolBuffer) _protocolChannel?.Source)?.ArrayPoolProxy;
                        await Task.Delay(2000, AsyncTasks.Token).ConfigureAwait(false);//TODO config
                        continue;
                    }
                    else if( channelTasks == null )
                    {
                        channelTasks = new Task[_protocolChannel.ConsumerCount];
                    }

                    for (int i = 0; i < _protocolChannel.ConsumerCount; i++)
                    {
                        channelTasks[i] = _protocolChannel.ConsumeAsync(async batch =>
                        {
                            try
                            {
                                await ProcessMsgBatchAsync(batch, _protocolChannel, async (msg, forward) =>
                                {
                                    var (message, extraData, packet) = msg;
                                    try
                                    {
                                        IoCcNeighbor ccNeighbor;
                                        //TODO optimize
                                        Node.Neighbors.TryGetValue(
                                            MakeId(IoCcIdentity.FromPubKey(packet.PublicKey.Span),
                                                IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData)),
                                            out var n);
                                        if (n == null)
                                            ccNeighbor = ((IoCcNeighborDiscovery)Node).LocalNeighbor;
                                        else
                                            ccNeighbor = (IoCcNeighbor) n;

                                        switch (message.GetType().Name)
                                        {
                                            case nameof(Ping):
                                                await ccNeighbor.ProcessAsync((Ping) message, extraData, packet);
                                                break;
                                            case nameof(Pong):
                                                await ccNeighbor.ProcessAsync((Pong) message, extraData, packet);
                                                break;
                                            case nameof(DiscoveryRequest):
                                                await ccNeighbor.ProcessAsync((DiscoveryRequest) message, extraData, packet);
                                                break;
                                            case nameof(DiscoveryResponse):
                                                await ccNeighbor.ProcessAsync((DiscoveryResponse) message, extraData, packet);
                                                break;
                                            case nameof(PeeringRequest):
                                                await ccNeighbor.ProcessAsync((PeeringRequest) message, extraData, packet);
                                                break;
                                            case nameof(PeeringResponse):
                                                await ccNeighbor.ProcessAsync((PeeringResponse) message, extraData, packet);
                                                break;
                                            case nameof(PeeringDrop):
                                                await ccNeighbor.ProcessAsync((PeeringDrop) message, extraData, packet);
                                                break;
                                        }
                                    }
                                    catch (TaskCanceledException e) { _logger.Trace(e, Description); }
                                    catch (OperationCanceledException e) { _logger.Trace(e, Description); }
                                    catch (NullReferenceException e) { _logger.Trace(e, Description); }
                                    catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
                                    catch (Exception e)
                                    {
                                        _logger.Error(e, $"{message.GetType().Name} [FAILED]: l = {packet.Data.Length}, {Id}");
                                    }
                                }).ConfigureAwait(false);
                            }
                            finally
                            {
                                if (batch != null && batch.State != IoJobMeta.JobState.Consumed)
                                    batch.State = IoJobMeta.JobState.ConsumeErr;
                            }
                        });

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

            _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} Stopped processing msgs from {Description}");
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
            if (!Verified || Peer == null)
                return;

            _logger.Debug($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringDrop)}: {Direction} Peer= {Peer?.Id ?? "null"}");

            try
            {
                await Peer.ZeroAsync(this).ConfigureAwait(false);
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
            if (!RoutedRequest || !Verified || Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Timestamp) > parm_max_time_error * 2)
            {
                if (!RoutedRequest)
                {
                    //We syn here (Instead of in process ping) to force the other party to do some work before we do work.
                    _logger.Debug($"{(RoutedRequest ? "V>" : "X>")}{nameof(PeeringRequest)}: DMZ/SYN => {extraData}");
                    await SendPingAsync(IoNodeAddress.CreateFromEndpoint("udp://", (IPEndPoint) extraData));
                    return;
                }
                else
                {
                    _logger.Trace($"{(RoutedRequest ? "V>" : "X>")}{nameof(PeeringRequest)}: Dropped!, {(Verified ? "verified" : "un-verified")}, age = {Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Timestamp)}");
                }
                return;
            }

            PeeringResponse peeringResponse = null;
            var wasInbound = Direction == Kind.Inbound;
            var wasOutbound = Direction == Kind.OutBound;

            //ACCEPT?
            if (Direction == Kind.Inbound || CcNode.InboundCount < CcNode.parm_max_inbound)
            {
                peeringResponse = new PeeringResponse
                {
                    ReqHash = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(request.ToByteArray())),
                    Status = true
                };
            } // FORWARD ACCEPT?
            else if (PeerConnectionAttempts < parm_zombie_max_connection_attempts &&
                    CcNode.OutboundCount < CcNode.parm_max_outbound)
            {
                await SendPeerRequestAsync().ConfigureAwait(false);
                _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} {Kind.Inbound} peering request [REJECTED], {(CcNode.InboundCount < CcNode.parm_max_inbound ? "Inbound Open" : "Inbound Closed")}, currently <<{Direction}>>: {Description}");
                return;
            }
            else
            {
                peeringResponse = new PeeringResponse
                {
                    ReqHash = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(request.ToByteArray())),
                    Status = false
                };
            }

            if (peeringResponse.Status && Interlocked.CompareExchange(ref _direction, (int) Kind.Inbound, (int) Kind.Undefined) == (int)Kind.Undefined)
            {
                await SendDiscoveryRequestAsync().ConfigureAwait(false);
            }
            else if (Direction == Kind.OutBound) //If it is outbound say no
            {
                _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} Peering {Kind.Inbound} Rejected: {Description} is already {Kind.OutBound}");
            }
            else if (wasInbound)
            {
                if (Peer != null && PeerConnectedAtLeastOnce && (!Peer.Source.IsOperational))// || !Peer.IsArbitrating)) //TODO
                {
                    _logger.Warn($"{(RoutedRequest ? "V>" : "X>")} Found zombie {Direction} peer({(PeerConnectedAtLeastOnce ? "C" : "DC")}) {Description}, Operational = {Peer?.Source?.IsOperational}, Arbitrating = {Peer?.IsArbitrating}");
                    Peer?.ZeroAsync(this).ConfigureAwait(false);
                }
                else if (Peer == null)
                {
                    _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} Peering Re-/Authorized... {Direction} ({(PeerConnectedAtLeastOnce ? "C" : "DC")}), {Description}");
                }
            }

            _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} {Kind.Inbound} peering request {(peeringResponse.Status ? "[ACCEPTED]" : "[REJECTED]")}({(CcNode.InboundCount < CcNode.parm_max_inbound ? "Inbound Open" : "Inbound Closed")}), current = {Direction}: {Description}");
            await SendMessageAsync(RemoteAddress, peeringResponse.ToByteString(), IoCcPeerMessage.MessageTypes.PeeringResponse).ConfigureAwait(false);
        }

        /// <summary>
        /// Peer response message from client
        /// </summary>
        /// <param name="response">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task ProcessAsync(PeeringResponse response, object extraData, Packet packet)
        {

            var request = _peerRequest; 

            if (!RoutedRequest || request == null)
            {
                _logger.Debug($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringResponse)}: Unexpected response from {extraData}, {RemoteAddress}");
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

            var alreadyOutbound = Direction == Kind.OutBound;
            if (response.Status && Interlocked.CompareExchange(ref _direction, (int)Kind.OutBound, (int)Kind.Undefined) == (int)Kind.Undefined)
            {
                if (await CcNode.ConnectToPeerAsync(this).ConfigureAwait(false))
                    await SendDiscoveryRequestAsync().ConfigureAwait(false);
                Interlocked.Increment(ref _connectionAttempts);
            }
            else if (Direction == Kind.Inbound)
            {
                _logger.Debug($"{(RoutedRequest ? "V>" : "X>")}{nameof(PeeringResponse)}: {nameof(Kind.OutBound)} request dropped, {nameof(Kind.Inbound)} received");
            }
            else if (alreadyOutbound)
            {
                //_logger.Fatal($"{(RoutedRequest?"V>":"X>")}{nameof(PeeringResponse)}: Not expected, already {nameof(Kind.OutBound)}");
                if (Peer != null && PeerConnectedAtLeastOnce && (!Peer.Source.IsOperational))// || !Peer.IsArbitrating))
                {
                    _logger.Warn($"{(RoutedRequest ? "V>" : "X>")} Found zombie {Direction} peer({(PeerConnectedAtLeastOnce ? "C" : "DC")}) {Description}, Operational = {Peer?.Source?.IsOperational}, Arbitrating = {Peer?.IsArbitrating}");
                    await Peer.ZeroAsync(this).ConfigureAwait(false);
                }
                else if (Peer == null && Interlocked.Read(ref _connectionAttempts) > 0)
                {
                    _logger.Debug($"{(RoutedRequest ? "V>" : "X>")} Peering reconnecting attempts {Interlocked.Read(ref _connectionAttempts)}... {Direction} ({(PeerConnectedAtLeastOnce ? "C" : "DC")}), {Description}");
                    if (await CcNode.ConnectToPeerAsync(this).ConfigureAwait(false))
                        await SendDiscoveryRequestAsync().ConfigureAwait(false);
                    Interlocked.Increment(ref _connectionAttempts);
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

                packet.Signature = ByteString.CopyFrom(CcNode.CcId.Sign(packet!.Data!.ToByteArray(), 0, packet.Data.Length));

                var msgRaw = packet.ToByteArray();

                
                    var sent = await ((IoUdpClient<IoCcPeerMessage>) Source).Socket.SendAsync(msgRaw, 0, msgRaw.Length,
                        dest.IpEndPoint).ConfigureAwait(false);
                    _logger.Trace($"{(RoutedRequest ? "V>" : "X>")} {Enum.GetName(typeof(IoCcPeerMessage.MessageTypes), packet.Type)}({GetHashCode()}): Sent {sent} bytes to {(RoutedRequest?$"{Identity.IdString()}":$"")}@{dest.IpEndPoint}");
                    return (sent, packet);
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

            if (!response.ReqHash.SequenceEqual(IoCcIdentity.Sha256.ComputeHash(discoveryRequest.ToByteArray())))
            {
                _logger.Debug($"{(RoutedRequest ? "V>" : "X>")}{nameof(DiscoveryResponse)}: Reject invalid hash {Description}");
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

                //Don't add already known neighbors
                var id = IoCcIdentity.FromPubKey(responsePeer.PublicKey.Span);
                if(Node.Neighbors.Values.Any(n=>n.Id.Contains(id.PkString())))
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
                    _logger.Debug($"{(RoutedRequest?"V>":"X>")}{nameof(DiscoveryResponse)}: Max service supported {parm_max_services}, got {responsePeer.Services.Map.Count}");
                    services = null;
                    break;
                }

                //sanity check
                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if(services == null || services.IoCcRecord.Endpoints.Count == 0)
                    continue;

                //create neighbor
                IoCcNeighbor newNeighbor = null;
                if( await Node.ZeroEnsureAsync(() =>
                {
                    newNeighbor = (IoCcNeighbor) Node.MallocNeighbor(Node, (IoNetClient<IoCcPeerMessage>) Source, Tuple.Create(id, services, newRemoteEp));

                    //Transfer?
                    if (!Node.Neighbors.TryAdd(newNeighbor.Id, newNeighbor)) return Task.FromResult(false);

                    Node.ZeroOnCascade(newNeighbor); //TODO: Maybe remove? Use the one that floods through source?

                    return newNeighbor.ZeroEnsureAsync(() =>
                    {
                        var sub = newNeighbor.ZeroEvent(source =>
                        {
                            try
                            {
                                if (Node.Neighbors.TryRemove(newNeighbor.Id, out var n))
                                {
                                    _logger.Trace($"{(RoutedRequest ? "V>" : "X>")}{nameof(DiscoveryResponse)}: Removed {n.Description} from {Description}");
                                }
                            }
                            catch
                            {
                                // ignored
                            }
                            return Task.CompletedTask;
                        });
                        return Task.FromResult(sub != null);
                    });
                }))
                {
                    if (!newNeighbor.Verified)
                    {
                        _logger.Debug($"{(RoutedRequest ? "V>" : "X>")}{nameof(DiscoveryResponse)}: DMZ/SYN, to = {newNeighbor.Description}");
                        await newNeighbor.EnsurePeerAsync(true).ConfigureAwait(false);
                        count++;
                    }
                }
                else
                {
                    await newNeighbor.ZeroAsync(this).ConfigureAwait(false);
                }
            }

            if(Node.Neighbors.Count < CcNode.MaxClients && count > 0)
                _logger.Debug($"{(RoutedRequest ? "V>" : "X>")}{nameof(PeeringResponse)}: Scanning {count}/{response.Peers.Count} neighbors from {Description} ...");

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
                ReqHash = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(request.ToByteArray())),
            };

            var count = 0;
            foreach (var ioNeighbor in NeighborDiscoveryNode.Neighbors.Values.Where(n=>((IoCcNeighbor)n).Verified).OrderBy(n=> (int)((IoCcNeighbor)n).Direction))
            {
                if(count == parm_max_discovery_peers)
                    break;
                
                if (ioNeighbor == this || !((IoCcNeighbor)ioNeighbor).RoutedRequest)
                    continue;

                if (((IoCcNeighbor) ioNeighbor).RemoteAddress.Equals(CcNode.ExtAddress))
                {
                    _logger.Fatal($"Found us {((IoCcNeighbor)ioNeighbor).RemoteAddress} in neighbors");
                    continue;
                }

                discoveryResponse.Peers.Add(new Peer
                {
                    PublicKey = ByteString.CopyFrom(((IoCcNeighbor)ioNeighbor).Identity.PublicKey),
                    Services = ((IoCcNeighbor)ioNeighbor).ServiceMap,
                    Ip = ((IoCcNeighbor)ioNeighbor).RemoteAddress.Ip
                });
                count++;
            }

            await SendMessageAsync(RemoteAddress, discoveryResponse.ToByteString(), IoCcPeerMessage.MessageTypes.DiscoveryResponse).ConfigureAwait(false);
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
                _logger.Trace($"{(RoutedRequest?"V>":"X>")}{nameof(Ping)}: Dropped stale, age = {age}s");
                return;
            }

            if (!RoutedRequest && ((IPEndPoint) extraData).Equals(ExtGossipAddress?.IpEndPoint))
            {
                _logger.Warn($"{(RoutedRequest?"V>":"X>")}{nameof(Ping)}: Dropping ping from self: {extraData}");
                return;
            }

            //TODO optimize
            var gossipAddress = ((IoCcNeighborDiscovery)Node).Services.IoCcRecord.Endpoints[IoCcService.Keys.gossip];
            var peeringAddress = ((IoCcNeighborDiscovery)Node).Services.IoCcRecord.Endpoints[IoCcService.Keys.peering];
            var fpcAddress = ((IoCcNeighborDiscovery)Node).Services.IoCcRecord.Endpoints[IoCcService.Keys.fpc];

            var pong = new Pong
            {
                ReqHash = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(ping.ToByteArray())),
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
                        _logger.Trace($"static peer address received: {toProxyAddress}, source detected = udp://{remoteEp}");
                    }
                    else
                    {
                        toProxyAddress = IoNodeAddress.CreateFromEndpoint("udp", remoteEp);
                        _logger.Trace($"automatic peer address detected: {toProxyAddress}, source declared = udp://{ping.SrcAddr}:{ping.SrcPort}");
                    }
                }

                //SEND SYN-ACK
                _logger.Debug($"{(RoutedRequest ? "V>" : "X>")}: Send SYN-ACK, to = {id}:{ping.SrcPort}");
                await SendMessageAsync(toAddress, pong.ToByteString(), IoCcPeerMessage.MessageTypes.Pong).ConfigureAwait(false);

                if (CcNode.UdpTunnelSupport && toAddress.Ip != toProxyAddress.Ip)
                    await SendMessageAsync(toAddress, pong.ToByteString(), IoCcPeerMessage.MessageTypes.Pong).ConfigureAwait(false);

                ////SEND SYN
                //_logger.Debug($"{(RoutedRequest ? "V>" : "X>")}: Send SYN, to = {id}:{ping.SrcPort}");
                //await SendPingAsync(toAddress).ConfigureAwait(false);

                //if (CcNode.UdpTunnelSupport && toProxyAddress.Ip != toAddress.Ip)
                //    await SendPingAsync(toProxyAddress).ConfigureAwait(false);
            }
            else//PROCESS ACK
            {
                //set ext address as seen by neighbor
                if (!Verified)
                {
                    ExtGossipAddress ??= IoNodeAddress.Create($"tcp://{ping.DstAddr}:{CcNode.Services.IoCcRecord.Endpoints[IoCcService.Keys.gossip].Port}");
                    Verified = true;
                    _logger.Debug($"ACK: {Description}");
                }

                _logger.Debug($"{(RoutedRequest ? "V>" : "X>")}: Send ACK SYN/KEEPALIVE, to = {Description}");
                await SendMessageAsync(data: pong.ToByteString(), type: IoCcPeerMessage.MessageTypes.Pong).ConfigureAwait(false);
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
            Ping pingRequest;
            string reqKey = null;

            if (RoutedRequest)
                pingRequest = _pingRequest ?? ((IoCcNeighborDiscovery) Node).LocalNeighbor
                    .MatchPingRequest(Convert.ToBase64String(pong.ReqHash.ToByteArray())).pingReq;
            else
                pingRequest = ((IoCcNeighborDiscovery) Node).LocalNeighbor.MatchPingRequest(reqKey = Convert.ToBase64String(pong.ReqHash.ToByteArray())).pingReq;

            if (pingRequest == null)
            {
                if (RoutedRequest)
                {
                    if(KeepAlives > 0)
                        _logger.Debug($"{(RoutedRequest?"V>":"X>")}{nameof(Pong)}({GetHashCode()}):  Unexpected!, t = {KeepAlives},  s = {LastKeepAliveReceived}, l = {Interlocked.Read(ref _keepAliveLoss)}, d = {(PeerUptime>0? (PeerUptime - KeepAliveSec).ToString():"N/A")}, v = {Verified}, id = {Description}");
                }
                else { } //ignore

                return;
            }
            
            var hash = IoCcIdentity.Sha256.ComputeHash(pingRequest.ToByteArray());

            if (!pong.ReqHash.SequenceEqual(hash))
            {
                _logger.Debug(!RoutedRequest
                    ? $"{(RoutedRequest?"V>":"X>")}{nameof(Pong)}: Invalid request {MakeId(IoCcIdentity.FromPubKey(packet.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData))}"
                    : $"{(RoutedRequest?"V>":"X>")}{nameof(Pong)}: Invalid hash {extraData} <=> {pingRequest}, {Convert.ToBase64String(hash)} - {Convert.ToBase64String(pong.ReqHash.ToByteArray())}");

                return;
            }

            if (!RoutedRequest)
            {
                if (!((IoCcNeighborDiscovery) Node).LocalNeighbor.MatchPingRequest(reqKey).matched)
                {
                    _logger.Debug($"{(RoutedRequest ? "V > " : "X > ")}{nameof(Pong)}: hash not found for {IoCcIdentity.FromPubKey(packet.PublicKey.Span).PkString()}, {Description}");
                }
                    

                //This is hacky to clean old requests
                if (_pingRequests.Count > CcNode.MaxClients * 2)
                {
                    foreach (var request in _pingRequests)
                    {
                        if (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Value.Timestamp > parm_ping_timeout)
                        {
                            _logger.Debug($"{(RoutedRequest ? "V>" : "X>")}{nameof(Pong)}: dropping ping not received: {request.Value.DstAddr} ({_pingRequests.Count})");
                        }
                    }
                }
            }
            else
            {
                _pingRequest = null;
                //_logger.Fatal($"UNSET({GetHashCode()}) {Description} ");
            }

            KeepAliveSec = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            Interlocked.Increment(ref _keepAlives);
            Interlocked.Decrement(ref _keepAliveLoss);

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
                //Do we have capacity for one more neighbor?
                if (Node.Neighbors.Count <= CcNode.MaxClients * 2 && !Node.Neighbors.TryGetValue(keyStr, out oldNeighbor))
                {
                    var remoteServices = new IoCcService();
                    foreach (var key in pong.Services.Map.Keys.ToList())
                        remoteServices.IoCcRecord.Endpoints.TryAdd(Enum.Parse<IoCcService.Keys>(key), IoNodeAddress.Create($"{pong.Services.Map[key].Network}://{((IPEndPoint)extraData).Address}:{pong.Services.Map[key].Port}"));

                    var newNeighbor = (IoCcNeighbor)Node.MallocNeighbor(Node, (IoNetClient<IoCcPeerMessage>)Source, Tuple.Create(idCheck, remoteServices, (IPEndPoint)extraData));

                    //Add new neighbor
                    if(await Node.ZeroEnsureAsync(() =>
                    {

                        //transfer?
                        if (!newNeighbor.RemoteAddress.IpEndPoint.Address.Equals(((IPEndPoint) extraData).Address) ||
                            newNeighbor.RemoteAddress.IpEndPoint.Port != ((IPEndPoint) extraData).Port ||
                            !Node.Neighbors.TryAdd(keyStr, newNeighbor)) return Task.FromResult(false);

                        Node.ZeroOnCascade(newNeighbor);

                        return newNeighbor.ZeroEnsureAsync(() =>
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
                            return Task.FromResult(sub != null);
                        });

                    }))
                    {
                        //SEND 
                        _logger.Debug($"{(RoutedRequest ? "V>" : "X>")}: Send ACK, to = {Description}");
                        await newNeighbor.EnsurePeerAsync(true).ConfigureAwait(false);
                    }
                    else
                    {
                        _logger.Warn($"{(RoutedRequest ? "V>" : "X>")} Create new neighbor {keyStr} skipped!");
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
                //set ext address as seen by neighbor
                ExtGossipAddress ??= IoNodeAddress.Create($"tcp://{pong.DstAddr}:{CcNode.Services.IoCcRecord.Endpoints[IoCcService.Keys.gossip].Port}");

                Verified = true;

                _logger.Debug($"ACK SYN: {Description}");

                if (CcNode.OutboundCount < CcNode.parm_max_outbound)
                {
                    _logger.Debug($"{(RoutedRequest ? "V>" : "X>")}(acksyn): {(CcNode.OutboundCount < CcNode.parm_max_outbound ? "Requesting..." : "Backup")}, to = {Description}, from nat = {ExtGossipAddress}");
                    await SendPeerRequestAsync().ConfigureAwait(false);
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
                    await SendMessageAsync(dest, pingRequest.ToByteString(), IoCcPeerMessage.MessageTypes.Ping);
                    _pingRequest = pingRequest;
                    Interlocked.Increment(ref _keepAliveLoss);
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

                    ccNeighbor ??= ((IoCcNeighborDiscovery)Node).LocalNeighbor;

                    if (!ccNeighbor.RoutedRequest)
                    {
                        await ccNeighbor.SendMessageAsync(dest, pingRequest.ToByteString(), IoCcPeerMessage.MessageTypes.Ping);
                        ccNeighbor._pingRequests.TryAdd(Convert.ToBase64String(IoCcIdentity.Sha256.ComputeHash(pingRequest.ToByteArray())), pingRequest);
                        Interlocked.Increment(ref ccNeighbor._keepAliveLoss);
                        return;
                    }

                    _logger.Debug($"{(RoutedRequest ? "V>" : "X>")}: Send DMZ/SYN, to = {ccNeighbor.Description}");
                    await ccNeighbor.SendPingAsync().ConfigureAwait(false);
                }
            }
            catch (NullReferenceException e) { _logger.Trace(e, Description); }
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
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
                var discoveryReq = _discoveryRequest;
                if(Zeroed() && !RoutedRequest && !Verified || discoveryReq != null && Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - discoveryReq.Timestamp) < parm_max_time_error * 2)
                    return;

                dest ??= RemoteAddress;
                var discoveryRequest = new DiscoveryRequest
                {
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / parm_max_time_error * parm_max_time_error + parm_max_time_error / 2
                };

                await SendMessageAsync(dest, discoveryRequest.ToByteString(), IoCcPeerMessage.MessageTypes.DiscoveryRequest);
                if(discoveryReq != null)
                    _logger.Trace($"_discoveryRequest lost {Description}, {Math.Abs(DateTimeOffset.Now.ToUnixTimeSeconds() - discoveryReq.Timestamp)} ");
                _discoveryRequest = discoveryRequest;
            }
            catch (NullReferenceException e) { _logger.Trace(e, Description); }
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
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
                var peerReq = _pingRequest;
                if(Zeroed() && !RoutedRequest || CcNode.OutboundCount >= CcNode.parm_max_outbound || !Verified || peerReq!= null && Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - peerReq.Timestamp) < parm_max_time_error * 2)
                    return;

                dest ??= RemoteAddress;
                PeeringRequest peerRequest = new PeeringRequest
                {
                    Salt = new Salt { ExpTime = (ulong)DateTimeOffset.UtcNow.AddHours(2).ToUnixTimeSeconds(), Bytes = GetSalt },
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / parm_max_time_error * parm_max_time_error + parm_max_time_error / 2
                };

                await SendMessageAsync(dest, peerRequest.ToByteString(), IoCcPeerMessage.MessageTypes.PeeringRequest);
                if (peerReq != null)
                    _logger.Trace($"_pingRequest lost {Description}, {Math.Abs(DateTimeOffset.Now.ToUnixTimeSeconds() - _pingRequest.Timestamp)} ");
                _peerRequest = peerRequest;
            }
            catch (NullReferenceException e){_logger.Trace(e, Description);}
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
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

                await SendMessageAsync(dest, dropRequest.ToByteString(), IoCcPeerMessage.MessageTypes.PeeringDrop).ConfigureAwait(false);
            }
            catch (NullReferenceException e) { _logger.Trace(e, Description); }
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
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
            PeerUptime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();


            //ioCcPeer.AttachNeighbor(this);

            _peerZeroSub = Peer.ZeroEvent(sender =>
            { 
                DetachPeer(); //TODO
                return SendPeerDropAsync();
            });

            _neighborZeroSub = ZeroEvent(async sender =>
            {
                try
                {
                    await Peer.ZeroAsync(this);
                }
                catch { }
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
            Interlocked.Exchange(ref _direction, 0);
            Verified = false;
            ExtGossipAddress = null;
            KeepAliveSec = 0;

            PeerUptime = 0;
            KeepAlives = 0;
            PeerConnectionAttempts = 0;
            PeerConnectedAtLeastOnce = false;
            //_pingRequest = null;
            //_peerRequest = null;
            //_discoveryRequest = null;

            _logger.Trace($"{(PeerConnectedAtLeastOnce?"Useful":"Useless")} peer detached: {peer?.Description}");
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
