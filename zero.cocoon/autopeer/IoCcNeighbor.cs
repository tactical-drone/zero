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

            if (extraData != null)
            {
                var extra = (Tuple<IoCcIdentity, IoCcService, IPEndPoint>) extraData;
                Identity = extra.Item1;
                Services = services ?? extra.Item2;
                RemoteAddress = IoNodeAddress.CreateFromEndpoint("udp", extra.Item3);
            }
            else
            {
                Identity = IoCcIdentity.Generate();
                Services = services ?? ((IoCcNeighborDiscovery)node).Services;
            }
            
            NeighborDiscoveryNode = (IoCcNeighborDiscovery)node;

            Task.Run(async () =>
            {
                while (!Spinners.IsCancellationRequested && !Zeroed())
                {
                    await Task.Delay(30000, Spinners.Token);
                    if (!Spinners.IsCancellationRequested && !Zeroed() && RemoteAddress != null)
                    {
                        if (!Verified)
                            await SendPingAsync();
                        else if (Direction == Kind.OutBound && Peer == null || (!Peer?.IsArbitrating??true) || (!Peer?.Source?.IsOperational??true))
                            await SendPingAsync();
                    }
                }
            });
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Discovery services
        /// </summary>
        protected IoCcNeighborDiscovery NeighborDiscoveryNode;

        /// <summary>
        /// The gossip peer associated with this neighbor
        /// </summary>
        protected IoCcPeer Peer;

        /// <summary>
        /// Indicates whether we have successfully established a connection before
        /// </summary>
        protected volatile bool PeerConnectedAtLeastOnce;

        /// <summary>
        /// The neighbor address
        /// </summary>
        public IoNodeAddress RemoteAddress { get; protected set; }

        /// <summary>
        /// Whether this neighbor contains the remote client connection information
        /// </summary>
        public bool RoutedRequest => RemoteAddress != null;

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
        public bool Verified { get; set; } = false;

        /// <summary>
        /// Who contacted who?
        /// </summary>
        public Kind Direction { get; set; } = Kind.Undefined;

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
        public IoCcNode CcNode => NeighborDiscoveryNode.CcNode;

        /// <summary>
        /// Receives protocol messages from here
        /// </summary>
        private IoChannel<IoCcProtocolMessage> _protocolChannel;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private volatile DiscoveryRequest _discoveryRequest;


        private SemaphoreSlim _pingRequestAutoResetEvent = new SemaphoreSlim(1);
        /// <summary>
        /// Used to Match requests
        /// </summary>
        private volatile Ping _pingRequest;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private volatile PeeringRequest _peerRequest;

        /// <summary>
        /// Current salt value
        /// </summary>
        private ByteString _curSalt;

        /// <summary>
        /// Generates a new salt
        /// </summary>
        private ByteString GetSalt => _curSalt = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(Encoding.ASCII.GetBytes((DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 120 * 60).ToString())), 0, parm_salt_length);

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
        public int parm_ping_timeout = 1000;

        /// <summary>
        /// Handle to peer zero sub
        /// </summary>
        private Func<IIoZeroable, Task> _peerZeroSub;

        /// <summary>
        /// Handle to neighbor zero sub
        /// </summary>
        private Func<IIoZeroable, Task> _neighborZeroSub;

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
                        mapping.Map.Add(service.Key.ToString(), new NetworkAddress { Network = service.Value.UrlNoPort, Port = (uint)service.Value.Port });
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
            _pingRequestAutoResetEvent.Dispose();
            base.ZeroUnmanaged();
        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override void ZeroManaged()
        {
            base.ZeroManaged();
            _logger.Debug($"{ToString()}: Zeroed {Description}");
        }

        /// <summary>
        /// Start processors for this neighbor
        /// </summary>
        /// <param name="spawnProducer">Spawns a source thread</param>
        /// <returns></returns>
        public override async Task SpawnProcessingAsync(bool spawnProducer = true)
        {
            var neighbor = base.SpawnProcessingAsync(spawnProducer);
            var protocol = ProcessAsync();

            await Task.WhenAll(neighbor, protocol).ConfigureAwait(false);

            if (neighbor.IsFaulted)
            {
                _logger.Fatal(neighbor.Exception, "Neighbor processing returned with errors!");
#pragma warning disable 4014
                Zero();
#pragma warning restore 4014
            }

            if (protocol.IsFaulted)
            {
                _logger.Fatal(protocol.Exception, "Protocol processing returned with errors!");
#pragma warning disable 4014
                Zero();
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

                _logger.Trace($"{consumer.TraceDescription} Processing `{protocolMsgs.Count}' protocol messages");
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
                _logger.Trace($"{consumer.TraceDescription} Processed `{protocolMsgs.Count}' consumer: t = `{stopwatch.ElapsedMilliseconds:D}', `{protocolMsgs.Count * 1000 / (stopwatch.ElapsedMilliseconds + 1):D} t/s'");
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

            _logger.Debug($"Processing peer msgs: `{Description}'");
            while (!Spinners.IsCancellationRequested && !Zeroed())
            {
                if (_protocolChannel == null)
                {
                    _logger.Warn($"Waiting for `{Description}' stream to spin up...");
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
                                Node.Neighbors.TryGetValue(MakeId(IoCcIdentity.FromPK(msg.Item3.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint)msg.Item2)), out var n);
                                if (n == null)
                                    ccNeighbor = this;
                                else
                                    ccNeighbor = (IoCcNeighbor)n;

                                switch (msg.Item1.GetType().Name)
                                {
                                    case nameof(Ping):
                                        ccNeighbor.Process((Ping)msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(Pong):
                                        ccNeighbor.Process((Pong)msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(DiscoveryRequest):
                                        ccNeighbor.Process((DiscoveryRequest)msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(DiscoveryResponse):
                                        ccNeighbor.Process((DiscoveryResponse)msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(PeeringRequest):
                                        ccNeighbor.Process((PeeringRequest)msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(PeeringResponse):
                                        ccNeighbor.Process((PeeringResponse)msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(PeeringDrop):
                                        ccNeighbor.Process((PeeringDrop)msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                }
                            }
                            catch (Exception e)
                            {
                                _logger.Error(e, "Unable to process protocol message");
                            }
#pragma warning restore 4014
                            return Task.CompletedTask;
                        }).ConfigureAwait(false);
                    }
                    finally
                    {
                        if (batch != null && batch.ProcessState != IoJob<IoCcProtocolMessage>.State.Consumed)
                            batch.ProcessState = IoJob<IoCcProtocolMessage>.State.ConsumeErr;
                    }
                }).ConfigureAwait(false);

                if (!_protocolChannel.Source.IsOperational)
                    break;
            }

            _logger.Debug($"Stopped processing peer msgs: `{Description}'");
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
            if (!RoutedRequest || (diff = Math.Abs((int)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Timestamp))) > 20)
            {
                _logger.Trace($"{nameof(PeeringDrop)}: Ignoring {diff}s old/invalid request({DateTimeOffset.FromUnixTimeSeconds(request.Timestamp)})");
                return;
            }

            //only verified nodes get to drop
            if (!Verified || Peer == null)
                return;

            _logger.Debug($"{nameof(PeeringDrop)}: {Direction} Peer= {Peer?.Id ?? "null"}");
            Peer?.Zero();
            
            //Attempt reconnect
            await SendPingAsync();
        }

        /// <summary>
        /// Peering Request message from client
        /// </summary>
        /// <param name="request">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task Process(PeeringRequest request, object extraData, Packet packet)
        {
            if (RemoteAddress == null)
            {
                _logger.Trace($"{nameof(PeeringRequest)}: Dropped request, not verified! ({DateTimeOffset.FromUnixTimeSeconds(request.Timestamp)})");
                return;
            }

            var peeringResponse = new PeeringResponse
            {
                ReqHash = ByteString.CopyFrom(SHA256.Create().ComputeHash(packet.Data.ToByteArray())),
                Status = CcNode.InboundCount < CcNode.parm_max_inbound
            };

            lock (this)
            {
                //If not selected, select inbound
                if (Direction == Kind.Undefined && peeringResponse.Status)
                {
                    Direction = Kind.Inbound;
                }
                else if (Direction == Kind.OutBound) //If it is outbound say no
                {
                    _logger.Debug($"Peering {Kind.Inbound} Rejected: {Id} is already {Kind.OutBound}");
                    peeringResponse.Status = false; //TODO does this work?
                }
                else if (peeringResponse.Status) //Double check existing conditions 
                {
                    if (Peer != null && (!Peer.Source.IsOperational || !Peer.IsArbitrating))
                    {
                        _logger.Warn($"Found zombie peer: {Id}, Operational = {Peer?.Source?.IsOperational}, Arbitrating = {Peer?.IsArbitrating}");
                        Peer?.Zero();
                    }
                    else
                        _logger.Debug($"Peering stands {Direction}: {Id}");
                }
            }

            _logger.Trace($"{nameof(PeeringResponse)}: Sent Allow Status = {peeringResponse.Status}, Capacity = {-CcNode.Neighbors.Count + CcNode.MaxClients}");

            await SendMessage(RemoteAddress, peeringResponse.ToByteString(), IoCcPeerMessage.MessageTypes.PeeringResponse);
        }

        /// <summary>
        /// Peer response message from client
        /// </summary>
        /// <param name="response">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private void Process(PeeringResponse response, object extraData, Packet packet)
        {
            if (_peerRequest == null || RemoteAddress == null)
            {
                _logger.Trace($"{nameof(PeeringResponse)}: Unexpected response from {extraData}, {RemoteAddress}");
                return;
            }

            var hash = IoCcIdentity.Sha256.ComputeHash(_peerRequest.ToByteArray());

            if (!response.ReqHash.SequenceEqual(hash))
            {
                if (RemoteAddress == null)
                    _logger.Debug($"{nameof(PeeringResponse)}: Got invalid response from {MakeId(IoCcIdentity.FromPK(packet.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint)extraData))}");
                else
                    _logger.Debug($"{nameof(PeeringResponse)}: Got invalid response hash from {extraData}, age = {DateTimeOffset.UtcNow.ToUnixTimeSeconds() - _peerRequest.Timestamp}s, {Convert.ToBase64String(hash)} - {Convert.ToBase64String(response.ReqHash.ToByteArray())}");

                return;
            }

            _peerRequest = null;

            _logger.Trace($"{nameof(PeeringResponse)}: Got status = {response.Status}");

            var alreadyOutbound = false;
            lock (this)
            {
                if (Direction == Kind.Undefined)
                    Direction = response.Status? Kind.OutBound : Direction;
                else if (Direction == Kind.Inbound)
                {
                    _logger.Debug($"{nameof(PeeringResponse)}: {nameof(Kind.OutBound)} request dropped, {nameof(Kind.Inbound)} received");
                }
                else if(Direction == Kind.OutBound)
                {
                    alreadyOutbound = true;
                }
            }

            if (!alreadyOutbound)
            {
                _logger.Info($"{Kind.OutBound} peering request {(response.Status?"[ACCEPTED]":"[REJECTED]")}, currently {Direction}: {Id}");

                if (Direction == Kind.OutBound)
                    CcNode.ConnectToPeer(this);
            }
            else
            {
                _logger.Fatal($"{nameof(PeeringResponse)}: Not expected, already {nameof(Kind.OutBound)}");
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
            dest ??= RemoteAddress;

            var packet = new Packet
            {
                Data = data,
                PublicKey = ByteString.CopyFrom(CcNode.CcId.PublicKey),
                Type = (uint)type
            };

            packet.Signature =
                ByteString.CopyFrom(CcNode.CcId.Sign(packet.Data.ToByteArray(), 0, packet.Data.Length));

            var msgRaw = packet.ToByteArray();

            var sent = await ((IoUdpClient<IoCcPeerMessage>)Source).Socket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint);
            _logger.Debug($"{Enum.GetName(typeof(IoCcPeerMessage.MessageTypes), packet.Type)}: Sent {sent} bytes to {dest.IpEndPoint} ({Id})");
            return (sent, packet);
        }

        /// <summary>
        /// Discovery response message
        /// </summary>
        /// <param name="response">The response</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task Process(DiscoveryResponse response, object extraData, Packet packet)
        {
            if (_discoveryRequest == null || RemoteAddress == null)
            {
                _logger.Debug($"{nameof(DiscoveryResponse)}: Dropped! Got unexpected response from {MakeId(IoCcIdentity.FromPK(packet.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint)extraData))}, RemoteAddress = {RemoteAddress}, request = {_discoveryRequest}");
                return;
            }

            if (response.ReqHash.SequenceEqual(IoCcIdentity.Sha256.ComputeHash(_discoveryRequest.ToByteArray())))
            {
                _logger.Debug($"{nameof(DiscoveryResponse)}: Got invalid request from {MakeId(IoCcIdentity.FromPK(packet.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint)extraData))}");
                return;
            }

            _discoveryRequest = null;

            _logger.Debug($"{nameof(PeeringResponse)}: Got {response.Peers.Count} peers ");
            foreach (var responsePeer in response.Peers)
            {
                var idCheck = IoCcIdentity.FromPK(responsePeer.PublicKey.Span);
                var remoteServices = IoNodeAddress.Create($"{responsePeer.Services.Map[IoCcService.Keys.gossip.ToString()].Network}://{responsePeer.Ip}:{responsePeer.Services.Map[IoCcService.Keys.gossip.ToString()].Port}");
                var newNeighbor = (IoCcNeighbor)Node.MallocNeighbor(Node, (IoNetClient<IoCcPeerMessage>)Source, Tuple.Create(idCheck, remoteServices));
                if (Node.Neighbors.TryAdd(newNeighbor.Id, newNeighbor))
                {
                    _logger.Debug($"Exchanged new peer info: {newNeighbor.Id}@{remoteServices}");
                    await newNeighbor.SendPingAsync(IoNodeAddress.CreateFromEndpoint("udp", (EndPoint)extraData));
                }
            }
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
            if (RemoteAddress == null || DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Timestamp > 20)
            {
                _logger.Trace($"{nameof(DiscoveryRequest)}: Dropped request, not verified! ({DateTimeOffset.FromUnixTimeSeconds(request.Timestamp)})");
                return;
            }

            //TODO accuracy param
            if (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Timestamp > 20 || request.Timestamp > DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 20)
            {
                _logger.Trace($"{nameof(DiscoveryRequest)}: Dropped misstimed request ({DateTimeOffset.FromUnixTimeSeconds(request.Timestamp)})");
                return;
            }

            var discoveryResponse = new DiscoveryResponse
            {
                ReqHash = ByteString.CopyFrom(SHA256.Create().ComputeHash(packet.Data.ToByteArray())),
                Peers = { new Peer { PublicKey = ByteString.CopyFrom(CcNode.CcId.PublicKey), Ip = Source.SourceUri, Services = new ServiceMap { Map = { new Dictionary<string, NetworkAddress> { { IoCcService.Keys.peering.ToString(), new NetworkAddress { Network = CcNode.ExtAddress.Ip, Port = (uint)CcNode.ExtAddress.Port } } } } } } }
            };

            foreach (var ioNeighbor in NeighborDiscoveryNode.Neighbors)
            {
                if (ioNeighbor.Value == this || ((IoCcNeighbor)ioNeighbor.Value).RemoteAddress == null)
                    continue;

                discoveryResponse.Peers.Add(new Peer
                {
                    PublicKey = ByteString.CopyFrom(((IoCcNeighbor)ioNeighbor.Value).Identity.PublicKey),
                    Services = ((IoCcNeighbor)ioNeighbor.Value).ServiceMap,
                    Ip = ((IoCcNeighbor)ioNeighbor.Value).RemoteAddress.Ip
                });
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
            var age = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - ping.Timestamp;
            if (age > 20 || age < -20) //TODO params
            {
                _logger.Trace($"{nameof(Ping)}: message dropped, to old. age = {age}s");
                return;
            }

            //TODO optimize
            var gossipAddress = ((IoCcNeighborDiscovery)Node).Services.IoCcRecord.Endpoints[IoCcService.Keys.gossip];
            var peeringAddress = ((IoCcNeighborDiscovery)Node).Services.IoCcRecord.Endpoints[IoCcService.Keys.peering];
            var fpcAddress = ((IoCcNeighborDiscovery)Node).Services.IoCcRecord.Endpoints[IoCcService.Keys.fpc];

            var pong = new Pong
            {
                ReqHash = ByteString.CopyFrom(SHA256.Create().ComputeHash(packet.Data.ToByteArray())),
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
            var id = MakeId(IoCcIdentity.FromPK(packet.PublicKey.Span), toAddress);

            if (RoutedRequest)
            {
                await SendMessage(data: pong.ToByteString(), type: IoCcPeerMessage.MessageTypes.Pong);

                Verified = true;
                //set ext address as seen by neighbor
                ExtGossipAddress ??= IoNodeAddress.Create($"tcp://{ping.DstAddr}:{CcNode.Services.IoCcRecord.Endpoints[IoCcService.Keys.gossip].Port}");

                if (Peer == null && PeerConnectedAtLeastOnce && CcNode.Neighbors.Count <= CcNode.MaxClients) //TODO 
                {
                    _logger.Info($"RE-/Verified peer {Id}, Peering = {CcNode.Neighbors.Count <= CcNode.MaxClients}, DMZ = {ExtGossipAddress}");
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
            }

            if (!Node.Neighbors.ContainsKey(id))
            {
                _logger.Info($"Probing new destination {toAddress}");
                await SendPingAsync(toAddress);
                if (CcNode.UdpTunnelSupport && toProxyAddress != null && toProxyAddress.Ip != toAddress.Ip)
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
            if (_pingRequest == null)
            {
                if (RoutedRequest)
                {
                    _logger.Debug($"{nameof(Pong)}({GetHashCode()}):  Unexpected! {Id}:{RemoteAddress.Port}");
                    Node.Neighbors.ToList().ForEach(kv=>_logger.Trace($"{kv.Value.Id}:{((IoNetClient<IoCcPeerMessage>)kv.Value.Source).RemoteAddress.Port}"));
                }
                else { } //ignore

                return;
            }

            var hash = IoCcIdentity.Sha256.ComputeHash(_pingRequest.ToByteArray());

            if (!pong.ReqHash.SequenceEqual(hash))
            {
                _logger.Debug(!RoutedRequest
                    ? $"{nameof(Pong)}: Got invalid request from {MakeId(IoCcIdentity.FromPK(packet.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData))}"
                    : $"{nameof(Pong)}: Got invalid request hash from {extraData} <=> {_pingRequest}, {Convert.ToBase64String(hash)} - {Convert.ToBase64String(pong.ReqHash.ToByteArray())}");

                return;
            }

            _pingRequest = null;
            
            //Unknown source IP
            if (!RoutedRequest)
            {
                _pingRequestAutoResetEvent.Release();

                var idCheck = IoCcIdentity.FromPK(packet.PublicKey.Span);
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
                    staleNeighbor.Zero();
#pragma warning restore 4014
                }

                if (!Node.Neighbors.TryGetValue(keyStr, out _))
                {
                    var remoteServices = new IoCcService();
                    foreach (var key in pong.Services.Map.Keys.ToList())
                        remoteServices.IoCcRecord.Endpoints.TryAdd(Enum.Parse<IoCcService.Keys>(key), IoNodeAddress.Create($"{pong.Services.Map[key].Network}://{((IPEndPoint)extraData).Address}:{pong.Services.Map[key].Port}"));

                    var newNeighbor = (IoCcNeighbor)Node.MallocNeighbor(Node, (IoNetClient<IoCcPeerMessage>)Source, Tuple.Create(idCheck, remoteServices, (IPEndPoint)extraData));

                    if (newNeighbor.RemoteAddress.IpEndPoint.Address.Equals(((IPEndPoint)extraData).Address) &&
                        newNeighbor.RemoteAddress.IpEndPoint.Port == ((IPEndPoint)extraData).Port &&
                        Node.Neighbors.TryAdd(keyStr, newNeighbor))
                    {
                        await newNeighbor.SendPingAsync();
                        await newNeighbor.SendDiscoveryRequestAsync(newNeighbor.RemoteAddress);
                    }
                    else
                    {
                        _logger.Trace($"Create new neighbor {keyStr} skipped!");
                    }
                }
                else
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
                    _logger.Info($"Discovered/Verified peer {Id}, Peering = {CcNode.Neighbors.Count <= CcNode.MaxClients}, DMZ = {ExtGossipAddress}");
                    await SendPeerRequestAsync();
                }
            }
            else if (Verified)
            {
                //Just check everything is cool
                if (Peer != null && (!Peer.IsArbitrating || !Peer.Source.IsOperational) && Direction != Kind.Undefined)
                {
                    _logger.Warn($"Found zombie Peer, closing: {Peer?.Id}");
#pragma warning disable 4014
                    Peer?.Zero();
#pragma warning restore 4014
                }
                else if(Peer == null && PeerConnectedAtLeastOnce) //TODO remove
                {
                    await SendPeerRequestAsync();
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
            dest ??= RemoteAddress;

            var pingRequest = new Ping
            {
                DstAddr = dest.IpEndPoint.Address.ToString(),
                NetworkId = 6,
                Version = 0,
                SrcAddr = "0.0.0.0", //TODO auto/manual option here
                SrcPort = (uint)CcNode.Services.IoCcRecord.Endpoints[IoCcService.Keys.peering].Port,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()/20 * 20 + 20 //TODO workaround for ping/pong spam rejections?
            };

            if (RoutedRequest)
            {
                _pingRequest = pingRequest;
                await SendMessage(dest, pingRequest.ToByteString(), IoCcPeerMessage.MessageTypes.Ping);
            }
            else
            {
                IoCcNeighbor ccNeighbor = null;
                foreach (var neighbor in Node.Neighbors.Values)
                {
                    if (!((IoCcNeighbor)neighbor).RoutedRequest)
                        continue;

                    if (((IoCcNeighbor)neighbor).RemoteAddress.Equals(dest))
                        ccNeighbor = (IoCcNeighbor)neighbor;
                }

                ccNeighbor ??= this;

                if (!ccNeighbor.RoutedRequest)
                {
                    if (!await _pingRequestAutoResetEvent.WaitAsync(parm_ping_timeout))
                    {
                        _logger.Debug($"{nameof(Ping)}:Probe failed {ccNeighbor.RemoteAddress ?? dest}");
                    }
                }

                ccNeighbor._pingRequest = pingRequest;
                await ccNeighbor.SendMessage( ccNeighbor.RemoteAddress??dest,pingRequest.ToByteString(), IoCcPeerMessage.MessageTypes.Ping);
                //_logger.Warn($"{nameof(Ping)}({ccNeighbor.GetHashCode()}): SENT! {Id}:{RemoteAddress?.Port}");
            }
        }

        /// <summary>
        /// Sends a discovery request
        /// </summary>
        /// <param name="dest">The destination address</param>
        /// <returns>Task</returns>
        private async Task SendDiscoveryRequestAsync(IoNodeAddress dest)
        {
            dest ??= RemoteAddress;

            _discoveryRequest = new DiscoveryRequest
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
            };

            await SendMessage(dest, _discoveryRequest.ToByteString(), IoCcPeerMessage.MessageTypes.DiscoveryRequest);
        }

        /// <summary>
        /// Sends a peer request
        /// </summary>
        /// <returns>Task</returns>
        private async Task SendPeerRequestAsync(IoNodeAddress dest = null)
        {
            if(CcNode.OutboundCount >= CcNode.parm_max_outbound)
                return;

            dest ??= RemoteAddress;

            _peerRequest = new PeeringRequest
            {
                Salt = new Salt { ExpTime = (ulong)DateTimeOffset.UtcNow.AddHours(2).ToUnixTimeSeconds(), Bytes = GetSalt },
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
            };

            await SendMessage(dest, _peerRequest.ToByteString(), IoCcPeerMessage.MessageTypes.PeeringRequest);
        }

        /// <summary>
        /// Tell peer to drop us when things go wrong. (why or when? cause it wont reconnect otherwise. This is a bug)
        /// </summary>
        /// <returns></returns>
        private async Task SendPeerDropAsync(IoNodeAddress dest = null)
        {
            dest ??= RemoteAddress;

            var dropRequest = new PeeringDrop
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
            };

            await SendMessage(dest, dropRequest.ToByteString(), IoCcPeerMessage.MessageTypes.PeeringDrop);

            //attempt to reconnect
            if (RoutedRequest)
                await SendPingAsync(dest);
        }

        //TODO complexity
        /// <summary>
        /// Attaches a gossip peer to this neighbor
        /// </summary>
        /// <param name="ioCcPeer">The peer</param>
        public void AttachPeer(IoCcPeer ioCcPeer)
        {
            if(Peer == ioCcPeer)
                return;

            Peer = ioCcPeer ?? throw new ArgumentNullException($"{nameof(ioCcPeer)}");

            if(Peer.IsArbitrating && Peer.Source.IsOperational)
                PeerConnectedAtLeastOnce = true;

            ioCcPeer.AttachNeighbor(this);

            _peerZeroSub = Peer.ZeroEvent(async sender =>
            {
                DetachPeer();
                await SendPeerDropAsync();
            });

            _neighborZeroSub = ZeroEvent(sender => Peer?.Zero());
        }

        /// <summary>
        /// Detaches a peer from this neighbor
        /// </summary>
        public void DetachPeer()
        {
            var peer = Peer;
            if(Peer == null)
                return;
            Peer = null;

            peer.Unsubscribe(_peerZeroSub);
            Unsubscribe(_neighborZeroSub);
            peer.DetachNeighbor();
            Direction = Kind.Undefined;
            Verified = false;
            ExtGossipAddress = null;

            if(peer != null)
                _logger.Info($"Detached peer {Id}");
        }
    }
}
