using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mime;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Microsoft.AspNetCore.Mvc.ModelBinding.Binders;
using NLog;
using Proto;
using zero.cocoon.identity;
using zero.cocoon.models;
using zero.cocoon.models.services;
using zero.core.conf;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using Logger = NLog.Logger;

namespace zero.cocoon.autopeer
{
    public class IoCcNeighbor : IoNeighbor<IoCcPeerMessage>
    {
        public IoCcNeighbor(IoNode<IoCcPeerMessage> node, IoNetClient<IoCcPeerMessage> ioNetClient, object extraData = null, IoCcService services = null) : base(node, ioNetClient, userData => new IoCcPeerMessage("peer rx", $"{ioNetClient.AddressString}", ioNetClient))
        {
            _logger = LogManager.GetCurrentClassLogger();
            Identity = ((Tuple<IoCcIdentity, IoCcService>)extraData)?.Item1 ?? IoCcIdentity.Generate();
            Address = ioNetClient.RemoteAddress;
            NeighborDiscoveryNode = (IoCcNeighborDiscovery)node;
            Services = services ?? ((Tuple<IoCcIdentity, IoCcService>)extraData)?.Item2 ?? ((IoCcNeighborDiscovery)node).Services;
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
        /// The neighbor address
        /// </summary>
        public IoNodeAddress Address { get; protected set; }

        /// <summary>
        /// Tcp Readahead
        /// </summary>
        public const int TcpReadAhead = 50;

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
        /// Who contacted who?
        /// </summary>
        public enum Kind
        {
            Undefined,
            Inbound,
            OutBound
        }

        /// <summary>
        /// Create an Id string
        /// </summary>
        /// <param name="identity">The crypto identity</param>
        /// <param name="address">The transport identity</param>
        /// <returns></returns>
        public static string MakeId(IoCcIdentity identity, IoNodeAddress address)
        {
            return $"{identity.IdString()}|{identity.PkString()}@{address.Ip}";
        }

        /// <summary>
        /// The Id
        /// </summary>
        public override string Id => $"{MakeId(Identity, Address??((IoUdpClient<IoCcPeerMessage>)Source).ListeningAddress)}";

        /// <summary>
        /// The neighbor services
        /// </summary>
        public IoCcService Services { get; protected set; }

        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_salt_length = 20;

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
        /// Start processors for this neighbor
        /// </summary>
        /// <param name="cancellationToken">The cancellation token</param>
        /// <param name="spawnProducer">Spawns a source thread</param>
        /// <returns></returns>
        public override async Task SpawnProcessingAsync(CancellationToken cancellationToken, bool spawnProducer = true)
        {
            var neighbor = base.SpawnProcessingAsync(cancellationToken, spawnProducer);
            var protocol = ProcessProtoMsgAsync();

            await Task.WhenAll(neighbor, protocol).ConfigureAwait(false);

            if (neighbor.IsFaulted)
            {
                _logger.Fatal(neighbor.Exception, "Neighbor processing returned with errors!");
                Spinners.Cancel();
            }

            if (protocol.IsFaulted)
            {
                _logger.Fatal(protocol.Exception, "Protocol processing returned with errors!");
                Spinners.Cancel();
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
        private async Task ProcessProtoMsgAsync()
        {
            if (_protocolChannel == null)
                _protocolChannel = Source.GetChannel<IoCcProtocolMessage>(nameof(IoCcNeighbor));

            _logger.Debug($"Starting persistence for `{Description}'");
            while (!Spinners.IsCancellationRequested)
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
                        await ProcessMsgBatchAsync(batch, _protocolChannel,
                        (msg
                                            , forward) =>
                        {
#pragma warning disable 4014
                            try
                            {
                                IoCcNeighbor ccNeighbor = null;
                                //TODO optimize
                                Node.Neighbors.TryGetValue(MakeId(IoCcIdentity.FromPK(msg.Item3.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp",(IPEndPoint)msg.Item2)), out var n);
                                if (n == null)
                                    ccNeighbor = this;
                                else
                                    ccNeighbor = (IoCcNeighbor)n;
                                
                                switch (msg.Item1.GetType().Name)
                                {
                                    case nameof(Ping):
                                        ccNeighbor.ProcessMsgAsync((Ping)msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(Pong):
                                        ccNeighbor.ProcessMsgAsync((Pong)msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(DiscoveryRequest):
                                        ccNeighbor.ProcessMsgAsync((DiscoveryRequest)msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(DiscoveryResponse):
                                        ccNeighbor.ProcessMsgAsync((DiscoveryResponse)msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(PeeringRequest):
                                        ccNeighbor.ProcessPeerReqAsync((PeeringRequest)msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(PeeringResponse):
                                        ccNeighbor.ProcessPeerReqAsync((PeeringResponse)msg.Item1, msg.Item2, msg.Item3);
                                        break;
                                    case nameof(PeeringDrop):
                                        ccNeighbor.ProcessPeerReqAsync((PeeringDrop)msg.Item1, msg.Item2, msg.Item3);
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

            _logger.Debug($"Shutting down persistence for `{Description}'");
        }

        /// <summary>
        /// Peer drop request
        /// </summary>
        /// <param name="request">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task ProcessPeerReqAsync(PeeringDrop request, object extraData, Packet packet)
        {
            if (Address == null || DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Timestamp > 20)
            {
                _logger.Trace($"{nameof(PeeringDrop)}: Ignoring old/invalid request({DateTimeOffset.FromUnixTimeSeconds(request.Timestamp)})");
                return;
            }

            //only verified nodes get to drop
            if(!Verified || Peer == null)
                return;

            //if (Direction == Kind.Inbound)
            
            _logger.Debug($"{nameof(PeeringDrop)}: {Direction} Peer= {Peer?.Id ?? "null"}");
            Peer.Close();
            Direction = Kind.Undefined;
            Verified = false;

            //Attempt reconnect
            await SendPingMsgAsync(Address);
        }

        /// <summary>
        /// Peering Request message
        /// </summary>
        /// <param name="request">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task ProcessPeerReqAsync(PeeringRequest request, object extraData, Packet packet)
        {
            if (Address == null)
            {
                _logger.Trace($"{nameof(PeeringRequest)}: Dropped request, not verified! ({DateTimeOffset.FromUnixTimeSeconds(request.Timestamp)})");
                return;
            }

            var peeringResponse = new PeeringResponse
            {
                ReqHash = ByteString.CopyFrom(SHA256.Create().ComputeHash(packet.Data.ToByteArray())),
                Status = ((IoCcNeighborDiscovery)Node).CcNode.Neighbors.Count < ((IoCcNeighborDiscovery)Node).CcNode.MaxClients
            };

            lock (this)
            {
                if (Direction == Kind.Undefined)
                {
                    Direction = peeringResponse.Status ? Kind.Inbound : Kind.Undefined;
                    ((IoCcNeighborDiscovery)Node).CcNode.ConnectToPeer(this);
                    _logger.Info($"Peering negotiated {Direction}: {Id}");
                }
                else if (Direction == Kind.OutBound)
                {
                    _logger.Debug($"Peering Rejected {Direction}: {Id} is already {Kind.Inbound}");
                    peeringResponse.Status = false; //TODO does this work?
                }
                else
                {
                    _logger.Info($"Peering stands {Direction}: {Id}");
                }
            }

            _logger.Trace($"{nameof(PeeringResponse)}: Sent Allow Status = {peeringResponse.Status}, Capacity = {-((IoCcNeighborDiscovery)Node).CcNode.Neighbors.Count + ((IoCcNeighborDiscovery)Node).CcNode.MaxClients}");

            await SendMessage(Address, peeringResponse.ToByteString(), IoCcPeerMessage.MessageTypes.PeeringResponse);
        }

        /// <summary>
        /// Peer response message
        /// </summary>
        /// <param name="response">The request</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private void ProcessPeerReqAsync(PeeringResponse response, object extraData, Packet packet)
        {
            if (_peerRequest == null || Address == null)
            {
                _logger.Trace($"{nameof(PeeringResponse)}: Unexpected request from {extraData}, {Address}");
                return;
            }

            var hash = IoCcIdentity.Sha256.ComputeHash(_peerRequest.ToByteArray());

            if (Address != null && !response.ReqHash.SequenceEqual(hash))
            {
                if (Address == null)
                    _logger.Debug($"{nameof(Pong)}: Got invalid request from {MakeId(IoCcIdentity.FromPK(packet.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint)extraData))}");
                else
                    _logger.Debug($"{nameof(Pong)}: Got invalid request hash from {extraData}, {Convert.ToBase64String(hash)} - {Convert.ToBase64String(response.ReqHash.ToByteArray())}");

                return;
            }

            _logger.Trace($"{nameof(PeeringResponse)}: Got status = {response.Status}");

            if (response.Status)
            {
                lock (this)
                {
                    if (Direction == Kind.Undefined)
                    {
                        Direction = response.Status ? Kind.OutBound : Kind.Undefined;
                        ((IoCcNeighborDiscovery)Node).CcNode.ConnectToPeer(this);
                    }
                    else
                        return;

                    _logger.Info($"Peering negotiated {Direction}: {Id}");
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
        private async Task<(int sent, Packet responsePacket)> SendMessage(IoNodeAddress dest, ByteString data, IoCcPeerMessage.MessageTypes type)
        {
            var packet = new Packet
            {
                Data = data,
                PublicKey = ByteString.CopyFrom(CcId.PublicKey),
                Type = (uint)type
            };

            packet.Signature =
                ByteString.CopyFrom(CcId.Sign(packet.Data.ToByteArray(), 0, packet.Data.Length));

            var msgRaw = packet.ToByteArray();

            var sent = await ((IoUdpClient<IoCcPeerMessage>)Source).Socket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint);
            _logger.Debug($"{Enum.GetName(typeof(IoCcPeerMessage.MessageTypes), packet.Type)}: Sent {sent} bytes to {dest.IpEndPoint}");
            return (sent, packet);
        }

        /// <summary>
        /// Discovery response message
        /// </summary>
        /// <param name="response">The response</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task ProcessMsgAsync(DiscoveryResponse response, object extraData, Packet packet)
        {

            if (Address != null && response.ReqHash.SequenceEqual(IoCcIdentity.Sha256.ComputeHash(_discoveryRequest.ToByteArray())))
            {
                if (Address != null)
                    _logger.Debug($"{nameof(DiscoveryResponse)}: Got invalid request from {MakeId(IoCcIdentity.FromPK(packet.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint)extraData))}");
                return;
            }

            _logger.Debug($"{nameof(PeeringResponse)}: Got {response.Peers.Count} peers ");
            foreach (var responsePeer in response.Peers)
            {
                var idCheck = IoCcIdentity.FromPK(responsePeer.PublicKey.Span);
                var remoteServices = IoNodeAddress.Create($"{responsePeer.Services.Map[IoCcService.Keys.gossip.ToString()].Network}://{responsePeer.Ip}:{responsePeer.Services.Map[IoCcService.Keys.gossip.ToString()].Port}");
                var newNeighbor = (IoCcNeighbor)Node.MallocNeighbor(Node, (IoNetClient<IoCcPeerMessage>)Source, Tuple.Create(idCheck, remoteServices));
                if (Node.Neighbors.TryAdd(newNeighbor.Id, newNeighbor))
                {
                    _logger.Debug($"Exchanged new peer info: {newNeighbor.Id}@{remoteServices}");
                    await SendPingMsgAsync(IoNodeAddress.CreateFromEndpoint("udp", (EndPoint)extraData));
                }
            }
        }

        /// <summary>
        /// The public ID of this node
        /// </summary>
        static IoCcIdentity CcId = IoCcIdentity.Generate(true); //TODO 

        /// <summary>
        /// Receives protocol messages from here
        /// </summary>
        private IoChannel<IoCcProtocolMessage> _protocolChannel;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private DiscoveryRequest _discoveryRequest;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private Ping _pingRequest;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private PeeringRequest _peerRequest;

        /// <summary>
        /// Current salt value
        /// </summary>
        private ByteString _curSalt;

        /// <summary>
        /// 
        /// </summary>
        private ByteString GetSalt => _curSalt = ByteString.CopyFrom(IoCcIdentity.Sha256.ComputeHash(Encoding.ASCII.GetBytes((DateTimeOffset.UtcNow.ToUnixTimeSeconds()/120*60).ToString())),0, parm_salt_length);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="extraData"></param>
        /// <param name="packet"></param>
        /// <returns></returns>
        private async Task ProcessMsgAsync(DiscoveryRequest request, object extraData, Packet packet)
        {
            //var hashStream = new MemoryStream(new byte[request.CalculateSize() + 1]);
            //hashStream.WriteByte((byte)IoCcPeerMessage.MessageTypes.DiscoveryRequest);
            //request.WriteTo(hashStream);
            //hashStream.Seek(0, SeekOrigin.Begin);

            if (Address == null || DateTimeOffset.UtcNow.ToUnixTimeSeconds() - request.Timestamp > 20)
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
                Peers = { new Peer { PublicKey = ByteString.CopyFrom(CcId.PublicKey), Ip = Source.SourceUri, Services = new ServiceMap { Map = { new Dictionary<string, NetworkAddress> { { IoCcService.Keys.peering.ToString(), new NetworkAddress { Network = ((IoCcNeighborDiscovery)Node).CcNode.ExtAddress.Ip, Port = (uint)((IoCcNeighborDiscovery)Node).CcNode.ExtAddress.Port } } } } } } }
            };

            foreach (var ioNeighbor in NeighborDiscoveryNode.Neighbors)
            {
                if (ioNeighbor.Value == this || ((IoCcNeighbor)ioNeighbor.Value).Address == null)
                    continue;

                discoveryResponse.Peers.Add(new Peer
                {
                    PublicKey = ByteString.CopyFrom(((IoCcNeighbor)ioNeighbor.Value).Identity.PublicKey),
                    Services = ((IoCcNeighbor)ioNeighbor.Value).ServiceMap,
                    Ip = ((IoCcNeighbor)ioNeighbor.Value).Address.Ip
                });
            }

            //using var poolMem = MemoryPool<byte>.Shared.Rent(discoveryResponse.CalculateSize() + 1);
            //MemoryMarshal.TryGetArray<byte>(poolMem.Memory, out var heapMem);
            //var heapStream = new MemoryStream(heapMem.Array);
            //heapStream.WriteByte((byte)IoCcPeerMessage.MessageTypes.DiscoveryResponse);
            //discoveryResponse.WriteTo(heapStream);

            await SendMessage(Address, discoveryResponse.ToByteString(), IoCcPeerMessage.MessageTypes.DiscoveryResponse);
        }

        /// <summary>
        /// Ping message
        /// </summary>
        /// <param name="ping">The ping packet</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task ProcessMsgAsync(Ping ping, object extraData, Packet packet)
        {
            var remoteEp = (IPEndPoint)extraData;
            var age = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - ping.Timestamp;
            if (age > 20 || age < -20) //TODO params
            {
                _logger.Trace($"{nameof(Ping)}: message dropped, to old. age = {age}s");
                return;
            }

            //await using var hashStream = new MemoryStream(new byte[ping.CalculateSize() + 1]);
            //hashStream.WriteByte((byte)IoCcPeerMessage.MessageTypes.Ping);
            //ping.WriteTo(hashStream);
            //var pos = hashStream.Position;
            //hashStream.Seek(0, SeekOrigin.Begin);

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

            if (Address != null)
            {
                await SendMessage(Address, pong.ToByteString(), IoCcPeerMessage.MessageTypes.Pong);
            }
            else
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
                await SendPingMsgAsync(toAddress);
                if (((IoCcNeighborDiscovery)Node).CcNode.UdpTunnelSupport &&  toProxyAddress != null && toProxyAddress.Ip != toAddress .Ip)
                    await SendPingMsgAsync(toProxyAddress);
            }
        }

        /// <summary>
        /// Pong message
        /// </summary>
        /// <param name="pong">The Pong packet</param>
        /// <param name="extraData">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async Task ProcessMsgAsync(Pong pong, object extraData, Packet packet)
        {
            var hash = IoCcIdentity.Sha256.ComputeHash(_pingRequest.ToByteArray());

            if (Address != null && !pong.ReqHash.SequenceEqual(hash))
            {
                if (Address == null)
                    _logger.Debug($"{nameof(Pong)}: Got invalid request from {MakeId(IoCcIdentity.FromPK(packet.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint)extraData))}");
                else
                    _logger.Debug($"{nameof(Pong)}: Got invalid request hash from {extraData}, {Convert.ToBase64String(hash)} - {Convert.ToBase64String(pong.ReqHash.ToByteArray())}");
                
                    
                return;
            }

            if (Address == null)
            {
                var idCheck = IoCcIdentity.FromPK(packet.PublicKey.Span);
                var keyStr = MakeId(idCheck, IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint)extraData));
                //TODO normalize key
                if (!Node.Neighbors.TryGetValue(keyStr, out _))
                {
                    var remoteServices = new IoCcService();
                    foreach (var key in pong.Services.Map.Keys.ToList())
                        remoteServices.IoCcRecord.Endpoints.TryAdd(Enum.Parse<IoCcService.Keys>(key), IoNodeAddress.Create($"{pong.Services.Map[key].Network}://{((IPEndPoint)extraData).Address}:{pong.Services.Map[key].Port}"));

                    var newNeighbor = (IoCcNeighbor)Node.MallocNeighbor(Node, (IoNetClient<IoCcPeerMessage>)Source, Tuple.Create(idCheck, remoteServices));

                    if (newNeighbor.Address.IpEndPoint.Address.Equals(((IPEndPoint)extraData).Address) &&
                        newNeighbor.Address.IpEndPoint.Port == ((IPEndPoint)extraData).Port &&
                        Node.Neighbors.TryAdd(keyStr, newNeighbor))
                    {
                        await newNeighbor.SendPingMsgAsync(newNeighbor.Address);
                        await newNeighbor.SendDiscoveryRequestAsync(newNeighbor.Address);
                    }
                    else
                    {
                        _logger.Trace($"Create new neighbor {keyStr} skipped!");
                    }
                }
            }
            else if (!Verified)
            {
                Verified = true;
                _logger.Info($"Discovered/Verified peer {Id}");
                if( ((IoCcNeighborDiscovery)Node).CcNode.Neighbors.Count < ((IoCcNeighborDiscovery)Node).CcNode.MaxClients) //TODO 
                    await SendPeerRequestAsync();
            }
        }

        /// <summary>
        /// Sends a ping packet
        /// </summary>
        /// <param name="dest">The destination address</param>
        /// <returns>Task</returns>
        private async Task SendPingMsgAsync(IoNodeAddress dest)
        {
            _pingRequest = new Ping
            {
                DstAddr = dest.IpEndPoint.Address.ToString(),
                NetworkId = 6,
                Version = 0,
                SrcAddr = "0.0.0.0", //TODO auto/manual option here
                SrcPort = (uint)((IoCcNeighborDiscovery)Node).CcNode.Services.IoCcRecord.Endpoints[IoCcService.Keys.peering].Port,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
            };

            if(Address != null)
                await SendMessage(dest, _pingRequest.ToByteString(), IoCcPeerMessage.MessageTypes.Ping);
            else
            {
                IoCcNeighbor ccNeighbor = null;
                foreach (var neighbor in Node.Neighbors.Values)
                {
                    if(((IoCcNeighbor)neighbor).Address == null)
                        continue;
                    
                    if (((IoCcNeighbor) neighbor).Address.IpPort == dest.IpPort)
                        ccNeighbor = (IoCcNeighbor) neighbor;
                }

                ccNeighbor ??= this;

                await ccNeighbor.SendMessage(dest, _pingRequest.ToByteString(), IoCcPeerMessage.MessageTypes.Ping);
            }
        }

        /// <summary>
        /// Sends a discovery request
        /// </summary>
        /// <param name="dest">The destination address</param>
        /// <returns>Task</returns>
        private async Task SendDiscoveryRequestAsync(IoNodeAddress dest)
        {
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
        private async Task SendPeerRequestAsync()
        {
            _peerRequest = new PeeringRequest
            {
                Salt = new Salt{ExpTime = (ulong) DateTimeOffset.UtcNow.AddHours(2).ToUnixTimeSeconds(), Bytes = GetSalt},
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
            };

            await SendMessage(Address, _peerRequest.ToByteString(), IoCcPeerMessage.MessageTypes.PeeringRequest);
        }

        /// <summary>
        /// Attaches a gossip peer to this neighbor
        /// </summary>
        /// <param name="ioCcPeer">The peer</param>
        public void AttachPeer(IoCcPeer ioCcPeer)
        {
            Peer = ioCcPeer;
        }

        /// <summary>
        /// Detaches a peer from this neighbor
        /// </summary>
        public void DetachPeer()
        {
            Peer = null;
        }
    }
}
