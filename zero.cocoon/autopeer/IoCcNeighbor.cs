using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
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
using Logger = NLog.Logger;

namespace zero.cocoon.autopeer
{
    public class IoCcNeighbor : IoNeighbor<IoCcPeerMessage>
    {
        public IoCcNeighbor(IoNode<IoCcPeerMessage> node, IoNetClient<IoCcPeerMessage> ioNetClient, object extraData = null, IoCcService services = null) : base(node, ioNetClient, userData => new IoCcPeerMessage("peer rx", $"{ioNetClient.AddressString}", ioNetClient))
        {
            _logger = LogManager.GetCurrentClassLogger();
            Identity = ((Tuple<IoCcIdentity, IoCcService>)extraData)?.Item1 ?? IoCcIdentity.Generate();
            NeighborDiscoveryNode = (IoCcNeighborDiscovery)node;
            Services = services ?? ((Tuple<IoCcIdentity, IoCcService>)extraData)?.Item2 ?? ((IoCcNeighborDiscovery)node).Services;
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        protected IoCcNeighborDiscovery NeighborDiscoveryNode;

        public const int TcpReadAhead = 50;

        public IoCcIdentity Identity { get; protected set; }

        public static string MakeId(IoCcIdentity identity, IoNodeAddress address)
        {
            return $"{identity.IdString()}|{identity.PkString()}@{address.HostStr}";
        }
        public override string Id => $"{Identity.IdString()}|{Identity.PkString()}@{((IoUdpClient<IoCcPeerMessage>)Producer).RemoteAddress.HostStr}";

        public IoCcService Services { get; protected set; }

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
        /// <param name="spawnProducer">Spawns a producer thread</param>
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
        private async Task ProcessMsgBatchAsync(IoConsumable<IoCcProtocolMessage> consumer,
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

                consumer.ProcessState = IoProducible<IoCcProtocolMessage>.State.Consumed;

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
                _protocolChannel = Producer.GetChannel<IoCcProtocolMessage>(nameof(IoCcNeighbor));

            _logger.Debug($"Starting persistence for `{Description}'");
            while (!Spinners.IsCancellationRequested)
            {
                _logger.Debug($"{GetHashCode()} <- checking for UDP mesages");
                if (_protocolChannel == null)
                {
                    _logger.Warn($"Waiting for `{Description}' stream to spin up...");
                    _protocolChannel = Producer.AttachProducer<IoCcProtocolMessage>(nameof(IoCcNeighbor));
                    await Task.Delay(2000);//TODO config
                    continue;
                }

                await _protocolChannel.ConsumeAsync(async batch =>
                {
                    try
                    {
                        _logger.Debug($"{GetHashCode()} <--- GOT for UDP mesages!!!");

                        await ProcessMsgBatchAsync(batch, _protocolChannel,
                        (message, forward) =>
                        {
#pragma warning disable 4014
                            try
                            {
                                switch (message.Item1.GetType().Name)
                                {
                                    case nameof(Ping):
                                        ProcessMsgAsync((Ping)message.Item1, message.Item2, message.Item3);
                                        break;
                                    case nameof(Pong):
                                        ProcessMsgAsync((Pong)message.Item1, message.Item2, message.Item3);
                                        break;
                                    case nameof(DiscoveryRequest):
                                        ProcessMsgAsync((DiscoveryRequest)message.Item1, message.Item2);
                                        break;
                                    case nameof(DiscoveryResponse):
                                        break;
                                    case nameof(PeeringRequest):
                                        ProcessPeerReqAsync((PeeringRequest)message.Item1, message.Item2, message.Item3);
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
                        if (batch != null && batch.ProcessState != IoProducible<IoCcProtocolMessage>.State.Consumed)
                            batch.ProcessState = IoProducible<IoCcProtocolMessage>.State.ConsumeErr;
                    }
                }).ConfigureAwait(false);

                if (!_protocolChannel.Producer.IsOperational)
                    break;
            }

            _logger.Debug($"Shutting down persistence for `{Description}'");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="extraData"></param>
        /// <param name="packet"></param>
        /// <returns></returns>
        private async Task ProcessPeerReqAsync(PeeringRequest request, object extraData, Packet packet)
        {

            var hashStream = new MemoryStream(new byte[request.CalculateSize() + 1]);
            hashStream.WriteByte((byte)IoCcPeerMessage.MessageTypes.PeeringRequest);
            request.WriteTo(hashStream);
            hashStream.Seek(0, SeekOrigin.Begin);

            //TODO: normalize
            var id = MakeId(IoCcIdentity.FromPK(packet.PublicKey.Span), IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData));
            //var id = $"{Base58Check.Base58CheckEncoding.EncodePlain(packet.PublicKey.Span.Slice(0, 8).ToArray())}|{IoCcIdentity.FromPK(packet.PublicKey.Span)}@udp://{extraData}";

            var peeringResponse = new PeeringResponse
            {
                ReqHash = ByteString.CopyFrom(SHA256.Create().ComputeHash(hashStream)),
                Status = Node.Neighbors.ContainsKey(id) //TODO Distance calc
            };

            using var poolMem = MemoryPool<byte>.Shared.Rent(peeringResponse.CalculateSize() + 1);
            MemoryMarshal.TryGetArray<byte>(poolMem.Memory, out var heapMem);
            var heapStream = new MemoryStream(heapMem.Array);
            heapStream.WriteByte((byte)IoCcPeerMessage.MessageTypes.PeeringResponse);
            peeringResponse.WriteTo(heapStream);

            var protoResponse = new Packet
            {
                Data = ByteString.CopyFrom(heapMem.Array, 0, (int)heapStream.Position),
                PublicKey = ByteString.CopyFrom(CcId.PublicKey)
            };

            protoResponse.Signature =
                ByteString.CopyFrom(CcId.Sign(protoResponse.Data.ToByteArray(), 0, protoResponse.Data.Length));

            var discRespMsgRaw = protoResponse.ToByteArray();

            var sent = await ((IoUdpClient<IoCcPeerMessage>)Producer).Socket.SendAsync(discRespMsgRaw, 0,
                discRespMsgRaw.Length, (EndPoint)extraData).ConfigureAwait(false);
            _logger.Debug($"{nameof(DiscoveryResponse)}: Sent {sent} bytes ({extraData})");
        }

        static IoCcIdentity CcId = IoCcIdentity.Generate(); //TODO 
        private IoChannel<IoCcProtocolMessage> _protocolChannel;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="extraData"></param>
        /// <returns></returns>
        private async Task ProcessMsgAsync(DiscoveryRequest request, object extraData)
        {
            var hashStream = new MemoryStream(new byte[request.CalculateSize() + 1]);
            hashStream.WriteByte((byte)IoCcPeerMessage.MessageTypes.DiscoveryRequest);
            request.WriteTo(hashStream);
            hashStream.Seek(0, SeekOrigin.Begin);


            var discoveryResponse = new DiscoveryResponse
            {
                ReqHash = ByteString.CopyFrom(SHA256.Create().ComputeHash(hashStream)),
                Peers = { new Peer { PublicKey = ByteString.CopyFrom(CcId.PublicKey), Ip = Producer.SourceUri, Services = new ServiceMap { Map = { new Dictionary<string, NetworkAddress> { { IoCcService.Keys.peering.ToString(), new NetworkAddress { Network = NeighborDiscoveryNode.Server.ListeningAddress.UrlNoPort, Port = (uint)NeighborDiscoveryNode.Server.ListeningAddress.Port } } } } } } }
            };

            foreach (var ioNeighbor in NeighborDiscoveryNode.Neighbors)
            {
                if (ioNeighbor.Value == this)
                    continue;

                discoveryResponse.Peers.Add(new Peer
                {
                    PublicKey = ByteString.CopyFrom(((IoCcNeighbor)ioNeighbor.Value).Identity.PublicKey),
                    Services = ((IoCcNeighbor)ioNeighbor.Value).ServiceMap
                });
            }

            using var poolMem = MemoryPool<byte>.Shared.Rent(discoveryResponse.CalculateSize() + 1);
            MemoryMarshal.TryGetArray<byte>(poolMem.Memory, out var heapMem);
            var heapStream = new MemoryStream(heapMem.Array);
            heapStream.WriteByte((byte)IoCcPeerMessage.MessageTypes.DiscoveryResponse);
            discoveryResponse.WriteTo(heapStream);

            var protoResponse = new Packet
            {
                Data = ByteString.CopyFrom(heapMem.Array, 0, (int)heapStream.Position),
                PublicKey = ByteString.CopyFrom(CcId.PublicKey)
            };

            protoResponse.Signature =
                ByteString.CopyFrom(CcId.Sign(protoResponse.Data.ToByteArray(), 0, protoResponse.Data.Length));

            var discRespMsgRaw = protoResponse.ToByteArray();

            var sent = await ((IoUdpClient<IoCcPeerMessage>)Producer).Socket.SendAsync(discRespMsgRaw, 0,
                discRespMsgRaw.Length, (EndPoint)extraData).ConfigureAwait(false);
            _logger.Debug($"{nameof(DiscoveryResponse)}: Sent {sent} bytes to {extraData}");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ping"></param>
        /// <param name="extraData"></param>
        /// <param name="packet"></param>
        /// <returns></returns>
        private async Task ProcessMsgAsync(Ping ping, object extraData, Packet packet)
        {
            var remoteEp = (IPEndPoint)extraData;
            var age = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - ping.Timestamp;
            if (age > 20 || age < -20) //TODO params
            {
                _logger.Trace($"{nameof(Ping)}: message dropped, to old. age = {age}s");
                return;
            }

            await using var hashStream = new MemoryStream(new byte[ping.CalculateSize() + 1]);
            hashStream.WriteByte((byte)IoCcPeerMessage.MessageTypes.Ping);
            ping.WriteTo(hashStream);
            var pos = hashStream.Position;
            hashStream.Seek(0, SeekOrigin.Begin);

            //TODO optimize
            var gossipAddress = ((IoCcNeighborDiscovery)Node).Services.IoCcRecord.Endpoints[IoCcService.Keys.peering];
            var peeringAddress = ((IoCcNeighborDiscovery)Node).Services.IoCcRecord.Endpoints[IoCcService.Keys.peering];
            var fpcAddress = ((IoCcNeighborDiscovery)Node).Services.IoCcRecord.Endpoints[IoCcService.Keys.peering];

            var pong = new Pong
            {
                ReqHash = ByteString.CopyFrom(SHA256.Create().ComputeHash(hashStream)),
                DstAddr = $"{remoteEp.Address}",//TODO, add port somehow
                Services = new ServiceMap
                {
                    Map =
                    {
                        {"peering", new NetworkAddress {Network = "udp", Port = (uint)peeringAddress.Port}},
                        {"gossip", new NetworkAddress {Network = "tcp", Port = (uint)gossipAddress.Port}},
                        {"fpc", new NetworkAddress {Network = "tcp", Port = (uint)fpcAddress.Port}}
                    }
                }
            };

            using var poolMem = MemoryPool<byte>.Shared.Rent(pong.CalculateSize() + 1);
            MemoryMarshal.TryGetArray<byte>(poolMem.Memory, out var heapMem);
            var heapStream = new MemoryStream(heapMem.Array);
            heapStream.WriteByte((byte)IoCcPeerMessage.MessageTypes.Pong);
            pong.WriteTo(heapStream);

            var responsePacket = new Packet
            {
                Data = ByteString.CopyFrom(heapMem.Array, 0, (int)heapStream.Position),
                PublicKey = ByteString.CopyFrom(CcId.PublicKey)
            };

            responsePacket.Signature =
                ByteString.CopyFrom(CcId.Sign(responsePacket.Data.ToByteArray(), 0, responsePacket.Data.Length));

            var pongMsgRaw = responsePacket.ToByteArray();

            IoNodeAddress dest = null;
            IoNodeAddress dest2 = IoNodeAddress.Create($"udp://{remoteEp.Address}:{ping.SrcPort}");

            if (ping.SrcAddr != "0.0.0.0" && remoteEp.Address.ToString() != ping.SrcAddr)
            {
                dest = IoNodeAddress.Create($"udp://{ping.SrcAddr}:{ping.SrcPort}");
                _logger.Trace($"static peer address received: {dest}, source detected = udp://{remoteEp}");
            }
            else
            {
                dest = IoNodeAddress.CreateFromEndpoint("udp", remoteEp);
                _logger.Trace($"automatic peer address detected: {dest}, source declared = udp://{ping.SrcAddr}:{ping.SrcPort}");
            }

            if (dest.Validated)
            {
                var sent = await ((IoUdpClient<IoCcPeerMessage>)Producer).Socket.SendAsync(pongMsgRaw, 0,
                    pongMsgRaw.Length, dest2.IpEndPoint); //TODO

                _logger.Trace($"{nameof(Pong)}({GetHashCode()}): Sent {sent} bytes to {dest2}");

                //retry on incoming proxy
                sent = await ((IoUdpClient<IoCcPeerMessage>)Producer).Socket.SendAsync(pongMsgRaw, 0,
                    pongMsgRaw.Length, dest.IpEndPoint); //TODO

                _logger.Trace($"{nameof(Pong)}({GetHashCode()}): Sent {sent} bytes to {dest}");

                var identity = IoCcIdentity.FromPK(packet.PublicKey.ToByteArray());

                //TODO Optimize
                if (!Node.Neighbors.ContainsKey(MakeId(identity, dest2)))
                {
                    await SendPingMsgAsync(dest2);
                }
            }
            else
            {
                _logger.Error($"{nameof(Ping)}: Invalid dest address {dest}, ({dest.ValidationErrorString})");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="pong"></param>
        /// <param name="extraData"></param>
        /// <param name="packet"></param>
        /// <returns></returns>
        private async Task ProcessMsgAsync(Pong pong, object extraData, Packet packet)
        {
            //var pk = Base58Check.Base58CheckEncoding.EncodePlain(packet.PublicKey.ToByteArray());
            //var idCheck = IoCcIdentity.FromPK(pk);
            //var keyStr = $"{Base58Check.Base58CheckEncoding.EncodePlain(idCheck.Id.AsSpan().Slice(0, 8).ToArray())}|{Base58Check.Base58CheckEncoding.EncodePlain(idCheck.PublicKey)}@udp://{pong.DstAddr}:{pong.Services.Map[IoCcService.Keys.peering.ToString()].Port}";
            var idCheck = IoCcIdentity.FromPK(packet.PublicKey.Span);
            var keyStr = MakeId(idCheck, IoNodeAddress.CreateFromEndpoint("udp", (IPEndPoint) extraData));
            //TODO normalize key
            if (!Node.Neighbors.TryGetValue(keyStr, out _))
            {
                var remoteServices = new IoCcService();
                foreach (var key in pong.Services.Map.Keys.ToList())
                    remoteServices.IoCcRecord.Endpoints.TryAdd(Enum.Parse<IoCcService.Keys>(key), IoNodeAddress.Create($"{pong.Services.Map[key].Network}://{extraData}"));

                //var newNeighbor = (IoCcNeighbor)await Node.SpawnConnectionAsync(IoNodeAddress.CreateFromEndpoint("udp", (EndPoint)extraData),  Tuple.Create(IoCcIdentity.FromPK(pk), remoteServices));
                //if (newNeighbor != null && false) //TODO, this entire if statement is full of unknowns
                //{
                //    //newNeighbor._protocolChannel = _protocolChannel;
                //    await newNeighbor.SendPingMsgAsync(IoNodeAddress.CreateFromEndpoint("udp", (EndPoint)extraData));
                //    //TODO: twice because the first time the src port is not set by the framework.
                //    await newNeighbor.SendPingMsgAsync(IoNodeAddress.CreateFromEndpoint("udp", (EndPoint)extraData));

                //    ((IoCcNeighborDiscovery)Node).CcNode.HandleVerifiedNeighbor(newNeighbor);

                //    var task = newNeighbor.SpawnProcessingAsync(Spinners.Token);
                //}
                var newNeighbor = (IoCcNeighbor)Node.MallocNeighbor(Node, (IoNetClient<IoCcPeerMessage>)Producer, Tuple.Create(idCheck, remoteServices));
                if (Node.Neighbors.TryAdd(keyStr, newNeighbor))
                {
                    _logger.Info($"Discovered/Verified peer {keyStr}");
                    ((IoCcNeighborDiscovery)Node).CcNode.HandleVerifiedNeighbor(newNeighbor);
                    await SendPingMsgAsync(IoNodeAddress.CreateFromEndpoint("udp", (EndPoint)extraData));
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dest"></param>
        /// <returns></returns>
        private async Task SendPingMsgAsync(IoNodeAddress dest)
        {
            var srcAddr = ((IoCcNeighborDiscovery)Node).CcNode.ExtAddress?.HostStr ??
                          ((IoUdpClient<IoCcPeerMessage>)Producer).Socket.LocalAddress;
            var ping = new Ping
            {
                DstAddr = dest.IpEndPoint.Address.ToString(),
                NetworkId = 6,
                Version = 0,
                //SrcAddr = srcAddr,
                SrcAddr = "0.0.0.0", //TODO auto/manual option here
                SrcPort = (uint)((IoCcNeighborDiscovery)Node).CcNode.Services.IoCcRecord.Endpoints[IoCcService.Keys.peering].Port,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
            };

            using var poolMem = MemoryPool<byte>.Shared.Rent(ping.CalculateSize() + 1);
            MemoryMarshal.TryGetArray<byte>(poolMem.Memory, out var heapMem);
            var heapStream = new MemoryStream(heapMem.Array);
            heapStream.WriteByte((byte)IoCcPeerMessage.MessageTypes.Ping);
            ping.WriteTo(heapStream);

            var responsePacket = new Packet
            {
                Data = ByteString.CopyFrom(heapMem.Array, 0, (int)heapStream.Position),
                PublicKey = ByteString.CopyFrom(CcId.PublicKey)
            };

            responsePacket.Signature =
                ByteString.CopyFrom(CcId.Sign(responsePacket.Data.ToByteArray(), 0, responsePacket.Data.Length));

            var pingMsgRaw = responsePacket.ToByteArray();

            var sent = await ((IoUdpClient<IoCcPeerMessage>)Producer).Socket.SendAsync(pingMsgRaw, 0,
                pingMsgRaw.Length, dest.IpEndPoint);
            _logger.Debug($"{nameof(Ping)}: Sent {sent} bytes to {dest.IpEndPoint}");
        }
    }
}
