using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using Proto;
using zero.cocoon.identity;
using zero.cocoon.models;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using Logger = NLog.Logger;

namespace zero.cocoon.autopeer
{
    class IoCcNeighbor : IoNeighbor<IoCcPeerMessage>
    {
        public IoCcNeighbor(IoNode<IoCcPeerMessage> node, IoNetClient<IoCcPeerMessage> ioNetClient, IoCcIdentity identity = null) : base(node, ioNetClient, userData => new IoCcPeerMessage("peer rx", $"{ioNetClient.AddressString}", ioNetClient))
        {
            _logger = LogManager.GetCurrentClassLogger();
            Identity = identity ?? IoCcIdentity.Generate();
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        public const int TcpReadAhead = 50;

        public IoCcIdentity Identity { get; protected set; } = IoCcIdentity.Generate();

        public override string Id => $"{Identity.IdString()}|{Identity.PkString()}";

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
            if(_protocolChannel == null)
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
                                        ProcessPeerReqAsync((PeeringRequest) message.Item1, message.Item2, message.Item3);
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

        private async Task ProcessPeerReqAsync(PeeringRequest request, object extraData, Packet packet)
        {
            var hashStream = new MemoryStream(new byte[request.CalculateSize() + 1]);
            hashStream.WriteByte((byte)IoCcPeerMessage.MessageTypes.DiscoveryRequest);
            request.WriteTo(hashStream);
            hashStream.Seek(0, SeekOrigin.Begin);

            //TODO: normalize
            var id = $"{Base58Check.Base58CheckEncoding.EncodePlain(packet.PublicKey.Span.Slice(0,8).ToArray())}|{IoCcIdentity.FromPK(packet.PublicKey.Span)}@udp://{extraData}";

            var peeringResponse = new PeeringResponse
            {
                ReqHash = ByteString.CopyFrom(SHA256.Create().ComputeHash(hashStream)),
                Status = Node.Neighbors.ContainsKey(id)
            };

            using var poolMem = MemoryPool<byte>.Shared.Rent(peeringResponse.CalculateSize() + 1);
            MemoryMarshal.TryGetArray<byte>(poolMem.Memory, out var heapMem);
            var heapStream = new MemoryStream(heapMem.Array);
            heapStream.WriteByte((byte)IoCcPeerMessage.MessageTypes.DiscoveryResponse);
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
                discRespMsgRaw.Length, extraData).ConfigureAwait(false);
            _logger.Debug($"{nameof(DiscoveryResponse)}: Sent {sent} bytes ({extraData})");
        }

        static IoCcIdentity CcId = IoCcIdentity.Generate(); //TODO 
        private IoChannel<IoCcProtocolMessage> _protocolChannel;

        private async Task ProcessMsgAsync(DiscoveryRequest request, object extraData)
        {
            var hashStream = new MemoryStream(new byte[request.CalculateSize() + 1]);
            hashStream.WriteByte((byte)IoCcPeerMessage.MessageTypes.DiscoveryRequest);
            request.WriteTo(hashStream);
            hashStream.Seek(0, SeekOrigin.Begin);

            var discoveryResponse = new DiscoveryResponse
            {
                ReqHash = ByteString.CopyFrom(SHA256.Create().ComputeHash(hashStream)),
                Peers = { new Peer{PublicKey = ByteString.CopyFrom(CcId.PublicKey), Ip = "udp://192.168.88.253:14627"} }
            };

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
                discRespMsgRaw.Length, extraData).ConfigureAwait(false);
            _logger.Debug($"{nameof(DiscoveryResponse)}: Sent {sent} bytes ({extraData})");
        }

        private async Task ProcessMsgAsync(Ping ping, object extraData, Packet packet)
        {
            await using var hashStream = new MemoryStream(new byte[ping.CalculateSize() + 1]);
            hashStream.WriteByte((byte)IoCcPeerMessage.MessageTypes.Ping);
            ping.WriteTo(hashStream);
            var pos = hashStream.Position;
            hashStream.Seek(0, SeekOrigin.Begin);

            var pong = new Pong
            {
                ReqHash = ByteString.CopyFrom(SHA256.Create().ComputeHash(hashStream)),
                DstAddr = $"{ping.DstAddr}:{ping.SrcPort}",
                Services = new ServiceMap
                {
                    Map =
                    {
                        {"peering", new NetworkAddress {Network = ping.DstAddr, Port = 14627}},
                        {"gossip", new NetworkAddress {Network = ping.DstAddr, Port = 14667}},
                        {"fpc", new NetworkAddress {Network = ping.DstAddr, Port = 10895}}
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

            var sent = await ((IoUdpClient<IoCcPeerMessage>)Producer).Socket.SendAsync(pongMsgRaw, 0,
                pongMsgRaw.Length, extraData);

            _logger.Debug($"{nameof(Pong)}({GetHashCode()}): Sent {sent} bytes to {extraData}");

            var pk = Base58Check.Base58CheckEncoding.EncodePlain(packet.PublicKey.ToByteArray());
            var identity = IoCcIdentity.FromPK(pk);

            //TODO Optimize
            if (!Node.Neighbors.ContainsKey($"{Base58Check.Base58CheckEncoding.EncodePlain(identity.Id.AsSpan().Slice(0,8).ToArray())}|{Base58Check.Base58CheckEncoding.EncodePlain(identity.PublicKey)}"))
            {
                await SendPingMsgAsync(IoNodeAddress.CreateFromEndpoint("udp", (EndPoint)extraData));
            }
        }

        private async Task ProcessMsgAsync(Pong pong, object extraData, Packet packet)
        {
            var pk = Base58Check.Base58CheckEncoding.EncodePlain(packet.PublicKey.ToByteArray());

            if (!Node.Neighbors.ContainsKey($"{Base58Check.Base58CheckEncoding.EncodePlain(packet.PublicKey.Span.Slice(0, 8).ToArray())}|{Base58Check.Base58CheckEncoding.EncodePlain(packet.PublicKey.ToByteArray())}"))
            {
                var newNeighbor = (IoCcNeighbor)await Node.SpawnConnectionAsync(IoNodeAddress.CreateFromEndpoint("udp", (EndPoint)extraData), IoCcIdentity.FromPK(pk));
                if (newNeighbor != null)
                {
                    newNeighbor._protocolChannel = _protocolChannel;
                    await newNeighbor.SendPingMsgAsync(IoNodeAddress.CreateFromEndpoint("udp", (EndPoint)extraData));
                    var task = newNeighbor.SpawnProcessingAsync(Spinners.Token, false);
                }
            }
        }

        private async Task SendPingMsgAsync(IoNodeAddress dest)
        {
            var ping = new Ping
            {
                DstAddr = dest.Url,
                NetworkId = 5,
                Version = 0,
                SrcAddr = ((IoUdpClient<IoCcPeerMessage>) Producer).Socket.LocalAddress,
                SrcPort = (uint) ((IoUdpClient<IoCcPeerMessage>) Producer).Socket.LocalPort,
                Timestamp = DateTimeOffset.Now.ToUnixTimeSeconds()
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
            _logger.Debug($"{nameof(Ping)}: Sent {sent} bytes to {dest}");
        }
    }
}
