using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
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
    class IoCcNeighbor<TKey> : IoNeighbor<IoCcPeerMessage<TKey>>
    {
        public IoCcNeighbor(IoNode<IoCcPeerMessage<TKey>> node, IoNetClient<IoCcPeerMessage<TKey>> ioNetClient) : base(node, ioNetClient, userData => new IoCcPeerMessage<TKey>("peer rx", $"{ioNetClient.AddressString}", ioNetClient))
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        public const int TcpReadAhead = 50;

        public IoCcIdentity Identity { get; protected set; } = IoCcIdentity.Generate();

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
        }

        /// <summary>
        /// Processes protocol messages
        /// </summary>
        /// <param name="consumer">The consumer that need processing</param>
        /// <param name="msgArbiter">The arbiter</param>
        /// <param name="processCallback">The process callback</param>
        /// <returns></returns>
        private async Task ProcessMsgBatchAsync(IoConsumable<IoCcProtocolMessage<TKey>> consumer,
            IoForward<IoCcProtocolMessage<TKey>> msgArbiter,
            Func<Tuple<IMessage, object>, IoForward<IoCcProtocolMessage<TKey>>, Task> processCallback)
        {
            if (consumer == null)
                return;

            var stopwatch = Stopwatch.StartNew();

            try
            {
                var protocolMsgs = ((IoCcProtocolMessage<TKey>)consumer).Messages;

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

                consumer.ProcessState = IoProducible<IoCcProtocolMessage<TKey>>.State.Consumed;

                stopwatch.Stop();
                _logger.Trace($"{consumer.TraceDescription} Processed `{protocolMsgs.Count}' consumer: t = `{stopwatch.ElapsedMilliseconds:D}', `{protocolMsgs.Count * 1000 / (stopwatch.ElapsedMilliseconds + 1):D} t/s'");
            }
            finally
            {
                ((IoCcProtocolMessage<TKey>)consumer).Messages = null;
            }
        }

        /// <summary>
        /// Persists consumer seen from this neighbor
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="dataSource">An interface to the data source</param>
        /// <returns></returns>
        private async Task ProcessProtoMsgAsync()
        {
            var transactionArbiter = Producer.GetDownstreamArbiter<IoCcProtocolMessage<TKey>>(nameof(IoCcNeighbor<IoCcProtocolMessage<TKey>>));

            _logger.Debug($"Starting persistence for `{Description}'");
            while (!Spinners.IsCancellationRequested)
            {
                if (transactionArbiter == null)
                {
                    _logger.Warn($"Waiting for `{Description}' stream to spin up...");
                    transactionArbiter = Producer.GetDownstreamArbiter<IoCcProtocolMessage<TKey>>(nameof(IoCcNeighbor<IoCcProtocolMessage<TKey>>));
                    await Task.Delay(2000);//TODO config
                    continue;
                }

                await transactionArbiter.ConsumeAsync(async batch =>
                {
                    try
                    {
                        await ProcessMsgBatchAsync(batch, transactionArbiter,
                        async (message, forward) =>
                        {
                            //await LoadTransactionAsync(ping, dataSource, batch, msgArbiter);
#pragma warning disable 4014
                            //Process milestone consumer
                            try
                            {
                                switch (message.Item1.GetType().Name)
                                {
                                    case nameof(Ping):
                                        ProcessMsgAsync((Ping)message.Item1, message.Item2);
                                        break;
                                    case nameof(Pong):
                                        break;
                                    case nameof(DiscoveryRequest):
                                        ProcessMsgAsync((DiscoveryRequest)message.Item1, message.Item2);
                                        break;
                                    case nameof(DiscoveryResponse):
                                        break;
                                }
                            }
                            catch (Exception e)
                            {
                                _logger.Error(e, "Unable to process protocol message");
                            }
#pragma warning restore 4014
                        }).ConfigureAwait(false);
                    }
                    finally
                    {
                        if (batch != null && batch.ProcessState != IoProducible<IoCcProtocolMessage<TKey>>.State.Consumed)
                            batch.ProcessState = IoProducible<IoCcProtocolMessage<TKey>>.State.ConsumeErr;
                    }
                }).ConfigureAwait(false);

                if (!transactionArbiter.Producer.IsOperational)
                    break;
            }

            _logger.Debug($"Shutting down persistence for `{Description}'");
        }

        static IoCcIdentity CcId = IoCcIdentity.Generate(); //TODO 

        private async Task ProcessMsgAsync(DiscoveryRequest response, object extraData)
        {
            var hashStream = new MemoryStream(new byte[response.CalculateSize() + 1]);
            hashStream.WriteByte((byte)IoCcPeerMessage<TKey>.MessageTypes.DiscoveryRequest);
            response.WriteTo(hashStream);
            hashStream.Seek(0, SeekOrigin.Begin);

            var discoveryResponse = new DiscoveryResponse
            {
                ReqHash = ByteString.CopyFrom(SHA256.Create().ComputeHash(hashStream)),
                Peers = { }
            };

            using var poolMem = MemoryPool<byte>.Shared.Rent(discoveryResponse.CalculateSize() + 1);
            MemoryMarshal.TryGetArray<byte>(poolMem.Memory, out var heapMem);
            var heapStream = new MemoryStream(heapMem.Array);
            heapStream.WriteByte((byte)IoCcPeerMessage<TKey>.MessageTypes.DiscoveryResponse);
            discoveryResponse.WriteTo(heapStream);

            var responsePacket = new Packet
            {
                Data = ByteString.CopyFrom(heapMem.Array, 0, (int)heapStream.Position),
                PublicKey = ByteString.CopyFrom(CcId.PublicKey)
            };

            responsePacket.Signature =
                ByteString.CopyFrom(CcId.Sign(responsePacket.Data.ToByteArray(), 0, responsePacket.Data.Length));

            var discRespMsgRaw = responsePacket.ToByteArray();

            var sent = await ((IoUdpClient<IoCcPeerMessage<TKey>>)Producer).Socket.SendAsync(discRespMsgRaw, 0,
                discRespMsgRaw.Length, extraData).ConfigureAwait(false);
            _logger.Debug($"{nameof(DiscoveryResponse)}: Sent {sent} bytes ({extraData})");
        }

        private async Task ProcessMsgAsync(Ping ping, object extraData)
        {
            await using var hashStream = new MemoryStream(new byte[ping.CalculateSize() + 1]);
            hashStream.WriteByte((byte)IoCcPeerMessage<TKey>.MessageTypes.Ping);
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
                        {"gossip", new NetworkAddress {Network = ping.DstAddr, Port = 14666}},
                        {"fpc", new NetworkAddress {Network = ping.DstAddr, Port = 10895}}
                    }
                }
            };

            using var poolMem = MemoryPool<byte>.Shared.Rent(pong.CalculateSize() + 1);
            MemoryMarshal.TryGetArray<byte>(poolMem.Memory, out var heapMem);
            var heapStream = new MemoryStream(heapMem.Array);
            heapStream.WriteByte((byte)IoCcPeerMessage<TKey>.MessageTypes.Pong);
            pong.WriteTo(heapStream);

            var responsePacket = new Packet
            {
                Data = ByteString.CopyFrom(heapMem.Array, 0, (int)heapStream.Position),
                PublicKey = ByteString.CopyFrom(CcId.PublicKey)
            };

            responsePacket.Signature =
                ByteString.CopyFrom(CcId.Sign(responsePacket.Data.ToByteArray(), 0, responsePacket.Data.Length));

            var pongMsgRaw = responsePacket.ToByteArray();

            var sent = await ((IoUdpClient<IoCcPeerMessage<TKey>>)Producer).Socket.SendAsync(pongMsgRaw, 0,
                pongMsgRaw.Length, extraData);
            _logger.Debug($"PONG: Sent {sent} bytes");
        }
    }
}
