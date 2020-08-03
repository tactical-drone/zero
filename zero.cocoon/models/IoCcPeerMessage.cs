using System;
using System.Buffers;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;

using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding.Binders;
using NLog;
using Proto;
using RestSharp.Extensions;
using zero.cocoon.identity;
using zero.core.conf;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using Ping = Proto.Ping;


namespace zero.cocoon.models
{
    public class IoCcPeerMessage<TKey> : IoMessage<IoCcPeerMessage<TKey>>
    {
        public IoCcPeerMessage(string jobDescription, string workDescription, IoProducer<IoCcPeerMessage<TKey>> producer) : base(jobDescription, workDescription, producer)
        {
            _logger = LogManager.GetCurrentClassLogger();

            DatumSize = 508;

            //Init buffers
            BufferSize = DatumSize * parm_datums_per_buffer;
            DatumProvisionLengthMax = DatumSize - 1;
            DatumProvisionLength = DatumProvisionLengthMax;
            Buffer = new sbyte[BufferSize + DatumProvisionLengthMax];
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Used to control how long we wait for the producer before we report it
        /// </summary>
        private readonly Stopwatch _producerStopwatch = new Stopwatch();

        /// <summary>
        /// The time a consumer will wait for a producer to release it before aborting in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_producer_wait_for_consumer_timeout = 5000; //TODO make this adapting    


        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_datums_per_buffer = 250;


        /// <summary>
        /// The number of bytes left to process in this buffer
        /// </summary>
        public int BytesLeftToProcess => BytesRead - (BufferOffset - DatumProvisionLengthMax);

        /// <summary>
        /// Userdata in the producer
        /// </summary>
        protected volatile object ProducerUserData;

        public enum MessageTypes
        {
            Ping = 10,
            Pong = 11,
            DiscoveryRequest = 12,
            DiscoveryResponse = 13,
            PeeringRequest = 20,
            PeeringResponse = 21,
            PeeringDrop = 22
        }

        public override async Task<State> ProduceAsync()
        {
            try
            {
                var sourceTaskSuccess = await Producer.ProduceAsync(async ioSocket =>
                {
                    //----------------------------------------------------------------------------
                    // BARRIER
                    // We are only allowed to run ahead of the consumer by some configurable
                    // amount of steps. Instead of say just filling up memory buffers.
                    // This allows us some kind of (anti DOS?) congestion control
                    //----------------------------------------------------------------------------
                    _producerStopwatch.Restart();
                    if (!await Producer.ProducerBarrier.WaitAsync(parm_producer_wait_for_consumer_timeout, Producer.Spinners.Token))
                    {
                        if (!Producer.Spinners.IsCancellationRequested)
                        {
                            ProcessState = State.ProduceTo;
                            _producerStopwatch.Stop();
                            _logger.Warn($"{TraceDescription} `{ProductionDescription}' timed out waiting for CONSUMER to release, Waited = `{_producerStopwatch.ElapsedMilliseconds}ms', Willing = `{parm_producer_wait_for_consumer_timeout}ms', " +
                                         $"CB = `{Producer.ConsumerBarrier.CurrentCount}'");

                            //TODO finish when config is fixed
                            //LocalConfigBus.AddOrUpdate(nameof(parm_consumer_wait_for_producer_timeout), a=>0, 
                            //    (k,v) => Interlocked.Read(ref Source.ServiceTimes[(int) State.Consumed]) /
                            //         (Interlocked.Read(ref Source.Counters[(int) State.Consumed]) * 2 + 1));                                                                    
                        }
                        else
                            ProcessState = State.ProdCancel;
                        return true;
                    }

                    if (Producer.Spinners.IsCancellationRequested)
                    {
                        ProcessState = State.ProdCancel;
                        return false;
                    }

                    //Async read the message from the message stream
                    if (Producer.IsOperational)
                    {
                        await ((IoSocket)ioSocket).ReadAsync((byte[])(Array)Buffer, BufferOffset, BufferSize).ContinueWith(
                            rx =>
                            {
                                switch (rx.Status)
                                {
                                    //Canceled
                                    case TaskStatus.Canceled:
                                    case TaskStatus.Faulted:
                                        ProcessState = rx.Status == TaskStatus.Canceled ? State.ProdCancel : State.ProduceErr;
                                        Producer.Spinners.Cancel();
                                        Producer.Close();
                                        _logger.Error(rx.Exception?.InnerException, $"{TraceDescription} ReadAsync from stream `{ProductionDescription}' returned with errors:");
                                        break;
                                    //Success
                                    case TaskStatus.RanToCompletion:
                                        BytesRead = rx.Result;

                                        //UDP signals source ip
                                        ProducerUserData = ((IoSocket)ioSocket).ExtraData();

                                        //Set how many datums we have available to process
                                        DatumCount = BytesLeftToProcess / DatumSize;
                                        DatumFragmentLength = BytesLeftToProcess % DatumSize;

                                        //Mark this job so that it does not go back into the heap until the remaining fragment has been picked up
                                        StillHasUnprocessedFragments = DatumFragmentLength > 0;

                                        ProcessState = State.Produced;

                                        _logger.Trace($"{TraceDescription} RX=> read=`{BytesRead}', ready=`{BytesLeftToProcess}', datumcount=`{DatumCount}', datumsize=`{DatumSize}', fragment=`{DatumFragmentLength}', buffer = `{BytesLeftToProcess}/{BufferSize + DatumProvisionLength}', buf = `{(int)(BytesLeftToProcess / (double)(BufferSize + DatumProvisionLength) * 100)}%'");

                                        break;
                                    default:
                                        ProcessState = State.ProduceErr;
                                        throw new InvalidAsynchronousStateException($"Job =`{ProductionDescription}', State={rx.Status}");
                                }
                            }, Producer.Spinners.Token);
                    }
                    else
                    {
                        Producer.Close();
                    }

                    if (Producer.Spinners.IsCancellationRequested)
                    {
                        ProcessState = State.Cancelled;
                        return false;
                    }
                    return true;
                });
            }
            catch (Exception e)
            {
                _logger.Warn(e, $"{TraceDescription} Producing job `{ProductionDescription}' returned with errors:");
            }
            finally
            {
                if (ProcessState == State.Producing)
                {
                    // Set the state to ProduceErr so that the consumer knows to abort consumption
                    ProcessState = State.ProduceErr;
                }
            }
            return ProcessState;
        }

        private void TransferPreviousBits()
        {
            if (Previous?.StillHasUnprocessedFragments ?? false)
            {
                var previousJobFragment = (IoMessage<IoCcPeerMessage<TKey>>)Previous;
                try
                {
                    var bytesToTransfer = previousJobFragment.DatumFragmentLength;
                    BufferOffset -= bytesToTransfer;
                    DatumProvisionLength -= bytesToTransfer;
                    DatumCount = BytesLeftToProcess / DatumSize;
                    DatumFragmentLength = BytesLeftToProcess % DatumSize;
                    StillHasUnprocessedFragments = DatumFragmentLength > 0;

                    Array.Copy(previousJobFragment.Buffer, previousJobFragment.BufferOffset, Buffer, BufferOffset, bytesToTransfer);
                }
                catch (Exception e) // we de-synced 
                {
                    _logger.Warn(e, $"{TraceDescription} We desynced!:");

                    Producer.Synced = false;
                    DatumCount = 0;
                    BytesRead = 0;
                    ProcessState = State.Consumed;
                    DatumFragmentLength = 0;
                    StillHasUnprocessedFragments = false;
                }
            }

        }

        private static Dictionary<int, Type> MsgTypes = new Dictionary<int, Type> { { 10, typeof(Ping) } };
        private static SHA256 SHA256 = SHA256.Create();

        static IoCcIdentity Id = IoCcIdentity.Generate();

        public override async Task<State> ConsumeAsync()
        {
            //TransferPreviousBits();

            var stream = ByteStream;
            try
            {
                var verified = false;
                for (var i = 0; i <= DatumCount; i++)
                {
                    var packet = Packet.Parser.ParseFrom(stream);

                    if (packet.Data != null)
                    {
                        var packetMsgRaw = packet.Data.ToByteArray(); //TODO remove copy

                        if (packet.Signature != null)
                        {
                            verified = Id.Verify(packetMsgRaw, 0, packetMsgRaw.Length, packet.PublicKey.ToByteArray(), 0, packet.Signature.ToByteArray(), 0);
                        }

                        var messageType = (MessageTypes)packet.Data[0];
                        _logger.Debug($"Got {(verified?"signed":"un-signed")} peering message type - {messageType}, bytesread = {BytesRead}, from {Producer}");

                        switch (messageType)
                        {
                            case MessageTypes.Ping:
                                await ProcessPingMsg(packet, packetMsgRaw);
                                break;

                            case MessageTypes.Pong:

                                break;

                            case MessageTypes.DiscoveryRequest:
                                await ProcessDiscoveryRequest(packet, packetMsgRaw);
                                break;
                            case MessageTypes.DiscoveryResponse:
                                break;
                            case MessageTypes.PeeringRequest:
                                break;
                            case MessageTypes.PeeringResponse:
                                break;
                            case MessageTypes.PeeringDrop:
                                break;
                            default:
                                _logger.Debug($"Unknown auto peer msg type = {Buffer[BufferOffset - 1]}");
                                break;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                _logger.Error(e, "Unmarshal Packet failed!");
            }
            finally
            {
                //BufferOffset += Math.Min(BytesLeftToProcess, DatumSize);
            }

            return ProcessState = State.Consumed;
        }

        private async Task ProcessDiscoveryRequest(Packet packet, byte[] packetMsgRaw)
        {
            try
            {
                var requestRaw = packet.Data.Span.Slice(1, packet.Data.Length - 1).ToArray();
                var request = DiscoveryRequest.Parser.ParseFrom(requestRaw);

                if (request != null)
                {
                    _logger.Debug($"{nameof(DiscoveryRequest)}: {DateTimeOffset.FromUnixTimeSeconds(request.Timestamp)}");

                    var response = new DiscoveryResponse
                    {
                        ReqHash = ByteString.CopyFrom(SHA256.ComputeHash(packetMsgRaw)),
                        Peers = { }
                    };

                    using var poolMem = MemoryPool<byte>.Shared.Rent(response.CalculateSize() + 1);
                    MemoryMarshal.TryGetArray<byte>(poolMem.Memory, out var heapMem);
                    var heapStream = new MemoryStream(heapMem.Array);
                    heapStream.WriteByte((byte)MessageTypes.DiscoveryResponse);
                    response.WriteTo(heapStream);

                    var responsePacket = new Packet
                    {
                        Data = ByteString.CopyFrom(heapMem.Array, 0, (int)heapStream.Position),
                        PublicKey = ByteString.CopyFrom(Id.PublicKey)
                    };

                    responsePacket.Signature =
                        ByteString.CopyFrom(Id.Sign(responsePacket.Data.ToByteArray(), 0, responsePacket.Data.Length));

                    var pongMsgRaw = responsePacket.ToByteArray();

                    var sent = await ((IoUdpClient<IoCcPeerMessage<TKey>>)Producer).Socket.SendAsync(pongMsgRaw, 0,
                        pongMsgRaw.Length, ProducerUserData);
                    _logger.Debug($"{nameof(DiscoveryResponse)}: Sent {sent} bytes");
                }
            }
            catch (Exception e)
            {
                _logger.Error(e, "Unable to parse discovery request message");
            }
        }

        private async Task ProcessPingMsg(Packet packet, byte[] packetMsgRaw)
        {
            try
            {
                var pingMsgRaw = packet.Data.Span.Slice(1, packet.Data.Length - 1).ToArray();
                var ping = Ping.Parser.ParseFrom(pingMsgRaw);

                if (ping != null)
                {
                    _logger.Debug(
                        $"{nameof(Ping)}: {ping.SrcAddr}:{ping.SrcPort} - {ping.DstAddr} networkId = {ping.NetworkId}, time = {DateTimeOffset.FromUnixTimeSeconds(ping.Timestamp)}, version = {ping.Version}, udp_source = {ProducerUserData}");

                    var pong = new Pong
                    {
                        ReqHash = ByteString.CopyFrom(SHA256.ComputeHash(packetMsgRaw)),
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
                    heapStream.WriteByte((byte)MessageTypes.Pong);
                    pong.WriteTo(heapStream);

                    var responsePacket = new Packet
                    {
                        Data = ByteString.CopyFrom(heapMem.Array, 0, (int)heapStream.Position),
                        PublicKey = ByteString.CopyFrom(Id.PublicKey)
                    };

                    responsePacket.Signature =
                        ByteString.CopyFrom(Id.Sign(responsePacket.Data.ToByteArray(), 0, responsePacket.Data.Length));

                    var pongMsgRaw = responsePacket.ToByteArray();

                    var sent = await ((IoUdpClient<IoCcPeerMessage<TKey>>)Producer).Socket.SendAsync(pongMsgRaw, 0,
                        pongMsgRaw.Length, ProducerUserData);
                    _logger.Debug($"PONG: Sent {sent} bytes");
                }
            }
            catch (Exception e)
            {
                _logger.Error(e, "Unable to parse peering message");
            }
        }
    }
}
