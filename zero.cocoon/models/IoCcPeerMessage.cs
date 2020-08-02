using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Runtime.InteropServices;
using System.Security.Cryptography;

using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using Proto;

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
                                        var bytesRead = rx.Result;
                                        BytesRead = bytesRead;
                                        ProducerUserData = ((IoSocket)ioSocket).ExtraData();

                                        //Set how many datums we have available to process
                                        DatumCount = BytesLeftToProcess / DatumSize;
                                        DatumFragmentLength = BytesLeftToProcess % DatumSize;

                                        //Mark this job so that it does not go back into the heap until the remaining fragment has been picked up
                                        StillHasUnprocessedFragments = DatumFragmentLength > 0;

                                        ProcessState = State.Produced;

                                        _logger.Trace($"{TraceDescription} RX=> read=`{bytesRead}', ready=`{BytesLeftToProcess}', datumcount=`{DatumCount}', datumsize=`{DatumSize}', fragment=`{DatumFragmentLength}', buffer = `{BytesLeftToProcess}/{BufferSize + DatumProvisionLength}', buf = `{(int)(BytesLeftToProcess / (double)(BufferSize + DatumProvisionLength) * 100)}%'");

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

            
            try
            {
                for (var i = 0; i <= DatumCount; i++)
                {

                    //var packet = Serializer.Deserialize<Packet>(((byte[]) (Array) Buffer).AsSpan().Slice(BufferOffset, BytesLeftToProcess));
                    var packet = Packet.Parser.ParseFrom(((byte[]) (Array) Buffer).AsSpan().Slice(BufferOffset, BytesLeftToProcess).ToArray());

                    if (packet.Data != null)
                    {
                        var messageType = (MessageTypes) packet.Data[0];
                        _logger.Debug(
                            $"Got peering message type `({messageType}){Enum.GetName(typeof(MessageTypes), messageType)}, bytesread = {BytesRead}, from {Producer}");



                        switch (messageType)
                        {
                            case MessageTypes.Ping:
                                //var ping = Serializer.Deserialize<Ping>(packet.Data.Span.Slice(1,packet.Data.Length - 1));
                                var ping = Ping.Parser.ParseFrom(packet.Data.Span.Slice(1, packet.Data.Length - 1).ToArray());

                                if (ping != null)
                                {
                                    _logger.Debug($"PING: {ping.SrcAddr}:{ping.SrcPort} - {ping.DstAddr} networkId = {ping.NetworkId}, time = {DateTimeOffset.FromUnixTimeSeconds(ping.Timestamp)}, version = {ping.Version}, udp_source = {ProducerUserData}");

                                    var pong = new Pong
                                    {
                                        ReqHash = ByteString.CopyFrom(SHA256.ComputeHash(packet.Data.ToByteArray())),
                                        //DstAddr = $"{ping.DstAddr}:{ping.SrcPort}",
                                        DstAddr = $"{ping.DstAddr}",
                                        Services = new ServiceMap
                                        {
                                            Map = { {"peering", new NetworkAddress{Network = ping.DstAddr, Port = 14627}},
                                                { "gossip", new NetworkAddress { Network = ping.DstAddr, Port = 14666 } },
                                                { "fpc", new NetworkAddress { Network = ping.DstAddr, Port = 10895 } }
                                            }
                                        }
                                    };

                                    

                                    var sendBuffer = new byte[508];
                                    var sendBuffer2 = new byte[508];
                                    var pongStream = new MemoryStream(sendBuffer);
                                    var packetStream = new MemoryStream(sendBuffer2);

                                    pongStream.WriteByte((byte) MessageTypes.Pong);
                                    pong.WriteTo(pongStream);
                                    
                                    //Serializer.Serialize(pongStream, pong);

                                    //pongStream.Flush();
                                    var bufferSize = pongStream.Position;
                                    //pongStream.Seek(0, SeekOrigin.Begin);

                                    var responsePacket = new Packet
                                    {
                                        Data = ByteString.CopyFrom(sendBuffer, 0, (int)bufferSize),
                                        PublicKey = ByteString.CopyFrom(Id.PublicKey)
                                    };

                                    responsePacket.Signature = ByteString.CopyFrom(Id.Sign(responsePacket.Data.ToByteArray(), 0, responsePacket.Data.Length));

                                    //Serializer.Serialize(packetStream, responsePacket);
                                    //Serializer.SerializeWithLengthPrefix(packetStream, packet, PrefixStyle.Fixed32);
                                    var data = responsePacket.ToByteArray();

                                    //packetStream.Flush();
                                    //bufferSize = packetStream.Position;

                                    var sent = await ((IoUdpClient<IoCcPeerMessage<TKey>>) Producer).Socket.SendAsync(data, 0, (int)data.Length, ProducerUserData);
                                    _logger.Debug($"PONG: Sent {sent} bytes");
                                }

                                break;

                            case MessageTypes.Pong:
                                break;

                            case MessageTypes.DiscoveryRequest:

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
                _logger.Error(e,"Unmarshal Packet failed!");
            }
            finally
            {
                //BufferOffset += Math.Min(BytesLeftToProcess, DatumSize);
            }

            return ProcessState = State.Consumed;
        }
    }
}
