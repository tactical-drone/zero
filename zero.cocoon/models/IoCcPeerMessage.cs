using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using Proto;
using zero.cocoon.autopeer;
using zero.cocoon.identity;
using zero.cocoon.models.sources;
using zero.core.conf;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using Ping = Proto.Ping;


namespace zero.cocoon.models
{
    class IoCcPeerMessage<TKey> : IoMessage<IoCcPeerMessage<TKey>>
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

            //forward to peer
            if (!Producer.ObjectStorage.ContainsKey(nameof(_peerChannel)))
            {
                _peerChannel = new IoCcProtocol<TKey>($"{nameof(_peerChannel)}", parm_forward_queue_length);
                if (!Producer.ObjectStorage.TryAdd(nameof(_peerChannel), _peerChannel))
                {
                    _peerChannel = (IoCcProtocol<TKey>)Producer.ObjectStorage[nameof(_peerChannel)];
                }
            }

            PeerChannel = producer.GetDownstreamArbiter(nameof(IoCcNeighbor<IoCcProtocolMessage<TKey>>), _peerChannel, userData => new IoCcProtocolMessage<TKey>(_peerChannel, -1 /*We block to control congestion*/));
            PeerChannel.parm_consumer_wait_for_producer_timeout = -1; //We block and never report slow production
            PeerChannel.parm_producer_start_retry_time = 0;
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The decoded tangle transaction
        /// </summary>
        private static IoCcProtocol<TKey> _peerChannel;

        /// <summary>
        /// The transaction broadcaster
        /// </summary>
        public IoForward<IoCcProtocolMessage<TKey>> PeerChannel;

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
        /// The amount of items that can be ready for production before blocking
        /// </summary>
        [IoParameter]
        public int parm_forward_queue_length = 4;


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

        static IoCcIdentity CcId = IoCcIdentity.Generate();

        public override async Task<State> ConsumeAsync()
        {
            //TransferPreviousBits();
            
            var stream = ByteStream;
            try
            {
                var verified = false;
                _msgBatch = new List<Tuple<IMessage, object>>();
                for (var i = 0; i <= DatumCount; i++)
                {
                    var packet = Packet.Parser.ParseFrom(stream);


                    if (packet.Data != null)
                    {

                        var packetMsgRaw = packet.Data.ToByteArray(); //TODO remove copy

                        if (packet.Signature != null)
                        {
                            verified = CcId.Verify(packetMsgRaw, 0, packetMsgRaw.Length, packet.PublicKey.ToByteArray(), 0, packet.Signature.ToByteArray(), 0);
                        }

                        var messageType = (MessageTypes)packet.Data[0];
                        _logger.Debug($"Got {(verified ? "signed" : "un-signed")} peering message type - {messageType}, bytesread = {BytesRead}, from {Producer}");

                        switch (messageType)
                        {
                            case MessageTypes.Ping:
                                ProcessPingMsgAsync(packet, packetMsgRaw);
                                break;

                            case MessageTypes.Pong:

                                break;

                            case MessageTypes.DiscoveryRequest:
                                ProcessDiscoveryRequest(packet, packetMsgRaw);
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

            if (_msgBatch.Count > 0)
            {
                await ForwardToPeerAsync(_msgBatch);
            }

            return ProcessState = State.Consumed;
        }

        private async Task ForwardToPeerAsync(List<Tuple<IMessage,object>> newInteropTransactions)
        {
            //cog the source
            await _peerChannel.ProduceAsync(source =>
            {
                if (_peerChannel.Arbiter.IsArbitrating) //TODO: For now, We don't want to block when neighbors cant process transactions
                    ((IoCcProtocol<TKey>)source).TxQueue.Add(newInteropTransactions);
                else
                    ((IoCcProtocol<TKey>)source).TxQueue.TryAdd(newInteropTransactions);

                return Task.FromResult(true);
            }).ConfigureAwait(false);

            //forward transactions
            if (!await PeerChannel.ProduceAsync(Producer.Spinners.Token).ConfigureAwait(false))
            {
                _logger.Warn($"{TraceDescription} Failed to forward to `{PeerChannel.Producer.Description}'");
            }
        }

        private void ProcessDiscoveryRequest(Packet packet, byte[] packetMsgRaw)
        {
            try
            {
                var requestRaw = packet.Data.Span.Slice(1, packet.Data.Length - 1).ToArray();
                var request = DiscoveryRequest.Parser.ParseFrom(requestRaw);

                if (request != null)
                {
                    _logger.Debug($"{nameof(DiscoveryRequest)}: {DateTimeOffset.FromUnixTimeSeconds(request.Timestamp)}");

                    _msgBatch.Add(Tuple.Create((IMessage)request,ProducerUserData));
                }
            }
            catch (Exception e)
            {
                _logger.Error(e, "Unable to parse discovery request message");
            }
        }

        List<Tuple<IMessage, object>> _msgBatch = new List<Tuple<IMessage, object>>();

        private void ProcessPingMsgAsync(Packet packet, byte[] packetMsgRaw)
        {
            try
            {
                var pingMsgRaw = packet.Data.Span.Slice(1, packet.Data.Length - 1).ToArray();
                var ping = Ping.Parser.ParseFrom(pingMsgRaw);

                if (ping != null)
                {
                    _logger.Debug(
                        $"{nameof(Ping)}: {ping.SrcAddr}:{ping.SrcPort} - {ping.DstAddr} networkId = {ping.NetworkId}, time = {DateTimeOffset.FromUnixTimeSeconds(ping.Timestamp)}, version = {ping.Version}, udp_source = {ProducerUserData}");

                    _msgBatch.Add(Tuple.Create((IMessage)ping, ProducerUserData));
                }
            }
            catch (Exception e)
            {
                _logger.Error(e, "Unable to parse peering message");
            }
        }
    }
}
