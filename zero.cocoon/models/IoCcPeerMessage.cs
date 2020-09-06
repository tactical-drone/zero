using System;
using System.Buffers;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.AspNetCore.SignalR.Protocol;
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
    public class IoCcPeerMessage : IoMessage<IoCcPeerMessage>
    {
        public IoCcPeerMessage(string loadDescription, string jobDescription, IoSource<IoCcPeerMessage> source) : base(
            loadDescription, jobDescription, source)
        {
            _logger = LogManager.GetCurrentClassLogger();

            _protocolMsgBatch = ArrayPool<Tuple<IMessage, object, Packet>>.Shared.Rent(parm_max_msg_batch_size);

            DatumSize = 508;

            //Init buffers
            BufferSize = DatumSize * parm_datums_per_buffer;
            DatumProvisionLengthMax = DatumSize - 1;
            //DatumProvisionLength = DatumProvisionLengthMax;
            Buffer = new sbyte[BufferSize + DatumProvisionLengthMax];

            if (!Source.ObjectStorage.ContainsKey(nameof(IoCcProtocolBuffer)))
            {
                var protocol = new IoCcProtocolBuffer(parm_forward_queue_length);
                if (Source.ObjectStorage.TryAdd(nameof(IoCcProtocolBuffer), protocol))
                {
                    Source.ZeroOnCascade(protocol);

                    ProtocolChannel = Source.AttachProducer(
                        nameof(IoCcNeighbor), 
                        false, 
                        protocol,
                        userData => new IoCcProtocolMessage(protocol, -1 /*We block to control congestion*/), 
                        1,2
                        );

                    ProtocolChannel.parm_consumer_wait_for_producer_timeout = -1; //We block and never report slow production
                    ProtocolChannel.parm_producer_start_retry_time = 0;
                }
            }
            else
            {
                ProtocolChannel = Source.AttachProducer<IoCcProtocolMessage>(nameof(IoCcNeighbor));
            }
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The decoded tangle transaction
        /// </summary>
        //private static IoCcProtocolBuffer _protocolBuffer;

        /// <summary>
        /// The transaction broadcaster
        /// </summary>
        public IoChannel<IoCcProtocolMessage> ProtocolChannel;

        /// <summary>
        /// Used to control how long we wait for the source before we report it
        /// </summary>
        private readonly Stopwatch _producerStopwatch = new Stopwatch();

        /// <summary>
        /// The time a consumer will wait for a source to release it before aborting in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_producer_wait_for_consumer_timeout = 5000; //TODO make this adapting    

        /// <summary>
        /// The amount of items that can be ready for production before blocking
        /// </summary>
        [IoParameter]
        public int parm_forward_queue_length = 1000;


        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_datums_per_buffer = 2;

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_ave_sec_ms = 100;

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_ave_msg_sec_hist = 10 * 2;

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_msg_batch_size = 10;

        /// <summary>
        /// 
        /// </summary>
        private int _msgCount = 0;

        /// <summary>
        /// 
        /// </summary>
        private long _msgRateCheckpoint = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        /// <summary>
        /// Userdata in the source
        /// </summary>
        protected volatile object ProducerUserData;

        public enum MessageTypes
        {
            Undefined = 0,
            Handshake = 1,
            Ping = 10,
            Pong = 11,
            DiscoveryRequest = 12,
            DiscoveryResponse = 13,
            PeeringRequest = 20,
            PeeringResponse = 21,
            PeeringDrop = 22
        }

        /// <summary>
        /// 
        /// </summary>
        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            ProducerUserData = null;
            ProtocolChannel = null;
            Buffer = null;
            _protocolMsgBatch = null;
#endif
        }

        /// <summary>
        /// 
        /// </summary>
        protected override void ZeroManaged()
        {
            if(_protocolMsgBatch != null)
                ArrayPool<Tuple<IMessage, object, Packet>>.Shared.Return(_protocolMsgBatch);
            base.ZeroManaged();
        }

        public override async Task<JobState> ProduceAsync()
        {
            try
            {
                var sourceTaskSuccess = await Source.ProduceAsync(async ioSocket =>
                {
                    //----------------------------------------------------------------------------
                    // BARRIER
                    // We are only allowed to run ahead of the consumer by some configurable
                    // amount of steps. Instead of say just filling up memory buffers.
                    // This allows us some kind of (anti DOS?) congestion control
                    //----------------------------------------------------------------------------
                    _producerStopwatch.Restart();
                    if (!Zeroed() && !await Source.ProducerBarrier.WaitAsync(parm_producer_wait_for_consumer_timeout, AsyncTasks.Token).ConfigureAwait(false))
                    {
                        if (!Zeroed())
                        {
                            State = JobState.ProduceTo;
                            _producerStopwatch.Stop();
                            _logger.Debug($"{TraceDescription} timed out waiting for PRODUCER, Waited = `{_producerStopwatch.ElapsedMilliseconds}ms', Willing = `{parm_producer_wait_for_consumer_timeout}ms', " +
                                         $"CB = `{Source.ConsumerBarrier.CurrentCount}'");

                            //TODO finish when config is fixed
                            //LocalConfigBus.AddOrUpdate(nameof(parm_consumer_wait_for_producer_timeout), a=>0, 
                            //    (k,v) => Interlocked.Read(ref Source.ServiceTimes[(int) JobState.Consumed]) /
                            //         (Interlocked.Read(ref Source.Counters[(int) JobState.Consumed]) * 2 + 1));                                                                    
                        }
                        else
                            State = JobState.ProdCancel;
                        return false;
                    }

                    if (Zeroed())
                    {
                        State = JobState.ProdCancel;
                        return false;
                    }

                    //Async read the message from the message stream
                    if (Source.IsOperational)
                    {
                        await ((IoSocket)ioSocket).ReadAsync((byte[])(Array)Buffer, BufferOffset, BufferSize).AsTask().ContinueWith(
                            rx =>
                            {
                                switch (rx.Status)
                                {
                                    //Canceled
                                    case TaskStatus.Canceled:
                                    case TaskStatus.Faulted:
                                        State = rx.Status == TaskStatus.Canceled ? JobState.ProdCancel : JobState.ProduceErr;
                                        Source.Zero(this);
                                        _logger.Error(rx.Exception?.InnerException, $"{TraceDescription} ReadAsync from stream returned with errors:");
                                        break;
                                    //Success
                                    case TaskStatus.RanToCompletion:

                                        //UDP signals source ip
                                        ProducerUserData = ((IoSocket)ioSocket).ExtraData();

                                        //Drop zero reads
                                        if (rx.Result == 0)
                                        {
                                            BytesRead = 0;
                                            State = JobState.ProduceTo;
                                            break;
                                        }

                                        //rate limit
                                        _msgCount++;
                                        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                                        var delta = now - _msgRateCheckpoint;
                                        if (_msgCount > parm_ave_sec_ms && (double)_msgCount * 1000 / delta > parm_ave_sec_ms)
                                        {
                                            BytesRead = 0;
                                            State = JobState.ProduceTo;
                                            _logger.Fatal($"Dropping spam {_msgCount}");
                                            _msgCount -= 2;
                                            break;
                                        }

                                        //hist reset
                                        if (delta > parm_ave_msg_sec_hist * 1000)
                                        {
                                            _msgRateCheckpoint = now;
                                            _msgCount = 0;
                                        }

                                        BytesRead = rx.Result;

                                        UpdateBufferMetaData();

                                        State = JobState.Produced;

                                        _logger.Trace($"RX=> {GetType().Name} ({Description}): read=`{BytesRead}', ready=`{BytesLeftToProcess}', datumcount=`{DatumCount}', datumsize=`{DatumSize}', fragment=`{DatumFragmentLength}', buffer = `{BytesLeftToProcess}/{BufferSize + DatumProvisionLengthMax}', buf = `{(int)(BytesLeftToProcess / (double)(BufferSize + DatumProvisionLengthMax) * 100)}%'");

                                        break;
                                    default:
                                        State = JobState.ProduceErr;
                                        throw new InvalidAsynchronousStateException($"Job =`{Description}', JobState={rx.Status}");
                                }
                            }, AsyncTasks.Token).ConfigureAwait(false);
                    }
                    else
                    {
                        _logger.Warn($"{GetType().Name}: Source {Source.Description} went non operational!");
                        State = JobState.Cancelled;
                        await Source.Zero(this).ConfigureAwait(false);
                    }

                    if (Zeroed())
                    {
                        State = JobState.Cancelled;
                        return false;
                    }
                    return true;
                }).ConfigureAwait(false);//don't .ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger.Warn(e, $"{TraceDescription} Producing job returned with errors:");
            }
            finally
            {
                if (State == JobState.Producing)
                {
                    // Set the state to ProduceErr so that the consumer knows to abort consumption
                    State = JobState.ProduceErr;
                }
            }
            return State;
        }

        /// <summary>
        /// Handle fragments
        /// </summary>
        private void TransferPreviousBits()
        {
            if (!(PreviousJob?.StillHasUnprocessedFragments ?? false)) return;

            var p = (IoMessage<IoCcPeerMessage>)PreviousJob;
            try
            {
                var bytesToTransfer = Math.Min(p.DatumFragmentLength, DatumProvisionLengthMax);
                Interlocked.Add(ref BufferOffset, -bytesToTransfer);
                Interlocked.Add(ref BytesRead, bytesToTransfer);
                
                UpdateBufferMetaData();

                Array.Copy(p.Buffer, p.BufferOffset + Math.Max(p.BytesLeftToProcess - DatumProvisionLengthMax, 0), Buffer, BufferOffset, bytesToTransfer);
            }
            catch (Exception e) // we de-synced 
            {
                _logger.Warn(e, $"{TraceDescription} We desynced!:");

                Source.Synced = false;
                DatumCount = 0;
                BytesRead = 0;
                State = JobState.ConInvalid;
                DatumFragmentLength = 0;
                StillHasUnprocessedFragments = false;
            }
        }

        /// <summary>
        /// CC Node
        /// </summary>
        protected IoCcNode CcNode => ((IoCcNeighbor)IoZero).CcNode;

        /// <summary>
        /// Cc Identity
        /// </summary>
        public IoCcIdentity CcId => CcNode.CcId;

        /// <summary>
        /// Message sink
        /// </summary>
        /// <returns>Processing state</returns>
        public override async Task<JobState> ConsumeAsync()
        {
            var stream = ByteStream;
            try
            {
                //TransferPreviousBits();

                if (BytesRead == 0 || Zeroed())
                    return State = JobState.ConInvalid;

                var verified = false;
                //_protocolMsgBatch = ArrayPool<Tuple<IMessage, object, Packet>>.Shared.Rent(1000);
                for (var i = 0; i <= DatumCount; i++)
                {
                    var read = stream.Position;
                    var packet = Packet.Parser.ParseFrom(stream);
                    Interlocked.Add(ref BufferOffset, (int) (stream.Position - read));

                    if (packet.Data != null && packet.Data.Length > 0)
                    {

                        var packetMsgRaw = packet.Data.ToByteArray(); //TODO remove copy

                        if (packet.Signature != null || packet.Signature?.Length != 0)
                        {
                            verified = CcId.Verify(packetMsgRaw, 0, packetMsgRaw.Length, packet.PublicKey.ToByteArray(), 0, packet.Signature.ToByteArray(), 0);
                        }

                        //var messageType = Enum.GetName(typeof(MessageTypes), packet.Data[0]);
                        var messageType = Enum.GetName(typeof(MessageTypes), packet.Type);
                        packet.Type = packet.Data[0];
                        _logger.Trace($"{messageType ?? "Unknown"}[{(verified ? "signed" : "un-signed")}], s = {BytesRead}, source = `{(IPEndPoint)ProducerUserData}'");

                        //Don't process unsigned or unknown messages
                        if (!verified || messageType == null)
                            continue;

                        switch (messageType)
                        {
                            case nameof(MessageTypes.Ping):
                                await ProcessRequest<Ping>(packet).ConfigureAwait(false);
                                break;
                            case nameof(MessageTypes.Pong):
                                await ProcessRequest<Pong>(packet).ConfigureAwait(false);
                                break;
                            case nameof(MessageTypes.DiscoveryRequest):
                                await ProcessRequest<DiscoveryRequest>(packet).ConfigureAwait(false);
                                break;
                            case nameof(MessageTypes.DiscoveryResponse):
                                await ProcessRequest<DiscoveryResponse>(packet).ConfigureAwait(false);
                                break;
                            case nameof(MessageTypes.PeeringRequest):
                                await ProcessRequest<PeeringRequest>(packet).ConfigureAwait(false);
                                break;
                            case nameof(MessageTypes.PeeringResponse):
                                await ProcessRequest<PeeringResponse>(packet).ConfigureAwait(false);
                                break;
                            case nameof(MessageTypes.PeeringDrop):
                                await ProcessRequest<PeeringDrop>(packet).ConfigureAwait(false);
                                break;
                            default:
                                _logger.Debug($"Unknown auto peer msg type = {packet.Type}");
                                break;
                        }

                        await ForwardToNeighborAsync().ConfigureAwait(false);
                    }
                }

                State = JobState.Consumed;
            }
            catch (Exception e)
            {
                _logger.Error(e, "Unmarshal Packet failed!");
            }
            finally
            {
                if (State == JobState.Consuming)
                    State = JobState.ConsumeErr;
                UpdateBufferMetaData();
            }

            //if (_protocolMsgBatch.Count > 0)
            //{
            //    await ForwardToNeighborAsync(_protocolMsgBatch);
            //}

            return State;
        }

        private int _protocolMsgBatchIndex = 0;

        private async Task ProcessRequest<T>(Packet packet)
        where T : IMessage<T>, new()
        {
            try
            {
                var parser = new MessageParser<T>(() => new T());
                var request = parser.ParseFrom(packet.Data);

                if (request != null)
                {
                    //_logger.Debug($"[{Base58Check.Base58CheckEncoding.Encode(packet.PublicKey.ToByteArray())}]{typeof(T).Name}: Received {packet.Data.Length}" );

                    if(_protocolMsgBatchIndex < parm_max_msg_batch_size)
                        _protocolMsgBatch[_protocolMsgBatchIndex++] = Tuple.Create((IMessage)request, ProducerUserData, packet);
                    else
                    {
                        await ForwardToNeighborAsync().ConfigureAwait(false);
                    }
                }
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unable to parse request type {typeof(T).Name} from {Base58Check.Base58CheckEncoding.Encode(packet.PublicKey.ToByteArray())}, size = {packet.Data.Length}");
            }
        }

        private async Task ForwardToNeighborAsync()
        {
            if(_protocolMsgBatchIndex == 0)
                return;

            if (_protocolMsgBatchIndex < parm_max_msg_batch_size )
                _protocolMsgBatch[_protocolMsgBatchIndex++] = null;

            //cog the source
            await ProtocolChannel.Source.ProduceAsync(source =>
            {
                if (ProtocolChannel.IsArbitrating) //TODO: For now, We don't want to block when neighbors cant process transactions
                    ((IoCcProtocolBuffer)source).MessageQueue.TryAdd(_protocolMsgBatch);
                else
                    ((IoCcProtocolBuffer)source).MessageQueue.Add(_protocolMsgBatch);

                _protocolMsgBatch = ArrayPool<Tuple<IMessage, object, Packet>>.Shared.Rent(parm_max_msg_batch_size);

                _protocolMsgBatchIndex = 0;

                return Task.FromResult(true);
            }).ConfigureAwait(false);

            //forward transactions
            if (!await ProtocolChannel.ProduceAsync().ConfigureAwait(false))
            {
                _logger.Warn($"{TraceDescription} Failed to forward to `{ProtocolChannel.Source.Description}'");
            }
        }

        private Tuple<IMessage, object, Packet>[] _protocolMsgBatch;
    }
}
