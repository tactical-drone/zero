using System;
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

            DatumSize = 508;

            //Init buffers
            BufferSize = DatumSize * parm_datums_per_buffer;
            DatumProvisionLengthMax = DatumSize - 1;
            DatumProvisionLength = DatumProvisionLengthMax;
            Buffer = new sbyte[BufferSize + DatumProvisionLengthMax];

            IoCcProtocolBuffer protocol = new IoCcProtocolBuffer(parm_forward_queue_length);
            if (Source.ObjectStorage.TryAdd(nameof(IoCcProtocolBuffer), protocol))
            {
                Source.ZeroOnCascade(protocol);

                ProtocolChannel = Source.AttachProducer(nameof(IoCcNeighbor), false, protocol,
                    userData => new IoCcProtocolMessage(protocol, -1 /*We block to control congestion*/));

                ProtocolChannel.parm_consumer_wait_for_producer_timeout = -1; //We block and never report slow production
                ProtocolChannel.parm_producer_start_retry_time = 0;
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
        public int parm_forward_queue_length = 4;


        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_datums_per_buffer = 250;

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


#endif
        }

        /// <summary>
        /// 
        /// </summary>
        protected override void ZeroManaged()
        {
            base.ZeroManaged();
        }

        public override async Task<State> ProduceAsync()
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
                    if (!Zeroed() && !await Source.ProducerBarrier.WaitAsync(parm_producer_wait_for_consumer_timeout, AsyncTasks.Token))
                    {
                        if (!Zeroed())
                        {
                            ProcessState = State.ProduceTo;
                            _producerStopwatch.Stop();
                            _logger.Debug($"{TraceDescription} timed out waiting for PRODUCER, Waited = `{_producerStopwatch.ElapsedMilliseconds}ms', Willing = `{parm_producer_wait_for_consumer_timeout}ms', " +
                                         $"CB = `{Source.ConsumerBarrier.CurrentCount}'");

                            //TODO finish when config is fixed
                            //LocalConfigBus.AddOrUpdate(nameof(parm_consumer_wait_for_producer_timeout), a=>0, 
                            //    (k,v) => Interlocked.Read(ref Source.ServiceTimes[(int) State.Consumed]) /
                            //         (Interlocked.Read(ref Source.Counters[(int) State.Consumed]) * 2 + 1));                                                                    
                        }
                        else
                            ProcessState = State.ProdCancel;
                        return false;
                    }

                    if (Zeroed())
                    {
                        ProcessState = State.ProdCancel;
                        return false;
                    }

                    //Async read the message from the message stream
                    if (Source.IsOperational)
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
                                        Source.Zero(this);
                                        _logger.Error(rx.Exception?.InnerException, $"{TraceDescription} ReadAsync from stream returned with errors:");
                                        break;
                                    //Success
                                    case TaskStatus.RanToCompletion:

                                        //UDP signals source ip
                                        ProducerUserData = ((IoSocket)ioSocket).ExtraData();

                                        //Drop zero reads
                                        if (rx.GetAwaiter().GetResult() == 0)
                                        {
                                            BytesRead = 0;
                                            ProcessState = State.ProduceTo;
                                            return;
                                        }
                                        
                                        //rate limit
                                        _msgCount++;
                                        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                                        var delta = now - _msgRateCheckpoint;
                                        if (_msgCount > parm_ave_sec_ms && (double)_msgCount * 1000 / delta > parm_ave_sec_ms )
                                        {
                                            BytesRead = 0;
                                            ProcessState = State.ProduceTo;
                                            _logger.Fatal($"Dropping spam {_msgCount}");
                                            _msgCount -= 2;
                                            return;
                                        }

                                        //hist reset
                                        if (delta > parm_ave_msg_sec_hist * 1000)
                                        {
                                            _msgRateCheckpoint = now;
                                            _msgCount = 0;
                                        }

                                        BytesRead = rx.Result;

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
                                        throw new InvalidAsynchronousStateException($"Job =`{Description}', State={rx.Status}");
                                }
                            }, AsyncTasks.Token);
                    }
                    else
                    {
#pragma warning disable 4014
                        Source.Zero(this);
#pragma warning restore 4014
                    }

                    if (Zeroed())
                    {
                        ProcessState = State.Cancelled;
                        return false;
                    }
                    return true;
                });//don't .ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger.Warn(e, $"{TraceDescription} Producing job returned with errors:");
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
                var previousJobFragment = (IoMessage<IoCcPeerMessage>)Previous;
                try
                {
                    var bytesToTransfer = previousJobFragment.DatumFragmentLength;
                    BufferOffset -= bytesToTransfer;
                    DatumProvisionLength -= bytesToTransfer;
                    DatumCount = BytesLeftToProcess / DatumSize;
                    DatumFragmentLength = BytesLeftToProcess % DatumSize;
                    StillHasUnprocessedFragments = DatumFragmentLength > 0;

                    //TODO
                    Array.Copy(previousJobFragment.Buffer, previousJobFragment.BufferOffset, Buffer, BufferOffset, bytesToTransfer);
                }
                catch (Exception e) // we de-synced 
                {
                    _logger.Warn(e, $"{TraceDescription} We desynced!:");

                    Source.Synced = false;
                    DatumCount = 0;
                    BytesRead = 0;
                    ProcessState = State.Consumed;
                    DatumFragmentLength = 0;
                    StillHasUnprocessedFragments = false;
                }
            }

        }


        protected IoCcNode CcNode => ((IoCcNeighbor) IoZero).CcNode;

        //public static IoCcIdentity CcId = IoCcIdentity.Generate(true);
        public IoCcIdentity CcId => CcNode.CcId;

        public override async Task<State> ConsumeAsync()
        {
            //TransferPreviousBits();

            if (BytesRead == 0)
                return ProcessState = State.ConInvalid;

            var stream = ByteStream;
            try
            {
                var verified = false;
                _protocolMsgBatch = new List<Tuple<IMessage, object, Packet>>();
                for (var i = 0; i <= DatumCount; i++)
                {
                    var packet = Packet.Parser.ParseFrom(stream);


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
                        _logger.Trace($"{messageType??"Unknown"}[{(verified ? "signed" : "un-signed")}], s = {BytesRead}, source = `{(IPEndPoint)ProducerUserData}'");

                        //Don't process unsigned or unknown messages
                        if(!verified || messageType == null)
                            continue;

                        switch (messageType)
                        {
                            case nameof(MessageTypes.Ping):
                                ProcessRequest<Ping>(packet);
                                break;
                            case nameof(MessageTypes.Pong):
                                ProcessRequest<Pong>(packet);
                                break;
                            case nameof(MessageTypes.DiscoveryRequest):
                                ProcessRequest<DiscoveryRequest>(packet);
                                break;
                            case nameof(MessageTypes.DiscoveryResponse):
                                ProcessRequest<DiscoveryResponse>(packet);
                                break;
                            case nameof(MessageTypes.PeeringRequest):
                                ProcessRequest<PeeringRequest>(packet);
                                break;
                            case nameof(MessageTypes.PeeringResponse):
                                ProcessRequest<PeeringResponse>(packet);
                                break;
                            case nameof(MessageTypes.PeeringDrop):
                                ProcessRequest<PeeringDrop>(packet);
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

            if (_protocolMsgBatch.Count > 0)
            {
                await ForwardToNeighborAsync(_protocolMsgBatch);
            }

            return ProcessState = State.Consumed;
        }

        private void ProcessRequest<T>(Packet packet)
        where T : IMessage<T>, new()
        {
            try
            {
                var parser = new MessageParser<T>(() => new T());
                var request = parser.ParseFrom(packet.Data);

                if (request != null)
                {
                    //_logger.Debug($"[{Base58Check.Base58CheckEncoding.Encode(packet.PublicKey.ToByteArray())}]{typeof(T).Name}: Received {packet.Data.Length}" );

                    _protocolMsgBatch.Add(Tuple.Create((IMessage)request, ProducerUserData, packet));
                }
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unable to parse request type {typeof(T).Name} from {Base58Check.Base58CheckEncoding.Encode(packet.PublicKey.ToByteArray())}, size = {packet.Data.Length}");
            }
        }

        private async Task ForwardToNeighborAsync(List<Tuple<IMessage, object, Packet>> newInteropTransactions)
        {
            //cog the source
            await ProtocolChannel.Source.ProduceAsync(source =>
            {
                if (ProtocolChannel.IsArbitrating) //TODO: For now, We don't want to block when neighbors cant process transactions
                    ((IoCcProtocolBuffer)source).MessageQueue.TryAdd(newInteropTransactions);
                else
                    ((IoCcProtocolBuffer)source).MessageQueue.Add(newInteropTransactions);

                return Task.FromResult(true);
            }).ConfigureAwait(false);

            //forward transactions
            if (!await ProtocolChannel.ProduceAsync().ConfigureAwait(false))
            {
                _logger.Warn($"{TraceDescription} Failed to forward to `{ProtocolChannel.Source.Description}'");
            }
        }

        List<Tuple<IMessage, object, Packet>> _protocolMsgBatch = new List<Tuple<IMessage, object, Packet>>();
    }
}
