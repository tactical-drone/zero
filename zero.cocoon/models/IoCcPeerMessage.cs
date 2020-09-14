using System;
using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Threading;
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
            Buffer = new sbyte[BufferSize + DatumProvisionLengthMax];
            ByteSegment = ByteBuffer;

            if (!Source.ObjectStorage.ContainsKey(nameof(IoCcProtocolBuffer)))
            {
                IoCcProtocolBuffer protocol = null;
                //var task = Task.Factory.StartNew(async () =>
                //{
                if (Source.ZeroEnsureAsync(() =>
                {
                    protocol = new IoCcProtocolBuffer(Source, parm_forward_queue_length, _arrayPool);
                    if (Source.ObjectStorage.TryAdd(nameof(IoCcProtocolBuffer), protocol))
                    {
                        return Task.FromResult(Source.ZeroOnCascade(protocol, true) != null);
                    }

                    return Task.FromResult(false);
                }).ConfigureAwait(false).GetAwaiter().GetResult())
                {

                    ProtocolChannel = Source.AttachProducer(
                        nameof(IoCcNeighbor),
                        false,
                        protocol,
                        userData => new IoCcProtocolMessage(protocol, -1 /*We block to control congestion*/),
                        1, 2
                    );

                    //ProtocolChannel.parm_consumer_wait_for_producer_timeout =
                    //    250; //We block and never report slow production
                    //ProtocolChannel.parm_producer_start_retry_time = 0;

                    _arrayPool = ((IoCcProtocolBuffer)ProtocolChannel.Source).ArrayPoolProxy;
                }
                else
                {
#pragma warning disable VSTHRD110 // Observe result of async calls
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    protocol.ZeroAsync(this);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
#pragma warning restore VSTHRD110 // Observe result of async calls
                }
                //});
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
        public int parm_forward_queue_length = 64; //TODO

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_datums_per_buffer = 40;

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_ave_sec_ms = 2000;

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
        /// How long to wait for the consumer before timing out
        /// </summary>
        public override int WaitForConsumerTimeout => parm_producer_wait_for_consumer_timeout;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            ProducerUserData = null;
            ProtocolChannel = null;
            _protocolMsgBatch = null;
            _arrayPool = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override Task ZeroManagedAsync()
        {
            if (_protocolMsgBatch != null)
                _arrayPool.Return(_protocolMsgBatch, true);

            return base.ZeroManagedAsync();
        }

        public override async Task<JobState> ProduceAsync(Func<IoJob<IoCcPeerMessage>, ValueTask<bool>> barrier)
        {
            try
            {
                await Source.ProduceAsync(async ioSocket =>
                {

                    //----------------------------------------------------------------------------
                    // BARRIER
                    // We are only allowed to run ahead of the consumer by some configurable
                    // amount of steps. Instead of say just filling up memory buffers.
                    // This allows us some kind of (anti DOS?) congestion control
                    //----------------------------------------------------------------------------
                    if (!await barrier(this))
                        return false;
                    try
                    {
                        //Async read the message from the message stream
                        if (Source.IsOperational)
                        {
                            var rx = await ((IoSocket)ioSocket).ReadAsync((byte[])(Array)Buffer, BufferOffset, BufferSize);


                            //Success
                            //UDP signals source ip
                            ProducerUserData = ((IoSocket)ioSocket).ExtraData();

                            //Drop zero reads
                            if (rx == 0)
                            {
                                BytesRead = 0;
                                State = JobState.ProduceTo;
                                return false;
                            }

                            //rate limit
                            _msgCount++;
                            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                            var delta = now - _msgRateCheckpoint;
                            if (_msgCount > parm_ave_sec_ms &&
                                (double)_msgCount * 1000 / delta > parm_ave_sec_ms)
                            {
                                BytesRead = 0;
                                State = JobState.ProduceTo;
                                _logger.Fatal($"Dropping spam {_msgCount}");
                                _msgCount -= 2;
                                return false;
                            }

                            //hist reset
                            if (delta > parm_ave_msg_sec_hist * 1000)
                            {
                                _msgRateCheckpoint = now;
                                _msgCount = 0;
                            }

                            BytesRead = rx;

                            UpdateBufferMetaData();

                            State = JobState.Produced;

                            //_logger.Trace($"RX=> {GetType().Name}[{Id}] ({Description}): read=`{BytesRead}', ready=`{BytesLeftToProcess}', datumcount=`{DatumCount}', datumsize=`{DatumSize}', fragment=`{DatumFragmentLength}', buffer = `{BytesLeftToProcess}/{BufferSize + DatumProvisionLengthMax}', buf = `{(int)(BytesLeftToProcess / (double)(BufferSize + DatumProvisionLengthMax) * 100)}%'");

                        }
                        else
                        {
                            _logger.Warn($"{GetType().Name}: Source {Source.Description} went non operational!");
                            State = JobState.Cancelled;
                            await Source.ZeroAsync(this).ConfigureAwait(false);
                        }

                        if (Zeroed())
                        {
                            State = JobState.Cancelled;
                            return false;
                        }

                        return true;
                    }
                    catch (NullReferenceException e) { _logger.Trace(e, Description); return false; }
                    catch (TaskCanceledException e) { _logger.Trace(e, Description); return false; }
                    catch (OperationCanceledException e) { _logger.Trace(e, Description); return false; }
                    catch (ObjectDisposedException e) { _logger.Trace(e, Description); return false; }
                    catch (Exception e)
                    {
                        State = JobState.ProduceErr;
                        await Source.ZeroAsync(this).ConfigureAwait(false);
                        _logger.Error(e, $"ReadAsync {Description}:");
                        return false;
                    }
                }).ConfigureAwait(false);
            }
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (NullReferenceException e) { _logger.Trace(e, Description); }
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            catch (Exception e)
            {
                _logger.Warn(e, $"Producing job for {Description} returned with errors:");
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
                //_protocolMsgBatch = ArrayPoolProxy<Tuple<IMessage, object, Packet>>.Shared.Rent(1000);
                for (var i = 0; i <= DatumCount; i++)
                {
                    var read = stream.Position;
                    var packet = Packet.Parser.ParseFrom(stream);
                    Interlocked.Add(ref BufferOffset, (int)(stream.Position - read));

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
                                await ProcessRequestAsync<Ping>(packet).ConfigureAwait(false);
                                break;
                            case nameof(MessageTypes.Pong):
                                await ProcessRequestAsync<Pong>(packet).ConfigureAwait(false);
                                break;
                            case nameof(MessageTypes.DiscoveryRequest):
                                await ProcessRequestAsync<DiscoveryRequest>(packet).ConfigureAwait(false);
                                break;
                            case nameof(MessageTypes.DiscoveryResponse):
                                await ProcessRequestAsync<DiscoveryResponse>(packet).ConfigureAwait(false);
                                break;
                            case nameof(MessageTypes.PeeringRequest):
                                await ProcessRequestAsync<PeeringRequest>(packet).ConfigureAwait(false);
                                break;
                            case nameof(MessageTypes.PeeringResponse):
                                await ProcessRequestAsync<PeeringResponse>(packet).ConfigureAwait(false);
                                break;
                            case nameof(MessageTypes.PeeringDrop):
                                await ProcessRequestAsync<PeeringDrop>(packet).ConfigureAwait(false);
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
            catch (NullReferenceException e) { _logger.Trace(e, Description); }
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (Exception e)
            {
                _logger.Error(e, $"Unmarshal Packet failed in {Description}");
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

        private volatile int _protocolMsgBatchIndex = 0;

        private async Task ProcessRequestAsync<T>(Packet packet)
        where T : IMessage<T>, new()
        {
            try
            {
                var parser = new MessageParser<T>(() => new T());
                var request = parser.ParseFrom(packet.Data);

                if (request != null)
                {
                    //_logger.Debug($"[{Base58Check.Base58CheckEncoding.Encode(packet.PublicKey.ToByteArray())}]{typeof(T).Name}: Received {packet.Data.Length}" );

                    if (_protocolMsgBatchIndex < parm_max_msg_batch_size)
                        _protocolMsgBatch[_protocolMsgBatchIndex++] = Tuple.Create((IMessage)request, ProducerUserData, packet);
                    else
                    {
                        await ForwardToNeighborAsync().ConfigureAwait(false);
                        _protocolMsgBatch[_protocolMsgBatchIndex++] = Tuple.Create((IMessage)request, ProducerUserData, packet);
                    }
                }
            }
            catch (NullReferenceException) { }
            catch (Exception e)
            {
                _logger.Error(e, $"Unable to parse request type {typeof(T).Name} from {Base58Check.Base58CheckEncoding.Encode(packet.PublicKey.ToByteArray())}, size = {packet.Data.Length}");
            }
        }

        private async Task ForwardToNeighborAsync()
        {
            try
            {
                if (_protocolMsgBatchIndex == 0)
                    return;

                if (_protocolMsgBatchIndex < parm_max_msg_batch_size)
                    _protocolMsgBatch[_protocolMsgBatchIndex++] = null;

                //cog the source
                await ProtocolChannel.Source.ProduceAsync(source =>
                {
                    //if (((IoCcProtocolBuffer) source).Count() ==
                    //    ((IoCcProtocolBuffer) source).MessageQueue.BoundedCapacity)
                    //{
                    //    _logger.Warn($"MessageQueue depleted: {((IoCcProtocolBuffer)source).MessageQueue.Count}");
                    //} //TODO

                    ((IoCcProtocolBuffer)source).Enqueue(_protocolMsgBatch);

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
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (NullReferenceException e) { _logger.Trace(e, Description); }
            catch (Exception e)
            {
                _logger.Debug(e, $"Forwarding from {Description} to {ProtocolChannel.Description} failed");
            }
        }

        /// <summary>
        /// Batch of messages
        /// </summary>
        private volatile Tuple<IMessage, object, Packet>[] _protocolMsgBatch;

        /// <summary>
        /// message heap
        /// </summary>
        private ArrayPool<Tuple<IMessage, object, Packet>> _arrayPool = ArrayPool<Tuple<IMessage, object, Packet>>.Create();
    }
}
