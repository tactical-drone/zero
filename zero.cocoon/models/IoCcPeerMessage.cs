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
using zero.core.misc;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.misc;

namespace zero.cocoon.models
{
    public class IoCcPeerMessage : IoMessage<IoCcPeerMessage>
    {
        public IoCcPeerMessage(string sinkDesc, string jobDesc, IoSource<IoCcPeerMessage> source) 
            : base(sinkDesc, jobDesc, source)
        {
            _logger = LogManager.GetCurrentClassLogger();

            _protocolMsgBatch = ArrayPool<ValueTuple<IIoZero, IMessage, object, Packet>>.Shared.Rent(parm_max_msg_batch_size);

            DatumSize = 508;

            //Init buffers
            BufferSize = DatumSize * parm_datums_per_buffer;
            DatumProvisionLengthMax = DatumSize - 1;
            Buffer = new sbyte[BufferSize + DatumProvisionLengthMax];
            ByteSegment = ByteBuffer;

            if (!MessageService.ObjectStorage.ContainsKey(nameof(IoCcProtocolBuffer)))
            {
                IoCcProtocolBuffer channelSource = null;

                //Transfer ownership
                if (MessageService.ZeroAtomicAsync((s,d) =>
                {
                    channelSource = new IoCcProtocolBuffer(MessageService, _arrayPool, 0, Source.ConcurrencyLevel * 2);
                    if (MessageService.ObjectStorage.TryAdd(nameof(IoCcProtocolBuffer), channelSource))
                    {
                        return Task.FromResult(MessageService.ZeroOnCascade(channelSource).success);
                    }

                    return Task.FromResult(false);
                }).ZeroBoostAsync().ConfigureAwait(false).GetAwaiter().GetResult())
                {
                    ProtocolChannel = MessageService.EnsureChannel(
                        nameof(IoCcNeighbor),
                        true,
                        channelSource,
                        userData => new IoCcProtocolMessage(channelSource, -1 /*We block to control congestion*/),
                        Source.ConcurrencyLevel, Source.ConcurrencyLevel
                    );

                    //get reference to a central mem pool
                    _arrayPool = ((IoCcProtocolBuffer) ProtocolChannel.Source).ArrayPoolProxy;
                }
                else
                {
#pragma warning disable VSTHRD110 // Observe result of async calls
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    channelSource.ZeroAsync(this);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
#pragma warning restore VSTHRD110 // Observe result of async calls
                }

                //});
            }
            else
            {
                ProtocolChannel = MessageService.EnsureChannel<IoCcProtocolMessage>(nameof(IoCcNeighbor));
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
        /// Base source
        /// </summary>
        protected IoUdpClient<IoCcPeerMessage> MessageService => (IoUdpClient<IoCcPeerMessage>) Source;

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
        [IoParameter] public int parm_prefetch_size = 0;
        
        /// <summary>
        /// The amount of items that can be ready for production before blocking
        /// </summary>
        [IoParameter] public int parm_concurrency_level = 2;

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_datums_per_buffer = 10;

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
        public int parm_max_msg_batch_size = 48;//TODO

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
        protected volatile object ProducerExtraData;

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
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            ProducerExtraData = null;
            ProtocolChannel = null;
            _protocolMsgBatch = null;
            _arrayPool = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            if (_protocolMsgBatch != null)
                _arrayPool.Return(_protocolMsgBatch, true);

            await base.ZeroManagedAsync().ConfigureAwait(false);
        }

        private readonly IPEndPoint _remoteEp = new IPEndPoint(IPAddress.Any, 0);
        public override async ValueTask<IoJobMeta.JobState> ProduceAsync(Func<IIoJob, IIoZero, ValueTask<bool>> barrier,
            IIoZero zeroClosure)
        {
            try
            {
                await MessageService.ProduceAsync(async (ioSocket, producerPressure, ioZero, ioJob) =>
                {
                    var _this = (IoCcPeerMessage)ioJob;
                    //----------------------------------------------------------------------------
                    // BARRIER
                    // We are only allowed to run ahead of the consumer by some configurable
                    // amount of steps. Instead of say just filling up memory buffers.
                    // This allows us some kind of (anti DOS?) congestion control
                    //----------------------------------------------------------------------------
                    if (!await producerPressure(ioJob, ioZero).ConfigureAwait(false))
                        return false;
                    try
                    {
                        //Async read the message from the message stream
                        if (_this.MessageService.IsOperational)
                        {
                            int rx = 0;

                            var readTask = ((IoSocket) ioSocket).ReadAsync(_this.ByteSegment, _this.BufferOffset,_this.BufferSize, _remoteEp, _this.MessageService.BlackList);
                            await readTask.OverBoostAsync().ConfigureAwait(false);

                            rx = readTask.Result;
                            
                            //Success
                            //UDP signals source ip
                            _this.ProducerExtraData = _remoteEp;

                            //Drop zero reads
                            if (rx == 0)
                            {
                                _this.BytesRead = 0;
                                _this.State = IoJobMeta.JobState.ProduceTo;
                                return false;
                            }

                            //rate limit
                            _this._msgCount++;
                            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                            var delta = now - _this._msgRateCheckpoint;
                            if (_this._msgCount > _this.parm_ave_sec_ms &&
                                (double) _this._msgCount * 1000 / delta > _this.parm_ave_sec_ms)
                            {
                                _this.BytesRead = 0;
                                _this.State = IoJobMeta.JobState.ProduceTo;
                                _this._logger.Fatal($"Dropping spam {_this._msgCount}");
                                _this._msgCount -= 2;
                                return false;
                            }

                            //hist reset
                            if (delta > _this.parm_ave_msg_sec_hist * 1000)
                            {
                                _this._msgRateCheckpoint = now;
                                _this._msgCount = 0;
                            }

                            _this.BytesRead = rx;

                            _this.UpdateBufferMetaData();

                            _this.State = IoJobMeta.JobState.Produced;

                            //_this._logger.Trace($"{_this.Description} => {GetType().Name}[{_this.Id}]: r = {_this.BytesRead}, r = {_this.BytesLeftToProcess}, dc = {_this.DatumCount}, ds = {_this.DatumSize}, f = {_this.DatumFragmentLength}, b = {_this.BytesLeftToProcess}/{_this.BufferSize + _this.DatumProvisionLengthMax}, b = {(int)(_this.BytesLeftToProcess / (double)(_this.BufferSize + _this.DatumProvisionLengthMax) * 100)}%");
                        }
                        else
                        {
                            _this._logger.Warn(
                                $"Source {_this.MessageService.Description} produce failed!");
                            _this.State = IoJobMeta.JobState.Cancelled;
                            await _this.MessageService.ZeroAsync(_this).ConfigureAwait(false);
                        }

                        if (_this.Zeroed())
                        {
                            _this.State = IoJobMeta.JobState.Cancelled;
                            return false;
                        }

                        return true;
                    }
                    catch (NullReferenceException e)
                    {
                        _this._logger.Trace(e, _this.Description);
                        return false;
                    }
                    catch (TaskCanceledException e)
                    {
                        _this._logger.Trace(e, _this.Description);
                        return false;
                    }
                    catch (OperationCanceledException e)
                    {
                        _this._logger.Trace(e, _this.Description);
                        return false;
                    }
                    catch (ObjectDisposedException e)
                    {
                        _this._logger.Trace(e, _this.Description);
                        return false;
                    }
                    catch (Exception e)
                    {
                        _this.State = IoJobMeta.JobState.ProduceErr;
                        await _this.MessageService.ZeroAsync(_this).ConfigureAwait(false);
                        _this._logger.Error(e, $"ReadAsync {_this.Description}:");
                        return false;
                    }
                }, barrier, zeroClosure, this).ConfigureAwait(false);
            }
            catch (TaskCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e, Description);
            }
            catch (ObjectDisposedException e)
            {
                _logger.Trace(e, Description);
            }
            catch (OperationCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (Exception e)
            {
                _logger.Warn(e, $"Producing job for {Description} returned with errors:");
            }
            finally
            {
                if (State == IoJobMeta.JobState.Producing)
                {
                    // Set the state to ProduceErr so that the consumer knows to abort consumption
                    State = IoJobMeta.JobState.ProduceErr;
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

            var p = (IoMessage<IoCcPeerMessage>) PreviousJob;
            try
            {
                var bytesToTransfer = Math.Min(p.DatumFragmentLength, DatumProvisionLengthMax);
                Interlocked.Add(ref BufferOffset, -bytesToTransfer);
                Interlocked.Add(ref BytesRead, bytesToTransfer);

                UpdateBufferMetaData();

                Array.Copy(p.Buffer, p.BufferOffset + Math.Max(p.BytesLeftToProcess - DatumProvisionLengthMax, 0),
                    Buffer, BufferOffset, bytesToTransfer);
            }
            catch (Exception e) // we de-synced 
            {
                _logger.Warn(e, $"{TraceDescription} We desynced!:");

                MessageService.Synced = false;
                DatumCount = 0;
                BytesRead = 0;
                State = IoJobMeta.JobState.ConInvalid;
                DatumFragmentLength = 0;
                StillHasUnprocessedFragments = false;
            }
        }

        /// <summary>
        /// CC Node
        /// </summary>
        protected IoCcNode CcNode => ((IoCcNeighbor) IoZero)?.CcNode;

        /// <summary>
        /// Cc Identity
        /// </summary>
        public IoCcIdentity CcId => CcNode.CcId;

        /// <summary>
        /// Message sink
        /// </summary>
        /// <returns>Processing state</returns>
        public override async ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            var stream = ByteStream;
            try
            {
                if (BytesRead == 0 || Zeroed())
                    return State = IoJobMeta.JobState.ConInvalid;

                var verified = false;
                for (var i = 0; i <= DatumCount && BytesLeftToProcess > 0; i++)
                {
                    if (DatumCount > 1)
                    {
                        _logger.Fatal($"{Description} ----> Datumcount = {DatumCount}");
                    }
                    Packet packet = null;
                    var read = stream.Position;

                    //deserialize
                    try
                    {
                        packet = Packet.Parser.ParseFrom(stream);
                    }
                    catch (Exception e)
                    {
                        read = stream.Position - read;
                        var tmpBufferOffset = BufferOffset;
                        Interlocked.Add(ref BufferOffset, (int)read);

                        if(!Zeroed() && !MessageService.Zeroed())
                            _logger.Debug(e, $"Parse failed: r = {read}/{BytesRead}/{BytesLeftToProcess}, d = {DatumCount}, b={BufferSpan.Slice(tmpBufferOffset -2 , 32).ToArray().HashSig()}, {Description}");

                        if (read > 0)
                        {
                            continue;
                        }
                        else
                            break;
                    }

                    //did we get anything?
                    read = stream.Position - read;

                    Interlocked.Add(ref BufferOffset, (int) read);

#if !DEBUG
                    if (read == 0)
                    {
                        continue;
                    }
#endif

                    if (DatumCount > 0)
                    {
                        if (i == 0 && BytesLeftToProcess > 0)
                        {
                            _logger.Debug($"MULTI<<D = {DatumCount}, r = {BytesRead}, d = {read}, l = {BytesLeftToProcess}>>");
                        }
                        if (i == 1 ) //&& read > 0)
                        {
                            _logger.Debug($"MULTI - READ <<D = {DatumCount}, r = {BytesRead}, d = {read}, l = {BytesLeftToProcess}>>");
                        }
                    }

                    //Sanity check the datas
                    if (packet == null || packet.Data == null || packet.Data.Length == 0)
                    {
                        continue;
                    }

                    var packetMsgRaw = packet.Data.Memory.AsArray(); 
                    if (packet.Signature != null && !packet.Signature.IsEmpty)
                    {
                        verified = CcId.Verify(packetMsgRaw, 0, packetMsgRaw.Length, packet.PublicKey.Memory.AsArray(),
                                0, packet.Signature.Memory.AsArray(), 0);
                        
                    }

                    var messageType = Enum.GetName(typeof(MessageTypes), packet.Type);
                    _logger.Trace($"<\\= {messageType ?? "Unknown"} {ProducerExtraData} /> {MessageService.IoNetSocket.LocalNodeAddress}<<[{(verified ? "signed" : "un-signed")}]{packet.Data.Memory.PayloadSig()}:, id = {Id}, o = {_currBatch}, r = {BytesRead}");

                    //Don't process unsigned or unknown messages
                    if (!verified || messageType == null)
                    {
                        continue;
                    }

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

                }

                //Release a waiter
                await ForwardToNeighborAsync().ConfigureAwait(false);

                State = IoJobMeta.JobState.Consumed;
            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e, Description);
            }
            catch (TaskCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (OperationCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (ObjectDisposedException e)
            {
                _logger.Trace(e, Description);
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unmarshal Packet failed in {Description}");
            }
            finally
            {
                if (State == IoJobMeta.JobState.Consuming)
                    State = IoJobMeta.JobState.ConsumeErr;
                UpdateBufferMetaData();
            }

            //if (_protocolMsgBatch.Count > 0)
            //{
            //    await ForwardToNeighborAsync(_protocolMsgBatch);
            //}

            return State;
        }

        /// <summary>
        /// offset into the batch
        /// </summary>
        private volatile int _currBatch;

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
                    IIoZero zero = null;
                    //if (((IoNetClient<IoCcPeerMessage>)Source).Socket.FfAddress != null)
                    //    zero = IoZero;

                    if (_currBatch >= parm_max_msg_batch_size)
                        await ForwardToNeighborAsync().ConfigureAwait(false);

                    var remoteEp = (IPEndPoint) ProducerExtraData;
                    _protocolMsgBatch[_currBatch] = ValueTuple.Create(zero, (IMessage)request, (object)IPEndPoint.Parse(remoteEp.ToString()), packet);
                    Interlocked.Increment(ref _currBatch);
                }
            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e, Description);
            }
            catch (Exception e)
            {
                _logger.Error(e,
                    $"Unable to parse request type {typeof(T).Name} from {Base58Check.Base58CheckEncoding.Encode(packet.PublicKey.Memory.AsArray())}, size = {packet.Data.Length}");
            }
        }

        private async Task ForwardToNeighborAsync()
        {
            try
            {
                if (_currBatch == 0)
                    return;

                if (_currBatch < parm_max_msg_batch_size)
                {
                    _protocolMsgBatch[_currBatch] = default;
                }
                
                //cog the source
                var cogSuccess = await ProtocolChannel.Source.ProduceAsync(async (source, _, __, ioJob) =>
                {
                    var _this = (IoCcPeerMessage) ioJob;

                    if (!await ((IoCcProtocolBuffer) source).EnqueueAsync(_this._protocolMsgBatch).ConfigureAwait(false))
                    {
                        _logger.Fatal($"{nameof(ForwardToNeighborAsync)}: Unable to q batch, {Description}");
                        return false;
                    }

                    //Retrieve batch buffer
                    try
                    {
                        _this._protocolMsgBatch = ArrayPool<ValueTuple<IIoZero, IMessage, object, Packet>>.Shared.Rent(_this.parm_max_msg_batch_size);
                    }
                    catch (Exception e)
                    {
                        _logger.Fatal(e,$"Unable to rent from mempool: {Description}");
                        return false;
                    }

                    _this._currBatch = 0;

                    return true;
                }, jobClosure: this).ConfigureAwait(false);

                ////forward transactions
                //if (cogSuccess)
                //{
                //    if (!await ProtocolChannel.ProduceAsync().ConfigureAwait(false))
                //    {
                //        _logger.Warn($"{TraceDescription} Failed to forward to `{ProtocolChannel.Source.Description}'");
                //    }
                //}
            }
            catch (TaskCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (OperationCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (ObjectDisposedException e)
            {
                _logger.Trace(e, Description);
            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e, Description);
            }
            catch (Exception e)
            {
                _logger.Debug(e, $"Forwarding from {Description} to {ProtocolChannel.Description} failed");
            }
        }

        /// <summary>
        /// Batch of messages
        /// </summary>
        private volatile ValueTuple<IIoZero, IMessage, object, Packet>[] _protocolMsgBatch;

        /// <summary>
        /// message heap
        /// </summary>
        private ArrayPool<ValueTuple<IIoZero, IMessage, object, Packet>> _arrayPool =
            ArrayPool<ValueTuple<IIoZero, IMessage, object, Packet>>.Create();
    }
}