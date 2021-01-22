using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using Proto;
using SimpleBase;
using zero.cocoon.autopeer;
using zero.cocoon.identity;
using zero.cocoon.models.batches;
using zero.core.misc;
using zero.core.models.protobuffer;
using zero.core.models.protobuffer.sources;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.cocoon.models
{
    public class CcDiscoveries : CcProtocMessage<Packet, CcDiscoveryBatch>
    {
        public CcDiscoveries(string sinkDesc, string jobDesc, IoSource<CcProtocMessage<Packet, CcDiscoveryBatch>> source) : base(sinkDesc, jobDesc, source)
        {
            _logger = LogManager.GetCurrentClassLogger();

            _protocolMsgBatch = ArrayPool<CcDiscoveryBatch>.Shared.Rent(parm_max_msg_batch_size);

            if (!MessageService.ObjectStorage.ContainsKey($"{nameof(CcProtocMessage<Packet, CcDiscoveryBatch>)}.Discovery"))
            {
                CcProtocBatchSource<Packet, CcDiscoveryBatch> channelSource = null;

                //Transfer ownership
                if (MessageService.ZeroAtomicAsync((s, u, d) =>
                {
                    channelSource = new CcProtocBatchSource<Packet, CcDiscoveryBatch>(MessageService, _arrayPool, 0, Source.ConcurrencyLevel * 2);
                    if (MessageService.ObjectStorage.TryAdd(nameof(CcProtocBatchSource<Packet, CcDiscoveryBatch[]>), channelSource))
                    {
                        return ValueTask.FromResult(MessageService.ZeroOnCascade(channelSource).success);
                    }

                    return ValueTask.FromResult(false);
                }).GetAwaiter().GetResult())
                {
                    ProtocolConduit = MessageService.AttachConduit(
                        nameof(CcAdjunct),
                        true,
                        channelSource,
                        userData => new CcProtocBatch<Packet, CcDiscoveryBatch>(channelSource, -1 /*We block to control congestion*/),
                        Source.ConcurrencyLevel * 2, Source.ConcurrencyLevel * 2
                    );

                    //get reference to a central mem pool
                    _arrayPool = ((CcProtocBatchSource<Packet, CcDiscoveryBatch>)ProtocolConduit.Source).ArrayPool;
                }
                else
                {
                    var t = channelSource.ZeroAsync(this);
                }
            }
            else
            {
                ProtocolConduit = MessageService.AttachConduit<CcProtocBatch<Packet, CcDiscoveryBatch>>(nameof(CcAdjunct));
            }
        }

        /// <summary>
        /// Discovery Message types
        /// </summary>
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
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Batch of messages
        /// </summary>
        private volatile CcDiscoveryBatch[] _protocolMsgBatch;

        /// <summary>
        /// message heap
        /// </summary>
        private ArrayPool<CcDiscoveryBatch> _arrayPool =
            ArrayPool<CcDiscoveryBatch>.Create();

        public ArrayPool<CcDiscoveryBatch> ArrayPool => _arrayPool;

        /// <summary>
        /// CC Node
        /// </summary>
        protected CcCollective CcCollective => ((CcAdjunct)IoZero)?.CcCollective;

        ///// <summary>
        ///// Cc Identity
        ///// </summary>
        public CcDesignation CcId => CcCollective.CcId;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
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

        public override async ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            var stream = ByteStream;
            try
            {
                //fail fast
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

                        if (!Zeroed() && !MessageService.Zeroed())
                            _logger.Debug(e, $"Parse failed: r = {read}/{BytesRead}/{BytesLeftToProcess}, d = {DatumCount}, b={BufferSpan.Slice(tmpBufferOffset - 2, 32).ToArray().HashSig()}, {Description}");

                        if (read > 0)
                        {
                            continue;
                        }
                        else
                            break;
                    }

                    //did we get anything?
                    read = stream.Position - read;

                    Interlocked.Add(ref BufferOffset, (int)read);

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
                        if (i == 1) //&& read > 0)
                        {
                            _logger.Debug($"MULTI - READ <<D = {DatumCount}, r = {BytesRead}, d = {read}, l = {BytesLeftToProcess}>>");
                        }
                    }

                    //Sanity check the data
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
                    _logger.Trace($"<\\= {messageType ?? "Unknown"} {ProducerExtraData} /> {MessageService.IoNetSocket.LocalNodeAddress}<<[{(verified ? "signed" : "un-signed")}]{packet.Data.Memory.PayloadSig()}:, id = {Id}, o = {CurrBatch}, r = {BytesRead}");

                    //Don't process unsigned or unknown messages
                    if (!verified || messageType == null)
                    {
                        continue;
                    }

                    switch ((MessageTypes)packet.Type)
                    {
                        case MessageTypes.Ping:
                            await ProcessRequestAsync<Ping>(packet).ConfigureAwait(false);
                            break;
                        case MessageTypes.Pong:
                            await ProcessRequestAsync<Pong>(packet).ConfigureAwait(false);
                            break;
                        case MessageTypes.DiscoveryRequest:
                            await ProcessRequestAsync<DiscoveryRequest>(packet).ConfigureAwait(false);
                            break;
                        case MessageTypes.DiscoveryResponse:
                            await ProcessRequestAsync<DiscoveryResponse>(packet).ConfigureAwait(false);
                            break;
                        case MessageTypes.PeeringRequest:
                            await ProcessRequestAsync<PeeringRequest>(packet).ConfigureAwait(false);
                            break;
                        case MessageTypes.PeeringResponse:
                            await ProcessRequestAsync<PeeringResponse>(packet).ConfigureAwait(false);
                            break;
                        case MessageTypes.PeeringDrop:
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

            return State;
        }

        /// <summary>
        /// Processes a generic request
        /// </summary>
        /// <param name="packet">The packet</param>
        /// <typeparam name="T">The expected type</typeparam>
        /// <returns>The task</returns>
        private async ValueTask ProcessRequestAsync<T>(Packet packet)
            where T : IMessage<T>, IMessage, new()
        {
            try
            {
                var parser = new MessageParser<T>(() => new T());
                var request = parser.ParseFrom(packet.Data);

                if (request != null)
                {
                    //_logger.Debug($"[{Base58Check.Base58CheckEncoding.Encode(packet.PublicKey.ToByteArray())}]{typeof(T).Name}: Received {packet.Data.Length}" );
                    IIoZero zero = null;
                    //if (((IoNetClient<CcPeerMessage>)Source).Socket.FfAddress != null)
                    //    zero = IoZero;

                    if (CurrBatch >= parm_max_msg_batch_size)
                        await ForwardToNeighborAsync().ConfigureAwait(false);

                    var remoteEp = new IPEndPoint(((IPEndPoint)ProducerExtraData).Address, ((IPEndPoint)ProducerExtraData).Port);
                    _protocolMsgBatch[CurrBatch] = new CcDiscoveryBatch
                    {
                        Zero = zero,
                        EmbeddedMsg = request,
                        UserData = remoteEp,
                        Message = packet
                    }; // TODO HEAPyFY
                    Interlocked.Increment(ref CurrBatch);
                }
            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e, Description);
            }
            catch (Exception e)
            {
                _logger.Error(e,
                    $"Unable to parse request type {typeof(T).Name} from {Base58.Bitcoin.Encode(packet.PublicKey.Memory.AsArray())}, size = {packet.Data.Length}");
            }
        }

        /// <summary>
        /// Forward jobs to conduit
        /// </summary>
        /// <returns>Task</returns>
        private async ValueTask ForwardToNeighborAsync()
        {
            try
            {
                if (CurrBatch == 0 || Zeroed())
                    return;

                if (CurrBatch < parm_max_msg_batch_size)
                {
                    _protocolMsgBatch[CurrBatch] = default;
                }

                //cog the source
                var cogSuccess = await ProtocolConduit.Source.ProduceAsync(async (source, _, __, ioJob) =>
                {
                    var _this = (CcDiscoveries)ioJob;

                    if (!await ((CcProtocBatchSource<Packet, CcDiscoveryBatch>)source).EnqueueAsync(_this._protocolMsgBatch).ConfigureAwait(false))
                    {
                        if (!((CcProtocBatchSource<Packet, CcDiscoveryBatch>)source).Zeroed())
                            _this._logger.Fatal($"{nameof(ForwardToNeighborAsync)}: Unable to q batch, {_this.Description}");
                        return false;
                    }

                    //Retrieve batch buffer
                    try
                    {
                        _this._protocolMsgBatch = ArrayPool<CcDiscoveryBatch>.Shared.Rent(_this.parm_max_msg_batch_size);
                    }
                    catch (Exception e)
                    {
                        _this._logger.Fatal(e, $"Unable to rent from mempool: {_this.Description}");
                        return false;
                    }

                    _this.CurrBatch = 0;

                    return true;
                }, jobClosure: this).ConfigureAwait(false);

                ////forward transactions
                // if (cogSuccess)
                // {
                //     if (!await ProtocolConduit.ProduceAsync().ConfigureAwait(false))
                //     {
                //         _logger.Warn($"{TraceDescription} Failed to forward to `{ProtocolConduit.Source.Description}'");
                //     }
                // }
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
                _logger.Debug(e, $"Forwarding from {Description} to {ProtocolConduit.Description} failed");
            }
        }
    }
}
