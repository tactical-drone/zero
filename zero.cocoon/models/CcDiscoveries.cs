using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
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
using zero.core.patterns.heap;

namespace zero.cocoon.models
{
    public class CcDiscoveries : CcProtocMessage<Packet, CcDiscoveryBatch>
    {
        public CcDiscoveries(string sinkDesc, string jobDesc, IoSource<CcProtocMessage<Packet, CcDiscoveryBatch>> source, int concurrencyLevel = 1) : base(sinkDesc, jobDesc, source)
        {
            _logger = LogManager.GetCurrentClassLogger();
            _concurrencyLevel = concurrencyLevel;
            _protocolMsgBatch = ArrayPool<CcDiscoveryBatch>.Shared.Rent(parm_max_msg_batch_size);
            _batchHeap = new IoHeap<CcDiscoveryBatch>(concurrencyLevel + 1) {Make = o => new CcDiscoveryBatch()};
        }

        public override async ValueTask<bool> ConstructAsync()
        {
            if (!MessageService.ObjectStorage.ContainsKey($"{nameof(CcProtocMessage<Packet, CcDiscoveryBatch>)}.Discovery"))
            {
                CcProtocBatchSource<Packet, CcDiscoveryBatch> channelSource = null;

                //Transfer ownership
                if (MessageService.ZeroAtomicAsync((s, u, d) =>
                {
                    channelSource = new CcProtocBatchSource<Packet, CcDiscoveryBatch>(MessageService, _arrayPool, 0, _concurrencyLevel);
                    if (MessageService.ObjectStorage.TryAdd(nameof(CcProtocBatchSource<Packet, CcDiscoveryBatch[]>), channelSource))
                    {
                        return ValueTask.FromResult(MessageService.ZeroOnCascade(channelSource).success);
                    }

                    return ValueTask.FromResult(false);
                }).GetAwaiter().GetResult())
                {
                    ProtocolConduit = await MessageService.AttachConduitAsync(
                        nameof(CcAdjunct),
                        true,
                        channelSource,
                        userData => new CcProtocBatch<Packet, CcDiscoveryBatch>(channelSource, -1 /*We block to control congestion*/),
                        _concurrencyLevel, _concurrencyLevel
                    );

                    //get reference to a central mem pool
                    if (ProtocolConduit != null)
                        _arrayPool = ((CcProtocBatchSource<Packet, CcDiscoveryBatch>)ProtocolConduit.Source).ArrayPool;
                    else
                        return false;
                    
                }
                else
                {
                    var t = channelSource.ZeroAsync(this);
                    ProtocolConduit = await MessageService.AttachConduitAsync<CcProtocBatch<Packet, CcDiscoveryBatch>>(nameof(CcAdjunct));
                }
            }
            else
            {
                ProtocolConduit = await MessageService.AttachConduitAsync<CcProtocBatch<Packet, CcDiscoveryBatch>>(nameof(CcAdjunct));
            }
            return await base.ConstructAsync() && ProtocolConduit != null;
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
        private Logger _logger;

        /// <summary>
        /// Batch of messages
        /// </summary>
        private volatile CcDiscoveryBatch[] _protocolMsgBatch;

        /// <summary>
        /// Batch heap items
        /// </summary>
        private IoHeap<CcDiscoveryBatch> _batchHeap;

        /// <summary>
        /// message heap
        /// </summary>
        private ArrayPool<CcDiscoveryBatch> _arrayPool =
            ArrayPool<CcDiscoveryBatch>.Create();

        /// <summary>
        /// The concurrency level
        /// </summary>
        private int _concurrencyLevel;

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
            _logger = null;
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
            TransferPreviousBits();
            try
            {
                //fail fast
                if (BytesRead == 0 || Zeroed())
                    return State = IoJobMeta.JobState.ConInvalid;

                var totalBytesProcessed = 0;
                var read = 0;
                var verified = false;

                while (BytesLeftToProcess > 0 && totalBytesProcessed < BytesRead)
                {
                    //if (DatumCount > 1)
                    //{
                    //    _logger.Fatal($"{Description} ----> Datumcount = {DatumCount}");
                    //}

                    Packet packet = null;
                    long curPos = 0;
                    read = 0;
                    //var s = new MemoryStream(ByteBuffer);
                    //deserialize
                    try
                    {
                        ByteStream.Seek(BufferOffset, SeekOrigin.Begin);
                        ByteStream.SetLength(BufferOffset + BytesLeftToProcess);
                        curPos = BufferOffset;

                        //trim zeroes
                        if (IoZero.SupportsSync)
                        {
                            bool trimmed = false;

                            while (ByteBuffer[curPos++] == 0 && totalBytesProcessed < BytesRead)
                            {
                                read++;
                                totalBytesProcessed++;
                                trimmed = true;
                            }

                            if (totalBytesProcessed == BytesRead)
                            {
                                //State = IoJobMeta.JobState.Consumed;
                                continue;
                            }
                                

                            if (trimmed)
                            {
                                ByteStream.Seek(BufferOffset + read, SeekOrigin.Begin);
                                ByteStream.SetLength(BufferOffset + BytesLeftToProcess - read);
                                curPos = BufferOffset + read;
                            }
                        }

                        //s.Seek(ByteStream.Position, SeekOrigin.Begin);
                        //s.SetLength(ByteStream.Position + BytesLeftToProcess);
                        //curPos = s.Position;
                        //CodedStream = new CodedInputStream(s);

                        //CodedStream = new CodedInputStream(ByteBuffer, BufferOffset, BytesRead);
                        try
                        {
                            packet = Packet.Parser.ParseFrom(ReadOnlySequence.Slice(BufferOffset, BytesRead));
                            read = packet.CalculateSize();
                        }
                        catch 
                        {
                            try
                            {
                                var s = new MemoryStream(ByteBuffer);
                                s.Seek(ByteStream.Position, SeekOrigin.Begin);
                                s.SetLength(ByteStream.Position + BytesLeftToProcess);
                                curPos = s.Position;
                                CodedStream = new CodedInputStream(s);

                                packet = Packet.Parser.ParseFrom(CodedStream);
                                read = (int)(CodedStream.Position - curPos);
                            }
                            catch 
                            {
                                packet = Packet.Parser.ParseFrom(BufferClone.AsSpan().Slice(BufferOffset, BytesRead).ToArray());
                                read = packet.CalculateSize();
                                //_logger.Warn("FFF");
                            }
                        }

                        totalBytesProcessed += read;
                        State = IoJobMeta.JobState.Consumed;
                    }
                    catch (Exception e)
                    {
                        State = IoJobMeta.JobState.ConsumeErr;
                        read = (int)(CodedStream.Position - curPos);
                        totalBytesProcessed += read;

                        //sync fragmented datums
                        if (IoZero.SupportsSync && totalBytesProcessed == BytesRead && State != IoJobMeta.JobState.Consumed)//TODO hacky
                        {
                            read = 0;
                            //State = IoJobMeta.JobState.Consumed;
                        }

                        var tmpBufferOffset = BufferOffset;

                        if (!Zeroed() && !MessageService.Zeroed())
                        {
                            _logger.Debug(e,
                                $"Parse failed: r = {totalBytesProcessed}/{BytesRead}/{BytesLeftToProcess}, d = {DatumCount}, b={BufferSpan.Slice(tmpBufferOffset - 2, 32).ToArray().HashSig()}, {Description}");

                            var r = new ReadOnlyMemory<byte>(ByteBuffer);
                            if (MemoryMarshal.TryGetArray((ReadOnlyMemory<byte>)MemoryBuffer, out var seg))
                            {
                                ByteStream.Seek(BufferOffset + read, SeekOrigin.Begin);
                                ByteStream.SetLength(BufferOffset + BytesRead - read);

                                StringWriter w = new StringWriter();
                                StringWriter w2 = new StringWriter();
                                w.Write($"{MemoryBuffer.GetHashCode()}({BytesRead}):");
                                w2.Write($"{BufferClone.GetHashCode()}({BytesRead}):");
                                var nullc = 0;
                                var nulld = 0;
                                for (var i = 0; i < BytesRead; i++)
                                {
                                    w.Write($" {seg[i + BufferOffset]}.");
                                    w2.Write($" {BufferClone[i + BufferOffset]}.");
                                }

                                _logger.Debug(w.ToString());
                                _logger.Warn(w2.ToString());
                            }
                        }

                        continue;
                    }
                    finally
                    {
                        Interlocked.Add(ref BufferOffset, (int)read);
                    }

                    if (read == 0)
                        break;
                    
                    //if (BytesLeftToProcess > 0)
                    //{
                    //    _logger.Debug($"MULTI<<D = {DatumCount}, r = {BytesRead}:{read}, T = {totalBytesProcessed}, l = {BytesLeftToProcess}>>");
                    //}
                    
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
                UpdateBufferMetaData();

                if (State != IoJobMeta.JobState.Consumed)
                {
                    State = IoJobMeta.JobState.ConsumeErr;

                    if (PreviousJob.StillHasUnprocessedFragments)
                    {
                        StillHasUnprocessedFragments = false;
                    }
                    else
                    {
                        _logger.Debug($"FRAGGED = {DatumCount}, {BytesRead}/{BytesLeftToProcess}");
                    }
                }

                MemoryBufferPin.Dispose();
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

                    _batchHeap.Take(out var batch);
                    if (batch == null)
                        throw new OutOfMemoryException(nameof(_batchHeap));

                    batch.Zero = zero;
                    batch.EmbeddedMsg = request;
                    batch.UserData = remoteEp;
                    batch.Message = packet;
                    batch.HeapRef = _batchHeap;
                    

                    _protocolMsgBatch[CurrBatch] = batch;

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
