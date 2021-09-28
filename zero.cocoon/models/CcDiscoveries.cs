﻿using System;
using System.Buffers;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
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
            _concurrencyLevel = concurrencyLevel;
            _protocolMsgBatch = _arrayPool.Rent(parm_max_msg_batch_size);
            _batchMsgHeap = new IoHeap<CcDiscoveryBatch>(concurrencyLevel) {Make = o => new CcDiscoveryBatch()};
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
                        return ValueTask.FromResult(MessageService.ZeroOnCascade(channelSource, true).success);
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
        /// Batch of messages
        /// </summary>
        private volatile CcDiscoveryBatch[] _protocolMsgBatch;

        /// <summary>
        /// Batch heap items
        /// </summary>
        private IoHeap<CcDiscoveryBatch> _batchMsgHeap;

        /// <summary>
        /// message heap
        /// </summary>
        private ArrayPool<CcDiscoveryBatch> _arrayPool = ArrayPool<CcDiscoveryBatch>.Create();

        /// <summary>
        /// The concurrency level
        /// </summary>
        private readonly int _concurrencyLevel;

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

            _batchMsgHeap.ZeroManaged(batch =>
            {
                batch.Zero = null;
                batch.Message = null;
                batch.EmbeddedMsg = null;
                batch.HeapRef = null;
            });

            await base.ZeroManagedAsync().ConfigureAwait(false);
        }

        public override async ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            try
            {
                //fail fast
                if (BytesRead == 0 || Zeroed())
                    return State = IoJobMeta.JobState.ConInvalid;
                
                var totalBytesProcessed = 0;
                var verified = false;

                while (totalBytesProcessed < BytesRead && State != IoJobMeta.JobState.ConInlined)
                {
                    Packet packet = null;
                    long curPos = 0;
                    var read = 0;
                    var currentOffset = BufferOffset;
                    //deserialize
                    try
                    {
                        //ByteStream.SetLength(BytesLeftToProcess);
                        //curPos = BufferOffset;

                        //trim zeroes
                        // if (IoZero.SyncRecoveryModeEnabled)
                        // {
                        //     bool trimmed = false;
                        //
                        //     while (Buffer[curPos++] == 0 && totalBytesProcessed < BytesRead)
                        //     {
                        //         read++;
                        //         totalBytesProcessed++;
                        //         trimmed = true;
                        //     }
                        //
                        //     if (totalBytesProcessed == BytesRead)
                        //     {
                        //         State = IoJobMeta.JobState.Consumed;
                        //         continue;
                        //     }
                        //         
                        //
                        //     if (trimmed)
                        //     {
                        //         ByteStream.Seek(BufferOffset + read, SeekOrigin.Begin);
                        //         ByteStream.SetLength(BytesLeftToProcess - read);
                        //         curPos = BufferOffset + read;
                        //     }
                        // }
                        
                        try
                        {
                            packet = Packet.Parser.ParseFrom(ReadOnlySequence.Slice(BufferOffset, BytesRead));
                            read = packet.CalculateSize();
                        }
                        catch
                        {
                            //try
                            {
                                var s = new MemoryStream(Buffer);
                                s.Seek(currentOffset, SeekOrigin.Begin);
                                curPos = s.Position;
                                CodedStream = new CodedInputStream(s);

                                packet = Packet.Parser.ParseFrom(CodedStream);
                                read = (int)(CodedStream.Position - curPos);
                            }
                            //catch
                            {
                                //if (!__enableZeroCopyDebug)
                                    //throw;

                                //packet = Packet.Parser.ParseFrom(__zeroCopyDebugBuffer.AsSpan().Slice(BufferOffset, BytesRead).ToArray());
                                //read = packet.CalculateSize();
                                //_logger.Warn($"FFF {_lastFail}/{_failCounter}");
                                //Interlocked.Exchange(ref _lastFail, _failCounter);
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
                        if (IoZero.SyncRecoveryModeEnabled && totalBytesProcessed == BytesRead && State != IoJobMeta.JobState.Consumed)//TODO hacky
                        {
                            read = 0;
                            State = IoJobMeta.JobState.ConInlined;
                        }

                        var tmpBufferOffset = BufferOffset;

                        if (!Zeroed() && !MessageService.Zeroed())
                        {
                            _logger.Debug(e,
                                $"Parse failed: r = {totalBytesProcessed}/{BytesRead}/{BytesLeftToProcess}, d = {DatumCount}, b={MemoryBuffer.Slice(tmpBufferOffset - 2, 32).ToArray().HashSig()}, {Description}");

                            // var r = new ReadOnlyMemory<byte>(Buffer);
                            // if (__enableZeroCopyDebug && MemoryMarshal.TryGetArray((ReadOnlyMemory<byte>)MemoryBuffer, out var seg))
                            // {
                            //     ByteStream.Seek(BufferOffset + read, SeekOrigin.Begin);
                            //     ByteStream.SetLength(BufferOffset + BytesRead - read);
                            //
                            //     StringWriter w = new StringWriter();
                            //     StringWriter w2 = new StringWriter();
                            //     w.Write($"{MemoryBuffer.GetHashCode()}({BytesRead}):");
                            //     w2.Write($"{__zeroCopyDebugBuffer.GetHashCode()}({BytesRead}):");
                            //     var nullc = 0;
                            //     var nulld = 0;
                            //     for (var i = 0; i < BytesRead; i++)
                            //     {
                            //         w.Write($"[{seg[i + BufferOffset]}]");
                            //         w2.Write($"[{__zeroCopyDebugBuffer[i + BufferOffset]}]");
                            //     }
                            //
                            //     _logger.Debug(w.ToString());
                            //     _logger.Warn(w2.ToString());
                            // }
                        }

                        continue;
                    }
                    finally
                    {
                        Interlocked.Add(ref BufferOffset, (int)read);
                    }

                    if (read == 0)
                        break;

                    if (BytesLeftToProcess > 0)
                    {
                        _logger.Debug($"MULTI<<D = {DatumCount}, r = {BytesRead}:{read}, T = {totalBytesProcessed}, l = {BytesLeftToProcess}>>");
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
                    _logger.Trace($"<\\= {messageType ?? "Unknown"} {ProducerExtraData} /> {MessageService.IoNetSocket.LocalNodeAddress}<<[{(verified ? "signed" : "un-signed")}]{packet.Data.Memory.PayloadSig()}:, id = {Id}, o = {CurrBatchSlot}, r = {BytesRead}");

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
                if (State != IoJobMeta.JobState.Consumed)
                {
                    if (PreviousJob.StillHasUnprocessedFragments)
                    {
                        State = IoJobMeta.JobState.ConsumeErr;
                    }
                    else
                    {
                        State = IoJobMeta.JobState.ConInlined;
                        _logger.Debug($"FRAGGED = {DatumCount}, {BytesRead}/{BytesLeftToProcess}");
                    }
                }

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

                    if (CurrBatchSlot == parm_max_msg_batch_size - 1)
                        await ForwardToNeighborAsync().ConfigureAwait(false);

                    var remoteEp = new IPEndPoint(((IPEndPoint)ProducerExtraData).Address, ((IPEndPoint)ProducerExtraData).Port);

                    _batchMsgHeap.Take(out var batchMsg);
                    if (batchMsg == null)
                        throw new OutOfMemoryException(nameof(_batchMsgHeap));

                    batchMsg.Zero = zero;
                    batchMsg.EmbeddedMsg = request;
                    batchMsg.UserData = remoteEp;
                    batchMsg.Message = packet;
                    batchMsg.HeapRef = _batchMsgHeap;
                    

                    _protocolMsgBatch[CurrBatchSlot] = batchMsg;

                    Interlocked.Increment(ref CurrBatchSlot);
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
                if (CurrBatchSlot == 0 || Zeroed())
                    return;

                if (CurrBatchSlot < parm_max_msg_batch_size)
                {
                    _protocolMsgBatch[CurrBatchSlot] = default;
                }

                //cog the source
                var cogSuccess = await ProtocolConduit.Source.ProduceAsync(async (source, _, __, ioJob) =>
                {
                    var _this = (CcDiscoveries)ioJob;

                    if (!await ((CcProtocBatchSource<Packet, CcDiscoveryBatch>)source).EnqueueAsync(_this._protocolMsgBatch).ConfigureAwait(false))
                    {
                        if (!((CcProtocBatchSource<Packet, CcDiscoveryBatch>)source).Zeroed())
                            _logger.Fatal($"{nameof(ForwardToNeighborAsync)}: Unable to q batch, {_this.Description}");
                        return false;
                    }

                    //Retrieve a new batch buffer
                    try
                    {
                        _this._protocolMsgBatch = _arrayPool.Rent(_this.parm_max_msg_batch_size);
                    }
                    catch (Exception e)
                    {
                        _logger.Fatal(e, $"Unable to rent from mempool: {_this.Description}");
                        return false;
                    }

                    _this.CurrBatchSlot = 0;

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
