using System;
using System.Buffers;
using System.IO;
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
using zero.core.patterns.misc;

namespace zero.cocoon.models
{
    public class CcDiscoveries : CcProtocMessage<Packet, CcDiscoveryMessage>
    {
        public CcDiscoveries(string sinkDesc, string jobDesc, IoSource<CcProtocMessage<Packet, CcDiscoveryMessage>> source) : base(sinkDesc, jobDesc, source)
        {
            _currentBatch = _arrayPool.Rent((int)parm_max_msg_batch_size);
            _batchMsgHeap = new IoHeap<CcDiscoveryMessage>(parm_max_msg_batch_size) {Make = o => new CcDiscoveryMessage()};//TODO config
        }

        public override async ValueTask<bool> ConstructAsync()
        {
            var conduitName = nameof(CcAdjunct);
            ProtocolConduit = await MessageService.CreateConduitOnceAsync<CcProtocBatch<Packet, CcDiscoveryMessage>>(conduitName).FastPath().ConfigureAwait(false);
            
            if (ProtocolConduit == null)
            {
                CcProtocBatchSource<Packet, CcDiscoveryMessage> channelSource = null;
                //TODO tuning
                channelSource = new CcProtocBatchSource<Packet, CcDiscoveryMessage>(Description, MessageService, _arrayPool , BufferSize/DatumSize, Source.ZeroConcurrencyLevel(), Source.ZeroConcurrencyLevel()*64, 8,8);
                ProtocolConduit = await MessageService.CreateConduitOnceAsync(
                    conduitName,
                    Source.ZeroConcurrencyLevel()*2,
                    true,
                    channelSource,
                    userData => new CcProtocBatch<Packet, CcDiscoveryMessage>(channelSource, channelSource.ZeroConcurrencyLevel())
                ).FastPath().ConfigureAwait(false);

                //get reference to a central mem pool
                if (ProtocolConduit != null)
                {
                    _arrayPool = ((CcProtocBatchSource<Packet, CcDiscoveryMessage>)ProtocolConduit.Source).ArrayPool;
                    if (_arrayPool != null)
                        return true;
                    
                    _logger.Fatal($"{Description}: {nameof(_arrayPool)} is null");
                }
                else
                    _logger.Fatal($"{Description}: {nameof(ProtocolConduit)} is null");

                if (ProtocolConduit != null) 
                    await ProtocolConduit.ZeroAsync(this).FastPath().ConfigureAwait(false);

                ProtocolConduit = null;
                
                return false;
            }
            
            return await base.ConstructAsync().FastPath().ConfigureAwait(false) && ProtocolConduit != null;
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            _currentBatch = null;
            _arrayPool = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(false);

            if (_currentBatch != null)
            {
                foreach (var ccDiscoveryBatch in _currentBatch)
                {
                    if(ccDiscoveryBatch == default)
                        break;
                            
                    await _batchMsgHeap.ReturnAsync(ccDiscoveryBatch).FastPath().ConfigureAwait(false);    
                }
                _arrayPool.Return(_currentBatch, true);
            }
            
            await _batchMsgHeap.ZeroManagedAsync<object>((batch,_) =>
            {
                batch.RemoteEndPoint = null;
                batch.Zero = null;
                batch.Message = null;
                batch.EmbeddedMsg = null;
                batch.HeapRef = null;
                return ValueTask.CompletedTask;
            }).FastPath().ConfigureAwait(false);
            
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
        private CcDiscoveryMessage[] _currentBatch;

        /// <summary>
        /// Batch item heap
        /// </summary>
        private IoHeap<CcDiscoveryMessage> _batchMsgHeap;

        /// <summary>
        /// message heap
        /// </summary>
        private ArrayPool<CcDiscoveryMessage> _arrayPool = ArrayPool<CcDiscoveryMessage>.Shared;

        /// <summary>
        /// CC Node
        /// </summary>
        protected CcCollective CcCollective => ((CcAdjunct)IoZero)?.CcCollective;

        ///// <summary>
        ///// Cc Identity
        ///// </summary>
        public CcDesignation CcId => CcCollective.CcId;


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
                                ByteStream.Seek(currentOffset, SeekOrigin.Begin);
                                curPos = ByteStream.Position;

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
                                $"Parse failed: r = {totalBytesProcessed}/{BytesRead}/{BytesLeftToProcess}, d = {DatumCount}, b={MemoryBuffer.Slice((int)(tmpBufferOffset - 2), 32).ToArray().HashSig()}, {Description}");

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
                        BufferOffset += (uint)read;
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
                    _logger.Trace($"<\\= {messageType ?? "Unknown"} {RemoteEndPoint} ~> {MessageService.IoNetSocket.LocalNodeAddress} ({CcCollective.CcId.IdString()})<<[{(verified ? "signed" : "un-signed")}]{packet.Data.Memory.PayloadSig()}:, <{MessageService.Description}> id = {Id}, o = {CurrBatchSlot}, r = {BytesRead}");

                    //Don't process unsigned or unknown messages
                    if (!verified || messageType == null)
                    {
                        continue;
                    }

                    switch ((MessageTypes)packet.Type)
                    {
                        case MessageTypes.Ping:
                            await ProcessRequestAsync<Ping>(packet).FastPath().ConfigureAwait(false);
                            break;
                        case MessageTypes.Pong:
                            await ProcessRequestAsync<Pong>(packet).FastPath().ConfigureAwait(false);
                            break;
                        case MessageTypes.DiscoveryRequest:
                            await ProcessRequestAsync<DiscoveryRequest>(packet).FastPath().ConfigureAwait(false);
                            break;
                        case MessageTypes.DiscoveryResponse:
                            await ProcessRequestAsync<DiscoveryResponse>(packet).FastPath().ConfigureAwait(false);
                            break;
                        case MessageTypes.PeeringRequest:
                            await ProcessRequestAsync<PeeringRequest>(packet).FastPath().ConfigureAwait(false);
                            break;
                        case MessageTypes.PeeringResponse:
                            await ProcessRequestAsync<PeeringResponse>(packet).FastPath().ConfigureAwait(false);
                            break;
                        case MessageTypes.PeeringDrop:
                            await ProcessRequestAsync<PeeringDrop>(packet).FastPath().ConfigureAwait(false);
                            break;
                        default:
                            _logger.Debug($"Unknown auto peer msg type = {packet.Type}");
                            break;
                    }

                }

                //TODO tuning
                if(CurrBatchSlot > _batchMsgHeap.MaxSize * 3 / 2)
                    _logger.Warn($"{nameof(_batchMsgHeap)} running lean {CurrBatchSlot}/{_batchMsgHeap.MaxSize}");
                //Release a waiter
                await ForwardToNeighborAsync().FastPath().ConfigureAwait(false);
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
                    if (PreviousJob.Syncing)
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
                        await ForwardToNeighborAsync().FastPath().ConfigureAwait(false);

                    var batchMsg = await _batchMsgHeap.TakeAsync().FastPath().ConfigureAwait(false);
                    if (batchMsg == null)
                        throw new OutOfMemoryException(nameof(_batchMsgHeap));

                    batchMsg.Zero = zero;
                    batchMsg.EmbeddedMsg = request;
                    batchMsg.RemoteEndPoint = RemoteEndPoint.ToString();
                    batchMsg.Message = packet;
                    batchMsg.HeapRef = _batchMsgHeap;

                    _currentBatch[CurrBatchSlot] = batchMsg;

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
                    _currentBatch[CurrBatchSlot] = default;
                }

                //cog the source
                await ProtocolConduit.Source.ProduceAsync<object>(static async (source, _, _, ioJob) =>
                {
                    var @this = (CcDiscoveries)ioJob;

                    try
                    {
                        if (source != null && !await ((CcProtocBatchSource<Packet, CcDiscoveryMessage>)source).EnqueueAsync(@this._currentBatch).FastPath().ConfigureAwait(false))
                        {
                            if (!((CcProtocBatchSource<Packet, CcDiscoveryMessage>)source).Zeroed())
                                _logger.Fatal($"{nameof(ForwardToNeighborAsync)}: Unable to q batch, {@this.Description}");
                            return false;
                        }
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e, $"{@this.Description}");
                        return false;
                    }

                    //Retrieve a new batch buffer
                    try
                    {
                        @this._currentBatch = @this._arrayPool.Rent((int)@this.parm_max_msg_batch_size);
                    }
                    catch (Exception e)
                    {
                        if(!@this.Zeroed())
                            _logger.Fatal(e, $"Unable to rent from mempool: {@this.Description}");
                        return false;
                    }

                    @this.CurrBatchSlot = 0;

                    return true;
                }, this).FastPath().ConfigureAwait(false);
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
