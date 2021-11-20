using System;
using System.IO;
using System.Runtime.CompilerServices;
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
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.cocoon.models
{
    public class CcDiscoveries : CcProtocMessage<Packet, CcDiscoveryBatch>
    {
        public CcDiscoveries(string sinkDesc, string jobDesc, IoSource<CcProtocMessage<Packet, CcDiscoveryBatch>> source) : base(sinkDesc, jobDesc, source)
        {
            _batchHeap = new IoHeap<CcDiscoveryBatch, CcDiscoveries>($"{nameof(_batchHeap)}: {sinkDesc} ~> {jobDesc}", parm_max_msg_batch_size){ Make = static (o, c) => new CcDiscoveryBatch(c._batchHeap,c.parm_max_msg_batch_size), Context = this};

            _currentBatch = _batchHeap.Take();
            if (_currentBatch == null)
                throw new OutOfMemoryException($"{sinkDesc}: {nameof(CcDiscoveries)}.{nameof(_currentBatch)}");
        }

        public override async ValueTask<bool> ConstructAsync()
        {
            var conduitId = nameof(CcAdjunct);
            ProtocolConduit = await MessageService.CreateConduitOnceAsync<CcProtocBatchJob<Packet, CcDiscoveryBatch>>(conduitId).FastPath().ConfigureAwait(Zc);

            var batchSize = 64;
            var cc = 32;
            if (ProtocolConduit == null)
            {
                //TODO tuning
                var channelSource = new CcProtocBatchSource<Packet, CcDiscoveryBatch>(Description, MessageService, batchSize, cc, cc, 0, 0);
                ProtocolConduit = await MessageService.CreateConduitOnceAsync(
                    conduitId,
                    cc,
                    channelSource,
                    static (o,s) => new CcProtocBatchJob<Packet, CcDiscoveryBatch>((IoSource<CcProtocBatchJob<Packet, CcDiscoveryBatch>>)s, s.ZeroConcurrencyLevel())
                ).FastPath().ConfigureAwait(Zc);
            }
            
            return await base.ConstructAsync().FastPath().ConfigureAwait(Zc) && ProtocolConduit != null;
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            _currentBatch = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);

            _currentBatch?.Dispose();
            //if (_currentBatch != null)
            //{

            //    for (int i = 0; i < _currentBatch.Count; i++)
            //    {
            //        var ccDiscoveryBatch = _currentBatch[i];
            //        if (ccDiscoveryBatch == default)
            //            break;

            //        await _batchMsgHeap.ReturnAsync(ccDiscoveryBatch).FastPath().ConfigureAwait(ZC);
            //    }
            //}
            
            //await _batchMsgHeap.ZeroManagedAsync<object>(static (batch,_) =>
            //{
            //    batch.Message = null;
            //    batch.EmbeddedMsg = null;
            //    return default;
            //}).FastPath().ConfigureAwait(ZC);

            await _batchHeap.ZeroManagedAsync<object>(static (batch, _) =>
            {
                batch.Dispose();
                return default;
            }).FastPath().ConfigureAwait(Zc);
        }

        /// <summary>
        /// If we are in teardown
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Zeroed()
        {
            return base.Zeroed() || Source.Zeroed();
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

        public override string Description => $"{base.Description}: {Source?.Description}";

        /// <summary>
        /// Batch of messages
        /// </summary>
        private CcDiscoveryBatch _currentBatch;

        /// <summary>
        /// Batch item heap
        /// </summary>
        private IoHeap<CcDiscoveryBatch, CcDiscoveries> _batchHeap;

        /// <summary>
        /// Batch item heap
        /// </summary>
        //private IoHeap<CcDiscoveryMessage, CcDiscoveries> _batchMsgHeap;

        /// <summary>
        /// CC Node
        /// </summary>
        protected CcCollective CcCollective => ((CcAdjunct)IoZero)?.CcCollective;

        ///// <summary>
        ///// Cc Identity
        ///// </summary>
        public CcDesignation CcId => CcCollective.CcId;

        public override ValueTask<IoJobMeta.JobState> ProduceAsync<T>(Func<IIoJob, T, ValueTask<bool>> barrier, T nanite)
        {
            _currentBatch.Filled = 0;
            return base.ProduceAsync(barrier, nanite);  
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
                        //if (IoZero.SyncRecoveryModeEnabled && totalBytesProcessed == BytesRead && State != IoJobMeta.JobState.Consumed)//TODO hacky
                        //{
                        //    read = 0;
                        //    State = IoJobMeta.JobState.ConInlined;
                        //}

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
                        BufferOffset += read;
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
#if DEBUG
                    _logger.Trace($"<\\= {messageType ?? "Unknown"} [{RemoteEndPoint} ~> {MessageService.IoNetSocket.LocalNodeAddress}], ({CcCollective.CcId.IdString()})<<[{(verified ? "signed" : "un-signed")}]{packet.Data.Memory.PayloadSig()}: <{MessageService.Description}> id = {Id}, r = {BytesRead}");
#endif

                    //Don't process unsigned or unknown messages
                    if (!verified || messageType == null)
                    {
                        continue;
                    }

                    switch ((MessageTypes)packet.Type)
                    {
                        case MessageTypes.Ping:
                            await ProcessRequestAsync<Ping>(packet).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.Pong:
                            await ProcessRequestAsync<Pong>(packet).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.DiscoveryRequest:
                            await ProcessRequestAsync<DiscoveryRequest>(packet).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.DiscoveryResponse:
                            await ProcessRequestAsync<DiscoveryResponse>(packet).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.PeeringRequest:
                            await ProcessRequestAsync<PeeringRequest>(packet).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.PeeringResponse:
                            await ProcessRequestAsync<PeeringResponse>(packet).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.PeeringDrop:
                            await ProcessRequestAsync<PeeringDrop>(packet).FastPath().ConfigureAwait(Zc);
                            break;
                        default:
                            _logger.Debug($"Unknown auto peer msg type = {packet.Type}");
                            break;
                    }
                }

                //TODO tuning
                if(_currentBatch.Filled > _batchHeap.MaxSize * 3 / 2)
                    _logger.Warn($"{nameof(_batchHeap)} running lean {_currentBatch.Filled}/{_batchHeap.MaxSize}, {_batchHeap}");
                //Release a waiter
                await ForwardToNeighborAsync().FastPath().ConfigureAwait(Zc);
            }
            catch when(!Zeroed()){}
            catch (Exception e) when (Zeroed())
            {
                _logger.Error(e, $"Unmarshal Packet failed in {Description}");
            }
            finally
            {
                try
                {
                    if (State != IoJobMeta.JobState.Consumed)
                    {
                        if (PreviousJob!= null && PreviousJob.Syncing)
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
                catch (Exception e) when(!Zeroed())
                {
                    _logger.Error(e);
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
                    //IIoZero zero = null;
                    //if (((IoNetClient<CcPeerMessage>)Source).Socket.FfAddress != null)
                    //    zero = IoZero;

                    if (_currentBatch.Filled == parm_max_msg_batch_size - 2)
                        await ForwardToNeighborAsync().FastPath().ConfigureAwait(Zc);

                    var batchMsg = _currentBatch[_currentBatch.Filled++];
                    batchMsg.EmbeddedMsg = request;
                    batchMsg.Message = packet;
                }
            }
            catch when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
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
                if (_currentBatch.Filled == 0 || Zeroed())
                    return;

                _currentBatch.RemoteEndPoint = RemoteEndPoint.ToString();
                //cog the source
                if (!await ProtocolConduit.Source.ProduceAsync<object>(static async (source, _, _, ioJob) =>
                {
                    var @this = (CcDiscoveries)ioJob;

                    try
                    {
                        if (source == null || !await ((CcProtocBatchSource<Packet, CcDiscoveryBatch>)source).EnqueueAsync(@this._currentBatch).FastPath().ConfigureAwait(@this.Zc))
                        {
                            if (source != null && !((CcProtocBatchSource<Packet, CcDiscoveryBatch>)source).Zeroed())
                            {
                                _logger.Fatal($"{nameof(ForwardToNeighborAsync)}: Unable to q batch, {@this.Description}");
                            }
                            return false;
                        }

                        @this._currentBatch = @this._batchHeap.Take();
                        if (@this._currentBatch == null)
                            throw new OutOfMemoryException($"{@this.Description}: {nameof(@this._batchHeap)}, c = {@this._batchHeap.Count}/{@this._batchHeap.MaxSize}, ref = {@this._batchHeap.ReferenceCount}");

                        @this._currentBatch.Filled = 0;

                        return true;
                    }
                    catch (Exception) when (@this.Zeroed() ) { }
                    catch (Exception e) when (!@this.Zeroed() )
                    {
                        _logger.Error(e, $"{@this.Description} - Forward failed!");
                    }

                    return false;
                }, this).FastPath().ConfigureAwait(Zc))
                {
                    if(!Zeroed())
                        _logger.Debug($"{nameof(ForwardToNeighborAsync)} - {Description}: Failed to produce jobs from {ProtocolConduit.Description}");
                }
            }
            catch when(Zeroed() || ProtocolConduit?.Source == null) {}
            catch (Exception e) when (!Zeroed() && ProtocolConduit is { Source: { } } && !ProtocolConduit.Source.Zeroed())
            {
                _logger.Fatal(e, $"Forwarding from {Description} to {ProtocolConduit.Description} failed");
            }
        }
    }
}
