using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using SimpleBase;
using zero.cocoon.autopeer;
using zero.cocoon.identity;
using zero.cocoon.models.batches;
using zero.core.conf;
using zero.core.feat.misc;
using zero.core.feat.models.protobuffer;
using zero.core.feat.models.protobuffer.sources;
using zero.core.misc;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using Zero.Models.Protobuf;

namespace zero.cocoon.models
{
    public class CcDiscoveries : CcProtocMessage<chroniton, CcDiscoveryBatch>
    {
        public CcDiscoveries(string sinkDesc, string jobDesc, IoSource<CcProtocMessage<chroniton, CcDiscoveryBatch>> source) : base(sinkDesc, jobDesc, source)
        {
            
        }

        public override async ValueTask<bool> ConstructAsync(object localContext)
        {

            if (!_configured)
            {
                IoZero = (IIoZero)localContext;
                _configured = true;
                if (!Source.Proxy && ((CcAdjunct)IoZero).CcCollective.ZeroDrone)
                {
                    parm_max_msg_batch_size *= 10;
                }

                string bashDesc;
#if DEBUG
                bashDesc = $"{nameof(_batchHeap)}: {Description}";
#else
                bashDesc = string.Empty;
#endif


                _batchHeap ??= new IoHeap<CcDiscoveryBatch, CcDiscoveries>(bashDesc, parm_max_msg_batch_size)
                {
                    Make = static (o, c) => new CcDiscoveryBatch(c._batchHeap, c.parm_max_msg_batch_size * 2),
                    Context = this,
                    Prep = (batch, _) => { batch.RemoteEndPoint = null; }
                };

                _currentBatch ??= _batchHeap.Take();
                if (_currentBatch == null)
                    throw new OutOfMemoryException($"{Description}: {nameof(CcDiscoveries)}.{nameof(_currentBatch)}");

                var conduitId = nameof(CcAdjunct);
                ProtocolConduit = await MessageService.CreateConduitOnceAsync<CcProtocBatchJob<chroniton, CcDiscoveryBatch>>(conduitId).FastPath().ConfigureAwait(Zc);

                var batchSize = parm_max_msg_batch_size;
                var cc = 8;
                if (ProtocolConduit == null)
                {
                    //TODO tuning
                    var channelSource = new CcProtocBatchSource<chroniton, CcDiscoveryBatch>(Description, MessageService, batchSize, cc * 2, cc, cc / 2);
                    ProtocolConduit = await MessageService.CreateConduitOnceAsync(
                        conduitId,
                        cc,
                        channelSource,
                        static (o, s) => new CcProtocBatchJob<chroniton, CcDiscoveryBatch>((IoSource<CcProtocBatchJob<chroniton, CcDiscoveryBatch>>)s, s.ZeroConcurrencyLevel())
                    ).FastPath().ConfigureAwait(Zc);
                }
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
            _batchHeap = null;
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
            Probe = 10,
            Probed = 11,
            Sweep = 12,
            Swept = 13,
            Fuse = 20,
            Fused = 21,
            Defuse = 22
        }

        /// <summary>
        /// Whether configuration has been applied
        /// </summary>
        private bool _configured;

        /// <summary>
        /// A description
        /// </summary>
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

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_msg_batch_size = 16;//TODO tuning 4 x MaxAdjuncts

        public override ValueTask<IoJobMeta.JobState> ProduceAsync<T>(Func<IIoJob, T, ValueTask<bool>> barrier, T nanite)
        {
            _currentBatch.Count = 0;
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
                    chroniton packet = null;
                    long curPos = 0;
                    var read = 0;
                    
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
                            packet = chroniton.Parser.ParseFrom(ReadOnlySequence.Slice(BufferOffset, BytesRead));
                            read = packet.CalculateSize();
                        }
                        catch
                        {
                            try
                            {
                                ByteStream.Seek(BufferOffset, SeekOrigin.Begin);
                                curPos = ByteStream.Position;

                                packet = chroniton.Parser.ParseFrom(CodedStream);
                                read = (int)(ByteStream.Position - curPos);
                            }
                            catch
                            {
                                // ignored
                            }

                            //catch
                            {
                                //if (!__enableZeroCopyDebug)
                                    //throw;

                                //packet = chroniton.Parser.ParseFrom(__zeroCopyDebugBuffer.AsSpan().Slice(BufferOffset, BytesRead).ToArray());
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
                        Interlocked.Add(ref BufferOffset, read);
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
                        verified = CcDesignation.Verify(packetMsgRaw, 0, packetMsgRaw.Length, packet.PublicKey.Memory.AsArray(),
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
                        case MessageTypes.Probe:
                            await ProcessRequestAsync(packet, CcProbeMessage.Parser).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.Probed:
                            await ProcessRequestAsync(packet, CcProbeResponse.Parser).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.Sweep:
                            await ProcessRequestAsync(packet, CcSweepRequest.Parser).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.Swept:
                            await ProcessRequestAsync(packet, CcSweepResponse.Parser).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.Fuse:
                            await ProcessRequestAsync(packet, CcFuseRequest.Parser).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.Fused:
                            await ProcessRequestAsync(packet, CcFuseResponse.Parser).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.Defuse:
                            await ProcessRequestAsync(packet, CcDefuseRequest.Parser).FastPath().ConfigureAwait(Zc);
                            break;
                        default:
                            _logger.Debug($"Unknown auto peer msg type = {packet.Type}");
                            break;
                    }
                }

                //TODO tuning
                if(_currentBatch.Count > _batchHeap.MaxSize * 3 / 2)
                    _logger.Warn($"{nameof(_batchHeap)} running lean {_currentBatch.Count}/{_batchHeap.MaxSize}, {_batchHeap}");
                //Release a waiter
                await ForwardToNeighborAsync().FastPath().ConfigureAwait(Zc);
            }
            catch when(!Zeroed()){}
            catch (Exception e) when (Zeroed())
            {
                _logger.Error(e, $"Unmarshal chroniton failed in {Description}");
            }
            finally
            {
                try
                {
                    if (State != IoJobMeta.JobState.Consumed)
                    {
                        if (PreviousJob is { Syncing: true })
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
                catch when(Zeroed()){}
                catch (Exception e) when(!Zeroed())
                {
                    _logger.Error(e, $"{nameof(State)}: re setting state failed!");
                }                
            }

            return State;
        }

        /// <summary>
        /// Processes a generic request
        /// </summary>
        /// <param name="packet">The packet</param>
        /// <param name="messageParser"></param>
        /// <typeparam name="T">The expected type</typeparam>
        /// <returns>The task</returns>
        private async ValueTask ProcessRequestAsync<T>(chroniton packet, MessageParser<T> messageParser)
            where T : IMessage<T>, IMessage, new()
        {
            try
            {
                //TODO opt:
                var request = messageParser.ParseFrom(packet.Data);
                
                if (request != null)
                {
                    //_logger.Debug($"[{Base58Check.Base58CheckEncoding.Encode(packet.PublicKey.ToByteArray())}]{typeof(T).Name}: Received {packet.Data.Length}" );
                    //IIoZero zero = null;
                    //if (((IoNetClient<CcPeerMessage>)Source).Socket.FfAddress != null)
                    //    zero = IoZero;

                    //var ingressEp = packet.Header.Ip.Src.GetEndpoint();

                    //byte[] ingressEpBytes;
                    //if (_currentBatch.Count == 0)
                    //{
                    //    _currentBatch.RemoteEndPoint = ingressEpBytes = ingressEp.AsBytes(_currentBatch.RemoteEndPoint);
                    //}
                    //else
                    //{
                    //    ingressEpBytes = ingressEp.AsBytes();
                    //}

                    //if (!Equals(RemoteEndPoint, ingressEp))
                    //{
                    //    _logger.Fatal($"{nameof(CcDiscoveries)}: Bad external routing, got {RemoteEndPoint}, wanted {ingressEp}, count = {_currentBatch.Count}");
                    //}

                    //bool routingSuccess = true;
                    //if (_currentBatch.Count >= parm_max_msg_batch_size - 2 || !(routingSuccess = _currentBatch.RemoteEndPoint.ArrayEqual(ingressEpBytes)))
                    //{
                    //    if (!routingSuccess)
                    //    {
                    //        _logger.Warn($"{nameof(CcDiscoveries)}: Internal msg routing SRC fragmented, got {_currentBatch.RemoteEndPoint.GetEndpoint()}, wanted {ingressEp}, count = {_currentBatch.Count}");
                    //    }
                    //    await ForwardToNeighborAsync().FastPath().ConfigureAwait(Zc);
                    //}

                    bool routingSuccess = true;

                    byte[] ingressEpBytes;
                    if (_currentBatch.Count == 0 || _currentBatch.RemoteEndPoint == null) 
                    {
                        _currentBatch.RemoteEndPoint = ingressEpBytes = RemoteEndPoint.AsBytes(_currentBatch.RemoteEndPoint);
                    }
                    else
                    {
                        ingressEpBytes = RemoteEndPoint.AsBytes();
                    }

                    if (_currentBatch.Count >= parm_max_msg_batch_size || !(routingSuccess = _currentBatch.RemoteEndPoint.ArrayEqual(ingressEpBytes)))
                    {
                        if (!routingSuccess)
                        {
                            _logger.Warn($"{nameof(CcDiscoveries)}: Internal msg routing SRC fragmented: next = {ingressEpBytes}, prev = {_currentBatch.RemoteEndPoint.GetEndpoint()}, count = {_currentBatch.Count}");
                        }
                        await ForwardToNeighborAsync().FastPath().ConfigureAwait(Zc);
                        _currentBatch.RemoteEndPoint = ingressEpBytes;
                    }

                    var batchMsg = _currentBatch[Interlocked.Increment(ref _currentBatch.Count) - 1];
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
                if (_currentBatch.Count == 0 || Zeroed())
                    return;

                //cog the source
                if (!await ProtocolConduit.Source.ProduceAsync<object>(static async (source, _, _, ioJob) =>
                {
                    var @this = (CcDiscoveries)ioJob;

                    try
                    {
                        if (source == null || !await ((CcProtocBatchSource<chroniton, CcDiscoveryBatch>)source).EnqueueAsync(@this._currentBatch).FastPath().ConfigureAwait(@this.Zc))
                        {
                            if (source != null && !((CcProtocBatchSource<chroniton, CcDiscoveryBatch>)source).Zeroed())
                            {
                                _logger.Fatal($"{nameof(ForwardToNeighborAsync)}: Unable to q batch, {@this.Description}");
                            }
                            return false;
                        }

                        @this._currentBatch = @this._batchHeap.Take();
                        if (@this._currentBatch == null)
                            throw new OutOfMemoryException($"{@this.Description}: {nameof(@this._batchHeap)}, c = {@this._batchHeap.Count}/{@this._batchHeap.MaxSize}, ref = {@this._batchHeap.ReferenceCount}");

                        @this._currentBatch.Count = 0;

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
