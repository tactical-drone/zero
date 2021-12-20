using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
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
        public CcDiscoveries(string sinkDesc, string jobDesc, IoSource<CcProtocMessage<chroniton, CcDiscoveryBatch>> source, bool groupByEp = false) : base(sinkDesc, jobDesc, source)
        {
            _groupByEp = groupByEp;
        }

        public override async ValueTask<bool> ConstructAsync(object localContext = null)
        {

            if (!_configured)
            {
                IoZero = (IIoZero)localContext;
                _configured = true;
                var cc = 2;
                var pf = 3;
                var ac = 1;
                if (!Source.Proxy && ((CcAdjunct)IoZero)!.CcCollective.ZeroDrone)
                {
                    parm_max_msg_batch_size *= 2;
                    //cc = 8;
                    //pf = 16;
                    //ac = 4;
                    //_groupByEp = true;
                }

#if DEBUG
                string bashDesc = $"{nameof(_batchHeap)}: {Description}";
#else
                string bashDesc = string.Empty;
#endif

                _batchHeap ??= new IoHeap<CcDiscoveryBatch, CcDiscoveries>(bashDesc, parm_max_msg_batch_size)
                {
                    Malloc = static (_, @this) => new CcDiscoveryBatch(@this._batchHeap, @this.parm_max_msg_batch_size, groupByEp:@this._groupByEp),
                    Context = this
                };

                _currentBatch ??= _batchHeap.Take();
                if (_currentBatch == null)
                    throw new OutOfMemoryException($"{Description}: {nameof(CcDiscoveries)}.{nameof(_currentBatch)}");

                const string conduitId = nameof(CcAdjunct);

                ProtocolConduit = await MessageService.CreateConduitOnceAsync<CcProtocBatchJob<chroniton, CcDiscoveryBatch>>(conduitId).FastPath().ConfigureAwait(Zc);

                if (ProtocolConduit == null)
                {
                    //TODO tuning
                    var channelSource = new CcProtocBatchSource<chroniton, CcDiscoveryBatch>(Description, MessageService, parm_max_msg_batch_size, pf, cc, ac, true);
                    ProtocolConduit = await MessageService.CreateConduitOnceAsync(
                        conduitId,
                        channelSource,
                        static (ioZero, _) => new CcProtocBatchJob<chroniton, CcDiscoveryBatch>(
                            (IoSource<CcProtocBatchJob<chroniton, CcDiscoveryBatch>>)((IIoConduit)ioZero).UpstreamSource, ((IIoConduit)ioZero).ZeroConcurrencyLevel()), cc).FastPath().ConfigureAwait(Zc);
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
            Scan = 12,
            Adjuncts = 13,
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

        /// <summary>
        /// Whether grouping by endpoint is supported
        /// </summary>
        private volatile bool _groupByEp;

        public override ValueTask<IoJobMeta.JobState> ProduceAsync<T>(IIoSource.IoZeroCongestion<T> barrier, T ioZero)
        {
            return base.ProduceAsync(barrier, ioZero);  
        }

        public override async ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            try
            {
                //fail fast
                if (BytesRead == 0 || Zeroed())
                    return State = IoJobMeta.JobState.ConInvalid;
                
                long totalBytesProcessed = 0;
                var verified = false;

                //if (BytesLeftToProcess < 4)
                //    return State = IoJobMeta.JobState.ConInlined;

                //int chunkSize = 0;
                //var chunk = ArraySegment[DatumProvisionLengthMax..];

                //chunkSize |= (byte)(chunk[3] & 0x000f);
                //chunkSize |= (byte)((chunk[2] << 4) & 0x00f0);
                //chunkSize |= (byte)((chunk[1] << 8) & 0x0f00);
                //chunkSize |= (byte)((chunk[0] << 12) & 0xf000);


                ByteStream.Seek(DatumProvisionLengthMax, SeekOrigin.Begin);
                while (totalBytesProcessed < BytesRead && State != IoJobMeta.JobState.ConInlined)
                {
                    chroniton packet = null;

                    long curPos = 0;
                    long read = 0;
                    //deserialize
                    try
                    {
                        try
                        {
                            var startPos = ByteStream.Position;
                            packet = chroniton.Parser.ParseDelimitedFrom(ByteStream);
                            //packet = chroniton.Parser.ParseDelimitedFrom(ReadOnlySequence.Slice(BufferOffset, BytesRead));
                            read = ByteStream.Position - startPos;
                        }
                        catch
                        {
                            try
                            {
                                ByteStream.Seek(DatumProvisionLengthMax, SeekOrigin.Begin);
                                curPos = ByteStream.Position;
                                packet = chroniton.Parser.ParseFrom(CodedStream);
                                read = (int)(ByteStream.Position - curPos);
                            }
                            catch
                            {
                                read = (int)(ByteStream.Position - curPos);
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

                        if (BytesRead == totalBytesProcessed)
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
                        }

                        continue;
                    }
                    finally
                    {
                        Interlocked.Add(ref BufferOffset, (int)read);
                    }

                    if (read == 0)
                        break;

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
                            await ZeroBatchRequestAsync(packet, CcProbeMessage.Parser).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.Probed:
                            await ZeroBatchRequestAsync(packet, CcProbeResponse.Parser).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.Scan:
                            await ZeroBatchRequestAsync(packet, CcScanRequest.Parser).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.Adjuncts:
                            await ZeroBatchRequestAsync(packet, CcAdjunctResponse.Parser).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.Fuse:
                            await ZeroBatchRequestAsync(packet, CcFuseRequest.Parser).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.Fused:
                            await ZeroBatchRequestAsync(packet, CcFuseResponse.Parser).FastPath().ConfigureAwait(Zc);
                            break;
                        case MessageTypes.Defuse:
                            await ZeroBatchRequestAsync(packet, CcDefuseRequest.Parser).FastPath().ConfigureAwait(Zc);
                            break;
                        default:
                            _logger.Debug($"Unknown auto peer msg type = {packet.Type}");
                            break;
                    }
                }

                //TODO tuning
                if(_currentBatch.Count > _batchHeap.MaxSize * 3 / 2)
                    _logger.Warn($"{nameof(_batchHeap)} running lean {_currentBatch.Count}/{_batchHeap.MaxSize}, {_batchHeap}, {_batchHeap.Description}");
                //Release a waiter
                await ZeroBatchAsync().FastPath().ConfigureAwait(Zc);
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
                            _logger.Debug($"[{Id}] FRAGGED = {DatumCount}, {BytesRead}/{BytesLeftToProcess}");
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
        /// Processes a generic request and adds it to a batch
        /// </summary>
        /// <param name="packet">The packet</param>
        /// <param name="messageParser"></param>
        /// <typeparam name="T">The expected type</typeparam>
        /// <returns>The task</returns>
        private async ValueTask ZeroBatchRequestAsync<T>(chroniton packet, MessageParser<T> messageParser)
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
                    //    await ZeroBatchAsync().FastPath().ConfigureAwait(Zc);
                    //}

                    //bool routingSuccess = true;

                    //byte[] ingressEpBytes;
                    //if (_currentBatch.Count == 0 || _currentBatch.RemoteEndPoint == null) 
                    //{
                    //    _currentBatch.RemoteEndPoint = ingressEpBytes = RemoteEndPoint.AsBytes(_currentBatch.RemoteEndPoint);
                    //}
                    //else
                    //{
                    //    ingressEpBytes = RemoteEndPoint.AsBytes();
                    //}

                    

                    if (!_groupByEp)
                    {
                        var batchMsg = _currentBatch[Interlocked.Increment(ref _currentBatch.Count) - 1];
                        batchMsg.EmbeddedMsg = request;
                        batchMsg.Message = packet;
                        batchMsg.EndPoint = RemoteEndPoint;
                    }
                    else
                    {
                        var batchMsg = _currentBatch[Interlocked.Increment(ref _currentBatch.Count) - 1];

                        batchMsg.EmbeddedMsg = request;
                        batchMsg.Message = packet;

                        var remoteEp = RemoteEndPoint;
                        if (!_currentBatch.GroupBy.TryAdd(RemoteEndPoint, Tuple.Create(remoteEp, new List<CcDiscoveryMessage>(_currentBatch.Messages))))
                        {
                            _currentBatch.GroupBy[remoteEp].Item2.Add(batchMsg);
                        }
                    }

                    if (_currentBatch.Count >= parm_max_msg_batch_size)
                    {
                        await ZeroBatchAsync().FastPath().ConfigureAwait(Zc);
                    }

                }
            }
            catch when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e,
                    $"Unable to parse request type {typeof(T).Name} from {Convert.ToBase64String(packet.PublicKey.Memory.AsArray())}, size = {packet.Data.Length}");
            }
        }

        /// <summary>
        /// Forwards a batch of messages
        /// </summary>
        /// <returns>Task</returns>
        private async ValueTask ZeroBatchAsync()
        {
            try
            {
                if (_currentBatch.Count == 0 || Zeroed())
                    return;

                //cog the source
                if (!await ProtocolConduit.UpstreamSource.ProduceAsync<object>(static async (source, _, _, ioJob) =>
                    {
                        var @this = (CcDiscoveries)ioJob;

                        try
                        {
                            if (source == null || !await ((CcProtocBatchSource<chroniton, CcDiscoveryBatch>)source).EnqueueAsync(@this._currentBatch).FastPath().ConfigureAwait(@this.Zc))
                            {
                                if (source != null && !((CcProtocBatchSource<chroniton, CcDiscoveryBatch>)source).Zeroed())
                                {
                                    _logger.Fatal($"{nameof(ZeroBatchAsync)}: Unable to q batch, {@this.Description}");
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
                    }, this, null, default).FastPath().ConfigureAwait(Zc))
                {
                    if(!Zeroed())
                        _logger.Debug($"{nameof(ZeroBatchAsync)} - {Description}: Failed to produce jobs from {ProtocolConduit.Description}");
                }
            }
            catch when(Zeroed() || ProtocolConduit?.UpstreamSource == null) {}
            catch (Exception e) when (!Zeroed() && ProtocolConduit is { UpstreamSource: { } } && !ProtocolConduit.UpstreamSource.Zeroed())
            {
                _logger.Fatal(e, $"Forwarding from {Description} to {ProtocolConduit.Description} failed");
            }
        }
    }
}
