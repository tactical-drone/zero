using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using K4os.Compression.LZ4;
using zero.cocoon.autopeer;
using zero.cocoon.identity;
using zero.cocoon.models.batches;
using zero.core.conf;
using zero.core.feat.misc;
using zero.core.feat.models;
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
                var ac = 0;
                if (!Source.Proxy && ((CcAdjunct)IoZero)!.CcCollective.ZeroDrone)
                {
                    parm_max_msg_batch_size *= 2;
                    cc *= 2;
                    pf *= 2;
                    //ac *= 1;
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
            return base.Zeroed() || Source.Zeroed() || IoZero.Zeroed();
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

        /// <summary>
        /// Consumer discovery messages
        /// </summary>
        /// <returns>The task</returns>
        public override async ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            //are we in recovery mode?
            var zeroRecovery = State == IoJobMeta.JobState.ZeroRecovery;
            var pos = BufferOffset;

            try
            {
                //fail fast
                if (BytesRead == 0)
                    return State = IoJobMeta.JobState.BadData;
                
                var verified = false;
                
                //Ensure that the previous job (which could have been launched out of sync) has completed
                if (zeroRecovery)
                {
                    var recoverPrevJob = ((CcDiscoveries)PreviousJob).ZeroRecovery;
                    
                    var recovery = new ValueTask<bool>(recoverPrevJob, recoverPrevJob.Version);
                    if (await recovery.FastPath().ConfigureAwait(Zc))
                        AddRecoveryBits();
                    
                    State = IoJobMeta.JobState.Consuming;
                }

                while (BytesLeftToProcess > 0)
                {
                    chroniton packet = null;

                    var read = 0;
                    //deserialize
                    try
                    {
                        int length;
                        while ((length = ZeroSync()) > 0)
                        {
                            if (length > BytesLeftToProcess)
                            {
                                State = IoJobMeta.JobState.Fragmented;
                                break;
                            }
                            
                            try
                            {
                                var unpackOffset = DatumProvisionLengthMax << 2;
                                Array.Clear(Buffer, unpackOffset, Buffer.Length - unpackOffset - 1);
                                var packetLen = LZ4Codec.Decode(Buffer, BufferOffset + sizeof(ulong), length, Buffer, unpackOffset, Buffer.Length - unpackOffset - 1);

                                read = length + sizeof(ulong);
                                Interlocked.Add(ref BufferOffset, read);

                                var zeroIdx = unpackOffset + packetLen + 1;
                                var c = 4;
                                while (zeroIdx < Buffer.Length && c --> 0 )
                                {
                                    Buffer[zeroIdx++] = 0;
                                }
                                
                                if (packetLen > 0)
                                    packet = chroniton.Parser.ParseFrom(ReadOnlySequence.Slice(unpackOffset, packetLen));
                            }
#if DEBUG
                            catch (Exception e)
                            {

                                _logger.Trace(e, $"Parse failed: buf[{BufferOffset}], r = {BytesRead - BytesLeftToProcess }/{BytesRead}/{BytesLeftToProcess }, d = {DatumCount}, {Description}");
                            }
#else
                            catch
                            {
                                State = IoJobMeta.JobState.BadData;
                                //_logger.Trace(e, $"Parse failed: buf[{BufferOffset}], r = {BytesRead - BytesLeftToProcess }/{BytesRead}/{BytesLeftToProcess }, d = {DatumCount}, syncing = {InRecovery}, {Description}");
                            }
#endif
                        }

                        if (read == 0 && length == -1)
                        {
                            State = IoJobMeta.JobState.Fragmented;
                            break;
                        }
                    }
                    catch (Exception e) when(!Zeroed())
                    {
                        State = IoJobMeta.JobState.ConsumeErr;
                        _logger.Debug(e, $"Parse failed: buf[{BufferOffset}], r = {BytesRead - BytesLeftToProcess }/{BytesRead}/{BytesLeftToProcess }, d = {DatumCount}, {Description}");
                        break;
                    }

                    if (State != IoJobMeta.JobState.Consuming)
                        break;

                    //Sanity check the data
                    if (packet == null || packet.Data == null || packet.Data.Length == 0)
                    {
                        State = IoJobMeta.JobState.BadData;
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
                    _logger.Trace($"<\\= {messageType ?? "Unknown"} [{RemoteEndPoint.GetEndpoint()} ~> {MessageService.IoNetSocket.LocalNodeAddress}], ({CcCollective.CcId.IdString()})<<[{(verified ? "signed" : "un-signed")}]{packet.Data.Memory.PayloadSig()}: <{MessageService.Description}> id = {Id}, r = {BytesRead}");
#endif

                    //Don't process unsigned or unknown messages
                    if (!verified || messageType == null)
                    {
                        State = IoJobMeta.JobState.BadData;
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
                            State = IoJobMeta.JobState.BadData;
                            break;
                    }
                    
                }
                
                //TODO tuning
                if (_currentBatch.Count > _batchHeap.Capacity * 3 / 2)
                    _logger.Warn($"{nameof(_batchHeap)} running lean {_currentBatch.Count}/{_batchHeap.Capacity}, {_batchHeap}, {_batchHeap.Description}");
                //Release a waiter
                await ZeroBatchAsync().FastPath().ConfigureAwait(Zc);
            }
            catch when(Zeroed()){State = IoJobMeta.JobState.ConsumeErr;}
            catch (Exception e) when (!Zeroed())
            {
                State = IoJobMeta.JobState.ConsumeErr;
                _logger.Error(e, $"Unmarshal chroniton failed in {Description}");
            }
            finally
            {
                try
                {
                    
                    if (BytesLeftToProcess == 0)
                        State = IoJobMeta.JobState.Consumed;
                    else if(BytesLeftToProcess != BytesRead)
                        State = IoJobMeta.JobState.Fragmented;
#if DEBUG
                    else if (State == IoJobMeta.JobState.Fragmented)
                    {
                        _logger.Debug($"[{Id}] FRAGGED = {DatumCount}, {BytesRead}/{BytesLeftToProcess }");
                    }
                    else
                    {
                        _logger.Fatal($"[{Id}] FRAGGED = {DatumCount}, {BytesRead}/{BytesLeftToProcess }");
                    }
#else
                    else if (State == IoJobMeta.JobState.Fragmented && !IoZero.ZeroRecoveryEnabled)
                    {
                        State = IoJobMeta.JobState.BadData;
                    }
#endif
                }
                catch when(Zeroed()){ State = IoJobMeta.JobState.ConsumeErr; }
                catch (Exception e) when(!Zeroed())
                {
                    State = IoJobMeta.JobState.ConsumeErr;
                    _logger.Error(e, $"{nameof(State)}: re setting state failed!");
                }

            }

            ////attempt zero recovery
            if (!zeroRecovery && BytesLeftToProcess > 0 && IoZero.ZeroRecoveryEnabled && PreviousJob != null)
            {
                State = IoJobMeta.JobState.ZeroRecovery;
                await ConsumeAsync().FastPath().ConfigureAwait(Zc);
            }

            return State;
        }

        /// <summary>
        /// Synchronizes the stream
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ZeroSync()
        {
            var read = 0;
            var buf = (ReadOnlySpan<byte>)Buffer.AsSpan(BufferOffset);
            try
            {
                while (read + sizeof(ulong) < BytesLeftToProcess)
                {
                    if (MemoryMarshal.Read<uint>(buf[(sizeof(ulong)>>1 + read)..]) == 0)
                    {
                        var p = MemoryMarshal.Read<ulong>(buf[read..]);
                        if (p != 0 && p < ushort.MaxValue && p <= (ulong)BytesLeftToProcess)
                        {
                            return (int)p;
                        }
                    }

                    read++;
                }
                return read = -1;
            }
            finally
            {
                if(read != -1 && read > 0)
                    Interlocked.Add(ref BufferOffset, read);
            }
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
                    if (!_groupByEp)
                    {
                        var batchMsg = _currentBatch[Interlocked.Increment(ref _currentBatch.Count) - 1];
                        batchMsg.EmbeddedMsg = request;
                        batchMsg.Chroniton = packet;
                        RemoteEndPoint.CopyTo(batchMsg.EndPoint,0);
                    }
                    else
                    {
                        var batchMsg = _currentBatch[Interlocked.Increment(ref _currentBatch.Count) - 1];

                        batchMsg.EmbeddedMsg = request;
                        batchMsg.Chroniton = packet;

                        var remoteEp = (byte[])RemoteEndPoint.Clone();
                        if (!_currentBatch.GroupBy.TryAdd(remoteEp, Tuple.Create(remoteEp, new List<CcDiscoveryMessage>(_currentBatch.Messages))))
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
                                throw new OutOfMemoryException($"{@this.Description}: {nameof(@this._batchHeap)}, c = {@this._batchHeap.Count}/{@this._batchHeap.Capacity}, ref = {@this._batchHeap.ReferenceCount}");

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
