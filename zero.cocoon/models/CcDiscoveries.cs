using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Google.Protobuf;
using K4os.Compression.LZ4;
using zero.cocoon.autopeer;
using zero.cocoon.identity;
using zero.cocoon.models.batches;
using zero.core.conf;
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

        public override async ValueTask<IIoHeapItem> HeapConstructAsync(object context)
        {
            if (ProtocolConduit != null)
                return null;

            IoZero = (IoZero<CcProtocMessage<chroniton, CcDiscoveryBatch>>)context;
            
            var pf = Source.PrefetchSize * 3;
            var cc = Source.ZeroConcurrencyLevel * 3; 

            if (!Source.Proxy && Adjunct.CcCollective.ZeroDrone)
            {
                parm_max_msg_batch_size *= 2;
                pf = Source.PrefetchSize * 3;
                cc = Source.ZeroConcurrencyLevel * 3;
            }

#if DEBUG
            string bashDesc = $"{nameof(_batchHeap)}: {Description}";
#else
            string bashDesc = string.Empty;
#endif
            //Create the conduit source
            const string conduitId = nameof(CcAdjunct);
            ProtocolConduit = await MessageService.CreateConduitOnceAsync<CcProtocBatchJob<chroniton, CcDiscoveryBatch>>(conduitId).FastPath();

            //Set the heap
            //TODO: tuning
            _batchHeap = new IoHeap<CcDiscoveryBatch, CcDiscoveries>(bashDesc, IoZero.parm_io_batch_size, static (_, @this) => 
                new CcDiscoveryBatch(@this._batchHeap, @this.parm_max_msg_batch_size, @this._groupByEp), autoScale:true)
                {
                    Context = this
                };

            // ReSharper disable once NonAtomicCompoundOperator
            _currentBatch ??= _batchHeap.Take();
            if (_currentBatch == null)
                throw new OutOfMemoryException($"{Description}: {nameof(CcDiscoveries)}.{nameof(_currentBatch)}");

            //create the channel
            if (ProtocolConduit == null)
            {
                //TODO tuning
                var source = new CcProtocBatchSource<chroniton, CcDiscoveryBatch>(Description, MessageService, pf, cc);
                ProtocolConduit = await MessageService.CreateConduitOnceAsync(conduitId, source,static (ioZero, _) => 
                    new CcProtocBatchJob<chroniton, CcDiscoveryBatch>((IoSource<CcProtocBatchJob<chroniton, CcDiscoveryBatch>>)((IIoZero)ioZero).IoSource, ((IIoZero)ioZero).ZeroConcurrencyLevel), cc).FastPath();
            }

            return this;
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
            await base.ZeroManagedAsync().FastPath();

            await _batchHeap.ZeroManagedAsync<object>(static (batch, _) =>
            {
                batch.Dispose();
                return default;
            }).FastPath();

            _currentBatch?.Dispose();
        }

        /// <summary>
        /// If we are in teardown
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Zeroed()
        {
            return base.Zeroed() || Source.Zeroed() || IoZero.Zeroed() || (ProtocolConduit?.Zeroed()??false);
        }

        /// <summary>
        /// Discovery Message types
        /// </summary>
        public enum MessageTypes
        {
            Undefined       = 0,
            Handshake       = 1,
            Probe           = 2,
            ProbeResponse   = 3,
            Scan            = 4,
            ScanResponse    = 5,
            Fuse            = 6,
            FuseResponse    = 7,
            Defuse          = 8
        }

        /// <summary>
        /// A description
        /// </summary>
        public override string Description => $"{base.Description} <- {Source?.Description}";

        /// <summary>
        /// Batch of messages
        /// </summary>
        private volatile CcDiscoveryBatch _currentBatch;

        /// <summary>
        /// Batch item heap
        /// </summary>
        private volatile IoHeap<CcDiscoveryBatch, CcDiscoveries> _batchHeap;

        /// <summary>
        /// CC Node
        /// </summary>
        protected CcCollective CcCollective => ((CcAdjunct)IoZero)?.CcCollective;

        /// <summary>
        /// The adjunct this job belongs to
        /// </summary>
        public CcAdjunct Adjunct => (CcAdjunct)IoZero;

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_msg_batch_size = 4;
        //TODO: tuning. The showcase implementation never goes beyond 1 because of how UDP delivers packets from the same endpoint... batching is pointless here.

        /// <summary>
        /// Whether grouping by endpoint is supported
        /// </summary>
        private readonly bool _groupByEp;

        /// <summary>
        /// Consumer discovery messages
        /// </summary>
        /// <returns>The task</returns>
        public override async ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            //are we in recovery mode?
            var zeroRecovery = State == IoJobMeta.JobState.ZeroRecovery;

            var connReset = State == IoJobMeta.JobState.ProdConnReset;
            await SetStateAsync(IoJobMeta.JobState.Consuming).FastPath();

            bool fastPath = false;

            if (connReset)
            {
                await ZeroBatchRequestAsync(null, chroniton.Parser).FastPath();
                await SetStateAsync(IoJobMeta.JobState.Consumed);
            }

            try
            {
                //fail fast
                if (BytesRead == 0)
                    return await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();

                //Ensure that the previous job (which could have been launched out of sync) has completed
                var prevJob = ((CcDiscoveries)PreviousJob)?.ZeroRecovery;
                if (prevJob != null)
                {
                    fastPath = IoZero.ZeroRecoveryEnabled && prevJob.GetStatus() == ValueTaskSourceStatus.Succeeded;
                    
                    if (zeroRecovery || fastPath)
                    {
                        if (fastPath && prevJob.GetResult(0))
                        {
                            await AddRecoveryBitsAsync().FastPath();
                        }
                        else
                        {
                            var prevJobTask = new ValueTask<bool>(prevJob, 0);
                            if (await prevJobTask.FastPath())
                                await AddRecoveryBitsAsync().FastPath();
                        }
                        if (zeroRecovery)
                            await SetStateAsync(IoJobMeta.JobState.Consuming).FastPath();
                    }
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
                            try
                            {
                                var unpackOffset = DatumProvisionLengthMax << 2;
                                
                                var packetLen = LZ4Codec.Decode(Buffer, BufferOffset + sizeof(ulong), length, Buffer, unpackOffset, Buffer.Length - unpackOffset);

#if TRACE
                                _logger.Trace($"<\\= {Source.Key} -> {Buffer[BufferOffset..(BufferOffset + length + sizeof(ulong))].PayloadSig("C")} -> {Buffer[unpackOffset..(unpackOffset + packetLen)].PayloadSig()}");
                                //_logger.Trace($"<-\\ {Buffer[unpackOffset..(unpackOffset + packetLen)].Print()}");
#endif

                                Interlocked.Add(ref BufferOffset, read = length + sizeof(ulong));
                                

                                if (packetLen > 0)
                                    packet = chroniton.Parser.ParseFrom(Buffer, unpackOffset, packetLen);
                                break;
                            }
#if DEBUG
                            catch (Exception e)
                            {
                                await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();
                                _logger.Trace(e, $"Parse failed: buf[{BufferOffset}], r = {BytesRead - BytesLeftToProcess }/{BytesRead}/{BytesLeftToProcess }, d = {DatumCount}, {Description}");
                            }
#else
                            catch
                            {
                                await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();
                                //_logger.Trace(e, $"Parse failed: buf[{BufferOffset}], r = {BytesRead - BytesLeftToProcess }/{BytesRead}/{BytesLeftToProcess }, d = {DatumCount}, syncing = {InRecovery}, {Description}");
                            }
#endif
                        }

                        if (read == 0 && length == -1)
                        {
                            await SetStateAsync(IoJobMeta.JobState.Fragmented).FastPath();
                            break;
                        }
                    }
                    catch (Exception e) when(!Zeroed())
                    {
                        await SetStateAsync(IoJobMeta.JobState.ConsumeErr).FastPath();
                        _logger.Debug(e, $"Parse failed: buf[{BufferOffset}], r = {BytesRead - BytesLeftToProcess }/{BytesRead}/{BytesLeftToProcess }, d = {DatumCount}, {Description}");
                        break;
                    }

                    if (State != IoJobMeta.JobState.Consuming)
                        break;

                    //Sanity check the data
                    if (packet == null || packet.Data.IsEmpty || packet.PublicKey.IsEmpty || packet.Signature.IsEmpty)
                    {
                        await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();
                        continue;
                    }

                    var packetMsgRaw = packet.Data.Memory.AsArray();
                    var verified = CcDesignation.Verify(packetMsgRaw, 0, packetMsgRaw.Length, packet.PublicKey.Memory.AsArray(), 0, packet.Signature.Memory.AsArray(), 0);
                    
                    var messageType = Enum.GetName(typeof(MessageTypes), packet.Type);
#if TRACE
                    _logger.Trace($"<\\= {messageType ?? "Unknown"} [{RemoteEndPoint.GetEndpoint()} ~> {MessageService.IoNetSocket.LocalNodeAddress}], ({CcCollective.CcId.IdString()})<<[{(verified ? "signed" : "un-signed")}]{packet.Data.Memory.PayloadSig()}: <{MessageService.Description}> id = {Id}, r = {BytesRead}");
#endif

                    //Don't process unsigned or unknown messages
                    if (!verified || messageType == null)
                    {
                        await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();
                        continue;
                    }
                    
                    switch ((MessageTypes)packet.Type)
                    {
                        case MessageTypes.Probe:
                            await ZeroBatchRequestAsync(packet, CcProbeMessage.Parser).FastPath();
                            break;
                        case MessageTypes.ProbeResponse:
                            await ZeroBatchRequestAsync(packet, CcProbeResponse.Parser).FastPath();
                            break;
                        case MessageTypes.Scan:
                            await ZeroBatchRequestAsync(packet, CcScanRequest.Parser).FastPath();
                            break;
                        case MessageTypes.ScanResponse:
                            await ZeroBatchRequestAsync(packet, CcAdjunctResponse.Parser).FastPath();
                            break;
                        case MessageTypes.Fuse:
                            await ZeroBatchRequestAsync(packet, CcFuseRequest.Parser).FastPath();
                            break;
                        case MessageTypes.FuseResponse:
                            await ZeroBatchRequestAsync(packet, CcFuseResponse.Parser).FastPath();
                            break;
                        case MessageTypes.Defuse:
                            await ZeroBatchRequestAsync(packet, CcDefuseRequest.Parser).FastPath();
                            break;
                        default:
                            _logger.Debug($"Unknown auto peer msg type = {packet.Type}");
                            await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();
                            break;
                    }
                    
                }
                
                //TODO tuning
                if (_currentBatch.Count > _batchHeap.Capacity * 3 / 2)
                    _logger.Warn($"{nameof(_batchHeap)} running lean {_currentBatch.Count}/{_batchHeap.Capacity}, {_batchHeap}, {_batchHeap.Description}");

                //Release a waiter
                await ZeroBatchAsync().FastPath();
            }
            catch when(Zeroed()){await SetStateAsync(IoJobMeta.JobState.ConsumeErr).FastPath();}
            catch (Exception e) when (!Zeroed())
            {
                await SetStateAsync(IoJobMeta.JobState.ConsumeErr).FastPath();
                _logger.Error(e, $"Unmarshal chroniton failed in {Description}");
            }
            finally
            {
                try
                {
                    if (BytesLeftToProcess == 0 && State == IoJobMeta.JobState.Consuming)
                        await SetStateAsync(IoJobMeta.JobState.Consumed).FastPath();
                    else if(BytesLeftToProcess != BytesRead)
                        await SetStateAsync(IoJobMeta.JobState.Fragmented).FastPath();
#if DEBUG
                    else switch (zeroRecovery)
                    {
                        case false when !fastPath && State == IoJobMeta.JobState.Fragmented:
                            _logger.Debug($"[{Id}] FRAGGED = {DatumCount}, {BytesRead}/{BytesLeftToProcess }");
                            break;
                        case false when !fastPath && BytesLeftToProcess > 0:
                            _logger.Fatal($"[{Id}] FRAGGED = {DatumCount}, {BytesRead}/{BytesLeftToProcess}");
                            break;
                    }
#endif
                    if (State == IoJobMeta.JobState.Fragmented && !IoZero.ZeroRecoveryEnabled)
                    {
                        await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();
                    }
                }
                catch when(Zeroed()){ await SetStateAsync(IoJobMeta.JobState.ConsumeErr).FastPath(); }
                catch (Exception e) when(!Zeroed())
                {
                    await SetStateAsync(IoJobMeta.JobState.ConsumeErr).FastPath();
                    _logger.Error(e, $"{nameof(State)}: re setting state failed!");
                }
            }

            ////attempt zero recovery
            if (IoZero.ZeroRecoveryEnabled && !Zeroed() && !fastPath && !zeroRecovery && BytesLeftToProcess > 0 && PreviousJob != null)
            {
                await SetStateAsync(IoJobMeta.JobState.ZeroRecovery).FastPath();
                if(await IoZero.PrimeForRecoveryAsync(this))
                    return await ConsumeAsync().FastPath();
                await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();
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
            if (packet == null)
            {
                if (!_groupByEp)
                {
                    var batchMsg = _currentBatch[Interlocked.Increment(ref _currentBatch.Count) - 1];
                    batchMsg.EmbeddedMsg = null;
                    batchMsg.Chroniton = null;
                    batchMsg.SourceState = 1;
                    RemoteEndPoint.CopyTo(batchMsg.EndPoint, 0);
                }
                else
                {
                    var batchMsg = _currentBatch[Interlocked.Increment(ref _currentBatch.Count) - 1];

                    batchMsg.EmbeddedMsg = null;
                    batchMsg.Chroniton = null;
                    batchMsg.SourceState = 1;

                    var remoteEp = (byte[])RemoteEndPoint.Clone();
                    if (!_currentBatch.GroupBy.TryAdd(remoteEp, Tuple.Create(remoteEp, new List<CcDiscoveryMessage>(_currentBatch.Messages))))
                    {
                        _currentBatch.GroupBy[remoteEp].Item2.Add(batchMsg);
                    }
                }
                return;
            }

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
                        batchMsg.SourceState = 0;
                        RemoteEndPoint.CopyTo(batchMsg.EndPoint,0);
                    }
                    else
                    {
                        var batchMsg = _currentBatch[Interlocked.Increment(ref _currentBatch.Count) - 1];

                        batchMsg.EmbeddedMsg = request;
                        batchMsg.Chroniton = packet;
                        batchMsg.SourceState = 0;
                        var remoteEp = (byte[])RemoteEndPoint.Clone();
                        if (!_currentBatch.GroupBy.TryAdd(remoteEp, Tuple.Create(remoteEp, new List<CcDiscoveryMessage>(_currentBatch.Messages))))
                        {
                            _currentBatch.GroupBy[remoteEp].Item2.Add(batchMsg);
                        }
                    }

                    if (_currentBatch.Count >= parm_max_msg_batch_size)
                        await ZeroBatchAsync().FastPath();
                }
            }
            catch when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e,$"Unable to parse request type {typeof(T).Name} from {Convert.ToBase64String(packet.PublicKey.Memory.AsArray())}, size = {packet.Data.Length}");
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

                //cog the producer
                if (!await ProtocolConduit.Source.ProduceAsync<object>(static (source, _, _, ioJob) =>
                    {
                        var @this = (CcDiscoveries)ioJob;

                        try
                        {
                            var chan = ((CcProtocBatchSource<chroniton, CcDiscoveryBatch>)source).Channel;
                            if (source == null || chan.Release(@this._currentBatch, forceAsync: true) != 1)
                            {
                                if (source != null && !((CcProtocBatchSource<chroniton, CcDiscoveryBatch>)source).Zeroed())
                                {
                                    _logger.Fatal($"{nameof(ZeroBatchAsync)}: Unable to q batch,{chan.Description} {@this.Description}");
                                }

                                return new ValueTask<bool>(false);
                            }

                            @this._currentBatch = @this._batchHeap.Take();
                            if (@this._currentBatch == null)
                                throw new OutOfMemoryException($"{@this.Description}: {nameof(@this._batchHeap)}, c = {@this._batchHeap.Count}/{@this._batchHeap.Capacity}, ref = {@this._batchHeap.ReferenceCount}");

                            @this._currentBatch.Count = 0;

                            return new ValueTask<bool>(true);
                        }
                        catch (Exception) when (@this.Zeroed()) { }
                        catch (Exception e) when (!@this.Zeroed())
                        {
                            _logger.Error(e, $"{@this.Description} - Forward failed!");
                        }

                        return new ValueTask<bool>(false);
                    }, this, null, null).FastPath())
                {
                    if (!Zeroed())
                        _logger.Trace($"{nameof(ZeroBatchAsync)}: Production [FAILED]: {ProtocolConduit.Description}");
                }
            }
            catch when (Zeroed()) { }
            catch (Exception e) when (!Zeroed())
            {
                _logger.Fatal(e, $"Forwarding from {Description} to {ProtocolConduit.Description} failed");
            }
        }
    }
}
