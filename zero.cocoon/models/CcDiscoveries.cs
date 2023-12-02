using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using K4os.Compression.LZ4;
using zero.cocoon.autopeer;
using zero.cocoon.identity;
using zero.cocoon.models.batches;
using zero.core.feat.models.protobuffer;
using zero.core.feat.models.protobuffer.sources;
using zero.core.misc;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using Zero.Models.Protobuf;

namespace zero.cocoon.models
{
    public class CcDiscoveries : CcProtocMessage<chroniton, CcDiscoveryBatch>
    {
        static CcDiscoveries()
        {
            BatchHeap = new IoHeap<CcDiscoveryBatch, chroniton>($"{nameof(BatchHeap)}:", 2048, static (_, _) =>
                new CcDiscoveryBatch(4), autoScale: true)
            {
                PopAction = (batch, _) =>
                {
                    batch.Reset();
                    batch.GroupBy?.Clear();
                }
            };
        }

        public CcDiscoveries(CcAdjunct ioZero, string sinkDesc, bool groupByEp = false) : base(sinkDesc, $"{ioZero?.Source.Key}",  ioZero?.Source)
        {
            IoZero = ioZero;
            _groupByEp = groupByEp;
        }

        public override long Signature { get; protected set; }

        public override async ValueTask<IIoHeapItem> HeapConstructAsync(object context)
        {
            if (ProtocolConduit != null)
                return null;

            //IoZero = (IoZero<CcProtocMessage<chroniton, CcDiscoveryBatch>>)context;

            //var pf = Source.PrefetchSize * 7;
            //var cc = Source.ZeroConcurrencyLevel * 6; 

            //if (!Source.Proxy && Adjunct.CcCollective.ZeroDrone)
            //{
            //    pf = Source.PrefetchSize * 3;
            //    cc = Source.ZeroConcurrencyLevel * 3;
            //}

            var pf = CcCollective.MaxAdjuncts;
            var cc = CcCollective.MaxAdjuncts;

            //if (!Source.Proxy && Adjunct.CcCollective.ZeroDrone)
            //{
            //    pf = Source.PrefetchSize * 3;
            //    cc = Source.ZeroConcurrencyLevel * 3;
            //}

            //Create the conduit source
            const string conduitId = nameof(CcAdjunct);
            Interlocked.Exchange(ref ProtocolConduit,
                await MessageService.CreateConduitOnceAsync<CcProtocBatchJob<chroniton, CcDiscoveryBatch>>(conduitId)
                    .FastPath());

            if (CurrentBatch == null)
                Interlocked.Exchange(ref CurrentBatch, BatchHeap.Take());

            if (CurrentBatch == null)
                throw new OutOfMemoryException($"{Description}: {nameof(CcDiscoveries)}.{nameof(CurrentBatch)}");

            //create the channel
            if (ProtocolConduit == null)
            {
                //TODO tuning
                var source = new CcProtocBatchSource<chroniton, CcDiscoveryBatch>(Description, MessageService, pf, cc);
                Interlocked.Exchange(ref ProtocolConduit, await MessageService.CreateConduitOnceAsync(conduitId, source,
                    static (ioZero, _) =>
                        new CcProtocBatchJob<chroniton, CcDiscoveryBatch>(
                            (IoSource<CcProtocBatchJob<chroniton, CcDiscoveryBatch>>)((IIoZero)ioZero).IoSource,
                            ((IIoZero)ioZero).ZeroConcurrencyLevel), cc).FastPath());
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
            //_currentBatch = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();

            await BatchHeap.ZeroManagedAsync<object>().FastPath();

            //_currentBatch?.Dispose();
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

        ///// <summary>
        ///// Heap access
        ///// </summary>
        //public static IoHeap<CcDiscoveryBatch, CcDiscoveries> Heap => BatchHeap;

        ///// <summary>
        ///// Batch of messages
        ///// </summary>
        //private CcDiscoveryBatch _currentBatch;

        ///// <summary>
        ///// Batch item heap
        ///// </summary>
        //private new static readonly IoHeap<CcDiscoveryBatch, CcDiscoveries> BatchHeap;

        /// <summary>
        /// CC Node
        /// </summary>
        protected CcCollective CcCollective => ((CcAdjunct)IoZero)?.CcCollective;

        /// <summary>
        /// The adjunct this job belongs to
        /// </summary>
        public CcAdjunct Adjunct => (CcAdjunct)IoZero;

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
                //await ZeroBatchRequestAsync(null, chroniton.Parser).FastPath();
                await SetStateAsync(IoJobMeta.JobState.Consumed);
                return State;
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

                    Signature = MemoryMarshal.Read<long>(packet.Sabot.Span);

                    var packetMsgRaw = packet.Data.Memory.AsArray();
                    var verified = CcDesignation.Verify(packetMsgRaw, 0, packetMsgRaw.Length, packet.PublicKey.Memory.AsArray(), 0, packet.Signature.Memory.AsArray(), 0);
                    
                    if(Adjunct.Designation.Primed)
                        verified &= CcDesignation.Signed(packet.Sabot.Memory.AsArray(), Adjunct.Designation.Sabot(packetMsgRaw), packet.Sabot.Length);

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

                    await ZeroBatchRequestAsync(packet).FastPath();
                }
                
                //TODO tuning
                //if (_currentBatch.Count > BatchHeap.Capacity * 3 / 2)
                //    _logger.Warn($"{nameof(BatchHeap)} running lean {_currentBatch.Count}/{BatchHeap.Capacity}, {BatchHeap}, {BatchHeap.Description}");

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
    }
}
