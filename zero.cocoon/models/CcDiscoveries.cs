using System;
using System.Collections.Generic;
using System.Diagnostics;
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
            
            var pf = 2;
            var cc = 1;

            if (!Source.Proxy && Adjunct.CcCollective.ZeroDrone)
            {
                parm_max_msg_batch_size *= 2;
                
                pf = 4;
                cc = 3;
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
            _batchHeap = new IoHeap<CcDiscoveryBatch, CcDiscoveries>(bashDesc, pf*2, static (_, @this) => new CcDiscoveryBatch(@this._batchHeap, @this.parm_max_msg_batch_size, @this._groupByEp))
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
                var channelSource = new CcProtocBatchSource<chroniton, CcDiscoveryBatch>(Description, MessageService, pf, cc);
                ProtocolConduit = await MessageService.CreateConduitOnceAsync(
                    conduitId,
                    channelSource,
                    static (ioZero, _) => new CcProtocBatchJob<chroniton, CcDiscoveryBatch>(
                        (IoSource<CcProtocBatchJob<chroniton, CcDiscoveryBatch>>)((IIoConduit)ioZero).UpstreamSource, ((IIoConduit)ioZero).ZeroConcurrencyLevel()), cc).FastPath();
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

            _currentBatch?.Dispose();
            
            await _batchHeap.ZeroManagedAsync<object>(static (batch, _) =>
            {
                batch.Dispose();
                return default;
            }).FastPath();
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
        public int parm_max_msg_batch_size = 32;//TODO tuning 4 x MaxAdjuncts

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
            var pos = BufferOffset;

            bool fastPath = false;
            try
            {
                //fail fast
                if (BytesRead == 0)
                    return await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();

                var verified = false;

                //Ensure that the previous job (which could have been launched out of sync) has completed
                var prevJob = ((CcDiscoveries)PreviousJob)?.ZeroRecovery;
                if (prevJob != null)
                {
                    fastPath = IoZero.ZeroRecoveryEnabled && prevJob.GetStatus((short)prevJob.Version) == ValueTaskSourceStatus.Succeeded;
                    
                    if (zeroRecovery || fastPath)
                    {
                        if (fastPath && prevJob.GetResult((short)prevJob.Version))
                        {
                            await AddRecoveryBitsAsync().FastPath();
                        }
                        else
                        {
                            var prevJobTask = new ValueTask<bool>(prevJob, (short)prevJob.Version);
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
                            if (length > BytesLeftToProcess)
                            {
                                await SetStateAsync(IoJobMeta.JobState.Fragmented).FastPath();
                                break;
                            }
                            
                            try
                            {
                                var unpackOffset = DatumProvisionLengthMax << 2;
                                var packetLen = LZ4Codec.Decode(Buffer, BufferOffset + sizeof(ulong), length, Buffer, unpackOffset, Buffer.Length - unpackOffset - 1);

                                read = length + sizeof(ulong);
                                Interlocked.Add(ref BufferOffset, read);

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
                    if (packet == null || packet.Data == null || packet.Data.Length == 0)
                    {
                        await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();
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
                        await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();
                        continue;
                    }
                    
                    switch ((MessageTypes)packet.Type)
                    {
                        case MessageTypes.Probe:
                            await ZeroBatchRequestAsync(packet, CcProbeMessage.Parser).FastPath();
                            break;
                        case MessageTypes.Probed:
                            await ZeroBatchRequestAsync(packet, CcProbeResponse.Parser).FastPath();
                            break;
                        case MessageTypes.Scan:
                            await ZeroBatchRequestAsync(packet, CcScanRequest.Parser).FastPath();
                            break;
                        case MessageTypes.Adjuncts:
                            await ZeroBatchRequestAsync(packet, CcAdjunctResponse.Parser).FastPath();
                            break;
                        case MessageTypes.Fuse:
                            await ZeroBatchRequestAsync(packet, CcFuseRequest.Parser).FastPath();
                            break;
                        case MessageTypes.Fused:
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
                        case false when !fastPath:
                            _logger.Fatal($"[{Id}] FRAGGED = {DatumCount}, {BytesRead}/{BytesLeftToProcess }");
                            break;
                    }
#else
                    else if (State == IoJobMeta.JobState.Fragmented && !IoZero.ZeroRecoveryEnabled)
                    {
                        await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();
                    }
#endif
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
                        await ZeroBatchAsync().FastPath();
                    }

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

                //cog the source
                if (!await ProtocolConduit.UpstreamSource.ProduceAsync<object>(static async (source, _, _, ioJob) =>
                    {
                        var @this = (CcDiscoveries)ioJob;

                        try
                        {
                            if (source == null || !await ((CcProtocBatchSource<chroniton, CcDiscoveryBatch>)source).EnqueueAsync(@this._currentBatch).FastPath())
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
                    }, this, null, default).FastPath())
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
