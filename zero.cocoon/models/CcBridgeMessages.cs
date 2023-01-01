using K4os.Compression.LZ4;
using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using zero.cocoon.models.batches;
using zero.cocoon.networking;
using zero.core.feat.models.protobuffer;
using zero.core.feat.models.protobuffer.sources;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using Zero.Models.Protobuf;

namespace zero.cocoon.models
{
    /// <summary>
    /// The network bridge
    /// </summary>
    public class CcBridgeMessages : CcProtocMessage<chroniton, CcFrameBatch>
    {
        static CcBridgeMessages()
        {
            BatchHeap = new IoHeap<CcFrameBatch, chroniton>($"{nameof(BatchHeap)}:", 2048, static (_, _) =>
                new CcFrameBatch(4), autoScale: true)
            {
                PopAction = (batch, ioZero) =>
                {
                    batch.Reset();
                    batch.Drone = (CcDrone)ioZero;
                }
            };
        }

        /// <summary>
        /// Constructs a new network bridge
        /// </summary>
        /// <param name="ioZero"></param>
        public CcBridgeMessages(CcDrone ioZero) : base("Bridge", "Frame", ioZero.Source)
        {
            IoZero = ioZero;
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();

            await BatchHeap.ZeroManagedAsync<object>(static (batch, _) =>
            {
                batch.Dispose();
                return new ValueTask(Task.CompletedTask);
            }).FastPath();
        }

        /// <summary>
        /// If we are in teardown
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Zeroed()
        {
            return base.Zeroed() || Source.Zeroed() || IoZero.Zeroed() || (ProtocolConduit?.Zeroed() ?? false);
        }

        /// <summary>
        /// A description
        /// </summary>
        public override string Description => $"{base.Description} <- {Source?.Description}";

        /// <summary>
        /// The Drone this job belongs to
        /// </summary>
        public CcDrone Drone => (CcDrone)IoZero;

        public override async ValueTask<IIoHeapItem> HeapConstructAsync(object context)
        {
            if (ProtocolConduit != null)
                return null;

            var pf = Drone.Adjunct.CcCollective.MaxAdjuncts;
            var cc = Drone.Adjunct.CcCollective.MaxDrones;

            //Create the conduit source
            const string conduitId = nameof(CcBridgeMessages);

            Interlocked.Exchange(ref ProtocolConduit,
                await MessageService.CreateConduitOnceAsync<CcProtocBatchJob<chroniton, CcFrameBatch>>(conduitId)
                    .FastPath());

            if (CurrentBatch == null)
                Interlocked.Exchange(ref CurrentBatch, BatchHeap.Take());

            if (CurrentBatch == null)
                throw new OutOfMemoryException($"{Description}: {nameof(CcDiscoveries)}.{nameof(CurrentBatch)}");

            //create the channel
            if (ProtocolConduit == null)
            {
                //TODO tuning
                var source = new CcProtocBatchSource<chroniton, CcFrameBatch>(Description, MessageService, pf, cc);

                //make the bridge discoverable
                Interlocked.Exchange(ref ProtocolConduit, await MessageService.CreateConduitOnceAsync(conduitId, source,
                    static (ioZero, _) =>
                        new CcProtocBatchJob<chroniton, CcFrameBatch>(
                            (IoSource<CcProtocBatchJob<chroniton, CcFrameBatch>>)((IIoZero)ioZero).IoSource,
                            ((IIoZero)ioZero).ZeroConcurrencyLevel), cc).FastPath());

                //launch the bridge processor
                CcNetBridge.Start(MessageService);
            }

            return this;
        }

        /// <summary>
        /// Process a bridge packet
        /// </summary>
        /// <returns>The state of processing</returns>
        public override async ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            try
            {
                //fail fast
                if (BytesRead == 0)
                    return await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();

                //process bytes
                while (BytesLeftToProcess > 0)
                {
                    try
                    {
                        int length;
                        if ((length = ZeroSync()) > 0)
                        {
                            if (length > BytesLeftToProcess)
                            {
                                await SetStateAsync(IoJobMeta.JobState.Fragmented).FastPath();
                                break;
                            }

                            var unpackOffset = DatumProvisionLengthMax << 2;
                            var packetLen = LZ4Codec.Decode(Buffer, BufferOffset + sizeof(ulong), length, Buffer,
                                unpackOffset, Buffer.Length - unpackOffset - 1);

                            if (packetLen > 0)
                            {
                                await ZeroBatchRequestAsync(chroniton.Parser.ParseFrom(Buffer, unpackOffset, packetLen)).FastPath();
                                Interlocked.Add(ref BufferOffset, length + sizeof(ulong));
                            }
                        }
                        else
                            return await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();
                    }
                    catch when(Zeroed()){}
                    catch (Exception e) when (!Zeroed())
                    {
                        await SetStateAsync(IoJobMeta.JobState.ConsumeErr).FastPath();
                        _logger.Debug(e,
                            $"Parse failed: buf[{BufferOffset}], r = {BytesRead - BytesLeftToProcess}/{BytesRead}/{BytesLeftToProcess}, d = {DatumCount}, {Description}");
                        break;
                    }
                }

                await ZeroBatchAsync().FastPath();

                return await SetStateAsync(IoJobMeta.JobState.Consumed).FastPath();
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger.Error(e, $"{nameof(ConsumeAsync)}: {IoZero.Description};");
            }

            return await SetStateAsync(IoJobMeta.JobState.ConsumeErr).FastPath();
        }
    }
}
