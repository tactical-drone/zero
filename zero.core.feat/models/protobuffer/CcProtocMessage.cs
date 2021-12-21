using System;
using System.Buffers;
using System.IO;
using System.Net;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using zero.core.conf;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;

namespace zero.core.feat.models.protobuffer
{
    public abstract class CcProtocMessage<TModel, TBatch> : IoMessage<CcProtocMessage<TModel, TBatch>>
    where TModel:IMessage where TBatch : class, IDisposable
    {
        protected CcProtocMessage(string sinkDesc, string jobDesc, IoSource<CcProtocMessage<TModel, TBatch>> source)
            : base(sinkDesc, jobDesc, source)
        {
            DatumSize = 1492; 
            
            //Init buffers
            BufferSize = DatumSize * parm_datums_per_buffer;//SET to MTU

            DatumProvisionLengthMax = DatumSize - 1;
            
            MemoryOwner = MemoryPool<byte>.Shared.Rent(BufferSize + DatumProvisionLengthMax);

            if (MemoryMarshal.TryGetArray((ReadOnlyMemory<byte>)MemoryOwner.Memory, out var malloc))
            {
                if (Buffer != null && Buffer.Length < BufferSize)
                {
                    throw new InternalBufferOverflowException($"Invalid buffer size of {BufferSize} < {Buffer.Length}");
                }
                ArraySegment = malloc;

                Buffer = ArraySegment.Array;
                
                ReadOnlySequence = new ReadOnlySequence<byte>(Buffer);
                MemoryBuffer = new Memory<byte>(Buffer);
                ByteStream = new MemoryStream(Buffer!);
            }
        }


        /// <summary>
        /// Message batch broadcast channel
        /// </summary>
        protected IoConduit<CcProtocBatchJob<TModel, TBatch>> ProtocolConduit;

        /// <summary>
        /// Base source
        /// </summary>
        protected IoNetClient<CcProtocMessage<TModel, TBatch>> MessageService => (IoNetClient<CcProtocMessage<TModel, TBatch>>)Source;


        /// <summary>
        /// The time a consumer will wait for a source to release it before aborting in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_producer_wait_for_consumer_timeout = 5000; //TODO make this adapting 

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_datums_per_buffer = 2;


        /// <summary>
        /// How long to wait for the consumer before timing out
        /// </summary>
        public override int WaitForConsumerTimeout => parm_producer_wait_for_consumer_timeout;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

            MemoryOwner.Dispose();
#if SAFE_RELEASE
            MemoryOwner = null;
            ProtocolConduit = null;
            ArraySegment = null;
            Buffer = null;
            ReadOnlySequence = default;
            MemoryBuffer = null;
            ByteStream = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);
            ArrayPool<byte>.Shared.Return(Buffer);
        }

        /// <summary>
        /// User data in the source
        /// </summary>
        protected byte[] RemoteEndPoint { get; } = new IPEndPoint(IPAddress.Any, 0).AsBytes();

        public override async ValueTask<IoJobMeta.JobState> ProduceAsync<T>(IIoSource.IoZeroCongestion<T> barrier,
            T ioZero)
        {
            try
            {
                await MessageService.ProduceAsync(static async (ioSocket, producerPressure, ioZero, ioJob) =>
                {
                    var job = (CcProtocMessage<TModel, TBatch>)ioJob;
                    try
                    {
                        //----------------------------------------------------------------------------
                        // BARRIER
                        // We are only allowed to run ahead of the consumer by some configurable
                        // amount of steps. Instead of say just filling up memory buffers.
                        // This allows us some kind of (anti DOS?) congestion control
                        //----------------------------------------------------------------------------
                        if (!await producerPressure(ioJob, ioZero).FastPath().ConfigureAwait(job.Zc))
                            return false;

                        //Async read the message from the message stream
                        if (job.MessageService.IsOperational && !job.Zeroed())
                        {
                            var read = await ((IoNetClient<CcProtocMessage<TModel, TBatch>>)ioSocket).IoNetSocket
                                .ReadAsync(job.MemoryBuffer, job.BufferOffset, job.BufferSize, job.RemoteEndPoint).FastPath().ConfigureAwait(job.Zc);

                            //Drop zero reads
                            if (read == 0)
                            {
                                job.State = IoJobMeta.JobState.ProduceTo;
                                return false;
                            }

                            Interlocked.Add(ref job.BytesRead, read);

                            job.State = IoJobMeta.JobState.Produced;

                            //_logger.Trace($"{job.Description} => {job.GetType().Name}[{job.Id}]: r = {job.BytesRead}, r = {job.BytesLeftToProcess}, dc = {job.DatumCount}, ds = {job.DatumSize}, f = {job.DatumFragmentLength}, b = {job.BytesLeftToProcess}/{job.BufferSize + job.DatumProvisionLengthMax}, b = {(int)(job.BytesLeftToProcess / (double)(job.BufferSize + job.DatumProvisionLengthMax) * 100)}%");
                        }
                        else
                        {
                            job.State = IoJobMeta.JobState.Cancelled;
                        }

                        if (job.Zeroed())
                        {
                            job.State = IoJobMeta.JobState.Cancelled;
                            return false;
                        }

                        return true;
                    }
                    catch when (job.Zeroed())
                    {
                    }
                    catch (Exception e) when (!job.Zeroed())
                    {
                        job.State = IoJobMeta.JobState.ProduceErr;
                        _logger.Error(e, $"ReadAsync {job.Description}:");
                    }

                    return false;
                }, this, barrier, ioZero).FastPath().ConfigureAwait(Zc);
            }
            catch when (Zeroed()) { }
            catch (Exception e)when (!Zeroed())
            {
                _logger?.Warn(e, $"Producing job for {Description} returned with errors:");
            }
            finally
            {
                if (State == IoJobMeta.JobState.Producing)
                {
                    // Set the state to ProduceErr so that the consumer knows to abort consumption
                    State = IoJobMeta.JobState.ProduceErr;
                }
            }

            return State;
        }
    }
}
