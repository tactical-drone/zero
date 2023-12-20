using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using zero.core.core;
using zero.core.feat.models.bundle;
using zero.core.feat.models.protobuffer.sources;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;

namespace zero.core.feat.models.protobuffer
{
    /// <summary>
    /// <see cref="CcProtocBatchSource{TModel,TBatch}"/> produces these <see cref="IIoJob"/>s
    ///
    /// These jobs contain a <see cref="Batch"/> of messages that were packed
    /// from <see cref="CcProtocMessage{TModel,TBatch}"/>s processed by <see cref="CcAdjunct"/>s from here: <see cref="CcAdjunct.ProcessAsync()"/>
    ///
    /// So, why not just put a <see cref="BlockingCollection{T}"/> on <see cref="IoNeighbor{TJob}"/>
    /// and send the messages straight from <see cref="CcProtocMessage{TModel,TBatch}"/> to that Q? We
    /// need the telemetry provided by <see cref="IoZero{TJob}"/> to see what is going on. We also control
    /// resources, concurrency, many small events into larger ones etc. Also in the case of how UDP sockets work,
    /// this pattern fits perfectly with the strategy of doing the least amount of work (just buffering) on the edges:
    ///
    /// <see cref="CcAdjunct"/> -> <see cref="CcProtocMessage{TModel,TBatch}"/>     -> <see cref="CcProtocBatchSource"/> -> <see cref="IoConduit{TJob}"/>
    /// <see cref="BlockingCollection{T}"/> -                 instead of this we use                     <see cref="IoConduit{TJob}"/>
    /// <see cref="CcAdjunct"/> <- <see cref="CcProtocBatchJobJob{TModel,TBatch}"/> <- <see cref="CcProtocBatchSource{TModel,TBatch}"/> <- <see cref="IoConduit{TJob}"/>
    /// </summary>
    public class CcProtocBatchJob<TModel, TBatch> : IoSink<CcProtocBatchJob<TModel, TBatch>>
    where TModel:IMessage
    where TBatch : class, IIoMessageBundle
    {
        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="source">This message is forwarded by <see cref="CcProtocBatchSource{TModel,TBatch}"/></param>
        /// <param name="concurrencyLevel"></param>
        public CcProtocBatchJob(IoSource<CcProtocBatchJob<TModel, TBatch>> source, int concurrencyLevel = 1)
            : base($"{nameof(CcProtocBatchJob<TModel, TBatch>)}", $"job: {nameof(CcProtocBatchJob<TModel, TBatch>)}", source, concurrencyLevel)
        {
            
        }
        
        /// <summary>
        /// The transaction that is ultimately consumed
        /// </summary>
        private TBatch _batch;

        /// <summary>
        /// sentinel
        /// </summary>
        public CcProtocBatchJob()
        {

        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
            _batch = default;
        }

        /// <summary>
        /// zero managed
        /// </summary>
        /// <returns></returns>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();
            await ClearAsync().FastPath();
        }

        protected override ValueTask AddRecoveryBitsAsync()
        {
            throw new NotImplementedException();
        }

        protected override bool ZeroEnsureRecovery()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// get
        /// </summary>
        /// <returns>The current batch</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TBatch Get()
        {
            return Interlocked.Exchange(ref _batch, null);
        }

        /// <summary>
        /// set
        /// </summary>
        /// <param name="batch">The current batch</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask SetAsync(TBatch batch)
        {
            if (_batch != null)
                await ClearAsync().FastPath();

            Interlocked.Exchange(ref _batch, batch);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask ClearAsync()
        {
            _batch?.Reset();
            return new ValueTask(Task.CompletedTask);
        }

        /// <summary>
        /// Callback the generates the next job
        /// </summary>        
        /// <returns>
        /// The state to indicated failure or success
        /// </returns>
        public override async ValueTask<IoJobMeta.JobState> ProduceAsync<T>(T ioZero)
        {
            if (!await Source.ProduceAsync(static async (source, ioJob )=>
            {
                var job = (CcProtocBatchJob<TModel, TBatch>)ioJob;

                try
                {
                    if ((job._batch = await ((CcProtocBatchSource<TModel, TBatch>)source).Channel.WaitAsync()) != null)
                    {
                        job.GenerateJobId();
                        return true;
                    }
                }
                catch (TaskCanceledException) { }
                catch when (job.Zeroed()) { }
                catch (Exception e) when(!job.Zeroed())
                {
                    _logger.Fatal(e,$"BatchChannel.TryDequeueAsync failed: {job.Description}"); 
                }

                return false;
            }, this).FastPath())
            {
                _logger.Trace($"{nameof(ProduceAsync)}: Production [FAILED]; {Description}");
                return await SetStateAsync(IoJobMeta.JobState.ProdSkipped).FastPath();
            }

            return await SetStateAsync(IoJobMeta.JobState.Produced).FastPath();
        }

        /// <summary>
        /// Consumes the job
        /// </summary>
        /// <returns>
        /// The state of the consumption
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            //No work is needed, we just mark the job as consumed (Batched and forwarded).
            return SetStateAsync(IoJobMeta.JobState.ConInlined);
        }
    }
}
