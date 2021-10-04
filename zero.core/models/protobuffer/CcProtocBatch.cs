using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Google.Protobuf;
using zero.core.core;
using zero.core.models.protobuffer.sources;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.misc;

namespace zero.core.models.protobuffer
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
    /// <see cref="CcAdjunct"/> <- <see cref="CcProtocBatch{TModel,TBatch}"/> <- <see cref="CcProtocBatchSource{TModel,TBatch}"/> <- <see cref="IoConduit{TJob}"/>
    /// </summary>
    public class CcProtocBatch<TModel, TBatch> : IoSink<CcProtocBatch<TModel, TBatch>>
    where TModel:IMessage where TBatch : IoNanoprobe
    {
        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="originatingSource">This message is forwarded by <see cref="CcProtocBatchSource{TModel,TBatch}"/></param>
        /// <param name="concurrencyLevel"></param>
        public CcProtocBatch(IoSource<CcProtocBatch<TModel, TBatch>> originatingSource, int concurrencyLevel = 1)
            : base("conduit", $"{nameof(CcProtocBatch<TModel, TBatch>)}", originatingSource, concurrencyLevel)
        {
            
        }
        
        /// <summary>
        /// Empty constructor
        /// </summary>
        public CcProtocBatch(){}


        /// <summary>
        /// The transaction that is ultimately consumed
        /// </summary>
        private volatile TBatch[] _batch;
        
        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
            _batch = null;
        }

        /// <summary>
        /// zero managed
        /// </summary>
        /// <returns></returns>
        public override async ValueTask ZeroManagedAsync()
        {
            await ClearAsync().FastPath().ConfigureAwait(false);
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(false);
        }

        /// <summary>
        /// get
        /// </summary>
        /// <returns>The current batch</returns>
        public TBatch[] Get()
        {
            var tmp = _batch;
            if (_batch != null)
            {
                _batch = null;
            }
            else
            {
                throw new NullReferenceException($"{Description}: Unable to fetch batch. Non set!");
            }
            
            return tmp;
        }

        /// <summary>
        /// set
        /// </summary>
        /// <param name="batch">The current batch</param>
        public async ValueTask SetAsync(TBatch[] batch)
        {
            if (_batch != null)
                await ClearAsync().FastPath().ConfigureAwait(false);
                    
            _batch = batch;
        }
        
        private async ValueTask ClearAsync()
        {
            if (_batch != null)
            {
                foreach (var msg in _batch)
                {
                    if (msg == default)
                        break;

                    try
                    {
                        await msg.ZeroAsync(this).FastPath().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        _logger.Trace(e, $"{Description}");
                    }
                }    
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public override void SyncPrevJob()
        {
            
        }

        /// <summary>
        /// 
        /// </summary>
        public override void JobSync()
        {
            
        }

        /// <summary>
        /// Callback the generates the next job
        /// </summary>        
        /// <returns>
        /// The state to indicated failure or success
        /// </returns>
        public override async ValueTask<IoJobMeta.JobState> ProduceAsync(Func<IIoJob, IIoZero, ValueTask<bool>> barrier, IIoZero zeroClosure)
        {
            if (!await Source.ProduceAsync(async (producer, backPressure, ioZero, ioJob )=>
            {
                var _this = (CcProtocBatch<TModel, TBatch>)ioJob;
                
                if (!await backPressure(ioJob, ioZero).FastPath().ConfigureAwait(false))
                    return false;

                try
                {
                    _this._batch = await ((CcProtocBatchSource<TModel, TBatch>) _this.Source).DequeueAsync().FastPath().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    _logger.Fatal(e,$"MessageQueue.TryDequeueAsync failed: {_this.Description}"); 
                }
                
                return _this._batch != null;
            }, barrier, zeroClosure, this).FastPath().ConfigureAwait(false))
            {
                return State = IoJobMeta.JobState.Error;
            }
            
            //If the originatingSource gave us nothing, mark this production to be skipped            
            return State = IoJobMeta.JobState.Produced;
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
            State = IoJobMeta.JobState.ConInlined;
            return ValueTask.FromResult(State);
        }
    }
}
