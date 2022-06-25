using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore.core;

namespace zero.core.feat.models.protobuffer.sources
{
    /// <summary>
    /// Used as a source of unmarshalled protobuf msgs by <see cref="IoConduit{TJob}"/>
    /// </summary>
    public class CcProtocBatchSource<TModel, TBatch> : IoSource<CcProtocBatchJob<TModel, TBatch>>
    where TModel : IMessage
    where TBatch : class, IDisposable
    {
        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="description">A description</param>
        /// <param name="ioSource">The source of this model</param>
        /// <param name="prefetchSize">Initial job prefetch from source</param>
        /// <param name="concurrencyLevel"></param>
        /// <param name="zeroAsyncMode"></param>
        public CcProtocBatchSource(string description, IIoSource ioSource, int prefetchSize, int concurrencyLevel, bool zeroAsyncMode = false) 
            : base(description, false, prefetchSize, concurrencyLevel, zeroAsyncMode)//TODO config
        {
            _logger = LogManager.GetCurrentClassLogger();

            UpstreamSource = ioSource;
            //BatchQueue = new IoQueue<TBatch>($"{nameof(CcProtocBatchSource<TModel, TBatch>)}: {ioSource.Description}", (prefetchSize + concurrencyLevel), prefetchSize + 1, IoQueue<TBatch>.Mode.Pressure | IoQueue<TBatch>.Mode.BackPressure);
            IIoZeroSemaphoreBase <TBatch> q = new IoZeroCore<TBatch>($"{nameof(CcProtocBatchSource<TModel, TBatch>)}: {ioSource.Description}", (prefetchSize + concurrencyLevel) * 2, AsyncTasks);
            BatchQueue = q.ZeroRef(ref q, _ => default);
        }

        /// <summary>
        /// The logger
        /// </summary>
        private Logger _logger;

        /// <summary>
        /// Used to load the next value to be produced
        /// </summary>
        //protected readonly IoQueue<TBatch> BatchQueue;
        protected readonly IIoZeroSemaphoreBase<TBatch> BatchQueue;

        /// <summary>
        /// Keys this instance.
        /// </summary>
        public override string Key => $"{nameof(CcProtocBatchSource<TModel, TBatch>)}({UpstreamSource?.Key})";
        
        /// <summary>
        /// A description
        /// </summary>
        public override string Description => Key;

        /// <summary>
        /// Gets a value indicating whether this instance is operational.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is operational; otherwise, <c>false</c>.
        /// </value>
        public override bool IsOperational() => !Zeroed();

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _logger = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();
            BatchQueue.ZeroSem();
            //await BatchQueue.ZeroManagedAsync(static (msgBatch,_) =>
            //{
            //    msgBatch.Value.Dispose();
            //    return default;
            //},this, zero:true).FastPath();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Zeroed()
        {
            return base.Zeroed() || BatchQueue.Zeroed() || UpstreamSource.Zeroed();
        }

        /// <summary>
        /// Enqueue a batch
        /// </summary>
        /// <param name="item">The messages</param>
        /// <returns>Async task</returns>
        public ValueTask<bool> EnqueueAsync(TBatch item)
        {
            try
            {
                return new ValueTask<bool>(BatchQueue.Release(item) == 1);
            }
            catch (Exception e)
            {
                if (!Zeroed())
                    _logger.Fatal(e, $"{nameof(EnqueueAsync)}: [FAILED], {BatchQueue.WaitCount}");
                return new ValueTask<bool>(false);
            }
        }

        /// <summary>
        /// Dequeue item
        /// </summary>
        /// <returns></returns>
        public ValueTask<TBatch> DequeueAsync()
        {
            try
            {
                //return BatchQueue.DequeueAsync();
                return BatchQueue.WaitAsync();
            }
            catch when (Zeroed()){}
            catch (Exception e)when (!Zeroed())
            {
                _logger.Trace(e, $"{Description}");
            }

            return default;
        }

        /// <summary>
        /// Queue count
        /// </summary>
        /// <returns>returns number of items in the q</returns>
        public uint Count => (uint)BatchQueue.WaitCount;

    }
}
