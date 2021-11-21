using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore;

namespace zero.core.models.protobuffer.sources
{
    /// <summary>
    /// Used as a source of unmarshalled protobuf msgs by <see cref="IoConduit{TJob}"/> for <see cref="CcAdjunct"/>
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
        /// <param name="batchSize">Batch size</param>
        /// <param name="prefetchSize">Initial job prefetch from source</param>
        /// <param name="concurrencyLevel"></param>
        /// <param name="maxAsyncSinks"></param>
        /// <param name="maxAsyncSources"></param>
        public CcProtocBatchSource(string description, IIoSource ioSource, int batchSize, int prefetchSize, int concurrencyLevel, int maxAsyncSources = 0) 
            : base(description, prefetchSize, concurrencyLevel, maxAsyncSources)//TODO config
        {
            _logger = LogManager.GetCurrentClassLogger();

            UpstreamSource = ioSource;

            //Set Q to be blocking
            //TODO tuning

            MessageQueue = new IoQueue<TBatch>($"{nameof(CcProtocBatchSource<TModel,TBatch>)}: {ioSource.Description}", batchSize, concurrencyLevel, true, false);
        }

        /// <summary>
        /// The logger
        /// </summary>
        private Logger _logger;

        /// <summary>
        /// Used to load the next value to be produced
        /// </summary>
        protected IoQueue<TBatch> MessageQueue;

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
        public override bool IsOperational => !Zeroed();

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _logger = null;
            MessageQueue = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);

            await MessageQueue.ZeroManagedAsync(static (msgBatch,_) =>
            {
                msgBatch.Dispose();
                return default;
            },this).FastPath().ConfigureAwait(Zc);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Zeroed()
        {
            return base.Zeroed() || MessageQueue.Zeroed;
        }

        /// <summary>
        /// Enqueue a batch
        /// </summary>
        /// <param name="item">The messages</param>
        /// <returns>Async task</returns>
        public async ValueTask<bool> EnqueueAsync(TBatch item)
        {
            try
            {
                var plugged = await MessageQueue.EnqueueAsync(item).FastPath().ConfigureAwait(Zc) != null;
                return plugged;
            }
            catch (Exception e)
            {
                if (!Zeroed())
                    _logger.Fatal(e, $"{nameof(EnqueueAsync)}: [FAILED], {MessageQueue.Count}");
                return false;
            }
        }

        /// <summary>
        /// Dequeue item
        /// </summary>
        /// <returns></returns>
        public async ValueTask<TBatch> DequeueAsync()
        {
            try
            {
                return await MessageQueue.DequeueAsync().FastPath().ConfigureAwait(Zc);
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
        public uint Count()
        {
            return (uint)MessageQueue.Count;
        }

        /// <summary>
        /// Produces the specified callback.
        /// </summary>
        /// <param name="callback">The callback.</param>
        /// <param name="jobClosure"></param>
        /// <param name="barrier"></param>
        /// <param name="nanite"></param>
        /// <returns>The async task</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override ValueTask<bool> ProduceAsync<T>(
            Func<IIoNanite, Func<IIoJob, T, ValueTask<bool>>, T, IIoJob, ValueTask<bool>> callback,
            IIoJob jobClosure = null,
            Func<IIoJob, T, ValueTask<bool>> barrier = null,
            T nanite = default)
        {
            try
            {
                return callback(this, barrier, nanite, jobClosure);
            }
            catch (Exception) when(Zeroed() || UpstreamSource.Zeroed()){}
            catch (Exception e) when (!Zeroed() && !UpstreamSource.Zeroed())
            {
                _logger.Error(e, $"Source `{Description??"N/A"}' callback failed:");
            }

            return new ValueTask<bool>(false);
        }
    }
}
