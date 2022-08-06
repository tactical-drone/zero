using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Google.Protobuf;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore;

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
            UpstreamSource = ioSource;
            BatchChannel = new IoZeroSemaphoreChannel<TBatch>($"{nameof(BatchChannel)}: {ioSource.Description}", prefetchSize + concurrencyLevel + 1);
        }

        /// <summary>
        /// Used to load the next value to be produced
        /// </summary>
        protected readonly IoZeroSemaphoreChannel<TBatch> BatchChannel;

        /// <summary>
        /// API (IO)
        /// </summary>
        public IoZeroSemaphoreChannel<TBatch> Channel => BatchChannel;

        /// <summary>
        /// Keys this instance.
        /// </summary>
        public override string Key => $"{nameof(CcProtocBatchSource<TModel, TBatch>)}({UpstreamSource?.Key})";
        
        /// <summary>
        /// A description
        /// </summary>
        public override string Description => $"{Key} - {BatchChannel.Description}";

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
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();
            BatchChannel.ZeroSem();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Zeroed()
        {
            return base.Zeroed() || UpstreamSource.Zeroed() || BatchChannel.Zeroed();
        }

    }
}
