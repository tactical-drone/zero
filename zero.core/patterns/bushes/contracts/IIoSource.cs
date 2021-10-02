using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using zero.core.data.contracts;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushes.contracts
{
    /// <summary>
    /// Universal source of stuff
    /// </summary>
    public interface IIoSource : IIoSourceBase, IIoNanoprobe
    {
        /// <summary>
        /// Keys this instance.
        /// </summary>
        /// <returns>The unique key of this instance</returns>
        string Key { get; }

        /// <summary>
        /// Apply source pressure
        /// </summary>
        public void Pressure(int releaseCount = 1);

        /// <summary>
        /// Wait for source pressure
        /// </summary>
        /// <returns></returns>
        public ValueTask<bool> WaitForPressureAsync();
        
        /// <summary>
        /// Apply sink pressure
        /// </summary>
        public void BackPressure(int releaseCount = 1);

        /// <summary>
        /// Wait for sink back pressure
        /// </summary>
        /// <returns></returns>
        public ValueTask<bool> WaitForBackPressureAsync();
        
        /// <summary>
        /// Enable prefetch
        /// </summary>
        bool PrefetchEnabled { get; }
        
        /// <summary>
        /// Apply prefetch pressure
        /// </summary>
        public void PrefetchPressure(int releaseCount = 1);
        
        /// <summary>
        /// Wait on prefetch pressure
        /// </summary>
        /// <returns></returns>
        public ValueTask<bool> WaitForPrefetchPressureAsync();
        
        /// <summary>
        /// Which source job is next in line
        /// </summary>
        ref long NextProducerId();

        /// <summary>
        /// Makes available normalized storage for all downstream usages
        /// </summary>
        ConcurrentDictionary<string, object> ObjectStorage { get; }
        
        /// <summary>
        /// Counters for <see cref="IoJobMeta.JobState"/>
        /// </summary>
        public long[] Counters { get; }

        /// <summary>
        /// Total service times per <see cref="Counters"/>
        /// </summary>
        public long[] ServiceTimes { get; }

        /// <summary>
        /// Print counters
        /// </summary>
        public void PrintCounters();

        /// <summary>
        /// Gets a value indicating whether this instance is operational.
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance is operational; otherwise, <c>false</c>.
        /// </value>
        bool IsOperational { get; }

        /// <summary>
        /// Gets a value indicating whether this <see cref="IoSource{TJob}"/> is synced.
        /// </summary>
        /// <value>
        ///   <c>true</c> if synced; otherwise, <c>false</c>.
        /// </value>
        bool Synced { get; set; }

        /// <summary>
        /// Used to identify work that was done recently
        /// </summary>
        IIoDupChecker RecentlyProcessed { get; set; }

        /// <summary>
        /// The amount of productions that can be made while consumption is behind
        /// </summary>
        int PrefetchSize { get; }

        /// <summary>
        /// The number of concurrent sinks allowed
        /// </summary>
        int MaxAsyncSinks { get; }

        /// <summary>
        /// The number of concurrent sources allowed
        /// </summary>
        int MaxAsyncSources { get; }

        /// <summary>
        /// If async workers are currently enabled
        /// </summary>
        public bool AsyncEnabled { get; }

        /// <summary>
        /// Executes the specified function in the context of the source
        /// </summary>
        /// <param name="callback">The function.</param>
        /// <param name="barrier">The barrier</param>
        /// <param name="zeroClosure"></param>
        /// <param name="jobClosure"></param>
        /// <returns></returns>
        ValueTask<bool> ProduceAsync(
            Func<IIoSourceBase, Func<IIoJob, IIoZero, ValueTask<bool>>, IIoZero, IIoJob, ValueTask<bool>> callback,
            Func<IIoJob, IIoZero, ValueTask<bool>> barrier = null, IIoZero zeroClosure = null, IIoJob jobClosure = null);

        /// <summary>
        /// Producers can forward new productions types <see cref="TFJob"/> via a channels of type <see cref="IIoConduit"/> to other producers.
        /// This function helps set up a channel using the supplied source. Channels are cached when created. Channels are associated with producers. 
        /// </summary>
        /// <typeparam name="TFJob">The type of job serviced</typeparam>
        /// <param name="id">The channel id</param>
        /// <param name="cascade"></param>
        /// <param name="channelSource">The source of this channel, if new</param>
        /// <param name="jobMalloc">Used to allocate jobs</param>
        /// <returns></returns>
        ValueTask<IoConduit<TFJob>> AttachConduitAsync<TFJob>(string id, bool cascade = false,
            IoSource<TFJob> channelSource = null, Func<object, IoSink<TFJob>> jobMalloc = null) where TFJob : IIoJob;
    }
}
