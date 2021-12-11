using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using zero.core.data.contracts;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushings.contracts
{
    /// <summary>
    /// Universal source of stuff
    /// </summary>
    public interface IIoSource : IIoNanite
    {
        /// <summary>
        /// Keys this instance.
        /// </summary>
        /// <returns>The unique key of this instance</returns>
        string Key { get; }

        /// <summary>
        /// UpstreamSource Source
        /// </summary>
        public IIoSource UpstreamSource { get; }

        /// <summary>
        /// Apply source pressure
        /// </summary>
        public ValueTask<int> PressureAsync(int releaseCount = 1);

        /// <summary>
        /// Wait for source pressure
        /// </summary>
        /// <returns></returns>
        public ValueTask<bool> WaitForPressureAsync();
        
        /// <summary>
        /// Apply sink pressure
        /// </summary>
        public int BackPressureAsync(int releaseCount = 1);

        /// <summary>
        /// Wait for sink back pressure
        /// </summary>
        /// <returns></returns>
        public ValueTask<bool> WaitForBackPressureAsync();
        
        /// <summary>
        /// Enable prefetch
        /// </summary>
        bool PressureEnabled { get; }

        /// <summary>
        /// Enable prefetch
        /// </summary>
        bool PrefetchEnabled { get; }


        /// <summary>
        /// Enable prefetch
        /// </summary>
        bool BackPressureEnabled { get; }

        /// <summary>
        /// Apply prefetch pressure
        /// </summary>
        public int PrefetchPressure(int releaseCount = 1);
        
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
        /// Gets a value indicating whether this source is originating or terminating
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance is Egress; Ingress otherwise, <c>false</c>.
        /// </value>
        bool IsOriginating { get; }

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
        /// The number of concurrent sources allowed
        /// </summary>
        int MaxAsyncSources { get; }

        /// <summary>
        /// If async workers are currently enabled
        /// </summary>
        public bool AsyncEnabled { get; }

        /// <summary>
        /// Whether this source is a proxy
        /// </summary>
        public bool Proxy  { get; }

        /// <summary>
        /// Number of jobs buffered
        /// </summary>
        int Count { get; }

        /// <summary>
        /// Executes the specified function in the context of the source
        /// </summary>
        /// <param name="callback">The function.</param>
        /// <param name="jobClosure"></param>
        /// <param name="barrier">The barrier</param>
        /// <param name="nanite"></param>
        /// <returns></returns>
        ValueTask<bool> ProduceAsync<T>(
            Func<IIoNanite, Func<IIoJob, T, ValueTask<bool>>, T, IIoJob, ValueTask<bool>> callback,
            IIoJob jobClosure = null,
            Func<IIoJob, T, ValueTask<bool>> barrier = null,
            T nanite = default);

        /// <summary>
        /// Producers can forward new productions types <see cref="TFJob"/> via a channels of type <see cref="IIoConduit"/> to other producers.
        /// This function helps set up a channel using the supplied source. Channels are cached when created. Channels are associated with producers. 
        /// </summary>
        /// <typeparam name="TFJob">The type of job serviced</typeparam>
        /// <param name="id">The channel id</param>
        /// <param name="concurrencyLevel"></param>
        /// <param name="channelSource">The source of this channel, if new</param>
        /// <param name="jobMalloc">Used to allocate jobs</param>
        /// <returns></returns>
        ValueTask<IoConduit<TFJob>> CreateConduitOnceAsync<TFJob>(string id,
            IoSource<TFJob> channelSource = null, Func<object, IIoNanite, IoSink<TFJob>> jobMalloc = null, int concurrencyLevel = 1) where TFJob : IIoJob;
        
    }
}
