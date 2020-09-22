using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.Threading;
using zero.core.data.contracts;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore;

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
        /// Source URI
        /// </summary>
        string SourceUri { get; }

        /// <summary>
        /// Sets an upstream source if we using <see cref="IIoChannel"/>
        /// </summary>
        public IIoSource Upstream { get; }

        /// <summary>
        /// Signal source pressure
        /// </summary>
        public void Pressure();

        /// <summary>
        /// Wait for source pressure
        /// </summary>
        /// <returns></returns>
        public ValueTask<bool> WaitForPressureAsync();
        
        /// <summary>
        /// Signal sink pressure
        /// </summary>
        public void BackPressure();

        /// <summary>
        /// Wait for sink pressure
        /// </summary>
        /// <returns></returns>
        public ValueTask<bool> WaitForBackPressureAsync();
        
        /// <summary>
        /// Enable prefetch
        /// </summary>
        bool PrefetchEnabled { get; }
        
        /// <summary>
        /// Signal prefetch pressure
        /// </summary>
        public void PrefetchPressure();
        
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
        /// Executes the specified function in the context of the source
        /// </summary>
        /// <param name="callback">The function.</param>
        /// <param name="barrier">The barrier</param>
        /// <param name="zeroClosure"></param>
        /// <param name="jobClosure"></param>
        /// <returns></returns>
        ValueTask<bool> ProduceAsync(
            Func<IIoSourceBase, Func<IIoJob, IIoZero, ValueTask<bool>>, IIoZero, IIoJob, Task<bool>> callback,
            Func<IIoJob, IIoZero, ValueTask<bool>> barrier = null, IIoZero zeroClosure = null, IIoJob jobClosure = null);

        /// <summary>
        /// Producers can forward new productions types <see cref="TFJob"/> via a channels of type <see cref="IoChannel{TFJob}"/> to other producers.
        /// This function helps set up a channel using the supplied source. Channels are cached when created. Channels are associated with producers. 
        /// </summary>
        /// <typeparam name="TFJob">The type of job serviced</typeparam>
        /// <param name="id">The channel id</param>
        /// <param name="cascade"></param>
        /// <param name="channelSource">The source of this channel, if new</param>
        /// <param name="jobMalloc">Used to allocate jobs</param>
        /// <param name="producers">Nr of concurrent producers</param>
        /// <param name="consumers">Nr of concurrent consumers</param>
        /// <returns></returns>
        IoChannel<TFJob> EnsureChannel<TFJob>(string id, bool cascade = false, IoSource<TFJob> channelSource = null,
            Func<object, IoLoad<TFJob>> jobMalloc = null, int producers = 1, int consumers = 1)
            where TFJob : IIoJob;
    }
}
