using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using zero.core.data.contracts;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushes.contracts
{
    public interface IIoSource : IIoSourceBase, IIoZeroable
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
        /// The source semaphore
        /// </summary>
        SemaphoreSlim ConsumerBarrier { get; }

        /// <summary>
        /// The consumer semaphore
        /// </summary>
        SemaphoreSlim ProducerBarrier { get; }

        /// <summary>
        /// The consumer semaphore
        /// </summary>o
        SemaphoreSlim ConsumeAheadBarrier { get; }

        /// <summary>
        /// The consumer semaphore
        /// </summary>
        SemaphoreSlim ProduceAheadBarrier { get; }

        /// <summary>
        /// Whether to only consume one at a time, but produce many at a time
        /// </summary>
        bool BlockOnConsumeAheadBarrier { get; }
        
        /// <summary>
        /// Whether to only consume one at a time, but produce many at a time
        /// </summary>
        bool BlockOnProduceAheadBarrier { get; }

        /// <summary>
        /// Which source job is next in line
        /// </summary>
        ref long NextProducerId();

        /// <summary>
        /// Makes available normalized storage for all downstream usages
        /// </summary>
        ConcurrentDictionary<string, object> ObjectStorage { get; }


        /// <summary>
        /// Counters for <see cref="IoJob{TJob}.State"/>
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
        /// <param name="func">The function.</param>
        /// <returns></returns>
        Task<bool> ProduceAsync(Func<IIoSourceBase, Task<bool>> func);

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
        IoChannel<TFJob> AttachProducer<TFJob>(string id, bool cascade = false, IoSource<TFJob> channelSource = null,
            Func<object, IoLoad<TFJob>> jobMalloc = null, int producers = 1, int consumers = 1)
            where TFJob : IIoJob;
    }
}
