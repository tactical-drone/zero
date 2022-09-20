using System;
using System.Collections.Concurrent;
using System.Threading;
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
        /// Flow direction
        /// </summary>
        public enum Heading
        {
            Undefined = 0,
            Ingress = 1,
            Egress = 2,
            Both = 3
        }


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
        public int Pressure(int releaseCount = 1);

        /// <summary>
        /// Wait for source pressure
        /// </summary>
        /// <returns></returns>
        public ValueTask<int> WaitForPressureAsync();
        
        /// <summary>
        /// Apply sink "back" pressure
        /// </summary>
        public int BackPressure(int releaseCount = 1, bool zeroAsync = false);

        /// <summary>
        /// Wait for sink back pressure
        /// </summary>
        /// <returns></returns>
        public ValueTask<int> WaitForBackPressureAsync();
        
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
        public int PrefetchPressure(int releaseCount = 1, bool zeroAsync = false);
        
        /// <summary>
        /// Wait on prefetch pressure
        /// </summary>
        /// <returns></returns>
        public ValueTask<int> WaitForPrefetchPressureAsync();
        
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
        /// Generated Unique job Ids seed
        /// </summary>
        public long NextJobIdSeed();

        /// <summary>
        /// The current job id
        /// </summary>
        /// <returns></returns>
        public long CurJobId { get; }

        /// <summary>
        /// Total service times per <see cref="Counters"/>
        /// </summary>
        public long[] ServiceTimes { get; }

        /// <summary>
        /// Print counters
        /// </summary>
        public string PrintCounters();

        /// <summary>
        /// Gets a value indicating whether this instance is operational.
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance is operational; otherwise, <c>false</c>.
        /// </value>
        bool IsOperational();

        /// <summary>
        /// The direction of data flow
        /// </summary>
        public Heading Direction { get; }

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
        bool ZeroAsyncMode { get; }

        /// <summary>
        /// Whether this source is a proxy
        /// </summary>
        public bool Proxy  { get; }

        /// <summary>
        /// Number of jobs buffered
        /// </summary>
        int BacklogCount { get; }

        /// <summary>
        /// Used to anchor a control rate at the consumer
        /// </summary>
        int Rate { get;}

        /// <summary>
        /// Used to anchor a control rate at the consumer
        /// </summary>
        int RateSet { get; }

        /// <summary>
        /// Current timestamp
        /// </summary>
        long ZeroTimeStamp { get; set; }

        /// <summary>
        /// Sets the current rate
        /// </summary>
        /// <param name="rate"></param>
        /// <param name="value"></param>
        /// <param name="cmp"></param>
        /// <returns></returns>
        int SetRate(int value, int cmp);

        /// <summary>
        /// Sets the current rate
        /// </summary>
        /// <param name="rate"></param>
        /// <param name="value"></param>
        /// <param name="cmp"></param>
        /// <returns></returns>
        int SetRateSet(int value, int cmp);

        /// <summary>
        /// Congestion control hooks
        /// </summary>
        /// <typeparam name="T">The type of <see cref="IIoZero"/></typeparam>
        /// <param name="job">The job being processed</param>
        /// <param name="ioZero">The engine processing the job</param>
        /// <returns></returns>
        delegate ValueTask<bool> IoZeroCongestion<in T>(IIoJob job, T ioZero);

        /// <summary>
        /// Produces a job, in the context of the source.
        /// </summary>
        /// <param name="produce">A production that produces a job from the source</param>
        /// <param name="ioJob">The job instance</param>
        /// <param name="barrier">The congestion barrier</param>
        /// <param name="ioZero">The pattern</param>
        /// <returns></returns>
        ValueTask<bool> ProduceAsync<T>(Func<IIoSource, IoZeroCongestion<T>, T, IIoJob, ValueTask<bool>> produce,
            IIoJob ioJob,
            IoZeroCongestion<T> barrier,
            T ioZero);

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
