using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.data.contracts;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Used by <see cref="IoZero{TJob}"/> as a source of work of type <see cref="TJob"/>
    /// </summary>
    public abstract class IoSource<TJob> : IoNanoprobe, IIoSource where TJob : IIoJob
    {
        /// <summary>
        /// Constructor
        /// </summary>
        protected IoSource(int prefetchSize, int concurrencyLevel)
        {
            PrefetchSize = prefetchSize;
            ConcurrencyLevel = concurrencyLevel;
            
            //todo GENERALIZE
            _pressure = new IoZeroSemaphoreSlim(AsyncTokenProxy, $"{GetType().Name}: {nameof(_pressure).Trim('_')}", expectedNrOfWaiters: concurrencyLevel * 2, enableAutoScale:false, capacity:prefetchSize * concurrencyLevel * 2, enableDeadlockDetection:true);
            _backPressure = new IoZeroSemaphoreSlim(AsyncTokenProxy,$"{GetType().Name}: {nameof(_backPressure).Trim('_')}", initialCount:1, expectedNrOfWaiters: concurrencyLevel, enableAutoScale: false, capacity: prefetchSize * concurrencyLevel, enableDeadlockDetection:true);
            _prefetchPressure = new IoZeroSemaphoreSlim(AsyncTokenProxy,$"{GetType().Name}: {nameof(_prefetchPressure).Trim('_')}", initialCount:1, expectedNrOfWaiters: concurrencyLevel, enableAutoScale: false, capacity: prefetchSize * concurrencyLevel, enableDeadlockDetection:true);

            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// A dictionary of downstream channels
        /// </summary>
        protected ConcurrentDictionary<string, IIoChannel> IoChannels = new ConcurrentDictionary<string, IIoChannel>();

        /// <summary>
        /// Keys this instance.
        /// </summary>
        /// <returns>The unique key of this instance</returns>
        public abstract string Key { get; }

        /// <summary>
        /// Description used as a key
        /// </summary>
        public abstract string SourceUri { get; }

        /// <summary>
        /// Sets an upstream source
        /// </summary>
        public IIoSource Upstream { get; protected set; }
        

        /// <summary>
        /// Counters for <see cref="IoJobMeta.JobState"/>
        /// </summary>
        public long[] Counters { get; protected set; } = new long[Enum.GetNames(typeof(IoJobMeta.JobState)).Length];

        /// <summary>
        /// Total service times per <see cref="Counters"/>
        /// </summary>
        public long[] ServiceTimes { get; protected set; } = new long[Enum.GetNames(typeof(IoJobMeta.JobState)).Length];
        
        /// <summary>
        /// The sink is being throttled against source
        /// </summary>
        private IIoZeroSemaphore _pressure;
        
        /// <summary>
        /// The source is being throttled by the sink 
        /// </summary>
        private IIoZeroSemaphore _backPressure;
        
        /// <summary>
        /// The source is bing throttled on prefetch config
        /// </summary>
        private IIoZeroSemaphore _prefetchPressure;
        
        /// <summary>
        /// Enable prefetch throttling (only allow a certain amount of prefetch
        /// in the presence of concurrent production
        /// </summary>
        public bool PrefetchEnabled { get; protected set; }
        
        /// <summary>
        /// The next job Id
        /// </summary>
        /// <returns>The next job Id</returns>
        ref long IIoSource.NextProducerId()
        {
            return ref _nextProducerId;
        }

        /// <summary>
        /// Makes available normalized storage for all downstream usages
        /// </summary>
        public ConcurrentDictionary<string, object> ObjectStorage { get; protected set; } = new ConcurrentDictionary<string, object>();

        /// <summary>
        /// Gets a value indicating whether this instance is operational.
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance is operational; otherwise, <c>false</c>.
        /// </value>
        public abstract bool IsOperational { get; }

        /// <summary>
        /// The amount of productions that can be made while consumption is behind
        /// </summary>
        public int PrefetchSize { get; protected set; }
        
        /// <summary>
        /// The amount of productions that can be made while consumption is behind
        /// </summary>
        public int ConcurrencyLevel { get; protected set; }

        /// <summary>
        /// Used to identify work that was done recently
        /// </summary>
        public IIoDupChecker RecentlyProcessed { get; set; }

        /// <summary>
        /// Which source job is next in line
        /// </summary>
        public long NextProducerId { get; protected set; }

        /// <summary>
        /// Gets a value indicating whether this <see cref="IoSource{TJob}"/> is synced.
        /// </summary>
        /// <value>
        ///   <c>true</c> if synced; otherwise, <c>false</c>.
        /// </value>
        public bool Synced { get; set; }

            /// <summary>
        /// <see cref="PrintCounters"/> only prints events that took longer that this value in microseconds
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected long parm_event_min_ave_display = 0;

        /// <summary>
        /// holds the next job Id
        /// </summary>
        private long _nextProducerId;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            //Unblock any blockers
            //ProduceBackPressure.Dispose();
            //ProducerPressure.Dispose();
            //ConsumeAheadBarrier.Dispose();
            //ProduceAheadBarrier.Dispose();

            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _pressure = null;
            _backPressure = null;
            _prefetchPressure = null;
            RecentlyProcessed = null;
            IoChannels = null;
            ObjectStorage = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            //foreach (var objectStorageValue in ObjectStorage.Values)
            //{
            //    await ((IIoZeroable)objectStorageValue).ZeroAsync(this).ConfigureAwait(false);
            //}


            _pressure.Zero();
            _backPressure.Zero();
            _prefetchPressure.Zero();

            ObjectStorage.Clear();
            IoChannels.Clear();

            try
            {
                await RecentlyProcessed.ZeroAsync(this).ConfigureAwait(false);
            }
            catch { }

            await base.ZeroManagedAsync().ConfigureAwait(false);

            _logger.Trace($"Closed {Description}");
        }

        /// <summary>
        /// Producers can forward new productions types <see cref="TFJob"/> via a channels of type <see cref="IoChannel{TFJob}"/> to other producers.
        /// This function helps set up a channel using the supplied source. Channels are cached when created. Channels are associated with producers.
        /// </summary>
        /// <typeparam name="TFJob">The type of job serviced</typeparam>
        /// <param name="id">The channel id</param>
        /// <param name="cascade">ZeroOnCascade close events</param>
        /// <param name="channelSource">The source of this channel, if new</param>
        /// <param name="jobMalloc">Used to allocate jobs</param>
        /// /// <param name="producers">Nr of concurrent producers</param>
        /// <param name="consumers">Nr of concurrent consumers</param>
        /// <returns></returns>
        public IoChannel<TFJob> EnsureChannel<TFJob>(string id, bool cascade = false, IoSource<TFJob> channelSource = null,
            Func<object, IoLoad<TFJob>> jobMalloc = null, int producers = 1, int consumers = 1)
        where TFJob : IIoJob
        {
            if (!IoChannels.ContainsKey(id))
            {
                if (channelSource == null || jobMalloc == null)
                {
                    _logger.Debug($"Waiting for channel {id} in {Description} to initialize...");
                    return null;
                }

                lock (this)
                {
                    var newChannel = new IoChannel<TFJob>($"`channel({id}>{channelSource.GetType().Name}>{typeof(TFJob).Name})'", channelSource, jobMalloc, producers, consumers);

                    ZeroEnsureAsync(s =>
                    {
                        if (!IoChannels.TryAdd(id, newChannel)) return Task.FromResult(false);
                        ZeroOnCascade(newChannel, cascade);
                        return Task.FromResult(true);
                    }).ConfigureAwait(false).GetAwaiter().GetResult();
                }
            }

            return (IoChannel<TFJob>)IoChannels[id];
        }

        /// <summary>
        /// Gets a channel with a certain Id
        /// </summary>
        /// <typeparam name="TFJob">The type that the channel speaks</typeparam>
        /// <param name="id">The id of the channel</param>
        /// <returns>The channel</returns>
        public IoChannel<TFJob> GetChannel<TFJob>(string id)
            where TFJob : IoLoad<TFJob>, IIoJob
        {
            try
            {
                return (IoChannel<TFJob>)IoChannels[id];
            }
            catch { }

            return null;
        }

        /// <summary>
        /// Print counters
        /// </summary>
        public void PrintCounters()
        {
            var heading = new StringBuilder();
            var str = new StringBuilder();

            heading.AppendLine();
            str.AppendLine();

            var padding = IoStateTransition<IoJobMeta.JobState>.StateStrPadding;

            for (var i = 0; i < IoJob<TJob>.StateMapSize; i++)
            {

                var count = Interlocked.Read(ref Counters[i]);
                if (count < 1)
                    continue;

                var ave = Interlocked.Read(ref ServiceTimes[i]) / (count);

                if (i > (int)IoJobMeta.JobState.Undefined  && i < (int)IoJobMeta.JobState.Finished)
                {
                    heading.Append($"{((IoJobMeta.JobState)i).ToString().PadLeft(padding)} {count.ToString().PadLeft(7)} | ");
                    str.Append($"{$"{ave:0,000.0}ms".ToString(CultureInfo.InvariantCulture).PadLeft(padding + 8)} | ");
                }
            }

            _logger.Info($"{Description} Counters: {heading}{str}");
        }

        /// <summary>
        /// Executes the specified function in the context of the source
        /// </summary>
        /// <param name="callback">The function.</param>
        /// <param name="barrier"></param>
        /// <param name="zeroClosure"></param>
        /// <param name="jobClosure"></param>
        /// <returns></returns>
        //public abstract Task<bool> ProduceAsync(Func<IIoSourceBase, Task<bool>> func);
        //public abstract Task<bool> ProduceAsync(Func<IIoSourceBase, Func<IoJob<IIoJob>, ValueTask<bool>>, Task<bool>> func, Func<IoJob<IIoJob>, ValueTask<bool>> barrier);
        public abstract ValueTask<bool> ProduceAsync(Func<IIoSourceBase, Func<IIoJob, IIoZero, ValueTask<bool>>, IIoZero, IIoJob, Task<bool>> callback, Func<IIoJob, IIoZero, ValueTask<bool>> barrier = null, IIoZero zeroClosure = null, IIoJob jobClosure = null);
        
        /// <summary>
        /// Signal source pressure
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Pressure()
        {
            _pressure.Release();
        }

        /// <summary>
        /// Wait for source pressure
        /// </summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public ValueTask<bool> WaitForPressureAsync()
        {
            return _pressure.WaitAsync();
        }

        /// <summary>
        /// Wait for source pressure
        /// </summary>
        /// <param name="consumed"></param>
        /// <param name="releaseCount">Number of waiters to unblock</param>
        /// <exception cref="NotImplementedException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void BackPressure(int releaseCount = 1)
        {
            _backPressure.Release(releaseCount);
        }

        /// <summary>
        /// Wait for sink pressure
        /// </summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public ValueTask<bool> WaitForBackPressureAsync()
        {
            return _backPressure.WaitAsync();
        }
        
        /// <summary>
        /// Signal prefetch pressures
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PrefetchPressure()
        {
            _prefetchPressure.Release();
        }

        /// <summary>
        /// Wait on prefetch pressure
        /// </summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public ValueTask<bool> WaitForPrefetchPressureAsync()
        {
            return _prefetchPressure.WaitAsync();
        }
    }
}
