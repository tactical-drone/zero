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
        protected IoSource(string description, int prefetchSize = 1, int concurrencyLevel = 1, int maxAsyncSinks = 0, int maxAsyncSources = 0) : base(description, concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
            
            if (prefetchSize > concurrencyLevel)
                throw new ArgumentOutOfRangeException($"{description}: invalid {nameof(concurrencyLevel)} = {concurrencyLevel}, must be at least {nameof(prefetchSize)} = {prefetchSize}");

            if (maxAsyncSinks > concurrencyLevel)
                throw new ArgumentOutOfRangeException($"{description}: invalid {nameof(concurrencyLevel)} = {concurrencyLevel}, must be at least {nameof(maxAsyncSinks)} = {maxAsyncSinks}");

            if (maxAsyncSources > concurrencyLevel)
                throw new ArgumentOutOfRangeException($"{description}: invalid {nameof(concurrencyLevel)} = {concurrencyLevel}, must be at least {nameof(maxAsyncSources)} = {maxAsyncSources}");

            PrefetchSize = prefetchSize;
            MaxAsyncSinks = maxAsyncSinks;
            MaxAsyncSources = maxAsyncSources;

            AsyncEnabled = MaxAsyncSinks > 0 || MaxAsyncSources > 0;

            var enableFairQ = false;
            var enableDeadlockDetection = true;
#if RELEASE
            enableDeadlockDetection = false;
#endif

            //todo GENERALIZE
            try
            {
                _pressure = new IoZeroSemaphoreSlim(AsyncTasks.Token, $"{nameof(_pressure)}, {description}",
                    maxBlockers: concurrencyLevel*2, maxAsyncWork:MaxAsyncSinks, enableAutoScale: false, enableFairQ: enableFairQ, enableDeadlockDetection: enableDeadlockDetection);

                _backPressure = new IoZeroSemaphoreSlim(AsyncTasks.Token, $"{nameof(_backPressure)}, {description}",
                    maxBlockers: concurrencyLevel*2,
                    maxAsyncWork: MaxAsyncSources,
                    initialCount: prefetchSize,
                    enableFairQ: enableFairQ, enableDeadlockDetection: enableDeadlockDetection);

                _prefetchPressure = new IoZeroSemaphoreSlim(AsyncTasks.Token, $"{nameof(_prefetchPressure)}, {description}"
                    , maxBlockers: concurrencyLevel*2,
                    maxAsyncWork: MaxAsyncSources,
                    initialCount: prefetchSize,
                    enableFairQ: enableFairQ, enableDeadlockDetection: enableDeadlockDetection);
            }
            catch (Exception e)
            {
                _logger.Fatal(e, $"CRITICAL! Failed to configure semaphores! Aborting!");
                throw;
            }

        }

        /// <summary>
        /// logger
        /// </summary>
        private Logger _logger;

        /// <summary>
        /// A dictionary of downstream channels
        /// </summary>
        public ConcurrentDictionary<string, IIoConduit> IoConduits { get; protected set; }  = new();

        /// <summary>
        /// Keys this instance.
        /// </summary>
        /// <returns>The unique key of this instance</returns>
        public abstract string Key { get; }

        /// <summary>
        /// Sets an upstream source
        /// </summary>
        public IIoSource Upstream { get; set; }
        
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
        /// Initial productions, typically larger than 0 for sources
        /// </summary>
        public int PrefetchSize { get; protected set; }

        /// <summary>
        /// The number of concurrent sinks allowed
        /// </summary>
        public int MaxAsyncSinks { get; protected set; }

        /// <summary>
        /// The number of concurrent sources allowed
        /// </summary>
        public int MaxAsyncSources { get; protected set; }

        /// <summary>
        /// If async workers are enabled
        /// </summary>
        public bool AsyncEnabled { get; protected set; }

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
            _logger = null;
            _pressure = null;
            _backPressure = null;
            _prefetchPressure = null;
            Upstream = null;
            RecentlyProcessed = null;
            IoConduits = null;
            ObjectStorage = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            _pressure.Zero();
            _backPressure.Zero();
            _prefetchPressure.Zero();

            foreach (var o in ObjectStorage)
            {
                if (o.Value is IIoNanite ioNanite)
                    await ioNanite.ZeroAsync(this).FastPath().ConfigureAwait(false);
            }
            ObjectStorage.Clear();

            foreach (var ioConduit in IoConduits.Values)
                await ioConduit.ZeroAsync(this).FastPath().ConfigureAwait(false);
            
            IoConduits.Clear();

            try
            {
                await RecentlyProcessed.ZeroAsync(this).ConfigureAwait(false);
            }
            catch { }

            await base.ZeroManagedAsync().FastPath().ConfigureAwait(false);

            _logger.Trace($"Closed {Description} from {ZeroedFrom}");
        }

        /// <summary>
        /// Producers can forward new productions types <see cref="TFJob"/> via a channels of type <see cref="IoConduit{TJob}"/> to other producers.
        /// This function helps set up a conduit using the supplied source. Channels are cached when created. Channels are associated with producers.
        /// </summary>
        /// <typeparam name="TFJob">The type of job serviced</typeparam>
        /// <param name="id">The conduit id</param>
        /// <param name="concurrencyLevel"></param>
        /// <param name="cascade">ZeroOnCascade close events</param>
        /// <param name="channelSource">The source of this conduit, if new</param>
        /// <param name="jobMalloc">Used to allocate jobs</param>
        /// ///
        /// <returns></returns>
        public async ValueTask<IoConduit<TFJob>> CreateConduitOnceAsync<TFJob>(string id,
            int concurrencyLevel = -1, bool cascade = false,
            IoSource<TFJob> channelSource = null,
            Func<object, IoSink<TFJob>> jobMalloc = null) where TFJob : IIoJob
        {
            if (!IoConduits.ContainsKey(id))
            {
                if (channelSource == null || jobMalloc == null)
                {
                    _logger.Trace($"Waiting for conduit {id} in {Description} to initialize...");
                    return null;
                }

                if (!await ZeroAtomicAsync(static async (nanite, parms, disposed) =>
                {
                    var (@this, id, channelSource, jobMalloc, concurrencyLevel) = parms;
                    var newChannel =
                        new IoConduit<TFJob>($"`conduit({id}>{channelSource.GetType().Name}>{typeof(TFJob).Name})'",
                            channelSource, jobMalloc, concurrencyLevel);

                    if (!@this.IoConduits.TryAdd(id, newChannel))
                    {
                        await newChannel.ZeroAsync(new IoNanoprobe("lost race")).FastPath().ConfigureAwait(false);
                        @this._logger.Trace($"Could not add {id}, already exists = {@this.IoConduits.ContainsKey(id)}");
                        return false;
                    }

                    return true;
                    // if (!ZeroOnCascade(newChannel, cascade).success)
                    // {
                    //     _logger.Trace($"Failed to set cascade on newly formed channel {id}");
                    //     return false;
                    // }
                    // else
                    // {
                    //     return true;
                    // }
                }, ValueTuple.Create(this, id,channelSource, jobMalloc, concurrencyLevel)).ConfigureAwait(false))
                {
                    if (!Zeroed())
                    {
                        try
                        {
                            return (IoConduit<TFJob>)IoConduits[id];
                        }
                        catch (Exception e)
                        {
                            _logger.Trace(e, $"Conduit {id} after race, not found");
                            throw;
                        }
                    }
                    else
                    {
                        return null;
                    }
                }
            }

            try
            {
                return (IoConduit<TFJob>)IoConduits[id];
            }
            catch (Exception e)
            {
                _logger.Fatal(e, $"Conduit {id} after race, not found");
                throw;
            }
        }

        
        /// <summary>
        /// Gets a conduit with a certain Id
        /// </summary>
        /// <typeparam name="TFJob">The type that the conduit speaks</typeparam>
        /// <param name="id">The id of the conduit</param>
        /// <returns>The conduit</returns>
        public IoConduit<TFJob> GetConduit<TFJob>(string id)
            where TFJob : IoSink<TFJob>, IIoJob
        {
            try
            {
                return (IoConduit<TFJob>)IoConduits[id];
            }
            catch { }

            return null;
        }

        /// <summary>
        /// Sets a conduit
        /// </summary>
        /// <typeparam name="TFJob"></typeparam>
        /// <param name="id">The conduit Id</param>
        /// <param name="conduit">The conduit</param>
        /// <returns>True if successful</returns>
        public bool SetConduit<TFJob>(string id, IoConduit<TFJob> conduit)
            where TFJob : IoSink<TFJob>, IIoJob
        {
            return IoConduits.TryAdd(id, conduit);
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

                if (i > (int)IoJobMeta.JobState.Undefined  && i < (int)IoJobMeta.JobState.Halted)
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
        /// <param name="jobClosure">The job being produced on</param>
        /// <param name="barrier">A synchronization barrier from</param>
        /// <param name="nanite">Optional callback state</param>
        /// <returns>True on success, false otherwise</returns>
        public abstract ValueTask<bool> ProduceAsync<T>(
            Func<IIoSourceBase, Func<IIoJob, T, ValueTask<bool>>, T, IIoJob, ValueTask<bool>> callback,
            IIoJob jobClosure = null,
            Func<IIoJob, T, ValueTask<bool>> barrier = null,
            T nanite = default);
        
        /// <summary>
        /// Signal source pressure
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<int> PressureAsync(int releaseCount = 1)
        {
            return _pressure.ReleaseAsync(releaseCount, MaxAsyncSources > 0);
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
        /// <param name="releaseCount">Number of waiters to unblock</param>
        /// <exception cref="NotImplementedException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<int> BackPressureAsync(int releaseCount = 1)
        {
            return _backPressure.ReleaseAsync(releaseCount, MaxAsyncSinks > 0);
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
        public ValueTask<int> PrefetchPressure(int releaseCount = 1)
        {
            return _prefetchPressure.ReleaseAsync(releaseCount, MaxAsyncSources > 0);
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
