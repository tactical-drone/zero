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
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore;

namespace zero.core.patterns.bushings
{
    /// <summary>
    /// Used by <see cref="IoZero{TJob}"/> as a source of work of type <see cref="IoZero{TJob}"/>
    /// </summary>
    public abstract class IoSource<TJob> : IoNanoprobe, IIoSource where TJob : IIoJob
    {
        /// <summary>
        /// Constructor
        /// </summary>
        protected IoSource(string description, bool proxy, int prefetchSize = 1, int concurrencyLevel = 1,
            bool zeroAsyncMode = false) : base(description, concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();

            //disableZero = false;
            //if (prefetchSize > concurrencyLevel * 2)
            //    throw new ArgumentOutOfRangeException($"{description}: invalid {nameof(prefetchSize)} = {prefetchSize}, must be at least {nameof(concurrencyLevel)} = {concurrencyLevel*2}");

            //if (maxAsyncSinks > concurrencyLevel)
            //    throw new ArgumentOutOfRangeException($"{description}: invalid {nameof(concurrencyLevel)} = {concurrencyLevel}, must be at least {nameof(maxAsyncSinks)} = {maxAsyncSinks}");

            //if (zeroAsyncMode > concurrencyLevel)
            //    throw new ArgumentOutOfRangeException($"{description}: invalid {nameof(concurrencyLevel)} = {concurrencyLevel}, must be at least {nameof(zeroAsyncMode)} = {zeroAsyncMode}");

            PrefetchEnabled = true;
            BackPressureEnabled = true;
            PrefetchSize = prefetchSize;
            ZeroAsyncMode = zeroAsyncMode;

            Proxy = proxy;

            try
            {
                if (PressureEnabled)
                {
                    _pressure = new IoZeroSemaphoreSlim(AsyncTasks, $"{nameof(_pressure)}: {description}",
                        maxBlockers: concurrencyLevel,
                        0,
                        zeroAsyncMode: ZeroAsyncMode); //TODO Prefetch - 1 or not?
                }

                if (BackPressureEnabled)
                {
                    _backPressure = new IoZeroSemaphoreSlim(AsyncTasks, $"{nameof(_backPressure)}: {description}",
                        maxBlockers: PrefetchSize,
                        initialCount: concurrencyLevel,
                        zeroAsyncMode: ZeroAsyncMode); //TODO Prefetch - 1 or not?
                }

                if (PrefetchEnabled)
                {
                    _prefetchPressure = new IoZeroSemaphoreSlim(AsyncTasks, $"{nameof(_prefetchPressure)}: {description}",
                        maxBlockers: PrefetchSize,
                        initialCount: PrefetchSize + concurrencyLevel,
                        zeroAsyncMode: ZeroAsyncMode);
                }
            }
            catch (Exception e)
            {
                _logger.Fatal(e, $"CRITICAL! Failed to configure semaphores! Aborting!");
                throw;
            }
            
        }

        public string QueueStatus => $"WAITING FOR: prefetch = {_prefetchPressure?.CurNrOfBlockers}/{_prefetchPressure?.Capacity}, back pressure = {_backPressure?.CurNrOfBlockers}/{_backPressure?.Capacity}, pressure = {_pressure?.CurNrOfBlockers}/{_pressure?.Capacity}";

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
        public IIoSource UpstreamSource { get; protected set; }
        
        /// <summary>
        /// Counters for <see cref="IoJobMeta.JobState"/>
        /// </summary>
        public long[] Counters { get; protected set; } = new long[Enum.GetNames(typeof(IoJobMeta.JobState)).Length];

        /// <summary>
        /// Seeds job Ids
        /// </summary>
        private long _jobIdSeed = 0;

        /// <summary>
        /// Total service times per <see cref="Counters"/>
        /// </summary>
        public long[] ServiceTimes { get; protected set; } = new long[Enum.GetNames(typeof(IoJobMeta.JobState)).Length];
        
        /// <summary>
        /// The sink is being throttled against source
        /// </summary>
        private IoZeroSemaphoreSlim _pressure;
        
        /// <summary>
        /// The source is being throttled by the sink 
        /// </summary>
        private IoZeroSemaphoreSlim _backPressure;
        
        /// <summary>
        /// The source is bing throttled on prefetch config
        /// </summary>
        private IoZeroSemaphoreSlim _prefetchPressure;

        public int PrefetchReady => _prefetchPressure.ReadyCount;
        public int BackPressureReady => _backPressure.ReadyCount;

        /// <summary>
        /// Enable prefetch throttling (only allow a certain amount of prefetch
        /// in the presence of concurrent production
        /// </summary>
        public bool PrefetchEnabled { get; protected set; }

        public bool PressureEnabled { get; protected set; }

        public bool BackPressureEnabled { get; protected set; }


        public bool DisableZero { get; protected set; }
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
        public virtual bool IsOperational()
        {
            return !Zeroed();
        }

        /// <summary>
        /// Gets a value indicating whether this source is originating or terminating
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance is Egress; Ingress otherwise, <c>false</c>.
        /// </value>
        public bool IsOriginating { get; }= false;

        /// <summary>
        /// Initial productions, typically larger than 0 for sources
        /// </summary>
        public int PrefetchSize { get; protected set; }

        /// <summary>
        /// The number of concurrent sources allowed
        /// </summary>
        public bool ZeroAsyncMode { get; protected set; }

        public bool Proxy { get; }

        /// <summary>
        /// Current number of items in the Q
        /// </summary>
        public int BacklogCount => _backPressure?.CurNrOfBlockers?? 0;

        private volatile int _rate;

        /// <summary>
        /// Used to rate limit
        /// </summary>
        public int Rate
        {
            get => _rate;
            internal set => _rate = value;
        }

        public int SetRate(int value, int cmp) => Interlocked.CompareExchange(ref _rate, value, cmp);
        
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
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _logger = null;
            _pressure = null;
            _backPressure = null;
            _prefetchPressure = null;
            UpstreamSource = null;
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
            await base.ZeroManagedAsync().FastPath();

            if(_pressure != null)
                await _pressure.Zero(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();
            if(_backPressure != null)
                await _backPressure.Zero(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();
            if(_prefetchPressure != null)
                await _prefetchPressure.Zero(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();

            var reason = $"{nameof(IoSource<TJob>)}: teardown";

            foreach (var o in ObjectStorage)
            {
                if (o.Value is IIoNanite ioNanite)
                    await ioNanite.Zero(this, reason).FastPath();
            }
            ObjectStorage.Clear();

            foreach (var ioConduit in IoConduits.Values)
                await ioConduit.Zero(this, reason).FastPath();
            
            IoConduits.Clear();

            try
            {
                RecentlyProcessed?.Zero(this, reason);
            }
            catch { }

#if DEBUG
            _logger.Trace($"Closed {Description} from {ZeroedFrom}: reason = {ZeroReason}");
#endif
        }

        /// <summary>
        /// Whether we are in teardown
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Zeroed()
        {
            if(UpstreamSource != null)
                return base.Zeroed() || UpstreamSource.Zeroed();

            return base.Zeroed();
        }

        /// <summary>
        /// Producers can forward new productions types <see cref="TFJob"/> via a channels of type <see cref="IoConduit{TJob}"/> to other producers.
        /// This function helps set up a conduit using the supplied source. Channels are cached when created. Channels are associated with producers.
        /// </summary>
        /// <typeparam name="TFJob">The type of job serviced</typeparam>
        /// <param name="id">The conduit id</param>
        /// <param name="concurrencyLevel"></param>
        /// <param name="channelSource">The source of this conduit, if new</param>
        /// <param name="jobMalloc">Used to allocate jobs</param>
        /// <returns></returns>
        public async ValueTask<IoConduit<TFJob>> CreateConduitOnceAsync<TFJob>(string id,
            IoSource<TFJob> channelSource = null,
            Func<object, IIoNanite, IoSink<TFJob>> jobMalloc = null, int concurrencyLevel = 1) where TFJob : IIoJob
        {
            if (channelSource != null && !IoConduits.ContainsKey(id))
            {
                if (!await ZeroAtomic(static async (_, @params, _) =>
                    {
                        var (@this, id, channelSource, jobMalloc, concurrencyLevel) = @params;
                        var newConduit = new IoConduit<TFJob>($"`conduit({id}>{ channelSource.UpstreamSource.Description} ~> { channelSource.Description}", channelSource, jobMalloc, concurrencyLevel);

                        if (!@this.IoConduits.TryAdd(id, newConduit))
                        {
                            await newConduit.Zero(@this,$"{nameof(CreateConduitOnceAsync)}: lost race").FastPath();
                            @this._logger.Trace($"Could not add {id}, already exists = {@this.IoConduits.ContainsKey(id)}");
                            return false;
                        }

                        return true;
                    }, ValueTuple.Create(this, id,channelSource, jobMalloc, concurrencyLevel)).FastPath())
                {
                    if (!Zeroed())
                    {
                        try
                        {
                            return (IoConduit<TFJob>)IoConduits[id];
                        }
                        catch when(Zeroed()){}
                        catch (Exception e)when(!Zeroed())
                        {
                            _logger.Trace(e, $"Conduit {id} after race, not found");
                        }
                    }

                    return null;
                }
            }

            try
            {
                return (IoConduit<TFJob>)IoConduits[id];
            }
            catch when(channelSource == null || Zeroed()){}
            catch (Exception e)when (channelSource !=null && !Zeroed())
            {
                _logger.Fatal(e, $"Conduit {id} after race, not found");
            }
            return null;
        }

        
        /// <summary>
        /// Gets a conduit with a certain Id
        /// </summary>
        /// <typeparam name="TFJob">The type that the conduit speaks</typeparam>
        /// <param name="id">The id of the conduit</param>
        /// <returns>The conduit</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IoConduit<TFJob> GetConduit<TFJob>(string id)
            where TFJob : IoSink<TFJob>, IIoJob
        {
            if (IoConduits.TryGetValue(id, out var ioConduit))
                return (IoConduit<TFJob>)ioConduit;
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
        public string PrintCounters()
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

                if (i is > (int)IoJobMeta.JobState.Undefined and < (int)IoJobMeta.JobState.Halted)
                {
                    heading.Append($"{((IoJobMeta.JobState)i).ToString().PadLeft(padding)} {count.ToString(),7} | ");
                    str.Append($"{$"{ave:0,000.0}ms".ToString(CultureInfo.InvariantCulture).PadLeft(padding + 8)} | ");
                }
            }

            return $"{Description} Counters: {heading}{str}";
        }

        /// <summary>
        /// Executes the specified function in the context of the source
        /// </summary>
        /// <param name="produce">The function.</param>
        /// <param name="ioJob">The job being produced on</param>
        /// <param name="barrier">A synchronization barrier from</param>
        /// <param name="ioZero">Optional produce state</param>
        /// <returns>True on success, false otherwise</returns>
        public virtual ValueTask<bool> ProduceAsync<T>(
            Func<IIoSource, IIoSource.IoZeroCongestion<T>, T, IIoJob, ValueTask<bool>> produce,
            IIoJob ioJob,
            IIoSource.IoZeroCongestion<T> barrier,
            T ioZero)
        {
            try
            {
                return produce(this, barrier, ioZero, ioJob);
            }
            catch (Exception) when (Zeroed()) {}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"Source `{Description ?? "N/A"}' produce failed:");
            }

            return new ValueTask<bool>(false);
        }

        /// <summary>
        /// Signal source pressure
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Pressure(int releaseCount = 1)
        {
            return PressureEnabled ? _pressure.Release(true, releaseCount) : releaseCount;
        }

        /// <summary>
        /// Wait for source pressure
        /// </summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> WaitForPressureAsync()
        {
            return PressureEnabled ? _pressure.WaitAsync() : new ValueTask<bool>(true);
        }

        /// <summary>
        /// Wait for source pressure
        /// </summary>
        /// <param name="releaseCount">Number of waiters to unblock</param>
        /// <exception cref="NotImplementedException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int BackPressure(int releaseCount = 1)
        {
            return BackPressureEnabled ? _backPressure.Release(true, releaseCount) : releaseCount;
        }

        /// <summary>
        /// Wait for sink pressure
        /// </summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> WaitForBackPressureAsync()
        {
            return BackPressureEnabled ? _backPressure.WaitAsync() : new ValueTask<bool>(true);
        }
        
        /// <summary>
        /// Signal prefetch pressures
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int PrefetchPressure(int releaseCount = 1)
        {
            return PrefetchEnabled ? _prefetchPressure.Release(true, releaseCount) : releaseCount;
        }

        /// <summary>
        /// Wait on prefetch pressure
        /// </summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> WaitForPrefetchPressureAsync()
        {
            return PrefetchEnabled ? _prefetchPressure.WaitAsync() : new ValueTask<bool>(true);
        }

        /// <summary>
        /// Seeds job Ids
        /// </summary>
        /// <returns>The next unique job Id</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long NextJobIdSeed()
        {
            return Interlocked.Increment(ref _jobIdSeed);
        }
    }
}
