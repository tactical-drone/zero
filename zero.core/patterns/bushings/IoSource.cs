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
using zero.core.misc;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore.core;

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

            //if (runContinuationsAsync > concurrencyLevel)
            //    throw new ArgumentOutOfRangeException($"{description}: invalid {nameof(concurrencyLevel)} = {concurrencyLevel}, must be at least {nameof(runContinuationsAsync)} = {runContinuationsAsync}");

            PrefetchSize = prefetchSize;
            ZeroAsyncMode = zeroAsyncMode;

            Proxy = proxy;

            try
            {
                //_backPressure = new IoZeroSemaphoreSlim(AsyncTasks, $"{nameof(_backPressure)}: {description}", PrefetchSize, PrefetchSize);
                IIoZeroSemaphoreBase<int> c = new IoZeroCore<int>(description, PrefetchSize, AsyncTasks, PrefetchSize);
                _backPressure = c.ZeroRef(ref c, _ => Environment.TickCount);
            }
            catch (Exception e)
            {
                _logger.Fatal(e, $"CRITICAL! Failed to configure semaphores! Aborting!");
                throw;
            }
        }

        public string QueueStatus => $"back pressure = {_backPressure?.WaitCount}/{_backPressure?.Capacity}";

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
        /// If the connection is Ingress or Egress or both
        /// </summary>
        public IIoSource.Heading Direction { get; protected internal set; }

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
        private long _jobIdSeed;

        /// <summary>
        /// Total service times per <see cref="Counters"/>
        /// </summary>
        public long[] ServiceTimes { get; protected set; } = new long[Enum.GetNames(typeof(IoJobMeta.JobState)).Length];
        
        /// <summary>
        /// The source is being throttled by the sink 
        /// </summary>
        private readonly IIoZeroSemaphoreBase<int> _backPressure;
        
        public int BackPressureReady => _backPressure.ReadyCount;

        //public bool BackPressureEnabled { get; protected set; }

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
        public int BacklogCount => _backPressure?.WaitCount?? 0;

        private int _rate = Environment.TickCount;

        private int _rateSet;

        /// <summary>
        /// Used to rate limit
        /// </summary>
        public int Rate
        {
            get => _rate;
            internal set => Volatile.Write(ref _rate, value);
        }

        /// <summary>
        /// Used to rate limit
        /// </summary>
        public int RateSet
        {
            get => _rateSet;
            internal set => Volatile.Write(ref _rateSet, value);
        }

        private long _zeroTimeStamp = -1;

        /// <summary>
        /// Current timestamp
        /// </summary>
        public long ZeroTimeStamp
        {
            get => _zeroTimeStamp;
            set => _zeroTimeStamp = value;
        }

        public int SetRate(int value, int cmp) => Interlocked.CompareExchange(ref _rate, value, cmp);

        public int SetRateSet(int value, int cmp) => Interlocked.CompareExchange(ref _rateSet, value, cmp);

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

            _backPressure?.ZeroSem();

            var reason = $"{nameof(IoSource<TJob>)}: teardown";

            foreach (var o in ObjectStorage)
            {
                if (o.Value is IIoNanite ioNanite)
                    await ioNanite.DisposeAsync(this, reason).FastPath();
            }
            ObjectStorage.Clear();

            foreach (var ioConduit in IoConduits.Values)
                await ioConduit.DisposeAsync(this, reason).FastPath();
            
            IoConduits.Clear();

            try
            {
                if(RecentlyProcessed != null)
                    await RecentlyProcessed.DisposeAsync(this, reason).FastPath();
            }
            catch
            {
                // ignored
            }

#if DEBUG
            if (UpTime.ElapsedUtcMs() > 2000)
                _logger.Trace($"Closed {Description}");
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
                //var locker = UpstreamSource ?? this;
                if (!await ZeroAtomicAsync(static (_, @params, _) => 
                    {
                        var (@this, id, channelSource, jobMalloc, concurrencyLevel) = @params;

                        if (@this.IoConduits.ContainsKey(id))
                            return new ValueTask<bool>(true);

                        var newConduit = new IoConduit<TFJob>($"`conduit({id}>{channelSource.UpstreamSource.Description} ~> {channelSource.Description}", @this, channelSource, jobMalloc);
                        if (!@this.IoConduits.TryAdd(id, newConduit))
                            // ReSharper disable once MethodHasAsyncOverload
                            newConduit.Dispose();
#if TRACE
                        else
                            @this._logger.Debug($"Added {nameof(IoConduit<TJob>)}: {id}; {@this.Description}");
#endif

                        return new ValueTask<bool>(true);
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
                    heading.Append($"{((IoJobMeta.JobState)i).ToString().PadLeft(padding)} {count,7} | ");
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
        /// <returns>True on success, false otherwise</returns>
        public virtual ValueTask<bool> ProduceAsync(Func<IIoSource, IIoJob, ValueTask<bool>> produce,
            IIoJob ioJob)
        {
            try
            {
                return produce(this, ioJob);
            }
            catch (Exception) when (Zeroed()) {}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"Source `{Description ?? "N/A"}' produce failed:");
            }

            return new ValueTask<bool>(false);
        }

        /// <summary>
        /// Wait for source pressure
        /// </summary>
        /// <param name="releaseCount">Number of waiters to unblock</param>
        /// <param name="zeroAsync"></param>
        /// <param name="prime"></param>
        /// <exception cref="NotImplementedException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int BackPressure(int releaseCount = 1, bool zeroAsync = false, bool prime = true)
        {
            return _backPressure.Release(Environment.TickCount, releaseCount, zeroAsync, prime);
        }

        /// <summary>
        /// Wait for sink pressure
        /// </summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<int> WaitForBackPressureAsync()
        {
            return _backPressure.WaitAsync();
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

        public long CurJobId => _jobIdSeed;
    }
}
