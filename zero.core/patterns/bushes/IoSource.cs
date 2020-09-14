using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MathNet.Numerics;
using Microsoft.VisualStudio.Threading;
using NLog;
using zero.core.conf;
using zero.core.data.contracts;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Used by <see cref="IoZero{TJob}"/> as a source of work of type <see cref="TJob"/>
    /// </summary>
    public abstract class IoSource<TJob> : IoConfigurable, IIoSource where TJob : IIoJob
    {
        /// <summary>
        /// Constructor
        /// </summary>
        protected IoSource(int readAheadBufferSize = 2) //TODO
        {
            ReadAheadBufferSize = readAheadBufferSize;
            ProducerBarrier = new AsyncAutoResetEvent(true);
            ProducerBarrier.Set();
            ConsumerBarrier = new AsyncAutoResetEvent(true);
            ProduceAheadBarrier = new AsyncAutoResetEvent(true);
            ConsumeAheadBarrier = new AsyncAutoResetEvent(true);

            //ConsumerBarrier = new SemaphoreSlim(0);
            //ProducerBarrier = new SemaphoreSlim(readAheadBufferSize);
            //ConsumeAheadBarrier = new SemaphoreSlim(1);
            //ProduceAheadBarrier = new SemaphoreSlim(1);
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
        public IIoSource UpstreamIoSource { get; protected set; }

        /// <summary>
        /// Counters for <see cref="IoJob{TJob}.JobState"/>
        /// </summary>
        public long[] Counters { get; protected set; } = new long[Enum.GetNames(typeof(IoJob<>.JobState)).Length];

        /// <summary>
        /// Total service times per <see cref="Counters"/>
        /// </summary>
        public long[] ServiceTimes { get; protected set; } = new long[Enum.GetNames(typeof(IoJob<>.JobState)).Length];


        public AsyncAutoResetEvent ProducerBarrier { get; protected set; }

        public AsyncAutoResetEvent ConsumerBarrier { get; protected set; }

        public AsyncAutoResetEvent ConsumeAheadBarrier { get; protected set; }

        public AsyncAutoResetEvent ProduceAheadBarrier { get; protected set; }
        ///// <summary>
        ///// The source semaphore
        ///// </summary>
        //public SemaphoreSlim ConsumerBarrier { get; protected set; }

        ///// <summary>
        ///// The consumer semaphore
        ///// </summary>
        //public SemaphoreSlim ProducerBarrier { get; protected set; }

        ///// <summary>
        ///// The consumer semaphore
        ///// </summary>
        //public SemaphoreSlim ConsumeAheadBarrier { get; protected set; }

        ///// <summary>
        ///// The consumer semaphore
        ///// </summary>
        //public SemaphoreSlim ProduceAheadBarrier { get; protected set; }


        /// <summary>
        /// Whether to only consume one at a time, but produce many at a time
        /// </summary>
        public bool BlockOnConsumeAheadBarrier { get; protected set; } = false;

        /// <summary>
        /// Whether to only consume one at a time, but produce many at a time
        /// </summary>
        public bool BlockOnProduceAheadBarrier { get; protected set; } = false;

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
        public int ReadAheadBufferSize { get; set; }

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
        /// 
        /// </summary>
        private long _nextProducerId;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        protected override void ZeroUnmanaged()
        {
            //Unblock any blockers
            //ProducerBarrier.Dispose();
            //ConsumerBarrier.Dispose();
            //ConsumeAheadBarrier.Dispose();
            //ProduceAheadBarrier.Dispose();

            base.ZeroUnmanaged();

#if SAFE_RELEASE
            ConsumerBarrier = null;
            ConsumeAheadBarrier = null;
            ProduceAheadBarrier = null;
            ProducerBarrier = null;
            RecentlyProcessed = null;
            IoChannels = null;
            ObjectStorage = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override async Task ZeroManagedAsync()
        {
            //foreach (var objectStorageValue in ObjectStorage.Values)
            //{
            //    ((IIoZeroable)objectStorageValue).ZeroAsync(this);
            //}

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
        public IoChannel<TFJob> AttachProducer<TFJob>(string id, bool cascade = false, IoSource<TFJob> channelSource = null,
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


                    //var longRunningAsyncWork = Task.Factory.StartNew(async delegate
                    //{
                        ZeroEnsureAsync(() =>
                        {
                            if (!IoChannels.TryAdd(id, newChannel)) return Task.FromResult(false);
                            ZeroOnCascade(newChannel, cascade);
                            return Task.FromResult(true);
                        }).ConfigureAwait(false).GetAwaiter().GetResult();
                    //});

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

            var padding = IoWorkStateTransition<TJob>.StateStrPadding;

            for (var i = 0; i < IoJob<TJob>.StateMapSize; i++)
            {

                var count = Interlocked.Read(ref Counters[i]);
                if (count < 1)
                    continue;

                var ave = Interlocked.Read(ref ServiceTimes[i]) / (count);

                if (i > (int)IoJob<TJob>.JobState.Undefined  && i < (int)IoJob<TJob>.JobState.Finished)
                {
                    heading.Append($"{((IoJob<TJob>.JobState)i).ToString().PadLeft(padding)} {count.ToString().PadLeft(7)} | ");
                    str.Append($"{$"{ave:0,000.0}ms".ToString(CultureInfo.InvariantCulture).PadLeft(padding + 8)} | ");
                }
            }

            _logger.Info($"{Description} Counters: {heading}{str}");
        }

        /// <summary>
        /// Executes the specified function in the context of the source
        /// </summary>
        /// <param name="func">The function.</param>
        /// <returns></returns>
        public abstract Task<bool> ProduceAsync(Func<IIoSourceBase, Task<bool>> func);
    }
}
