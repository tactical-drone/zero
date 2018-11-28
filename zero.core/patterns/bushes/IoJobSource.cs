using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Used by <see cref="IoProducerConsumer{TConsumer,TProducer,TJob}"/> as a source of work of type <see cref="TJob"/>
    /// </summary>
    public abstract class IoJobSource <TJob>
    where TJob:IIoJob
    {
        /// <summary>
        /// Constructor
        /// </summary>
        protected IoJobSource(int readAhead)
        {            
            ConsumerBarrier = new SemaphoreSlim(0); //TODO make configurable
            ProducerBarrier = new SemaphoreSlim(readAhead); 
            _logger = LogManager.GetCurrentClassLogger();
            Spinners = new CancellationTokenSource();
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Used to signal shutdown
        /// </summary>
        public CancellationTokenSource Spinners;

        /// <summary>
        /// Keys this instance.
        /// </summary>
        /// <returns>The unique key of this instance</returns>
        public abstract int Key { get; }

        /// <summary>
        /// Description used as a key
        /// </summary>
        public abstract string Description { get; }

        /// <summary>
        /// Counters for <see cref="IoProducable{TProducer}.State"/>
        /// </summary>
        public long[] Counters { get; set; } = new long[Enum.GetNames(typeof(IoProducable<>.State)).Length];

        /// <summary>
        /// Total service times per <see cref="Counters"/>
        /// </summary>
        public long[] ServiceTimes { get; set; } = new long[Enum.GetNames(typeof(IoProducable<>.State)).Length];
        
        /// <summary>
        /// The producer semaphore
        /// </summary>
        public SemaphoreSlim ConsumerBarrier { get; protected set; }

        /// <summary>
        /// The consumer semaphore
        /// </summary>
        public SemaphoreSlim ProducerBarrier { get; protected set; }

        /// <summary>
        /// Gets a value indicating whether this instance is operational.
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance is operational; otherwise, <c>false</c>.
        /// </value>
        public abstract bool IsOperational { get; }

        /// <summary>
        /// <see cref="PrintCounters"/> only prints events that took longer that this value in microseconds
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected long parm_event_min_ave_display = 0;

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

            for (var i = 0; i < IoProducable<TJob>.StateMapSize; i++)
            {

                var count = Interlocked.Read(ref Counters[i]);
                if (count < 1)
                    continue;

                var ave = Interlocked.Read(ref ServiceTimes[i]) / (count);

                if ( i>0 )
                {
                    heading.Append(
                        $"{((IoProducable<TJob>.State)i).ToString().PadLeft(padding)} {count.ToString().PadLeft(7)} | ");
                    str.Append(string.Format("{0} | ",
                        (string.Format("{0:0,000.0}ms", ave)).ToString(CultureInfo.InvariantCulture)
                        .PadLeft(padding + 8)));
                }
            }

            _logger.Trace($"`{Description}' Counters: {heading}{str}");
        }

        /// <summary>
        /// Closes this source
        /// </summary>
        public abstract void Close();

        /// <summary>
        /// Executes the specified function in the context of the source
        /// </summary>
        /// <param name="func">The function.</param>
        /// <returns></returns>
        public abstract Task<Task> Execute(Func<IIoJobSource, Task<Task>> func);
    }
}
