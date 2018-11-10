using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Threading;
using NLog;
using zero.core.conf;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Producer consumer concurrency management hooks
    /// </summary>
    public abstract class IoConcurrentProcess : IoConfigurable
    {
        /// <summary>
        /// Constructor
        /// </summary>
        protected IoConcurrentProcess()
        {            
            ProduceSemaphore = new Semaphore(0, 1);
            ConsumeSemaphore = new SemaphoreSlim(1);
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
        /// Maximum concurrent producers
        /// </summary>
        private readonly int _maxProducers;

        /// <summary>
        /// Maximum concurrent consumers
        /// </summary>
        private readonly int _maxConsumers;

        /// <summary>
        /// Description //TODO
        /// </summary>
        protected virtual string Description => ToString();

        /// <summary>
        /// Counters for <see cref="IoProducable{TSource}.State"/>
        /// </summary>
        public long[] Counters = new long[Enum.GetNames(typeof(IoProducable<>.State)).Length];

        /// <summary>
        /// Total service times per <see cref="Counters"/>
        /// </summary>
        public long[] ServiceTimes = new long[Enum.GetNames(typeof(IoProducable<>.State)).Length];

        //TODO remove publics for DOS
        /// <summary>
        /// The producer semaphore
        /// </summary>
        public Semaphore ProduceSemaphore;

        /// <summary>
        /// The cosumer semaphore
        /// </summary>
        public SemaphoreSlim ConsumeSemaphore;

        /// <summary>
        /// <see cref="PrintCounters"/> only prints events that took lonnger that this value in microseconds
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

            var padding = IoWorkStateTransition<IoConcurrentProcess>.StateStrPadding;


            for (var i = 0; i < IoProducable<IoConcurrentProcess>.StateMapSize; i++)
            {

                var count = Interlocked.Read(ref Counters[i]);
                if (count < 1)
                    continue;

                var ave = Interlocked.Read(ref ServiceTimes[i]) / (count);

                //if (ave > parm_event_min_ave_display)
                {
                    heading.Append(
                        $"{((IoProducable<IoConcurrentProcess>.State)i).ToString().PadLeft(padding)} {count.ToString().PadLeft(7)} | ");
                    str.Append(string.Format("{0} | ",
                        (string.Format("{0:0,000.0}ms", ave)).ToString(CultureInfo.InvariantCulture)
                        .PadLeft(padding + 8)));
                }
            }

            _logger.Info($"`{Description}' Counters: {heading}{str}");
        }
    }
}
