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
    /// Used by <see cref="IoProducerConsumer{TConsumer,TProducer}"/> as a source of work
    /// </summary>
    public abstract class IoJobSource : IoConfigurable
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
        /// Description //TODO
        /// </summary>
        protected virtual string Description => ToString();

        /// <summary>
        /// Counters for <see cref="IoProducable{TProducer}.State"/>
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
        public SemaphoreSlim ConsumerBarrier;

        /// <summary>
        /// The consumer semaphore
        /// </summary>
        public SemaphoreSlim ProducerBarrier;

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

            var padding = IoWorkStateTransition<IoJobSource>.StateStrPadding;


            for (var i = 0; i < IoProducable<IoJobSource>.StateMapSize; i++)
            {

                var count = Interlocked.Read(ref Counters[i]);
                if (count < 1)
                    continue;

                var ave = Interlocked.Read(ref ServiceTimes[i]) / (count);

                if ( i>0 )
                {
                    heading.Append(
                        $"{((IoProducable<IoJobSource>.State)i).ToString().PadLeft(padding)} {count.ToString().PadLeft(7)} | ");
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
    }
}
