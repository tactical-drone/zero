using System;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Used as a simple job forwarding source consumer queue, by <see cref="IoZero{TJob}"/>
    /// </summary>
    /// <typeparam name="TJob">The type of the job.</typeparam>
    /// <seealso cref="IoZero{TJob}" />
    /// <seealso cref="IIoConduit" />
    public class IoConduit<TJob>:IoZero<TJob>, IIoConduit
        where TJob : IIoJob
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoConduit{TJob}"/> class.
        /// </summary>
        /// <param name="description">A description of the channel destination</param>
        /// <param name="source">The source of the work to be done</param>
        /// <param name="mallocJob">A callback to malloc individual consumer jobs from the heap</param>
        /// <param name="concurrencyLevel">Concurrency Level</param>
        public IoConduit(string description, IoSource<TJob> source, Func<object, IIoNanite, IoSink<TJob>> mallocJob, int concurrencyLevel = 1) : base(description, source, mallocJob, false, true, concurrencyLevel)
        {
            
        }
    }
}
