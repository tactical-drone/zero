using System;
using NLog;
using zero.core.patterns.bushes.contracts;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Used as a simple job forwarding source consumer queue, by <see cref="IoZero{TJob}"/>
    /// </summary>
    /// <typeparam name="TJob">The type of the job.</typeparam>
    /// <seealso cref="IoZero{TJob}" />
    /// <seealso cref="IIoChannel" />
    public class IoChannel<TJob>:IoZero<TJob>, IIoChannel
        where TJob : IIoJob
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoChannel{TJob}"/> class.
        /// </summary>
        /// <param name="description">A description of the channel destination</param>
        /// <param name="source">The source of the work to be done</param>
        /// <param name="mallocJob">A callback to malloc individual consumer jobs from the heap</param>
        public IoChannel(string description, IoSource<TJob> source, Func<object, IoLoad<TJob>> mallocJob) : base(description, source, mallocJob)
        {
            source.SetUpstreamChannel(this);
        }
    }
}
