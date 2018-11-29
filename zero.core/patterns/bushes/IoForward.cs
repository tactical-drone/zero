using System;
using zero.core.patterns.bushes.contracts;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Used as a simple job forwarding producer consumer queue, by <see cref="IoProducerConsumer{TJob}"/>
    /// </summary>
    /// <typeparam name="TJob">The type of the job.</typeparam>
    /// <seealso cref="zero.core.patterns.bushes.IoProducerConsumer{TJob}" />
    /// <seealso cref="zero.core.patterns.bushes.contracts.IIoForward" />
    public class IoForward<TJob>:IoProducerConsumer<TJob>, IIoForward
        where TJob : IIoWorker
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoForward{TJob}"/> class.
        /// </summary>
        /// <param name="description"></param>
        /// <param name="source">The source of the work to be done</param>
        /// <param name="mallocMessage">A callback to malloc individual consumer jobs from the heap</param>
        public IoForward(string description, IoProducer<TJob> source, Func<object, IoConsumable<TJob>> mallocMessage) : base(description, source, mallocMessage)
        {
        }        
    }
}
