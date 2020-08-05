using System;
using NLog;
using zero.core.patterns.bushes.contracts;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Used as a simple job forwarding producer consumer queue, by <see cref="IoProducerConsumer{TJob}"/>
    /// </summary>
    /// <typeparam name="TJob">The type of the job.</typeparam>
    /// <seealso cref="zero.core.patterns.bushes.IoProducerConsumer{TJob}" />
    /// <seealso cref="IIoChannel" />
    public class IoChannel<TJob>:IoProducerConsumer<TJob>, IIoChannel
        where TJob : IIoWorker
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoChannel{TJob}"/> class.
        /// </summary>
        /// <param name="description">A description of the channel destination</param>
        /// <param name="producer">The source of the work to be done</param>
        /// <param name="mallocMessage">A callback to malloc individual consumer jobs from the heap</param>
        public IoChannel(string description, IoProducer<TJob> producer, Func<object, IoConsumable<TJob>> mallocMessage) : base(description, producer, mallocMessage)
        {
            producer.SetUpstreamChannel(this);
        }        
    }
}
