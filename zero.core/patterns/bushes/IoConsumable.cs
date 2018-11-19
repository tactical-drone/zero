using System;
using System.Threading.Tasks;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Jobs that need to be consumed by the workers
    /// </summary>
    /// <typeparam name="TProducer"></typeparam>
    public abstract class IoConsumable<TProducer> : IoProducable<TProducer>
    where TProducer : IoJobSource
    {
        /// <summary>
        /// A description of the work
        /// </summary>
        public string JobDescription;

        /// <summary>
        /// The overall description of the work that needs to be done and the job that is doing it
        /// </summary>
        public override string Description => $"({Id}) {JobDescription} {WorkDescription}";        
    }
}
