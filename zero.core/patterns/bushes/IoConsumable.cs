using System.Threading.Tasks;
using zero.core.patterns.bushes.contracts;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Jobs that need to be consumed by the workers
    /// </summary>
    /// <typeparam name="TJob">The type of the job</typeparam>
    public abstract class IoConsumable<TJob> : IoProduceble<TJob>
        where TJob : IIoWorker
        
    {
        /// <summary>
        /// A description of the work
        /// </summary>
        public string JobDescription;

        /// <summary>
        /// The overall description of the work that needs to be done and the job that is doing it
        /// </summary>
        public override string ProductionDescription => $"({Id}) {JobDescription} {WorkDescription}";

        /// <summary>
        /// Consumes the job
        /// </summary>
        /// <returns>The state of the consumption</returns>
        public abstract Task<State> ConsumeAsync();
    }
}
