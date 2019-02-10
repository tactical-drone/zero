using System.Threading.Tasks;
using zero.core.patterns.bushes.contracts;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Jobs that need to be consumed by the workers
    /// </summary>
    /// <typeparam name="TJob">The type of the job</typeparam>
    public abstract class IoConsumable<TJob> : IoProducible<TJob>
        where TJob : IIoWorker
        
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="producer"></param>
        /// <param name="workDescription">A description of the work that will be done</param>
        /// <param name="jobDescription">A description of the job</param>
        protected IoConsumable(string jobDescription, string workDescription, IoProducer<TJob> producer) : base(workDescription, producer)
        {
            JobDescription = jobDescription;
        }        

        /// <summary>
        /// A description of the work
        /// </summary>
        public string JobDescription { get; protected set; }
        
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
