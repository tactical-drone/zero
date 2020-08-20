using System.Threading.Tasks;
using zero.core.patterns.bushes.contracts;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// <see cref="IoJob{TJob}"/> is consumed.
    /// </summary>
    /// <typeparam name="TJob">The type of the job</typeparam>
    public abstract class IoLoad<TJob> : IoJob<TJob>
        where TJob : IIoJob
        
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="loadDescription"></param>
        /// <param name="jobDescription"></param>
        /// <param name="source"></param>
        protected IoLoad(string loadDescription, string jobDescription, IoSource<TJob> source) : base(jobDescription, source)
        {
            _loadDescription = loadDescription;
        }

        /// <summary>
        /// A description of the load
        /// </summary>
        private readonly string _loadDescription;

        /// <inheritdoc />
        /// <summary>
        /// The overall description of the work that needs to be done and the job that is doing it
        /// </summary>
        //public override string ProductionDescription => $"{Source.ChannelSource?.Description} {Source.Description} {LoadDescription} {base.Description}";
        public override string Description => $"{base.Description} | {_loadDescription}";

        /// <summary>
        /// Zero
        /// </summary>
        public IIoZero Zero { get; set; }

        /// <summary>
        /// Consumes the job
        /// </summary>
        /// <returns>The state of the consumption</returns>
        public abstract Task<State> ConsumeAsync();
    }
}
