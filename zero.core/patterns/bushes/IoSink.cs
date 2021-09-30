using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Sink where <see cref="IoJob{TJob}"/> is consumed.
    /// </summary>
    /// <typeparam name="TJob">The type of the job</typeparam>
    public abstract class IoSink<TJob> : IoJob<TJob>
        where TJob : IIoJob
        
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="sinkDesc"></param>
        /// <param name="jobDesc"></param>
        /// <param name="source"></param>
        protected IoSink(string sinkDesc, string jobDesc, IoSource<TJob> source) : base(jobDesc, source)
        {
            _sinkDesc = sinkDesc;
        }

        /// <summary>
        /// A description of the load
        /// </summary>
        private readonly string _sinkDesc;

        private string _description;

        /// <inheritdoc />
        /// <summary>
        /// The overall description of the work that needs to be done and the job that is doing it
        /// </summary>
        //public override string ProductionDescription => $"{Source.ChannelSource?.Description} {Source.Description} {LoadDescription} {base.Description}";
        public override string Description
        {
            get
            {
                if(_description == null)
                    return _description = $"{_sinkDesc}";
                return _description;
            }
        }

        /// <summary>
        /// Zero handle
        /// </summary>
        public IIoZero IoZero { get; set; }

        /// <summary>
        /// Consumes the job
        /// </summary>
        /// <returns>The state of the consumption</returns>
        public abstract ValueTask<IoJobMeta.JobState> ConsumeAsync();


        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            IoZero = null;
#endif
        }

        /// <summary>
        /// ZeroAsync managed
        /// </summary>
        public override ValueTask ZeroManagedAsync()
        {
            return base.ZeroManagedAsync();
        }

        /// <summary>
        /// Handle fragments
        /// </summary>
        public abstract void SyncPrevJob();

        /// <summary>
        /// Updates buffer meta data
        /// </summary>
        public abstract void JobSync();

        /// <summary>
        /// Used to debug
        /// </summary>
        /// <returns></returns>
        public virtual bool Verify([CallerMemberName] string desc = "", [CallerLineNumber] int sourceLineNumber = 0)
        {
            return true;
        }
    }
}
