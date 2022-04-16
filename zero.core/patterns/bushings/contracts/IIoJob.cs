using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushings.contracts
{
    public interface IIoJob : IIoHeapItem, IIoNanite
    {
        /// <summary>
        /// The Id of this job
        /// </summary>
        long Id { get; }
        
        /// <summary>
        /// Final state of this production
        /// </summary>
        IoJobMeta.JobState FinalState { get; set; }
        
        /// <summary>
        /// Current state of this production
        /// </summary>
        IoJobMeta.JobState State { get; set; }
        
        /// <summary>
        /// A previous incomplete job that needs to be processed with this job
        /// </summary>
        IIoJob PreviousJob { get; }

        /// <summary>
        /// The source of these jobs
        /// </summary>
        IIoSource Source { get; }
    }
}
