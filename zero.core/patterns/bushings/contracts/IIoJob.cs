﻿using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;

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
        
        /// <summary>
        /// Whether this job needs to be synced with a future job
        /// because the future job contains the tail end of this job
        /// </summary>
        bool Syncing { get; }
    }
}
