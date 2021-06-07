using System;
using System.Threading.Tasks;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore;

namespace zero.core.patterns.bushes.contracts
{
    public interface IIoJob : IIoHeapItem, IIoNanoprobe
    {
        long Id { get; }
        IoJobMeta.JobState FinalState { get; set; }
        IoJobMeta.JobState State { get; set; }
        IIoJob PreviousJob { get; }
        IIoSource Source { get; }
        
        bool StillHasUnprocessedFragments { get; }

        ValueTask<IoJobMeta.JobState> ProduceAsync(Func<IIoJob, IIoZero, ValueTask<bool>> barrier, IIoZero zeroClosure);
    }
}
