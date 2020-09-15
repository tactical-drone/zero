using System;
using System.Threading.Tasks;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushes.contracts
{
    public interface IIoJob : IIoHeapItem, IIoZeroable
    {
        long Id { get; }
        IoJobMeta.JobState State { get; set; }
        IIoJob PreviousJob { get; }
        IIoSource Source { get; }
        
        bool StillHasUnprocessedFragments { get; }

        Task<IoJobMeta.JobState> ProduceAsync(Func<IIoJob, IIoZero, ValueTask<bool>> barrier, IIoZero zeroClosure);
    }
}
