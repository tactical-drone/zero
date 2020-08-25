using System;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushes.contracts
{
    /// <summary>
    /// Empty worker stub used to signal that a consumer should not forward jobs
    /// </summary>
    /// <seealso cref="IIoJob" />
    public class IoJobStub: IoZeroable, IIoJob
    {
        public IIoHeapItem Constructor()
        {
            throw new NotImplementedException();
        }

        public override string Description { get; } = "Job Stub";
        public long Id { get; } = -1;
        public IIoJob Previous { get; } = null;
        public IIoSource Source { get; } = null;
        public bool StillHasUnprocessedFragments { get; } = false;
    }
}
