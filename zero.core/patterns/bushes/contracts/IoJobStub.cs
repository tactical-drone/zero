using System;
using zero.core.patterns.heap;

namespace zero.core.patterns.bushes.contracts
{
    /// <summary>
    /// Empty worker stub used to signal that a consumer should not forward jobs
    /// </summary>
    /// <seealso cref="IIoJob" />
    public class IoJobStub:IIoJob
    {
        public IIoHeapItem Constructor()
        {
            throw new NotImplementedException();
        }

        public string Description { get; } = "Job Stub";
        public long Id { get; }
        public IIoJob Previous { get; }
        public IIoSource Source { get; }
        public bool StillHasUnprocessedFragments { get; }
    }
}
