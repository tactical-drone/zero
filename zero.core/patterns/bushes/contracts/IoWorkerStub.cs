using System;
using zero.core.patterns.heap;

namespace zero.core.patterns.bushes.contracts
{
    /// <summary>
    /// Empty worker stub used to signal that a consumer should not forward jobs
    /// </summary>
    /// <seealso cref="zero.core.patterns.bushes.contracts.IIoWorker" />
    public class IoWorkerStub:IIoWorker
    {
        public IIoHeapItem Constructor()
        {
            throw new NotImplementedException();
        }
    }
}
