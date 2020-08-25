using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushes.contracts
{
    public interface IIoJob : IIoHeapItem, IIoZeroable
    {
        long Id { get; }
        IIoJob Previous { get; }
        IIoSource Source { get; }
        
        bool StillHasUnprocessedFragments { get; }
    }
}
