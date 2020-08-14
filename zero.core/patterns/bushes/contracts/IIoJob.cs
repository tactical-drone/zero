using zero.core.patterns.heap;

namespace zero.core.patterns.bushes.contracts
{
    public interface IIoJob : IIoHeapItem
    {
        string Description { get; }

        long Id { get; }
        IIoJob Previous { get; }
        IIoSource Source { get; }
        
        bool StillHasUnprocessedFragments { get; }
    }
}
