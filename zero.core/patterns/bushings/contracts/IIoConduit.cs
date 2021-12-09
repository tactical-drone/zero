using zero.core.patterns.misc;

namespace zero.core.patterns.bushings.contracts
{
    public interface IIoConduit : IIoNanite {
        IIoSource UpstreamSource { get; }
    }
}
