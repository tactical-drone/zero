using System.Threading.Tasks;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushes.contracts
{
    public interface IIoZero: IIoNanite
    {
        IIoSource IoSource { get; }
        
        bool IsArbitrating { get; }

        bool SyncRecoveryModeEnabled { get; }

        ValueTask BlockOnReplicateAsync();
    }
}
