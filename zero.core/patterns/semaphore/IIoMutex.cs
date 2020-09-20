using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore
{
    public interface IIoMutex : IIoZeroable
    {
        void Configure(bool signalled = false, bool allowInliningContinuations = true);
        ValueTask SetAsync();
        ValueTask<bool> WaitAsync();
    }
}