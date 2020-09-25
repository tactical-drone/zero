using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore
{
    public interface IIoZeroSemaphore: IValueTaskSource<bool>
    {
        void ZeroRef(ref IIoZeroSemaphore @ref, CancellationToken asyncToken);
        void Set(int count = 1);
        ValueTask<bool> WaitAsync();
        void Zero();
    }
}