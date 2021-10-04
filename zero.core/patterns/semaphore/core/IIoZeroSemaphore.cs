using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore.core
{
    public interface IIoZeroSemaphore: IValueTaskSource<bool>
    {
        void ZeroRef(ref IIoZeroSemaphore @ref, CancellationToken asyncToken);
        int Release(int releaseCount = 1, bool async = false);
        ValueTask<bool> WaitAsync();
        void Zero();
        int ReadyCount { get; }
        uint NrOfBlockers { get; }
        void SignalWorker();
        bool Zeroed();
    }
}