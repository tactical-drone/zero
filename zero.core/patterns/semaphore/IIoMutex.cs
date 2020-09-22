using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore
{
    public interface IIoMutex
    {
        void Configure( CancellationTokenSource asyncTasks, bool signalled = false,
            bool allowInliningContinuations = true);
        void Set();
        ValueTask<bool> WaitAsync();
    }
}