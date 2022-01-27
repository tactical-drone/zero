using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore.core
{
    public interface IIoZeroSemaphore: IValueTaskSource<bool>
    {
        void ZeroRef(ref IIoZeroSemaphore @ref);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int Release(int releaseCount = 1, bool bestEffort = false);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask<bool> WaitAsync();
        void ZeroSem();
        int ReadyCount { get; }
        int CurNrOfBlockers { get; }
        public int MaxAsyncWorkers { get; }
        public int Capacity { get; }
        bool RunContinuationsAsynchronously { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected internal int ZeroDecAsyncCount();
        
        bool Zeroed();
    }
}