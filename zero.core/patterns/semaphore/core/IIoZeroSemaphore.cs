using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore.core
{
    public interface IIoZeroSemaphore: IValueTaskSource<bool>
    {
        void ZeroRef(ref IIoZeroSemaphore @ref, CancellationTokenSource asyncToken);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int Release(int releaseCount = 1, bool bestEffort = false);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask<bool> WaitAsync();
        void ZeroSem();
        int ReadyCount { get; }
        int CurNrOfBlockers { get; }
        public int MaxAsyncWorkers { get; }
        public int Capacity { get; }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int ZeroEnter();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int ZeroCount();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int ZeroIncCount();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int ZeroDecCount();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int ZeroAddCount(int value);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long ZeroHead();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long ZeroTail();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long ZeroNextTail();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long ZeroNextHead();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long ZeroPrevTail();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long ZeroPrevHead();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int ZeroIncWait();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int ZeroDecWait();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int ZeroWaitCount();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int ZeroIncAsyncCount();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int ZeroDecAsyncCount();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int ZeroAsyncCount();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal short ZeroToken();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal short ZeroTokenBump();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool Zeroed();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool IsCancellationRequested();
    }
}