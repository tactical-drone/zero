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
        bool RunContinuationsAsynchronously { get; }


        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal int ZeroEnter();
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal int ZeroCount();
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal int ZeroIncCount();
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal int ZeroDecCount();
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal int ZeroAddCount(int value);
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal long ZeroHead();
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal long ZeroTail();
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal long ZeroNextTail();
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal long ZeroNextHead();
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal long ZeroPrevTail();
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal long ZeroPrevHead();
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal int ZeroIncWait();
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal int ZeroDecWait();
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal int ZeroWaitCount();
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal int ZeroIncAsyncCount();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected internal int ZeroDecAsyncCount();
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal int ZeroAsyncCount();
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //protected internal short ZeroToken();
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected internal short ZeroTokenBump();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool Zeroed();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool IsCancellationRequested();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ZeroThrow();
    }
}