using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore.core
{
    public interface IIoZeroSemaphore: IValueTaskSource<bool>
    {
        void ZeroRef(ref IIoZeroSemaphore @ref, CancellationToken asyncToken);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask<int> ReleaseAsync(int releaseCount = 1, bool async = false);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask<bool> WaitAsync();
        void Zero();
        int ReadyCount { get; }
        uint NrOfBlockers { get; }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int ZeroCount();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int ZeroIncCount();
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int ZeroDecCount();

        internal int ZeroAddCount(int value);


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal uint ZeroHead();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal uint ZeroTail();
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal uint ZeroNextTail();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal uint ZeroNextHead();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal uint ZeroPrevTail();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal uint ZeroPrevHead();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal short ZeroToken();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal short ZeroTokenBump();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SignalWorker();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool Zeroed();
    }
}