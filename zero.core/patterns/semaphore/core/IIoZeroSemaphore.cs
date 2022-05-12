using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore.core
{
    public interface IIoZeroSemaphore : IIoZeroSemaphoreBase<bool>{}
    public interface IIoZeroSemaphoreBase<T>: IValueTaskSource<T>
    {
        IIoZeroSemaphoreBase<T> ZeroRef(ref IIoZeroSemaphoreBase<T> @ref, T init);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int Release(T value, int releaseCount, bool forceAsync = false);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int Release(T value, bool async = false);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int Release(T[] value, bool async = false);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask<T> WaitAsync();
        void ZeroSem();
        int ReadyCount { get; }
        int CurNrOfBlockers { get; }
        public bool ZeroAsyncMode { get; }
        public int Capacity { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected internal int ZeroDecAsyncCount();
        
        bool Zeroed();
    }
}