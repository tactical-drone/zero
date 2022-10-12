using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore.core
{
    public interface IIoZeroSemaphore : IIoZeroSemaphoreBase<bool>{}
    public interface IIoZeroSemaphoreBase<T>: IValueTaskSource<T>
    {
        IIoZeroSemaphoreBase<T> ZeroRef(ref IIoZeroSemaphoreBase<T> @ref, Func<object, T> primeResult = default, object context = null);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int Release(T value, int releaseCount, bool forceAsync = false);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int Release(T value, bool forceAsync = false);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int Release(T[] value, bool forceAsync = false);

        ValueTask<T> WaitAsync();
        void ZeroSem();
        int ReadyCount { get; }
        int WaitCount { get; }
        public bool ZeroAsyncMode { get; }
        public long TotalOps { get; }
        public string Description { get; }
        public int Capacity { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected internal int ZeroDecAsyncCount();
        
        bool Zeroed();
    }
}