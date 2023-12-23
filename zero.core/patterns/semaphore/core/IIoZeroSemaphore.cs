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
        int Release(T value, int releaseCount, bool forceAsync = false, bool prime = true);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool Release(T value, bool forceAsync = false, bool prime = true);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int Release(T[] value, bool forceAsync = false, bool prime = true);

        ValueTask<T> WaitAsync();
        void ZeroSem();
        int ReadyCount { get; }
        int WaitCount { get; }
        public bool ZeroAsyncMode { get; }
        public long TotalOps { get; }
        public string Description { get; }
        public int Capacity { get; }

        bool Zeroed();

        /// <summary>
        /// The number of cores processed per second
        /// </summary>
        /// <param name="reset">Whether to reset hysteresis</param>
        /// <returns>The current completions per second</returns>
        double Cps(bool reset = false);
    }
}