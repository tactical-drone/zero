using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.semaphore
{
    /// <summary>
    /// Wraps a <see cref="IIoZeroSemaphoreBase{T}"/> for use
    /// </summary>
    public class IoZeroSemaphoreChannel<T>: IoNanoprobe, IIoZeroSemaphoreBase<T>
    {
        public IoZeroSemaphoreChannel(string description = "IoZeroSemaphoreSlim", int maxBlockers = 1, int initialCount = 0,
            bool zeroAsyncMode = false) : base($"{nameof(IoZeroSemaphoreSlim)}: {description}", maxBlockers)
        {
            //IIoZeroSemaphoreBase<T> c = new IoZeroSemaphore<T>(description, maxBlockers, initialCount, zeroAsyncMode);
            IIoZeroSemaphoreBase<T> c = new IoZeroCore<T>(description, maxBlockers, AsyncTasks, initialCount, zeroAsyncMode);
            _semaphore = c.ZeroRef(ref c, default);
        }

        private readonly IIoZeroSemaphoreBase<T> _semaphore;
        public long TotalOps => _semaphore.TotalOps;
        public override string Description => _semaphore.Description;

        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();
            _semaphore.ZeroSem();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T GetResult(short token) => _semaphore.GetResult(token);
        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTaskSourceStatus GetStatus(short token) => _semaphore.GetStatus(token);
        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => _semaphore.OnCompleted(continuation, state, token, flags);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IIoZeroSemaphoreBase<T> ZeroRef(ref IIoZeroSemaphoreBase<T> @ref, Func<object, T> primeResult, object context = null) => _semaphore.ZeroRef(ref @ref, primeResult);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T value, int releaseCount, bool forceAsync = false, bool prime = true) => _semaphore.Release(value, releaseCount, forceAsync);
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T value, bool forceAsync = false, bool prime = true) =>_semaphore.Release(value, forceAsync);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T[] value, bool forceAsync = false, bool prime = true) => _semaphore.Release(value, forceAsync);
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> WaitAsync() => _semaphore.WaitAsync();
        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ZeroSem()
        {
            _semaphore.ZeroSem();
            Dispose();
        }

        public int ReadyCount => _semaphore.ReadyCount;
        public int WaitCount => _semaphore.WaitCount;
        public bool ZeroAsyncMode => _semaphore.ZeroAsyncMode;
        public int Capacity => _semaphore.Capacity;
        
        public long Tail => ((IoZeroSemaphore<T>)_semaphore).Tail;
        public long Head => ((IoZeroSemaphore<T>)_semaphore).Head;

        public override bool Zeroed()
        {
            return _semaphore.Zeroed() || base.Zeroed();
        }

        public double Cps(bool reset = false) => _semaphore.Cps(reset);
    }
}