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

        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
        }

        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();
            _semaphore.ZeroSem();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T GetResult(short token)
        {
            return _semaphore.GetResult(token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTaskSourceStatus GetStatus(short token)
        {
            return _semaphore.GetStatus(token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            _semaphore.OnCompleted(continuation, state, token, flags);
        }

        public IIoZeroSemaphoreBase<T> ZeroRef(ref IIoZeroSemaphoreBase<T> @ref, Func<object, T> primeResult,
            object context = null) => _semaphore.ZeroRef(ref @ref, primeResult);

        public int Release(T value, int releaseCount, bool forceAsync = false)
        {
            return _semaphore.Release(value, releaseCount);
        }

        public int Release(T value, bool forceAsync = false)
        {
            return _semaphore.Release(value, forceAsync);
        }

        public int Release(T[] value, bool forceAsync = false)
        {
            return _semaphore.Release(value, forceAsync);
        }

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> WaitAsync()
        {
            return _semaphore.WaitAsync();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ZeroSem()
        {
            _semaphore.ZeroSem();
        }

        public int ReadyCount => _semaphore.ReadyCount;
        
        public int WaitCount => _semaphore.WaitCount;
        public bool ZeroAsyncMode => _semaphore.ZeroAsyncMode;
        public int Capacity => _semaphore.Capacity;
        int IIoZeroSemaphoreBase<T>.ZeroDecAsyncCount()
        {
            throw new NotImplementedException();
        }

        public long Tail => ((IoZeroSemaphore<T>)_semaphore).Tail;
        public long Head => ((IoZeroSemaphore<T>)_semaphore).Head;

        public override bool Zeroed()
        {
            return _semaphore.Zeroed() || base.Zeroed();
        }
    }
}