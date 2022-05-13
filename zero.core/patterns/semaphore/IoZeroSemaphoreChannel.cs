using System;
using System.Runtime.CompilerServices;
using System.Threading;
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
        public IoZeroSemaphoreChannel(CancellationTokenSource asyncTasks,
            string description = "IoZeroSemaphoreSlim", int maxBlockers = 1, int initialCount = 0,
            bool zeroAsyncMode = false,
            bool enableAutoScale = false, bool enableFairQ = false, bool enableDeadlockDetection = false) : base($"{nameof(IoZeroSemaphoreSlim)}: {description}", maxBlockers)
        {
            //_semaphore = new IoZeroSemaphore<T>(description, maxBlockers, initialCount, zeroAsyncMode, enableAutoScale: enableAutoScale, cancellationTokenSource: asyncTasks);
            _semaphore = new IoZeroSemCore<T>(description, maxBlockers, initialCount, zeroAsyncMode);
            _semaphore.ZeroRef(ref _semaphore, default);
        }

        private IIoZeroSemaphoreBase<T> _semaphore;

        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
            _semaphore = null;
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

        public int Release(T value, bool async = false)
        {
            return _semaphore.Release(value, async);
        }

        public int Release(T[] value, bool async = false)
        {
            return _semaphore.Release(value, async);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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