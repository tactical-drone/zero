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
    /// Wraps a <see cref="IoZeroSemaphore"/> for use
    /// </summary>
    public class IoZeroSemaphoreSlim: IoNanoprobe, IIoZeroSemaphore
    {
        public IoZeroSemaphoreSlim(CancellationTokenSource asyncTasks,
            string description = "IoZeroSemaphoreSlim", int maxBlockers = 1, int initialCount = 0,
            bool zeroAsyncMode = false,
            bool enableAutoScale = false, bool enableFairQ = false, bool enableDeadlockDetection = false) : base($"{nameof(IoZeroSemaphoreSlim)}: {description}", maxBlockers)
        {
            _semaphore = new IoZeroSemaphore<bool>(description, maxBlockers, initialCount, zeroAsyncMode, enableAutoScale: enableAutoScale, enableFairQ: enableFairQ, enableDeadlockDetection: enableDeadlockDetection, cancellationTokenSource: asyncTasks);
            _semaphore.ZeroRef(ref _semaphore, true);
        }

        private IIoZeroSemaphoreBase<bool> _semaphore;

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
        public bool GetResult(short token)
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

        public IIoZeroSemaphoreBase<bool> ZeroRef(ref IIoZeroSemaphoreBase<bool> @ref, bool init) => _semaphore.ZeroRef(ref @ref, init);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(int releaseCount = 1, bool bestCase = false)
        {
            return Release(true, releaseCount, bestCase);
        }

        public int Release(bool value, int releaseCount, bool bestCase = false)
        {
            return _semaphore.Release(value, releaseCount, bestCase);
        }

        public int Release(bool value, bool bestCase = false)
        {
            return _semaphore.Release(value, bestCase);
        }

        public int Release(bool[] value, bool bestCase = false)
        {
            return _semaphore.Release(value, bestCase);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> WaitAsync()
        {
            return _semaphore.WaitAsync();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ZeroSem()
        {
            _semaphore.ZeroSem();
        }

        public int ReadyCount => _semaphore.ReadyCount;
        
        public int CurNrOfBlockers => _semaphore.CurNrOfBlockers;
        public bool ZeroAsyncMode => _semaphore.ZeroAsyncMode;
        public int Capacity => _semaphore.Capacity;
        int IIoZeroSemaphoreBase<bool>.ZeroDecAsyncCount()
        {
            throw new NotImplementedException();
        }

        public long Tail => ((IoZeroSemaphore<bool>)_semaphore).Tail;
        public long Head => ((IoZeroSemaphore<bool>)_semaphore).Head;
    }
}