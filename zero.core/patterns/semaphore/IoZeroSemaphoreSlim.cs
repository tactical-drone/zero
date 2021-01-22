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
        public IoZeroSemaphoreSlim(CancellationTokenSource asyncTasks, string description = "IoZeroSemaphoreSlim", int maxCount = 1, int initialCount = 0,
            bool enableAutoScale = false, bool enableFairQ = false, bool enableDeadlockDetection = false) : base($"{nameof(IoZeroSemaphoreSlim)}")
        {
            _semaphore = new IoZeroSemaphore(description, maxCount, initialCount, enableAutoScale, enableFairQ, enableDeadlockDetection);
            _semaphore.ZeroRef(ref _semaphore, asyncTasks.Token);
        }

        private IIoZeroSemaphore _semaphore;

        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
            _semaphore = null;
        }

        public override ValueTask ZeroManagedAsync()
        {
            _semaphore?.Zero();
            return base.ZeroManagedAsync();
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
        
        public void ZeroRef(ref IIoZeroSemaphore @ref, CancellationToken asyncToken)
        {
            _semaphore.ZeroRef(ref @ref, asyncToken);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(int releaseCount = 1)
        {
            return _semaphore.Release(releaseCount);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> WaitAsync()
        {
            return _semaphore.WaitAsync();
        }
        
        public void Zero()
        {
            _semaphore.Zero();
        }

        public int CurrentCount => _semaphore.CurrentCount;
    }
}