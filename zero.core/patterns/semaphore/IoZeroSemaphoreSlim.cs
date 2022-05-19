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
            //IIoZeroSemaphoreBase<bool> newSem = new IoZeroSemaphore<bool>(description, maxBlockers, initialCount, zeroAsyncMode, enableAutoScale: enableAutoScale, cancellationTokenSource: asyncTasks);
            IIoZeroSemaphoreBase<bool> newSem = new IoZeroCore<bool>(description, maxBlockers, initialCount, zeroAsyncMode);
            _semaphore = newSem.ZeroRef(ref newSem, _ => true);
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

        public IIoZeroSemaphoreBase<bool> ZeroRef(ref IIoZeroSemaphoreBase<bool> @ref, Func<object, bool> primeResult,
            object context = null) => _semaphore.ZeroRef(ref @ref, primeResult);

        
        public int Release(bool value, int releaseCount, bool forceAsync = false)
        {
            return _semaphore.Release(value, releaseCount, forceAsync);
        }

        public int Release(bool value, bool forceAsync = false)
        {
            return _semaphore.Release(value, forceAsync);
        }

        public int Release(bool[] value, bool forceAsync = false)
        {
            return _semaphore.Release(value, forceAsync);
        }

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

        public int WaitCount => _semaphore.WaitCount;
        public bool ZeroAsyncMode => _semaphore.ZeroAsyncMode;
        public int Capacity => _semaphore.Capacity;
        int IIoZeroSemaphoreBase<bool>.ZeroDecAsyncCount()
        {
            throw new NotImplementedException();
        }

        public long Tail => ((IoZeroCore<bool>)_semaphore).Tail;
        public long Head => ((IoZeroCore<bool>)_semaphore).Head;
        public long EgressCount => ((IoZeroSemaphore<bool>)_semaphore).EgressCount;

        public override bool Zeroed()
        {
            return _semaphore.Zeroed() || base.Zeroed();
        }

        public long DecWaitCount()
        {
            throw new NotImplementedException();
        }

        public long IncWaitCount()
        {
            throw new NotImplementedException();
        }

        public long IncReadyCount()
        {
            throw new NotImplementedException();
        }

        public long DecReadyCount()
        {
            throw new NotImplementedException();
        }
    }
}