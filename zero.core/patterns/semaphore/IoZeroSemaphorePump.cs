﻿using System;
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
    public class IoZeroSemaphorePump<T>: IoNanoprobe, IIoZeroSemaphoreBase<T>
    {
        public IoZeroSemaphorePump(CancellationTokenSource asyncTasks,
            string description = "IoZeroSemaphoreSlim", int maxBlockers = 1, int initialCount = 0,
            bool zeroAsyncMode = false,
            bool enableAutoScale = false, bool enableFairQ = false, bool enableDeadlockDetection = false) : base($"{nameof(IoZeroSemaphoreSlim)}: {description}", maxBlockers)
        {
            _semaphore = new IoZeroSemaphore<T>(description, maxBlockers, initialCount, zeroAsyncMode, enableAutoScale: enableAutoScale, cancellationTokenSource: asyncTasks);
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

        public IIoZeroSemaphoreBase<T> ZeroRef(ref IIoZeroSemaphoreBase<T> @ref, T init) => _semaphore.ZeroRef(ref @ref, init);

        public int Release(T value, int releaseCount, bool bestCase = false)
        {
            return _semaphore.Release(value, releaseCount, bestCase);
        }

        public int Release(T value, bool bestCase = false)
        {
            return _semaphore.Release(value, bestCase);
        }

        public int Release(T[] value, bool bestCase = false)
        {
            return _semaphore.Release(value, bestCase);
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
        
        public int CurNrOfBlockers => _semaphore.CurNrOfBlockers;
        public bool ZeroAsyncMode => _semaphore.ZeroAsyncMode;
        public int Capacity => _semaphore.Capacity;
        int IIoZeroSemaphoreBase<T>.ZeroDecAsyncCount()
        {
            throw new NotImplementedException();
        }

        public long Tail => ((IoZeroSemaphore<T>)_semaphore).Tail;
        public long Head => ((IoZeroSemaphore<T>)_semaphore).Head;
    }
}