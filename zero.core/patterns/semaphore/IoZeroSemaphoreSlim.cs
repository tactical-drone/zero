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
    /// Wraps a <see cref="IoZeroSemaphore"/> for use
    /// </summary>
    public class IoZeroSemaphoreSlim: IoNanoprobe, IIoZeroSemaphoreBase<int>
    {
        public IoZeroSemaphoreSlim(CancellationTokenSource asyncTasks,
            string description = "IoZeroSemaphoreSlim", int maxBlockers = 1, int initialCount = 0,
            bool zeroAsyncMode = false) : base($"{nameof(IoZeroSemaphoreSlim)}: {description}", maxBlockers)
        {
            //IIoZeroSemaphoreBase<int> newSem = new IoZeroSemaphore<int>(description, maxBlockers, initialCount, zeroAsyncMode, enableAutoScale: enableAutoScale, cancellationTokenSource: asyncTasks);
            IIoZeroSemaphoreBase<int> newSem = new IoZeroCore<int>(description, maxBlockers, asyncTasks,initialCount, zeroAsyncMode);
            _semaphore = newSem.ZeroRef(ref newSem, _ => Environment.TickCount);
        }

        private readonly IIoZeroSemaphoreBase<int> _semaphore;

        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();
            _semaphore.ZeroSem();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetResult(short token) => _semaphore.GetResult(token);
        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTaskSourceStatus GetStatus(short token) => _semaphore.GetStatus(token);
        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => _semaphore.OnCompleted(continuation, state, token, flags);
        

        public IIoZeroSemaphoreBase<int> ZeroRef(ref IIoZeroSemaphoreBase<int> @ref, Func<object, int> primeResult,
            object context = null) => _semaphore.ZeroRef(ref @ref, primeResult);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(int value, int releaseCount, bool forceAsync = false, bool prime = true) => _semaphore.Release(value, releaseCount, forceAsync, prime);
        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Release(int value, bool forceAsync = false, bool prime = true) => _semaphore.Release(value, forceAsync, prime);
        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(int[] value, bool forceAsync = false, bool prime = true) => _semaphore.Release(value, forceAsync, prime);
        

        public ValueTask<int> WaitAsync() => _semaphore.WaitAsync();
        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ZeroSem() => _semaphore.ZeroSem();
        

        public int ReadyCount => _semaphore.ReadyCount;

        public int WaitCount => _semaphore.WaitCount;
        public bool ZeroAsyncMode => _semaphore.ZeroAsyncMode;
        public long TotalOps => _semaphore.TotalOps;
        public int Capacity => _semaphore.Capacity;
        public long EgressCount => ((IoZeroSemaphore<int>)_semaphore).EgressCount;

        public override bool Zeroed()
        {
            return _semaphore.Zeroed() || base.Zeroed();
        }

        public double Cps(bool reset = false) => _semaphore.Cps(reset);

    }
}