﻿using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.semaphore
{
    /// <summary>
    /// Wraps a <see cref="IoZeroSemaphore"/> for use
    /// </summary>
    public class IoZeroSemaphoreSlim:IIoZeroSemaphore
    {
        public IoZeroSemaphoreSlim(CancellationTokenSource asyncTasks, string description = "", int capacity = 1, int initialCount = 0, int expectedNrOfWaiters = 1,
            bool enableAutoScale = false, int zeroVersion = 0)
        {
            _semaphore = new IoZeroSemaphore(description, capacity, initialCount, expectedNrOfWaiters, enableAutoScale, zeroVersion);
            _semaphore.ZeroRef(ref _semaphore, asyncTasks.Token);
        }

        private readonly IIoZeroSemaphore _semaphore;
        
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
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            _semaphore.OnCompleted(continuation, state, token, flags);
        }
        
        public void ZeroRef(ref IIoZeroSemaphore @ref, CancellationToken asyncToken)
        {
            _semaphore.ZeroRef(ref @ref, asyncToken);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(int count = 1)
        {
            _semaphore.Set(count);
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
    }
}