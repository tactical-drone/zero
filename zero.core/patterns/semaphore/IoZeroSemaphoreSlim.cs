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
    public class IoZeroSemaphoreSlim: IoNanoprobe, IIoZeroSemaphore
    {
        public IoZeroSemaphoreSlim(CancellationToken asyncTasks, 
            string description = "IoZeroSemaphoreSlim", int maxBlockers = 1, uint maxAsyncWork = 0, int initialCount = 0,
            bool enableAutoScale = false, bool enableFairQ = false, bool enableDeadlockDetection = false) : base($"{nameof(IoZeroSemaphoreSlim)}", maxBlockers)
        {
            _semaphore = new IoZeroSemaphore(description, maxBlockers, initialCount, maxAsyncWork, enableAutoScale: enableAutoScale, enableFairQ: enableFairQ, enableDeadlockDetection: enableDeadlockDetection);
            _semaphore.ZeroRef(ref _semaphore, asyncTasks);
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
        public ValueTask<int> ReleaseAsync(int releaseCount = 1, bool async = false)
        {
            return _semaphore.ReleaseAsync(releaseCount, async);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> WaitAsync()
        {
            return _semaphore.WaitAsync();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Zero()
        {
            _semaphore?.Zero();
        }

        public int ReadyCount => _semaphore.ReadyCount;
        
        public uint CurNrOfBlockers => _semaphore.CurNrOfBlockers;
        public uint MaxAsyncWorkers => _semaphore.MaxAsyncWorkers;
        public int Capacity => _semaphore.Capacity;

        int IIoZeroSemaphore.ZeroCount()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroIncCount()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroDecCount()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroAddCount(int value)
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroHead()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroTail()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroNextTail()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroNextHead()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroPrevTail()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroPrevHead()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroIncWait()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroDecWait()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroWaitCount()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroIncAsyncWait()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroDecAsyncWait()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroAsyncCount()
        {
            throw new NotImplementedException();
        }

        short IIoZeroSemaphore.ZeroToken()
        {
            throw new NotImplementedException();
        }

        short IIoZeroSemaphore.ZeroTokenBump()
        {
            throw new NotImplementedException();
        }
    }
}