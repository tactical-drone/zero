﻿using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Microsoft.VisualStudio.Threading;
using zero.core.patterns.semaphore.core;
//#if REF

namespace zero.core.feat.patterns.semaphore
{
    public class IoZeroRefMut : IIoZeroSemaphore
    {
        public IoZeroRefMut(CancellationToken asyncTasks, bool allowInline = false)
        {
            _semaphore = new AsyncAutoResetEvent(allowInline);
            RunContinuationsAsynchronously = allowInline;
            _semaphore.Set();
            _cancellationToken = asyncTasks;
        }
        
        private AsyncAutoResetEvent _semaphore;
        private readonly CancellationToken _cancellationToken;

        public bool GetResult(short token)
        {
            throw new NotImplementedException();
        }

        public ValueTaskSourceStatus GetStatus(short token)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            throw new NotImplementedException();
        }

        public IIoZeroSemaphore ZeroRef(ref IIoZeroSemaphore @ref)
        {
            throw new NotImplementedException();
        }

        public IIoZeroSemaphoreBase<bool> ZeroRef(ref IIoZeroSemaphoreBase<bool> @ref, Func<object, bool> primeResult,
            object context = null)
        {
            throw new NotImplementedException();
        }

        public int Release(bool value, int releaseCount, bool forceAsync = false, bool prime = true)
        {
            throw new NotImplementedException();
        }

        public bool Release(bool value, bool forceAsync = false, bool prime = true)
        {
            _semaphore.Set();
            return true;
        }

        public int Release(bool[] value, bool forceAsync = false, bool prime = true)
        {
            throw new NotImplementedException();
        }

        public async ValueTask<bool> WaitAsync()
        {
            await _semaphore.WaitAsync(_cancellationToken);
            return true;
        }

        public void ZeroSem()
        {
            _semaphore = null;
        }

        public int ReadyCount => 0;
        public int WaitCount { get; }
        public bool ZeroAsyncMode { get; }
        public long TotalOps => 0;
        public string Description => nameof(AsyncAutoResetEvent);


        public int Capacity => 1;
        public bool RunContinuationsAsynchronously { get; }

        public bool Zeroed()
        {
            throw new NotImplementedException();
        }

        public double Cps(bool reset = false) => 0.0;
        

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