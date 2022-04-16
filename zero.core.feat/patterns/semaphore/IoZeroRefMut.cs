﻿using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Microsoft.VisualStudio.Threading;
using zero.core.patterns.misc;
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

        public void ZeroRef(ref IIoZeroSemaphore @ref)
        {
            throw new NotImplementedException();
        }

        public int Release(int releaseCount, bool bestCase = false)
        {
            _semaphore.Set();
            return 1;
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

        int IIoZeroSemaphore.CurNrOfBlockers => 0;

        int IIoZeroSemaphore.MaxAsyncWorkers => 0;

        public int Capacity => 1;
        public bool RunContinuationsAsynchronously { get; }

        int IIoZeroSemaphore.ZeroDecAsyncCount()
        {
            throw new NotImplementedException();
        }

        public bool Zeroed()
        {
            throw new NotImplementedException();
        }
    }
}