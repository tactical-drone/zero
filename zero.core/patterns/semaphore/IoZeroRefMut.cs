using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Microsoft.VisualStudio.Threading;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.semaphore
{
    public class IoZeroRefMut : IIoZeroSemaphore
    {
        
        public IoZeroRefMut(CancellationToken asyncTasks, bool allowInline = true)
        {
            _semaphore = new AsyncAutoResetEvent(allowInline);
            _semaphore.Set();
            _cancellationToken = asyncTasks;
        }
        
        private readonly AsyncAutoResetEvent _semaphore;
        private CancellationToken _cancellationToken;
        
        public bool GetResult(short token)
        {
            throw new NotImplementedException();
        }

        public ValueTaskSourceStatus GetStatus(short token)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            throw new NotImplementedException();
        }

        public void ZeroRef(ref IIoZeroSemaphore @ref, CancellationToken asyncToken)
        {
            throw new NotImplementedException();
        }

        public ValueTask<int> ReleaseAsync(int releaseCount = 1, bool async = false)
        {
            _semaphore.Set();
            return ValueTask.FromResult(1);
        }

        public async ValueTask<bool> WaitAsync()
        {
            await _semaphore.WaitAsync(_cancellationToken).ConfigureAwait(false);
            return true;
        }

        public void Zero()
        {
            throw new NotImplementedException();
        }

        public int ReadyCount { get; }
        public uint CurNrOfBlockers { get; }
        public uint MaxAsyncWorkers { get; }
        public int Capacity { get; }

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

        public void SignalWorker()
        {
            throw new NotImplementedException();
        }

        public bool Zeroed()
        {
            throw new NotImplementedException();
        }
    }
}