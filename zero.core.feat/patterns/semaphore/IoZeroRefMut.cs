using System;
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

        public int Release(bool value, int releaseCount, bool forceAsync = false)
        {
            throw new NotImplementedException();
        }

        public int Release(bool value, bool forceAsync = false)
        {
            _semaphore.Set();
            return 1;
        }

        public int Release(bool[] value, bool forceAsync = false)
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
        public string Description => nameof(AsyncAutoResetEvent);


        public int Capacity => 1;
        public bool RunContinuationsAsynchronously { get; }

        int IIoZeroSemaphoreBase<bool>.ZeroDecAsyncCount()
        {
            throw new NotImplementedException();
        }

        public bool Zeroed()
        {
            throw new NotImplementedException();
        }
    }
}