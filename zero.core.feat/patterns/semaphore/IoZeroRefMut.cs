using System;
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
        public IoZeroRefMut(CancellationToken asyncTasks, bool allowInline = true)
        {
            _semaphore = new AsyncAutoResetEvent(allowInline);
            RunContinuationsAsynchronously = allowInline;
            _semaphore.Set();
            _cancellationToken = asyncTasks;
        }
        
        private AsyncAutoResetEvent _semaphore;
        private readonly CancellationToken _cancellationToken;
        private readonly bool _zc = IoNanoprobe.ContinueOnCapturedContext;
        
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

        int IIoZeroSemaphore.Release(int releaseCount, bool bestEffort)
        {
            _semaphore.Set();
            return 1;
        }

        public async ValueTask<bool> WaitAsync()
        {
            await _semaphore.WaitAsync(_cancellationToken).ConfigureAwait(_zc);
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

        //int IIoZeroSemaphore.ZeroEnter()
        //{
        //    throw new NotImplementedException();
        //}

        //int IIoZeroSemaphore.ZeroCount()
        //{
        //    throw new NotImplementedException();
        //}

        //int IIoZeroSemaphore.ZeroIncCount()
        //{
        //    throw new NotImplementedException();
        //}

        //int IIoZeroSemaphore.ZeroDecCount()
        //{
        //    throw new NotImplementedException();
        //}

        //int IIoZeroSemaphore.ZeroAddCount(int value)
        //{
        //    throw new NotImplementedException();
        //}

        //long IIoZeroSemaphore.ZeroHead()
        //{
        //    throw new NotImplementedException();
        //}

        //long IIoZeroSemaphore.ZeroTail()
        //{
        //    throw new NotImplementedException();
        //}

        //long IIoZeroSemaphore.ZeroNextTail()
        //{
        //    throw new NotImplementedException();
        //}

        //long IIoZeroSemaphore.ZeroNextHead()
        //{
        //    throw new NotImplementedException();
        //}

        //long IIoZeroSemaphore.ZeroPrevTail()
        //{
        //    throw new NotImplementedException();
        //}

        //long IIoZeroSemaphore.ZeroPrevHead()
        //{
        //    throw new NotImplementedException();
        //}

        //int IIoZeroSemaphore.ZeroIncWait()
        //{
        //    throw new NotImplementedException();
        //}

        //int IIoZeroSemaphore.ZeroDecWait()
        //{
        //    throw new NotImplementedException();
        //}

        //int IIoZeroSemaphore.ZeroWaitCount()
        //{
        //    throw new NotImplementedException();
        //}

        //int IIoZeroSemaphore.ZeroIncAsyncCount()
        //{
        //    throw new NotImplementedException();
        //}

        int IIoZeroSemaphore.ZeroDecAsyncCount()
        {
            throw new NotImplementedException();
        }

        //int IIoZeroSemaphore.ZeroAsyncCount()
        //{
        //    throw new NotImplementedException();
        //}

        //short IIoZeroSemaphore.ZeroToken()
        //{
        //    throw new NotImplementedException();
        //}

        short IIoZeroSemaphore.ZeroTokenBump()
        {
            throw new NotImplementedException();
        }

        public bool Zeroed()
        {
            throw new NotImplementedException();
        }

        public bool IsCancellationRequested()
        {
            throw new NotImplementedException();
        }

        public void ZeroThrow()
        {
            throw new NotImplementedException();
        }
    }
}