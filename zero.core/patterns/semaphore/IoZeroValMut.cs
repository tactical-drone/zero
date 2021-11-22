using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.semaphore
{
    internal class IoZeroValMut : IIoZeroSemaphore
    {        
        IoManualResetValueTaskSource<bool> _pressure = new IoManualResetValueTaskSource<bool>();
        private int _fallThrough;

        public int ReadyCount => throw new NotImplementedException();

        public int CurNrOfBlockers => throw new NotImplementedException();

        public int MaxAsyncWorkers => throw new NotImplementedException();

        public int Capacity => throw new NotImplementedException();

        public bool Zc = IoNanoprobe.ContinueOnCapturedContext;

        int IIoZeroSemaphore.ReadyCount => throw new NotImplementedException();

        int IIoZeroSemaphore.CurNrOfBlockers => throw new NotImplementedException();

        int IIoZeroSemaphore.MaxAsyncWorkers => throw new NotImplementedException();

        int IIoZeroSemaphore.Capacity => throw new NotImplementedException();

        public bool GetResult(short token)
        {
            throw new NotImplementedException();
        }

        public ValueTaskSourceStatus GetStatus(short token)
        {
            throw new NotImplementedException();
        }

        public bool IsCancellationRequested()
        {
            throw new NotImplementedException();
        }

        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            throw new NotImplementedException();
        }
        
        public ValueTask<int> ReleaseAsync(int releaseCount = 1, bool async = false, bool bestEffort = false)
        {
            throw new NotImplementedException();
        }

        public void Release()
        {
            _pressure.SetResult(true);            
            _pressure.Reset();
        }

        public ValueTask<bool> WaitAsync()
        {
            if (Interlocked.CompareExchange(ref _fallThrough, 1, 0) == 0)
                return new ValueTask<bool>(true);
                
            return _pressure.WaitAsync();
        }

        public void Zero()
        {
            throw new NotImplementedException();
        }

        public bool Zeroed()
        {
            throw new NotImplementedException();
        }

        public void ZeroRef(ref IIoZeroSemaphore @ref, CancellationTokenSource asyncToken)
        {
            throw new NotImplementedException();
        }

        bool IValueTaskSource<bool>.GetResult(short token)
        {
            throw new NotImplementedException();
        }

        ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token)
        {
            throw new NotImplementedException();
        }

        bool IIoZeroSemaphore.IsCancellationRequested()
        {
            throw new NotImplementedException();
        }

        void IValueTaskSource<bool>.OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            throw new NotImplementedException();
        }

#pragma warning disable CS1066 // The default value specified will have no effect because it applies to a member that is used in contexts that do not allow optional arguments
        int IIoZeroSemaphore.Release(int releaseCount, bool bestEffort = false)
#pragma warning restore CS1066 // The default value specified will have no effect because it applies to a member that is used in contexts that do not allow optional arguments
        {
            throw new NotImplementedException();
        }

        ValueTask<bool> IIoZeroSemaphore.WaitAsync()
        {
            throw new NotImplementedException();
        }

        void IIoZeroSemaphore.ZeroSem()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroAddCount(int value)
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroAsyncCount()
        {
            throw new NotImplementedException();
        }

        bool IIoZeroSemaphore.ZeroEnter()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroCount()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroDecAsyncCount()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroDecCount()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroDecWait()
        {
            throw new NotImplementedException();
        }

        bool IIoZeroSemaphore.Zeroed()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroHead()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroIncAsyncCount()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroIncCount()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroIncWait()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroNextHead()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroNextTail()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroPrevHead()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroPrevTail()
        {
            throw new NotImplementedException();
        }

        void IIoZeroSemaphore.ZeroRef(ref IIoZeroSemaphore @ref, CancellationTokenSource asyncToken)
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroTail()
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

        int IIoZeroSemaphore.ZeroWaitCount()
        {
            throw new NotImplementedException();
        }
    }
}
