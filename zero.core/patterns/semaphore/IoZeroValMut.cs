using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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

        public uint CurNrOfBlockers => throw new NotImplementedException();

        public uint MaxAsyncWorkers => throw new NotImplementedException();

        public int Capacity => throw new NotImplementedException();

        public bool Zc = true;

        int IIoZeroSemaphore.ReadyCount => throw new NotImplementedException();

        uint IIoZeroSemaphore.CurNrOfBlockers => throw new NotImplementedException();

        uint IIoZeroSemaphore.MaxAsyncWorkers => throw new NotImplementedException();

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
        
        public ValueTask<int> ReleaseAsync(int releaseCount = 1, bool async = false)
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
                return ValueTask.FromResult(true);
                
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

        ValueTask<int> IIoZeroSemaphore.ReleaseAsync(int releaseCount, bool async)
        {
            throw new NotImplementedException();
        }

        ValueTask<bool> IIoZeroSemaphore.WaitAsync()
        {
            throw new NotImplementedException();
        }

        void IIoZeroSemaphore.Zero()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroAddCount(int value)
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroAsyncCount()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroCount()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroDecAsyncCount()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroDecCount()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroDecWait()
        {
            throw new NotImplementedException();
        }

        bool IIoZeroSemaphore.Zeroed()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroHead()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroIncAsyncCount()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroIncCount()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroIncWait()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroNextHead()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroNextTail()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroPrevHead()
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroPrevTail()
        {
            throw new NotImplementedException();
        }

        void IIoZeroSemaphore.ZeroRef(ref IIoZeroSemaphore @ref, CancellationTokenSource asyncToken)
        {
            throw new NotImplementedException();
        }

        uint IIoZeroSemaphore.ZeroTail()
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

        uint IIoZeroSemaphore.ZeroWaitCount()
        {
            throw new NotImplementedException();
        }
    }
}
