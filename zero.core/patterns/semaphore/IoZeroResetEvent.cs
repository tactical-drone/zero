using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.semaphore
{
    /// <summary>
    /// An auto reset event that supports one waiter only
    /// </summary>
    public class IoZeroResetEvent : IIoZeroSemaphore
    {
        public IoZeroResetEvent(bool open = false)
        {
            if(open)
                _pressure.SetResult(true);
        }

        private IoManualResetValueTaskSource<bool> _pressure = new();

        public int CurNrOfBlockers => _pressure.GetStatus(_pressure.Version) == ValueTaskSourceStatus.Pending ? 1 : 0;

        public int ReadyCount => _pressure.GetStatus(_pressure.Version) == ValueTaskSourceStatus.Succeeded ? 1 : 0;

        public int MaxAsyncWorkers => 0;

        public int Capacity => 1;
        public bool RunContinuationsAsynchronously => _pressure.RunContinuationsAsynchronously;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetResult(short token)
        {
            return _pressure.GetResult(token);
        }

        public ValueTaskSourceStatus GetStatus(short token)
        {
            return _pressure.GetStatus(token);
        }

        public bool IsCancellationRequested()
        {
            return _pressure.GetStatus(_pressure.Version) == ValueTaskSourceStatus.Canceled;
        }

        public void ZeroThrow()
        {
            throw new NotImplementedException();
        }

        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            throw new NotImplementedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(int releaseCount = 1, bool bestEffort = false)
        {
            try
            {
                _pressure.SetResult(true);
                return 1;
            }
            catch when (!bestEffort)
            {
                throw;
            }
            catch when (bestEffort)
            {
                return 0;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> WaitAsync()
        {
            var p = _pressure.WaitAsync();

            return p == default ? new ValueTask<bool>(true) : p;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ZeroSem()
        {
            _pressure.SetResult(false);
        }

        int IIoZeroSemaphore.ZeroAddCount(int value)
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroAsyncCount()
        {
            throw new NotImplementedException();
        }

        int IIoZeroSemaphore.ZeroEnter()
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

        public bool Zeroed()
        {
            throw new NotImplementedException();
        }

        long IIoZeroSemaphore.ZeroHead()
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

        long IIoZeroSemaphore.ZeroNextHead()
        {
            throw new NotImplementedException();
        }

        long IIoZeroSemaphore.ZeroNextTail()
        {
            throw new NotImplementedException();
        }

        long IIoZeroSemaphore.ZeroPrevHead()
        {
            throw new NotImplementedException();
        }

        long IIoZeroSemaphore.ZeroPrevTail()
        {
            throw new NotImplementedException();
        }

        public void ZeroRef(ref IIoZeroSemaphore @ref, CancellationTokenSource asyncToken)
        {
            throw new NotImplementedException();
        }

        long IIoZeroSemaphore.ZeroTail()
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

        public short Version => _pressure.Version;
        
    }
}
