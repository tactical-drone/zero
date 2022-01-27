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

        private readonly IoZeroResetValueTaskSource<bool> _pressure = new();

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
        int IIoZeroSemaphore.ZeroDecAsyncCount()
        {
            throw new NotImplementedException();
        }
        public bool Zeroed()
        {
            throw new NotImplementedException();
        }

        public void ZeroRef(ref IIoZeroSemaphore @ref)
        {
            throw new NotImplementedException();
        }

        public short Version => _pressure.Version;
        
    }
}
