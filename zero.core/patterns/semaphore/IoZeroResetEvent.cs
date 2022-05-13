using System;
using System.Diagnostics;
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

        public int WaitCount => _pressure.GetStatus((short)_pressure.Version) == ValueTaskSourceStatus.Pending ? 1 : 0;

        public int ReadyCount => _pressure.GetStatus((short)_pressure.Version) == ValueTaskSourceStatus.Succeeded ? 1 : 0;

        public bool ZeroAsyncMode => false;

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


        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            throw new NotImplementedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(int releaseCount = 1, bool async = false)
        {
            Debug.Assert(releaseCount == 1);
            try
            {
                _pressure.SetResult(true);
            }
            catch
            {
                return -1;
            }
            return releaseCount;
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

        public int Release(bool value, bool async = false)
        {
            throw new NotImplementedException();
        }

        public int Release(bool[] value, bool async = false)
        {
            throw new NotImplementedException();
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

        int IIoZeroSemaphoreBase<bool>.ZeroDecAsyncCount()
        {
            throw new NotImplementedException();
        }

        public bool Zeroed()
        {
            throw new NotImplementedException();
        }

        public int Version => _pressure.Version;
        
    }
}
