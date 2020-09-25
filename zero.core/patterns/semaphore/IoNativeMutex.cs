using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Microsoft.VisualStudio.Threading;

namespace zero.core.patterns.semaphore
{
    public struct IoNativeMutex: IIoMutex
    {
        public IoNativeMutex(CancellationTokenSource asyncTasks, bool initialState = false,  bool allowInliningContinuations = true)
        {
            _asyncToken = asyncTasks.Token;
            _mutex = new AsyncAutoResetEvent(allowInliningContinuations);
            Configure(asyncTasks, initialState, allowInliningContinuations);
        }

        private readonly AsyncAutoResetEvent _mutex;
        private CancellationToken _asyncToken;
        
        public void Configure(CancellationTokenSource asyncTasks, bool signalled = false, bool allowInliningContinuations = true)
        {
            _asyncToken = asyncTasks.Token;
            if(signalled)
                Set();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set()
        {
            _mutex.Set();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<bool> WaitAsync()
        {
            if (_asyncToken.IsCancellationRequested)
                return false;
            
            var wait = true;
            try
            {
                await _mutex.WaitAsync(_asyncToken).ConfigureAwait(false);
                if (_asyncToken.IsCancellationRequested)
                    return false;
            }
            catch
            {
                wait = false;
            }
            return wait;
        }

        public int GetWaited()
        {
            throw new NotImplementedException();
        }

        public void SetWaited()
        {
            throw new NotImplementedException();
        }

        public int GetHooked()
        {
            throw new NotImplementedException();
        }

        public void SetHooked()
        {
            throw new NotImplementedException();
        }

        public void SetResult(bool result)
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public void ByRef(ref IIoMutex root)
        {
            
        }

        public short Version()
        {
            throw new NotImplementedException();
        }

        public ref IIoMutex GetRef(ref IIoMutex mutex)
        {
            throw new NotImplementedException();
        }

        public short GetCurFrame()
        {
            throw new NotImplementedException();
        }

        public bool SetWaiter(Action<object> continuation, object state)
        {
            throw new NotImplementedException();
        }

        public ref IIoMutex GetRef()
        {
            throw new NotImplementedException();
        }

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
    }
}