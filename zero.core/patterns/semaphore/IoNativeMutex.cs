using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
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
    }
}