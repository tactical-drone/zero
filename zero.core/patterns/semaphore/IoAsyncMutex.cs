using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Microsoft.VisualStudio.Threading;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore
{
    public class IoAsyncMutex : IoZeroable, IIoMutex
    {
        /// <summary>
        /// Constructor 
        /// </summary>
        /// <param name="signalled">Initial states</param>
        /// <param name="allowInliningContinuations">Allow inline completions</param>
        /// <param name="token">Cancellation token</param>
        public IoAsyncMutex(bool signalled = false, bool allowInliningContinuations = true)
        {
            _signalledResult = ValueTask.FromResult(true);
            Configure(signalled, allowInliningContinuations);
        }

        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
        }

        protected override Task ZeroManagedAsync()
        {
            _manualReset.SetException(new TaskCanceledException(Description));
            _taskCompletionSource.TrySetCanceled(AsyncTasks.Token);
            return base.ZeroManagedAsync();
        }

        private string _description;

        public override string Description
        {
            get
            {
                if (_description != null)
                    return _description;
                return _description = $"{nameof(IoAsyncMutex)}({_manualReset.Version})";
            }
        }
        
        private readonly ValueTask<bool> _signalledResult;
        private ManualResetValueTaskSourceCore<bool> _manualReset;
        private ValueTask<bool> _awaitable;
        private TaskCompletionSource<bool> _taskCompletionSource;

        public void Configure(bool signalled = false, bool allowInliningContinuations = true)
        {
            _taskCompletionSource = new TaskCompletionSource<bool>(allowInliningContinuations
                ? TaskCreationOptions.None
                : TaskCreationOptions.RunContinuationsAsynchronously);
            
            _awaitable = new ValueTask<bool>(_taskCompletionSource.Task);
            
            _manualReset.OnCompleted(m =>
                {
                    ((IoAsyncMutex) m)._taskCompletionSource.SetResult(true);
                }, this,
                _manualReset.Version, ValueTaskSourceOnCompletedFlags.None);

            if (signalled)
                _manualReset.SetResult(true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask SetAsync()
        {
            _manualReset.SetResult(true);
            return ValueTask.CompletedTask;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> WaitAsync()
        {
            //fail fast
            if (AsyncTasks.IsCancellationRequested)
            {
                Reset();
                return ValueTask.FromCanceled<bool>(AsyncTasks.Token);
            }

            //fast path
            if (_manualReset.GetStatus(_manualReset.Version) == ValueTaskSourceStatus.Succeeded && _manualReset.GetResult(_manualReset.Version))
            {
                Reset();
                return _signalledResult;
            }
            
            //slow path
            _taskCompletionSource = new TaskCompletionSource<bool>(true
                ? TaskCreationOptions.None
                : TaskCreationOptions.RunContinuationsAsynchronously);

            return new ValueTask<bool>(_taskCompletionSource.Task);
            
            //return _awaitable;
        }

        public void Reset()
        {
            _manualReset.Reset();
            _manualReset.OnCompleted(m =>
                {
                    ((IoAsyncMutex) m)._taskCompletionSource.SetResult(true);
                }, this,
                _manualReset.Version, ValueTaskSourceOnCompletedFlags.None);
        }
        
    }
}