using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore
{
    public struct IoAutoMutex : IValueTaskSource<bool>, IIoMutex
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="signalled">Initial state</param>
        /// <param name="asyncToken">Cancellation token</param>
        /// <param name="allowInliningContinuations">Disable force async</param>
        public IoAutoMutex(CancellationTokenSource asyncTasks, bool signalled = false, bool allowInliningContinuations = true) : this()
        {
            //_signalAwaiters = new ConcurrentQueue<ZeroCompletionSource<bool>>();
            Configure( asyncTasks, signalled, allowInliningContinuations);
        }

        //private ConcurrentQueue<ZeroCompletionSource<bool>> _signalAwaiters;

        private volatile int _signalled;

        /// <summary>
        /// Configure
        /// </summary>
        /// <param name="signalled">Initial state</param>
        /// <param name="asyncToken"></param>
        /// <param name="allowInliningContinuations"></param>
        public void Configure(CancellationTokenSource asyncTasks, bool signalled = false, bool allowInliningContinuations = true)
        {
            // _csHeap = ZeroOnCascade(new IoHeap<ZeroCompletionSource<bool>>(10, this)
            //     {Make = o => ((IoAutoMutex)o).ZeroOnCascade(new ZeroCompletionSource<bool>(allowInliningContinuations))});
            // lock (this)
            // {
            //     _csHeap ??= new IoHeap<ZeroCompletionSource<bool>>(10){Make = o => ((IoAutoMutex) o).ZeroOnCascade(new ZeroCompletionSource<bool>(allowInliningContinuations))};
            // }
            
            _signalled = signalled ? 1 : 0;
            _allowInliningContinuations = allowInliningContinuations;
            _status = ValueTaskSourceStatus.Pending;
            _token = 0;
            _asyncToken = asyncTasks.Token;
        }

        
        
        /// <summary>
        /// Signal
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set()
        {
            //get a waiter
            // if (_signalAwaiters.TryDequeue(out var toRelease))
            // {
            //     toRelease.TrySetResult(!AsyncTasks.IsCancellationRequested);
            //     //await _csHeap.Return(toRelease).ConfigureAwait(false);
            //     //await toRelease.ZeroAsync(this);
            // }
            // else
            //     Interlocked.Exchange(ref _signalled, 1);
        }

        private static readonly ValueTask<bool> FalseSentinel = new ValueTask<bool>(false);
        private static readonly ValueTask<bool> TrueSentinal = new ValueTask<bool>(true);
        private volatile short _token;
        private volatile ValueTaskSourceStatus _status;
        private bool _allowInliningContinuations;
        private CancellationToken _asyncToken;

        /// <summary>
        /// Wait
        /// </summary>
        /// <returns></returns>
        /// <exception cref="OutOfMemoryException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> WaitAsync()
        {
            //fail fast
            if (_asyncToken.IsCancellationRequested)
                return FalseSentinel;

            //were we signalled?
            //ZeroCompletionSource<bool> waiter;
            if (Interlocked.CompareExchange(ref _signalled, 0, 1) == 1)
            {
                return TrueSentinal;
            }
            else
            {
                // var takeTask = _csHeap.Take(this);
                // await takeTask.OverBoostAsync().ConfigureAwait(false);
                // waiter = takeTask.Result;
                //waiter = ZeroOnCascade(new ZeroCompletionSource<bool>(true));
//                 waiter = new ZeroCompletionSource<bool>(true);
//
// #if DEBUG
//                 if (waiter == null)
//                     throw new OutOfMemoryException(
//                         $"{nameof(IoHeap<ZeroCompletionSource<bool>>)}: Heap depleted: taken = {_signalAwaiters.Count}, max = {_csHeap.MaxSize}");
// #else
//                 if (waiter == null)
//                 {
//                     ZeroAsync(this).ConfigureAwait(false);
//                     return FalseResult;
//                 }
// #endif

                // if (Zeroed())
                //     waiter.TrySetCanceled(AsyncTasks.Token);
                // else
                //     _signalAwaiters.Enqueue(waiter);
                
                return new ValueTask<bool>(this, _token);
            }
            
            //return new ZeroBoost<bool>(waiter.Task);
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

        public void ByRef(ref IIoMutex root)
        {
            throw new NotImplementedException();
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

        /// <summary>
        /// Not applicable
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public void Zero()
        {
            throw new NotImplementedException();
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ValidateToken(short token)
        {
            if(token != _token)
                throw new InvalidOperationException($"invalid token = {token} != {_token}");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetResult(short token)
        {
            ValidateToken(token);

            return _signalled == 1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTaskSourceStatus GetStatus(short token)
        {
            ValidateToken(token);
            return _status;
        }

        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            ValidateToken(token);
            //TODO
        }
    }
}