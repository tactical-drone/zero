using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.Threading;

namespace zero.core.patterns.semaphore
{
    /// <summary>
    /// An async semaphore that supports back pressure, but no timeouts
    /// </summary>
    public class IoSemaphore
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="initialCount">The initial open slots</param>
        /// <param name="maxCapacity">Max open slots</param>
        /// <param name="cancellationToken">The cancellation token</param>
        /// <param name="allowInliningAwaiters">Allow awaiters to complete synchronously</param>
        public IoSemaphore(CancellationToken cancellationToken, int initialCount = 0, int maxCapacity = 1, bool allowInliningAwaiters = true)
        {
            _cancellationToken = cancellationToken;
            _count = initialCount;
            _maxCapacity = maxCapacity;
            _allowInliningAwaiters = allowInliningAwaiters;

            var reg = _cancellationToken.Register(s => ((IoSemaphore)s).Release(),this);
        }

        /// <summary>
        /// A queue of folks awaiting signals.
        /// </summary>
        private readonly ConcurrentQueue<TaskCompletionSource<bool>> _signalAwaiters = new ConcurrentQueue<TaskCompletionSource<bool>>();

        /// <summary>
        /// Max Capacity
        /// </summary>
        private readonly int _maxCapacity;

        /// <summary>
        /// Initial count
        /// </summary>
        private volatile int _count;

        /// <summary>
        /// Allow synchronous awaiters
        /// </summary>
        private readonly bool _allowInliningAwaiters;

        /// <summary>
        /// The cancellation token used
        /// </summary>
        private readonly CancellationToken _cancellationToken;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        public void Release(int count = 1)
        {
            var dequeued = 0;
            while(!_signalAwaiters.IsEmpty && 
                  (dequeued < count || _cancellationToken.IsCancellationRequested) && 
                  _signalAwaiters.TryDequeue(out var completionSource))
            {
                dequeued++;
                completionSource.SetResult(_cancellationToken.IsCancellationRequested);
                Interlocked.Increment(ref _count);
            }
        }

        /// <summary>
        /// Blocks if there are no slots available
        /// </summary>
        /// <returns>True if success, false otherwise</returns>
        public async ValueTask<bool> WaitAsync()
        {
            //fail fast
            if (_cancellationToken.IsCancellationRequested)
                return false;

            //fast path
            if (Interlocked.Decrement(ref _count) > -1)
                return true;

            var waiter = new TaskCompletionSource<bool>(this, !_allowInliningAwaiters ? TaskCreationOptions.RunContinuationsAsynchronously:TaskCreationOptions.None);

            //fail fast
            if (_cancellationToken.IsCancellationRequested)
            {
                return false;
            }
            else
            {
                _signalAwaiters.Enqueue(waiter);
            }

            return await waiter.Task;
        }
    }
}
