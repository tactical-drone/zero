using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore
{
    /// <summary>
    /// An async semaphore that supports back pressure, but no timeouts
    /// </summary>
    public class IoSemaphore : IoZeroable, IIoSemaphore
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="initialCount">The initial open slots</param>
        /// <param name="maxCapacity">Max open slots</param>
        /// <param name="enableRangeCheck">Range check semaphore</param>
        /// <param name="allowInliningAwaiters">Allow awaiters to complete synchronously</param>
        /// <param name="options"></param>
        public IoSemaphore(int initialCount = 0, int maxCapacity = 1, bool enableRangeCheck = false,
            bool allowInliningAwaiters = true, TaskCreationOptions options = TaskCreationOptions.None)
        {
#if DEBUG
            enableRangeCheck = true;
#endif

            Configure(initialCount, maxCapacity, enableRangeCheck, allowInliningAwaiters, options);
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
            _signalAwaiters = null;
        }

        /// <summary>
        /// zero managed
        /// </summary>
        /// <returns></returns>
        protected override async Task ZeroManagedAsync()
        {
            //Unblock all blockerss
            foreach (var zeroCompletionSource in _signalAwaiters)
                await zeroCompletionSource.ZeroAsync(this).ConfigureAwait(false);

            _signalAwaiters.Clear();

            await base.ZeroManagedAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// A queue of folks awaiting signals.
        /// </summary>
        private ConcurrentQueue<ZeroCompletionSource<bool>> _signalAwaiters =
            new ConcurrentQueue<ZeroCompletionSource<bool>>();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private IoHeap<ZeroCompletionSource<bool>> _csHeap;

        /// <summary>
        /// Max Capacity
        /// </summary>
        private int _maxCapacity;

        /// <summary>
        /// Initial count
        /// </summary>
        private volatile int _count;

        /// <summary>
        /// Is range check enabled?
        /// </summary>
        private bool _enableRangeCheck;

        public void Configure(int initialCount, int maxCapacity = 1, bool rangeCheck = false,
            bool allowInliningContinuations = true, TaskCreationOptions options = TaskCreationOptions.None)
        {
            if (initialCount < 0 || maxCapacity < 1 || initialCount > maxCapacity)
                throw new ArgumentException($"invalid initial = {initialCount} & maxCapacity = {maxCapacity} supplied");

            _enableRangeCheck = rangeCheck;

            _csHeap = new IoHeap<ZeroCompletionSource<bool>>(maxCapacity)
            {
                Make = o => ((IoSemaphore)o).ZeroOnCascade(new ZeroCompletionSource<bool>(allowInliningContinuations, options))
            };

            _count = initialCount;
            _maxCapacity = maxCapacity;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        public async ValueTask ReleaseAsync(int count = 1)
        {
            //trivial case
            if (count == 0)
                return;

            //range check
#if DEBUG
            if (_enableRangeCheck && _count + count >= _maxCapacity)
                throw new SemaphoreFullException($"Unable to release {count}, only {_maxCapacity - _count} slots left");
#endif

            var dequeued = 0;
            while (dequeued < count && _count < _maxCapacity && _signalAwaiters.TryDequeue(out var completionSource))
            {
                await _csHeap.ReturnAsync(completionSource).ConfigureAwait(false);
                dequeued++;
                completionSource.SetResult(!AsyncTasks.IsCancellationRequested);
                Interlocked.Increment(ref _count);
            }

            //range check
#if DEBUG
            if (_enableRangeCheck && dequeued < count)
            {
                throw new SemaphoreFullException(
                    $"requested {count} release signals, sent = {dequeued}, missing = {dequeued - count}");
            }
#endif
        }

        private static readonly ValueTask<bool> FalseResult = new ValueTask<bool>(false);
        private static readonly ValueTask<bool> TrueResult = new ValueTask<bool>(true);

        /// <summary>
        /// Blocks if there are no slots available
        /// </summary>
        /// <returns>True if success, false otherwise</returns>
        public async ValueTask<bool> WaitAsync()
        {
            //fail fast
            if (Zeroed())
                return false;

            //fast path
            if (Interlocked.Decrement(ref _count) > -1)
                return true;

            var takeTask = _csHeap.TakeAsync(this);
            
            await takeTask.OverBoostAsync().ConfigureAwait(false);
            
            var waiter = takeTask.Result;

#if DEBUG
            if (waiter == null)
                throw new OutOfMemoryException(
                    $"{nameof(IoHeap<ZeroCompletionSource<bool>>)}: Heap depleted: taken = {_signalAwaiters.Count}, max = {_csHeap.MaxSize}");
#endif

            //fail fast
            if (Zeroed())
            {
                return false;
            }
            else
            {
                _signalAwaiters.Enqueue(waiter);
            }

            return await new ValueTask<bool>(waiter.Task);
        }
    }
}