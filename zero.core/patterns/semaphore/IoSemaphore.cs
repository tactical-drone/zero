﻿using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore
{
    /// <summary>
    /// An async semaphore that supports back pressure, but no timeouts
    /// </summary>
    public struct IoSemaphore : IValueTaskSource<bool>, IIoSemaphore
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="asyncTasks"></param>
        /// <param name="initialCount">The initial open slots</param>
        /// <param name="maxCapacity">Max open slots</param>
        /// <param name="enableRangeCheck">Range check semaphore</param>
        /// <param name="allowInliningAwaiters">Allow awaiters to complete synchronously</param>
        /// <param name="options"></param>
        public IoSemaphore(CancellationTokenSource asyncTasks, int initialCount = 0, int maxCapacity = 1, bool enableRangeCheck = false,
            bool allowInliningAwaiters = true, TaskCreationOptions options = TaskCreationOptions.None):this()
        {
#if DEBUG
            enableRangeCheck = true;
#endif
            Configure(asyncTasks, initialCount, maxCapacity, enableRangeCheck, allowInliningAwaiters, options);
        }
        
        /// <summary>
        /// A queue of folks awaiting signals.
        /// </summary>
        //private ConcurrentQueue<ZeroCompletionSource<bool>> _signalAwaiters = new ConcurrentQueue<ZeroCompletionSource<bool>>();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        //private IoHeap<ZeroCompletionSource<bool>> _csHeap;

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

        public void Configure(CancellationTokenSource asyncTasks, int initialCount = 0, int maxCapacity = 1, bool rangeCheck = false,
            bool allowInliningContinuations = true, TaskCreationOptions options = TaskCreationOptions.None)
        {
            if (initialCount < 0 || maxCapacity < 1 || initialCount > maxCapacity)
                throw new ArgumentException($"invalid initial = {initialCount} & maxCapacity = {maxCapacity} supplied");

            _asyncToken = asyncTasks.Token;
            _enableRangeCheck = rangeCheck;

            // _csHeap = new IoHeap<ZeroCompletionSource<bool>>(maxCapacity)
            // {
            //     Make = o => ((IoSemaphore)o).ZeroOnCascade(new ZeroCompletionSource<bool>(allowInliningContinuations, options))
            // };

            _count = initialCount;
            _maxCapacity = maxCapacity;

            _token = 0;
            _status = ValueTaskSourceStatus.Pending;
        }

        
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        public void Release(int count = 1)
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
            // while (dequeued < count && _count < _maxCapacity && _signalAwaiters.TryDequeue(out var completionSource))
            // {
            //     //await _csHeap.Return(completionSource).ConfigureAwait(false);
            //     dequeued++;
            //     completionSource.SetResult(!AsyncTasks.IsCancellationRequested);
            //     Interlocked.Increment(ref _count);
            // }

            //range check
#if DEBUG
            if (_enableRangeCheck && dequeued < count)
            {
                throw new SemaphoreFullException(
                    $"requested {count} release signals, sent = {dequeued}, missing = {dequeued - count}");
            }
#endif
        }

        private static readonly ValueTask<bool> FalseSentinel = new ValueTask<bool>(false);
        private static readonly ValueTask<bool> TrueSentinel = new ValueTask<bool>(true);
        
        private volatile short _token;
        private volatile ValueTaskSourceStatus _status;
        private CancellationToken _asyncToken;

        /// <summary>
        /// Blocks if there are no slots available
        /// </summary>
        /// <returns>True if success, false otherwise</returns>
        public ValueTask<bool> WaitAsync()
        {
            //fail fast
            // if (Zeroed())
            //     return FalseSentinel;

            //fast path
            if (Interlocked.Decrement(ref _count) > -1)
                return TrueSentinel;

//             var takeTask = _csHeap.Take(this);
//             
//             await takeTask.OverBoostAsync().ConfigureAwait(false);
//             
//             var waiter = takeTask.Result;
//
// #if DEBUG
//             if (waiter == null)
//                 throw new OutOfMemoryException(
//                     $"{nameof(IoHeap<ZeroCompletionSource<bool>>)}: Heap depleted: taken = {_signalAwaiters.Count}, max = {_csHeap.MaxSize}");
// #endif

            //fail fast
            //if (Zeroed())
            {
                return FalseSentinel;
            }
            //else
            {
                //_signalAwaiters.Enqueue(waiter);
            }

            //return await new ZeroBoost<bool>(waiter.Task);
            return new ValueTask<bool>(this, _token);
        }
        
        public bool GetResult(short token)
        {
            ValidateToken(token);
            return _count < _maxCapacity;
        }

        private void ValidateToken(in short token)
        {
            // if(_token!=token)
            //     throw new InvalidOperationException($"invalid token = {token} != {_token}, {Description}");
        }

        public ValueTaskSourceStatus GetStatus(short token)
        {
            return _status;
        }

        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            throw new NotImplementedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(IIoZeroable other)
        {
            //return NpId == other.NpId;
            return true;
        }

        //public CancellationTokenSource AsyncToken => _nanoprobe.AsyncToken;
    }
}