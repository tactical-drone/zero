//#define TRACE
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
using zero.core.misc;
using zero.core.patterns.queue;

namespace zero.core.patterns.semaphore.core
{
    /// <summary>
    /// A new core semaphore based on learnings from <see cref="IoZeroSemaphore{T}"/> that uses
    /// <see cref="IoManualResetValueTaskSourceCore{TResult}"/> which is based on <see cref="ManualResetValueTaskSourceCore{TResult}"/>better;
    ///
    /// Status: Tests OK
    ///
    /// Note: This struct does not need to derive from <see cref="IIoZeroSemaphoreBase{T}"/>, it does not use the <see cref="IValueTaskSource"/> parts of the interface.
    /// Because this core is supposed to be interchangeable with <see cref="IoZeroSemaphore{T}"/> that does need it, the interface is derived from here.
    /// This should be a standalone struct with no derivations
    /// </summary>
    /// <typeparam name="T">The type that this primitive marshals</typeparam>
    [StructLayout(LayoutKind.Auto)]
    public struct IoZeroCore<T>:IIoZeroSemaphoreBase<T>
    {
        #region Memory management
        public IoZeroCore(string description, int capacity, CancellationTokenSource asyncTasks, int ready = 0, bool zeroAsyncMode = false)
        {
            if(capacity > short.MaxValue / 3)
                throw new ArgumentOutOfRangeException(nameof(capacity));

            if(ready > capacity)
                throw new ArgumentOutOfRangeException(nameof(ready));

            _b_head = 0;
            _b_tail = 0;

            _zeroed = 0;
            _description = description;
            _capacity = capacity;
            capacity *= 3;
            ZeroAsyncMode = zeroAsyncMode;

            _waiters = Channel.CreateBounded<IIoManualResetValueTaskSourceCore<T>>(new BoundedChannelOptions(capacity)
            {
                SingleWriter = false,
                SingleReader = false,
            });

            _results = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
            {
                SingleWriter = false,
                SingleReader = false,
            });
            _heap = Channel.CreateBounded<IIoManualResetValueTaskSourceCore<T>>(capacity);

            _b_tail = ready;

            _primeReady = _ => default;
            _primeContext = null;
            _asyncTasks = asyncTasks;
            //_zeroRef = null;
        }

        public IIoZeroSemaphoreBase<T> ZeroRef(ref IIoZeroSemaphoreBase<T> @ref, Func<object, T> primeResult = default,
            object context = null)
        {
            if (@ref == null)
                throw new ArgumentNullException(nameof(@ref));

            //_primeReady = primeResult ?? throw new ArgumentNullException(nameof(primeResult));
            _primeReady = primeResult;
            _primeContext = context;

            if (_b_tail > 0 && primeResult == null)
                throw new ArgumentOutOfRangeException(nameof(primeResult));

            for (int i = 0; i < _b_tail; i++)
            {
                _results.Writer.TryWrite(_primeReady!(_primeContext));
            }

            //return _zeroRef = @ref;
            return @ref;
        }

        public void ZeroSem()
        {
            if (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return;

            while (_waiters.Reader.TryRead(out var cancelled))
            {
                try
                {
                    cancelled.RunContinuationsAsynchronouslyAlways = false;
                    cancelled.RunContinuationsAsynchronously = false;
                    cancelled.SetException(new TaskCanceledException($"{nameof(ZeroSem)}: [TEARDOWN DIRECT] {Description}"));
                    //cancelled.SetResult(_primeReady(_primeContext));
                }
                catch
                {
                    // ignored
                }
            }

        }

        public bool Zeroed() => _zeroed > 0 || _asyncTasks.IsCancellationRequested;
        #endregion Memory management

        #region Aligned
        private long _b_head;
        private long _b_tail;
        private readonly Channel<IIoManualResetValueTaskSourceCore<T>> _waiters;
        private readonly Channel<T> _results;
        private readonly Channel<IIoManualResetValueTaskSourceCore<T>> _heap;
        private Func<object, T> _primeReady;
        private object _primeContext;
        private readonly string _description;
        private readonly int _capacity;
        private volatile int _zeroed;
        private readonly CancellationTokenSource _asyncTasks;
        #endregion

        #region Properties
        private readonly int ModCapacity => _capacity * 3;
        //private IIoZeroSemaphoreBase<T> _zeroRef;
        #endregion

        #region State
        public string Description =>
            $"{nameof(IoZeroSemCore<T>)}: r = {ReadyCount}/{_capacity}, w = {WaitCount}/{_capacity}, z = {_zeroed > 0}, b_H = {_b_head % ModCapacity} ({_b_head}), b_T = {_b_tail % ModCapacity} ({_b_tail}), {_description}";//, n_H = {_n_head % ModCapacity} ({_n_head}), n_T = {_n_tail % ModCapacity} ({_n_tail})";
        public int Capacity => _capacity;
        public int WaitCount => _waiters.Reader.Count;
        public int ReadyCount => _results.Reader.Count;
        public bool ZeroAsyncMode { get; }
        public long Tail => _b_tail;
        public long Head => _b_head;
        #endregion

        #region Core
        /// <summary>
        /// Dequeue a slow core and unblock it using the <see cref="value"/> provided
        /// </summary>
        /// <param name="value">Send this value to the blocker</param>
        /// <param name="released">The number of blockers released with <see cref="value"/></param>
        /// <param name="forceAsync"></param>
        /// <returns>If a waiter was unblocked, false otherwise</returns>
#if RELEASE
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private bool ZeroSetResult(T value, out int released, bool forceAsync = false)
        {
            //unblock
            while (_waiters.Reader.TryPeek(out var peek) && _waiters.Reader.TryRead(out var waiter))
            {
                try
                {
                    if (!waiter.Burned)
                    {
                        waiter.RunContinuationsAsynchronously = forceAsync || ZeroAsyncMode;
                        waiter.SetResult(value);
                        released = 1;
                        return true;
                    }
                }
                catch
                {
                    // ignored
                }
            }

            //insane
            if (_results.Reader.Count >= ModCapacity)
            {
                released = 0;
                return false;
            }

            //prime
            if (_results.Writer.TryWrite(value))
            {
                //unblock on race with prime
                if (_waiters.Reader.TryPeek(out var peek) && _waiters.Reader.TryRead(out var waiter))
                {
                    if (!waiter.Burned)
                    {
                        try
                        {
                            waiter.RunContinuationsAsynchronously = forceAsync || ZeroAsyncMode;
                            waiter.SetResult(value);
                            released = 1;
                            return true;
                        }
                        catch
                        {
                            // ignored
                        }
                    }
                }
                released = 1;
                return true;
            }

            released = 0;
            return false;
        }


        /// <summary>
        /// Creates a new blocking core and releases the current thread to the pool
        /// </summary>
        /// <param name="slowTaskCore">The resulting core that will most likely result in a block</param>
        /// <returns>True if there was a core created, false if all <see cref="_capacity"/> cores are still blocked</returns>
#if RELEASE
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#else
        [MethodImpl(MethodImplOptions.NoInlining)]
#endif
        private bool ZeroBlock(out ValueTask<T> slowTaskCore)
        {
            Debug.Assert(Zeroed() || WaitCount <= ModCapacity);

            slowTaskCore = default;
            
            //fast path
            if (_waiters.Reader.Count == 0 && _results.Reader.TryRead(out var result))
            {
                slowTaskCore = new ValueTask<T>(result);
                return true;
            }

            //heap
            IIoManualResetValueTaskSourceCore<T> waiter;
            if (_heap.Reader.TryRead(out var cached))
            {
                waiter = cached;
                waiter.Reset();
            }
            else
            {
                waiter = new IoManualResetValueTaskSourceCore<T> { AutoReset = false };
                waiter.Reset(static state =>
                {
                    var (@this, waiter) = (ValueTuple<IoZeroCore<T>, IIoManualResetValueTaskSourceCore<T>>)state;
                    @this._heap.Writer.TryWrite(waiter);
                }, (this, waiter));
            }
            
            //block
            if (_waiters.Writer.TryWrite(waiter))
            {
                //unblock on race with primed result
                if (_waiters.Reader.Count == 1 && _results.Reader.TryRead(out var race))
                {
                    try
                    {
                        waiter.GetResult(0);//burn the core so that the racer can avoid it
                        throw new InvalidOperationException(nameof(ZeroBlock));
                    }
                    catch
                    {
                        // ignored
                    }

                    slowTaskCore = new ValueTask<T>(race);
                    return true;
                }

                slowTaskCore = new ValueTask<T>(waiter, 0);
                return true;
            }

            return false;
        }
#endregion

#region API
        public T GetResult(short token) => throw new NotImplementedException(nameof(GetResult));

        public ValueTaskSourceStatus GetStatus(short token) => throw new NotImplementedException(nameof(GetStatus));

        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => throw new NotImplementedException(nameof(OnCompleted));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T value, int releaseCount, bool forceAsync = false)
        {
            var released = 0;
            for (var i = 0; i < releaseCount; i++)
                released += Release(value, forceAsync);
            
            return released;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T[] value, bool forceAsync = false)
        {
            var released = 0;
            foreach (var t in value)
                released += Release(t, forceAsync);

            return released;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T value, bool forceAsync = false)
        {
            if (ZeroSetResult(value, out var release,forceAsync)) return release;
            return 0;
        }

        /// <summary>
        /// Wait for a signal
        /// </summary>
        /// <returns>A value task core that pumps values when signaled</returns>
        /// <exception cref="InvalidOperationException">When invalid concurrency levels are detected.</exception>
        public ValueTask<T> WaitAsync()
        {
            // => slow core
            if (!Zeroed() && ZeroBlock(out var slowCore))
                return slowCore;
            
            // => API implementation error
            if(!Zeroed())
                throw new InvalidOperationException($"{nameof(IoZeroCore<T>)}: Invalid concurrency level detected, check that {_capacity} matches or exceeds the expected level of concurrent blockers expected. {Description}");

            return default;
        }

        int IIoZeroSemaphoreBase<T>.ZeroDecAsyncCount()
        {
            throw new NotImplementedException();
        }
#endregion
    }
}
