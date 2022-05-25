﻿//#define TRACE
using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

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
        public IoZeroCore(string description, int capacity, int ready = 0, bool zeroAsyncMode = false, bool contextUnsafe = false)
        {
            if(capacity > short.MaxValue >> 1)
                throw new ArgumentOutOfRangeException(nameof(capacity));

            if(ready > capacity)
                throw new ArgumentOutOfRangeException(nameof(ready));

            _b_head = 0;
            _b_tail = 0;

            _zeroed = 0;
            _description = description;
            _capacity = capacity;
            capacity *= 2;
            ZeroAsyncMode = zeroAsyncMode;

            //_blocking = new IIoManualResetValueTaskSourceCore<T>[capacity];

            _blocking = ArrayPool<IIoManualResetValueTaskSourceCore<T>>.Shared.Rent(capacity);
            for (short i = 0; i < capacity; i++)
            {
                
                var core = _blocking[i] = new IoManualResetValueTaskSourceCore<T>
                {
                    RunContinuationsUnsafe = contextUnsafe,
                    RunContinuationsAsynchronouslyAlways = zeroAsyncMode, 
                    AutoReset = true
                };
                core.Prime(i);
            }

            _b_tail = ready;

            _primeReady = _ => default;
            _primeContext = null;
            //_zeroRef = null;
        }

        public IIoZeroSemaphoreBase<T> ZeroRef(ref IIoZeroSemaphoreBase<T> @ref, Func<object, T> primeResult = default,
            object context = null)
        {
            //for (var i = 0; i < ModCapacity; i++)
            //    _blocking[i].BurnContext = @ref;

            if (@ref == null)
                throw new ArgumentNullException(nameof(@ref));

            _primeReady = primeResult;
            _primeContext = context;

            if (_b_tail > 0 && primeResult == null)
                throw new ArgumentOutOfRangeException(nameof(primeResult));

            for (int i = 0; i < _b_tail; i++)
            {
                var core = _blocking[i];
                core.SetResult(_primeReady!(_primeContext));
            }

            //return _zeroRef = @ref;
            return @ref;
        }

        public void ZeroSem()
        {
            if (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return;

            var operationCanceledException = new TaskCanceledException($"{nameof(ZeroSem)}: [TEARDOWN DIRECT] {Description}");

            ////flush waiters
            for (var i = Math.Min(_b_head, _b_tail); i < ModCapacity; i++)
            {
                try
                {
                    _blocking[i % ModCapacity].SetException(operationCanceledException);
                    _blocking[i % ModCapacity] = default;
                }
                catch
                {
                    // ignored
                }
            }

            for (var i = 0; i < ModCapacity; i++)
            {
                try
                {
                    _blocking[i % ModCapacity].SetException(operationCanceledException);
                }
                catch
                {
                    // ignored
                }
            }

            ArrayPool<IIoManualResetValueTaskSourceCore<T>>.Shared.Return(_blocking, true);
        }

        public bool Zeroed() => _zeroed > 0;
        #endregion Memory management

        #region Aligned
        private long _b_head;
        private readonly IIoManualResetValueTaskSourceCore<T>[] _blocking;
        private Func<object, T> _primeReady;
        private object _primeContext;
        private readonly string _description;
        private readonly int _capacity;
        private volatile int _zeroed;
        private long _b_tail;
        #endregion

        #region Properties
        private readonly int ModCapacity => _capacity<<1;
        //private IIoZeroSemaphoreBase<T> _zeroRef;
        #endregion

        #region State
        public string Description =>
            $"{nameof(IoZeroSemCore<T>)}: r = {ReadyCount}/{_capacity}, w = {WaitCount}/{_capacity}, z = {_zeroed > 0}, b_H = {_b_head % ModCapacity} ({_b_head}), b_T = {_b_tail % ModCapacity} ({_b_tail}), {_description}";//, n_H = {_n_head % ModCapacity} ({_n_head}), n_T = {_n_tail % ModCapacity} ({_n_tail})";
        public int Capacity => _capacity;
        public int WaitCount => (int)(_b_head > _b_tail? _b_head - _b_tail : 0);
        public int ReadyCount => (int)(_b_tail > _b_head? _b_tail - _b_head : 0);
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
            if (ReadyCount >= _capacity)
            {
                released = 0;
                return false;
            }
            long cap;

            long origHead;
            long origTail;
            var idx = _b_tail.ZeroNext(cap = (origTail = _b_tail) >= (origHead = _b_head)? origHead + _capacity: origTail + _capacity);
            if (idx != cap)
            {
                var slowCore = _blocking[idx % ModCapacity];
                slowCore.RunContinuationsAsynchronously = forceAsync;
                slowCore.SetResult(value);
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
#endif
        private bool ZeroBlock(out ValueTask<T> slowTaskCore)
        {
            Debug.Assert(WaitCount <= _capacity);

            long idx;
            long cap;

            slowTaskCore = default;
            var retry = _capacity;
            race:

            long origTail;
            long origHead;
            if ((idx = _b_head.ZeroNext(cap = (origHead = _b_head) >= (origTail = _b_tail)? origTail + _capacity: origHead + _capacity)) != cap)
            {
                var slowCore = _blocking[idx %= ModCapacity];
                Debug.Assert(!slowCore.Burned);
                slowTaskCore = !slowCore.Primed ? new ValueTask<T>(slowCore, (short)idx) : new ValueTask<T>(slowCore.GetResult((short)idx));
                return true;
            }
            if(retry-->0)
                goto race;
            
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
            if (ZeroBlock(out var slowCore))
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
