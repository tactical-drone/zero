//#define TRACE
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;

namespace zero.core.patterns.semaphore.core
{
    /// <summary>
    /// A new core semaphore based on learnings from <see cref="IoZeroSemaphore{T}"/> that uses
    /// <see cref="IoManualResetValueTaskSourceCore{TResult}"/> which is based on <see cref="ManualResetValueTaskSourceCore{TResult}"/>better;
    ///
    /// Status: Tests OK
    /// </summary>
    /// <typeparam name="T">The type that this primitive marshals</typeparam>
    [StructLayout(LayoutKind.Sequential, Pack = 64)]
    public struct IoZeroCore<T>:IIoZeroSemaphoreBase<T>
    {
        public IoZeroCore(string description, int capacity, int ready = 0, bool zeroAsyncMode = false)
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

            _blocking = new IIoManualResetValueTaskSourceCore<T>[capacity];

            for (short i = 0; i < capacity; i++)
            {
                var core = _blocking[i] = new IoManualResetValueTaskSourceCore<T>
                {
                    RunContinuationsAsynchronously = zeroAsyncMode, 
                    AutoReset = true,
                    _burnResult = (queued, state) =>
                    {
                        var @this = (IIoZeroSemaphoreBase<T>)state;

                        if (queued)
                        {
                           
                            var w = @this.IncWaitCount();
#if TRACE
                            Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] w++ = {w} (Blocked)");
#endif
                        }
                        else //inline
                        {
#if TRACE
                            Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] [INLINE] ");
#endif
                            var r = @this.DecReadyCount();
#if TRACE
                            Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] r-- = {r} (S)");
#endif
                        }
                    }
                };
            }

            _waitCount = 0;
            _readyCount = (int)(_b_tail = ready);

            _primeReady = _ => default;
            _primeContext = null;
            _zeroRef = null;
        }

        public IIoZeroSemaphoreBase<T> ZeroRef(ref IIoZeroSemaphoreBase<T> @ref, Func<object, T> primeResult = default,
            object context = null)
        {
            for (var i = 0; i < ModCapacity; i++)
                _blocking[i].BurnContext = @ref;

            if (@ref == null)
                throw new ArgumentNullException(nameof(@ref));

            _primeReady = primeResult;
            _primeContext = context;

            if (_readyCount > 0 && primeResult == null)
                throw new ArgumentOutOfRangeException(nameof(primeResult));

            var origPrime = _readyCount;
            var ready = _readyCount;
            for (int i = 0; i < ready; i++)
            {
                var core = _blocking[i];
                core.SetResult(_primeReady!(_primeContext));
            }

            _readyCount = origPrime;
            return _zeroRef = @ref;
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
                    _blocking[i % ModCapacity] = null;
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
                    if(_blocking[i] != null)
                    {
                        _blocking[i % ModCapacity].SetException(operationCanceledException);
                        _blocking[i % ModCapacity] = null;
                    }
                }
                catch
                {
                    // ignored
                }
            }

            _readyCount = _waitCount = 0;
        }

        public bool Zeroed() => _zeroed > 0;
#if RELEASE
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public long DecWaitCount()
        {
#if DEBUG
            //var r = Interlocked.Decrement(ref _waitCount);
            var r = _waitCount.ZeroPrev(0);
            return r;
#else
            //return Interlocked.Decrement(ref _waitCount);
            return _waitCount.ZeroPrev(0);
#endif
        }
#if RELEASE
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public long IncWaitCount()
        {
#if DEBUG
            var ret = _waitCount.ZeroNext(_capacity);
            //var ret = Interlocked.Increment(ref _waitCount);
            Debug.Assert(ret <= _capacity);
            Debug.Assert(ret >= 0);
            return ret;
#else
            //Interlocked.Increment(ref _waitCount);
            return _waitCount.ZeroNext(_capacity);
#endif
        }
#if RELEASE
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public long IncReadyCount()
        {
            var r = _readyCount.ZeroNext(_capacity);
            //var r = Interlocked.Increment(ref _readyCount);
#if DEBUG
            Debug.Assert(r <= _capacity);
            Debug.Assert(r >= 0);
            return r;
#else
            //return Interlocked.Increment(ref _readyCount);
#endif
            return _readyCount.ZeroNext(_capacity);
        }
#if RELEASE
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public long DecReadyCount()
        {
            return _readyCount.ZeroPrev(0);
            //return Interlocked.Decrement(ref _readyCount);
        }

        #region Aligned
        private long _b_head;
        private long _b_tail;
        
        private readonly IIoManualResetValueTaskSourceCore<T>[] _blocking;
        public int _readyCount;
        public int _waitCount;
        private readonly int _capacity;
        #endregion

        #region Properties
        private readonly int ModCapacity => _capacity<<1;
        private IIoZeroSemaphoreBase<T> _zeroRef;
        private readonly string _description;
        private volatile int _zeroed;
        private Func<object, T> _primeReady;
        private object _primeContext;
        #endregion

        #region State
        public string Description =>
            $"{nameof(IoZeroSemCore<T>)}: {_description}, r = {_readyCount}/{_capacity}, w = {_waitCount}/{_capacity}, z = {_zeroed > 0}, b_H = {_b_head % ModCapacity} ({_b_head}), b_T = {_b_tail % ModCapacity} ({_b_tail})";//, n_H = {_n_head % ModCapacity} ({_n_head}), n_T = {_n_tail % ModCapacity} ({_n_tail})";
        public int Capacity => _capacity;
        public int ReadyCount => _readyCount;
        public int WaitCount => _waitCount;
        public bool ZeroAsyncMode { get; }
        public long Tail => _b_head;
        public long Head => _b_head;
        #endregion

        /// <summary>
        /// Dequeue a slow core and unblock it using the <see cref="value"/> provided
        /// </summary>
        /// <param name="value">Send this value to the blocker</param>
        /// <param name="released">The number of blockers released with <see cref="value"/></param>
        /// <param name="forceAsync"></param>
        /// <returns>If a waiter was unblocked, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ZeroSetResult(T value, out int released, bool forceAsync = false)
        {
#if debug
            var origreadyCount = _readyCount;
#endif

            long cap;
            var origTail = _b_tail;
            var origHead = _b_head;
            Thread.MemoryBarrier();
            var idx = _b_tail.ZeroNext(cap = origTail > origHead || origTail == origHead && ReadyCount < _capacity? origHead + _capacity: origTail + _capacity);
            if (idx != cap)
            {
#if TRACE
                Console.WriteLine("SET <--------------");
#endif
                //Interlocked.Increment(ref _b_head);
                var slowCore = _blocking[idx % ModCapacity];
                slowCore.RunContinuationsAsynchronously = forceAsync;

                if (slowCore.Blocking)
                {
                    _zeroRef.DecWaitCount();
                    slowCore.SetResult(value);
                    released = 1;
#if TRACE
                    Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] w-- = {w} (unblock) ");
                    Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - Unblocked, (P) ");//id = [{idx%ModCapacity}]{idx:00}, status = {slowCore}");
#endif
                    return true;
                }

#if TRACE
                Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - ForkSet, id = [{idx % ModCapacity}]{idx:00}, orig = [{origTail % ModCapacity}]{origTail:00}, r = {ReadyCount}, w = {WaitCount} status = {slowCore}, origHead = {origHead}, origTail = {origTail} {Description}");
#endif
                slowCore.SetResult(value, static (async, @this) =>
                {
                    if (async)
                    {
                        @this.DecWaitCount();
                        
                        //#if TRACE
                        // Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - Unblocked,     ");//id = [{idx%ModCapacity}]{idx:00}, status = {slowCore}");
                        //#endif
                    }
                    else
                    {
                        var r = @this.IncReadyCount();
                            
#if TRACE
                        Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] r++ = {r} (S)");
#endif

#if TRACE
                        Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - Banked,       "); //id = [{idx % ModCapacity}]{idx:00}, status = {slowCore}");
#endif
                    }
                }, _zeroRef);
                
                released = 1;
                return true;
            }
#if TRACE
            Console.WriteLine("EMPTY");
#endif
            released = 0;
            return false;
        }

        /// <summary>
        /// Creates a new blocking core and releases the current thread to the pool
        /// </summary>
        /// <param name="slowTaskCore">The resulting core that will most likely result in a block</param>
        /// <returns>True if there was a core created, false if all <see cref="_capacity"/> cores are still blocked</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ZeroBlock(out ValueTask<T> slowTaskCore)
        {
            Debug.Assert(WaitCount <= _capacity);

            long idx;
            long cap;

            slowTaskCore = default;

            var retry = _capacity;
            race:
            var origTail = _b_tail;
            var origHead = _b_head;
            Thread.MemoryBarrier();
            if ((idx = _b_head.ZeroNext(cap = origHead >= origTail ? origTail + _capacity: _b_head + _capacity)) != cap)
            {
                var slowCore = _blocking[idx %= ModCapacity];
                slowCore.Prime((short)idx);
                slowTaskCore = new ValueTask<T>(slowCore, (short)idx);

                if (slowCore.Primed)
                {
#if TRACE
                    Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - Fast Path, id = [{idx % ModCapacity}]{idx:00}, status = {slowCore}");
#endif
                    var r = DecReadyCount();
#if TRACE
                    Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] r-- = {r} (S)");
#endif
                }
                else
                {
#if TRACE
                    Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - Blocked,     id = [{idx % ModCapacity}]{idx:00}, status = {slowCore}");
#endif
                }
                return true;
            }
            if(retry-->0)
                goto race;
            
            return false;
        }

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
        /// <returns>A task core that pumps the value when signaled</returns>
        /// <exception cref="InvalidOperationException">When invalid concurrency levels are detected.</exception>
        public ValueTask<T> WaitAsync()
        {
            var retry = 300;
            retry:

            // => slow path
            if (ZeroBlock(out var slowCore))
                return slowCore;
            
            if (retry-- > 0)
            {
                Thread.Yield();
                goto retry;
            }
            
            // => API implementation error
            throw new InvalidOperationException($"{nameof(IoZeroCore<T>)}: Invalid concurrency level detected, check that {_capacity} matches or exceeds expected level of concurrent blockers expected. {Description}");
        }

        int IIoZeroSemaphoreBase<T>.ZeroDecAsyncCount()
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
