#define TRACE
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
    /// <see cref="IoManualResetValueTaskSourceCore{TResult}"/> better;
    ///
    /// Status: Passes smoke tests but sometimes glitches out, reason unknown...
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
            _b_cur = 0;
            //_n_head = 0;
            //_n_tail = 0;

            _zeroed = 0;
            _description = description;
            _capacity = capacity;
            capacity *= 2;
            ZeroAsyncMode = zeroAsyncMode;

            _blocking = new IIoManualResetValueTaskSourceCore<T>[capacity];
            //_nonBlocking = new IIoManualResetValueTaskSourceCore<T>[capacity];

            for (short i = 0; i < capacity; i++)
            {
                var core = _blocking[i] = new IoManualResetValueTaskSourceCore<T>
                {
                    RunContinuationsAsynchronously = zeroAsyncMode, 
                    AutoReset = true,
                    _burnResult = (async, state) =>
                    {
                        var @this = (IIoZeroSemaphoreBase<T>)state;

                        if (async)
                        {
#if TRACE
                            Console.WriteLine(
                                $"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - Blocking, (C) ");//id = [{idx % ModCapacity}]{idx:00}, status = {slowCore}");
#endif
                            //Interlocked.Increment(ref _b_tail);

                            @this.DecWaitCount();
                            Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] w = {@this.WaitCount} (C)");
                        }
                        else

                        {
#if TRACE
                            Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - Non-Blocking, (C)"); //id = [{idx % ModCapacity}]{idx:00}, status = {slowCore}");
#endif
                            //@this.IncCur();
                            @this.DecReadyCount();
                        }
                    }
                };
                //core.SetResult(default);
                //core.GetResult(default);
                
                //core = _nonBlocking[i] = new IoManualResetValueTaskSourceCore<T> {RunContinuationsAsynchronously = zeroAsyncMode, AutoReset = false };
                //core.SetResult(default);
                //core.GetResult(default);
            }

            //_n_tail = ready;
            //_readyCount = (int)(_b_tail = ready);
            _waitCount = 0;
            _readyCount = ready;

            _primeReady = _ => default;
            _primeContext = null;
            _zeroRef = null;
        }

        public IIoZeroSemaphoreBase<T> ZeroRef(ref IIoZeroSemaphoreBase<T> @ref, Func<object, T> primeResult = default,
            object context = null)
        {
            for (int i = 0; i < _capacity; i++)
                _nonBlocking[i].BurnContext = @ref;

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
                var core = _nonBlocking[i];
                core.SetResult(_primeReady!(_primeContext));
            }

            _readyCount = origPrime;
            return _zeroRef = @ref;
        }

        public void ZeroSem()
        {
            if (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return;

            var operationCanceledException = new OperationCanceledException($"{nameof(ZeroSem)}: [TEARDOWN DIRECT] {Description}");

            ////flush waiters
            //while (_b_head < _b_tail)
            for (var i = _b_head; i < _capacity; i++)
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
        }

        public bool Zeroed() => _zeroed > 0;
        public void DecWaitCount()
        {
            Interlocked.Decrement(ref _waitCount);
        }

        public void IncWaitCount()
        {
            Interlocked.Increment(ref _waitCount);
        }

        public void IncReadyCount()
        {
            Interlocked.Increment(ref _readyCount);
        }

        public void DecReadyCount()
        {
            Interlocked.Decrement(ref _readyCount);
        }

        public void IncCur()
        {
            Interlocked.Increment(ref _b_cur);
        }

        #region Aligned

        private long _b_head;
        private long _b_tail;
        private long _b_cur;
        //private long _n_head => _b_head;
        //private long _n_tail => _b_tail;
        private readonly IIoManualResetValueTaskSourceCore<T>[] _blocking;
        private readonly IIoManualResetValueTaskSourceCore<T>[] _nonBlocking => _blocking;
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
            $"{nameof(IoZeroSemCore<T>)}: {_description}, r = {_readyCount}/{_capacity}, w = {_waitCount}/{_capacity}, z = {_zeroed > 0}, b_H = {_b_head % ModCapacity} ({_b_head}),"; //b_T = {_b_tail % ModCapacity} ({_b_tail}), n_H = {_n_head % ModCapacity} ({_n_head}), n_T = {_n_tail % ModCapacity} ({_n_tail})";
        public int Capacity => _capacity;
        //public int ReadyCount => (int)(_n_tail - _n_head);
        public int ReadyCount => _readyCount;
        public volatile int _readyCount;
        //public int WaitCount => (int)(_b_tail - _b_head);
        public int WaitCount => _waitCount;
        public volatile int _waitCount;
        
        public bool ZeroAsyncMode { get; }
        public long Tail => _b_head;
        public long Head => _b_head;

        #endregion

        #region internal

        /// <summary>
        /// Dequeue a slow core and unblock it using the <see cref="value"/> provided
        /// </summary>
        /// <param name="value">Send this value to the blocker</param>
        /// <param name="released">The number of blockers released with <see cref="value"/></param>
        /// <param name="forceAsync"></param>
        /// <returns>If a waiter was unblocked, false otherwise</returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private bool ZeroSetResult(T value, out int released, bool forceAsync = false)
        {
            var prefire = _blocking[_b_cur % ModCapacity];
            if (ReadyCount >= _capacity && !prefire.Blocking)
            {
#if TRACE
                Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - id =[{_b_cur % ModCapacity}]{_b_cur} - [IDLE]");
#endif
                released = 0;
                return false;
            }

            long cap;

            //var idx = _b_cur.ZeroNext(cap = _b_tail > _b_cur? _b_tail : _b_cur + 1);
            var idx = _b_tail.ZeroNext(cap = _b_head + _capacity);
            if (idx != cap)
            {
                //Interlocked.Increment(ref _b_head);
                //Interlocked.Increment(ref _b_tail);
                
                //Debug.Assert(idx < _b_tail);
                var slowCore = _blocking[idx % ModCapacity];
                //Debug.Assert(idx <= _b_tail);

                if (slowCore.Blocking)
                {
                    Interlocked.Decrement(ref _waitCount);
                    slowCore.SetResult(value);

                    released = 1;
                    Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - Released, (P) ");//id = [{idx%ModCapacity}]{idx:00}, status = {slowCore}");
                    return true;
                }

                slowCore.SetResult(value, static (wasBlocking, @this) =>
                {
                    if (wasBlocking)
                    {
                        @this.DecWaitCount();
                        Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] w = {@this.WaitCount} (S) ");
#if TRACE
                        Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - Released,     ");//id = [{idx%ModCapacity}]{idx:00}, status = {slowCore}");
#endif                  
                    }
                    else
                    {
                        @this.IncReadyCount();
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
        [MethodImpl(MethodImplOptions.NoInlining)]
        private bool ZeroBlock(out ValueTask<T> slowTaskCore)
        {
            
            if (WaitCount >= _capacity)
                throw new InvalidOperationException($"{nameof(ZeroBlock)}: Invalid concurrency detected, increase sizeof capacity {Description}");

            Debug.Assert(WaitCount <= _capacity);

            long idx;
            long cap;

            slowTaskCore = default;

            //            if (WaitCount == 0 && ReadyCount == 1)
            //            {
            //                if ((idx = _b_cur.ZeroNext(cap = _b_cur + _capacity)) != cap)
            //                {
            //                    Interlocked.Increment(ref _b_tail);

            //                    //Debug.Assert(idx < _b_head + _capacity);
            //                    var slowCore = _blocking[idx %= ModCapacity];
            //                    slowCore.Prime((short)idx);
            //                    slowTaskCore = new ValueTask<T>(slowCore, (short)idx);

            //                    if (slowCore.Primed)
            //                    {
            //#if TRACE
            //                        Console.WriteLine(
            //                            $"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - Non-Blocking, id = [{idx % ModCapacity}]{idx:00}, status = {slowCore}");
            //#endif
            //                        Interlocked.Increment(ref _b_cur);
            //                        Interlocked.Decrement(ref _readyCount);
            //                    }
            //                    else
            //                    {
            //#if TRACE
            //                        Console.WriteLine(
            //                            $"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - Blocking,     id = [{idx % ModCapacity}]{idx:00}, status = {slowCore}");
            //#endif
            //                        //Interlocked.Increment(ref _b_tail);
            //                        Interlocked.Increment(ref _waitCount);
            //                        Console.WriteLine(
            //                            $"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] w = {_waitCount} (B)");
            //                    }

            //                    return true;
            //                }
            //            }


            //if ((idx = _b_cur.ZeroNext(cap = _b_head > _b_cur ? _b_head : _b_cur + 1)) != cap)
            if ((idx = _b_head.ZeroNext(cap = _b_head + _capacity)) != cap)
            {
                //Interlocked.Increment(ref _b_tail);

                //Debug.Assert(idx < _b_head + _capacity);
                var slowCore = _blocking[idx %= ModCapacity];
                slowCore.Prime((short)idx);
                slowTaskCore = new ValueTask<T>(slowCore, (short)idx);

                if (slowCore.Primed)
                {
#if TRACE
                    Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - Non-Blocking, id = [{idx % ModCapacity}]{idx:00}, status = {slowCore}");
#endif
                    //Interlocked.Increment(ref _b_cur);
                    Interlocked.Decrement(ref _readyCount);
                }
//                else
//                {
//#if TRACE
//                    Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - Blocking,     id = [{idx % ModCapacity}]{idx:00}, status = {slowCore}");
//#endif
//                    //Interlocked.Increment(ref _b_tail);
//                    Interlocked.Increment(ref _waitCount);
//                    Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] w = {_waitCount} (B)");
//                }
                return true;
//                if (slowCore.Primed)
//                {
//#if TRACE
//                    Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - Non-Blocking, id = [{idx % ModCapacity}]{idx:00}, status = {slowCore}");
//#endif
//                    Interlocked.Increment(ref _b_cur);
//                    Interlocked.Decrement(ref _readyCount);
//                }

                ////                else
                ////                {
                ////#if TRACE
                ////                    Console.WriteLine($"<{Environment.TickCount}>[{Thread.CurrentThread.ManagedThreadId:00}] - Blocking,     id = [{idx % ModCapacity}]{idx:00}, status = {slowCore}");
                ////#endif
                ////                    //Interlocked.Increment(ref _b_tail);
                ////                    Interlocked.Increment(ref _waitCount);
                ////                }


                //                return true;
            }

            return false;
        }

#endregion

#region API
        public T GetResult(short token) => throw new NotImplementedException(nameof(GetResult));

        public ValueTaskSourceStatus GetStatus(short token) => throw new NotImplementedException(nameof(GetStatus));

        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => throw new NotImplementedException(nameof(OnCompleted));

        [MethodImpl(MethodImplOptions.NoInlining)]
        public int Release(T value, int releaseCount, bool forceAsync = false)
        {
            var released = 0;
            for (var i = 0; i < releaseCount; i++)
                released += Release(value, forceAsync);
            
            return released;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public int Release(T[] value, bool forceAsync = false)
        {
            var released = 0;
            foreach (var t in value)
                released += Release(t, forceAsync);
            return released;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public int Release(T value, bool forceAsync = false)
        {
            //if (!forceAsync && _waitCount == 0 && _readyCount > _capacity)
            //    return 0;
            try
            {
                if (ZeroSetResult(value, out var release)) return release;

                //if (!forceAsync && _waitCount == 0 && _readyCount > _capacity)
                //    return 0;

                //if (ZeroPrime(value, out var release)){return release;}
            }
            catch
            {
                //if (!forceAsync && _waitCount == 0 && _readyCount > _capacity)
                //    return 0;

                //if (ZeroPrime(value, out var release)) return release;
            }
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
            // => fast path
            //            if (_waitCount == 0 && ZeroFastPrime(out var fastCore))
            //            {
            //#if TRACE
            //                Console.WriteLine("F");       
            //#endif
            //                return fastCore;
            //            }


            // => slow path
            if (ZeroBlock(out var slowCore))
            {
                return slowCore;
            }

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
