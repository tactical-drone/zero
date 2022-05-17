﻿//#define TRACE
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
using zero.core.misc;

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
            if(capacity - 1 > short.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(capacity));

            if(ready > capacity)
                throw new ArgumentOutOfRangeException(nameof(ready));

            _b_head = 0;
            _b_tail = 0;
            _n_head = 0;
            _n_tail = 0;

            _zeroed = 0;
            _description = description;
            _capacity = capacity;
            capacity *= 2;
            ZeroAsyncMode = zeroAsyncMode;

            _blocking = new IIoManualResetValueTaskSourceCore<T>[capacity];
            _nonBlocking = new IIoManualResetValueTaskSourceCore<T>[capacity];

            for (short i = 0; i < capacity; i++)
            {
                var core = _blocking[i] = new IoManualResetValueTaskSourceCore<T>{RunContinuationsAsynchronously = zeroAsyncMode, AutoReset = false};
                core.SetResult(default);
                core.GetResult(default);
                
                core = _nonBlocking[i] = new IoManualResetValueTaskSourceCore<T> {RunContinuationsAsynchronously = zeroAsyncMode, AutoReset = false };
                core.SetResult(default);
                core.GetResult(default);
            }

            _n_tail = ready;

            _primeReady = _ => default;
            _primeContext = null;
        }

        public IIoZeroSemaphoreBase<T> ZeroRef(ref IIoZeroSemaphoreBase<T> @ref, Func<object, T> primeResult = default,
            object context = null)
        {
            _primeReady = primeResult;
            _primeContext = context;

            if (_n_tail > 0 && primeResult == null)
                throw new ArgumentOutOfRangeException(nameof(primeResult));

            for (int i = 0; i < _n_tail; i++)
            {
                var core = _nonBlocking[i];
                core.Reset();
                core.SetResult(_primeReady!(_primeContext));
            }
                

            return @ref;
        }

        public void ZeroSem()
        {
            if (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return;

            ////flush waiters
            while (_b_head < _b_tail)
            {
                try
                {
                    _blocking[(Interlocked.Increment(ref _b_head) - 1) % ModCapacity].SetException(new TaskCanceledException($"{Description}"));
                }
                catch
                {
                    // ignored
                }
            }
        }

        public bool Zeroed() => _zeroed > 0;

        #region Aligned

        private long _b_head;
        private long _b_tail;
        private long _n_head;
        private long _n_tail;
        private readonly IIoManualResetValueTaskSourceCore<T>[] _blocking;
        private readonly IIoManualResetValueTaskSourceCore<T>[] _nonBlocking;
        private readonly int _capacity;
        #endregion

        #region Properties
        //private readonly int _modCapacity => _capacity;
        private readonly int ModCapacity => _capacity<<1;
        private readonly string _description;
        private volatile int _zeroed;
        private Func<object, T> _primeReady;
        private object _primeContext;
        #endregion

        #region State
        public string Description => $"{nameof(IoZeroSemCore<T>)}: {_description}, r = {ReadyCount}, w = {WaitCount}, z = {_zeroed > 0}, b_H = {_b_head % ModCapacity} ({_b_head}), b_T = {_b_tail % ModCapacity} ({_b_tail}), n_H = {_n_head % ModCapacity} ({_n_head}), n_T = {_n_tail % ModCapacity} ({_n_tail})";
        public int Capacity => _capacity;
        public int ReadyCount => (int)(_n_tail - _n_head);
        public int WaitCount => (int)(_b_tail - _b_head);
        public bool ZeroAsyncMode { get; }
        public long Tail => _b_tail;
        public long Head => _b_head;

        #endregion

        #region internal
        /// <summary>
        /// Dequeue a slow core and unblock it using the <see cref="value"/> provided
        /// </summary>
        /// <param name="value">Send this value to the blocker</param>
        /// <param name="released">The number of blockers released with <see cref="value"/></param>
        /// <returns>If a waiter was unblocked, false otherwise</returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private bool ZeroSet(T value, out int released)
        {
            if (WaitCount == 0)
            {
                released = 0;
                return false;
            }

            long cap;
            var idx = _b_head.ZeroNextBounded(cap = _b_tail);
            if (idx != cap)
            {
                Debug.Assert(idx < _b_tail);
                var slowCore = _blocking[idx % ModCapacity];
                
                while (!slowCore.Blocking)
                {
                    Thread.Yield();
                }

                //Debug.Assert(slowCore.Blocking);
                if (slowCore.Blocking)
                {
                    try
                    {
                        Debug.Assert(slowCore.Blocking);
                        slowCore.SetResult(value);
                        released = 1;
                        return true;
                    }
                    catch
                    {
                        // ignored
                    }
                }
                else
                {
                    Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}]: non-blocking set core at idx = {idx%ModCapacity} <- {_b_tail % ModCapacity} <- {_b_head % ModCapacity}: primed = {slowCore.Primed}, blocking = {slowCore.Blocking}, burned = {slowCore.Burned}, {slowCore.GetStatus((short)slowCore.Version)} - {Description}");
                }

                Interlocked.Decrement(ref _b_head);//TODO: What is happening here?
            }

            released = 0;
            return false;
        }

        /// <summary>
        /// Creates a non blocking core primed with<see cref="value"/>
        /// </summary>
        /// <param name="value">The value to cog</param>
        /// <param name="primed">The number of values primed</param>
        /// <returns>True if a value was primed, false otherwise</returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private bool ZeroPrime(T value, out int primed)
        {
            try
            {
                if (ReadyCount == _capacity)
                {
                    primed = 0;
                    return false;
                }
                    
                long cap;
                long idx;
                if ((idx = _n_tail.ZeroNextBounded(cap = _n_head + _capacity)) != cap)
                {
                    Debug.Assert(idx < _n_head + _capacity);
                    
                    var core = _nonBlocking[idx %= ModCapacity];

                    //while (!core.Burned)
                    //{
                    //    if (core.Burned)
                    //    {
                    //        var v = ((IoManualResetValueTaskSourceCore<T>)core).Result;
                            
                    //        Console.WriteLine($"Burned at {Convert.ToInt64(v).ElapsedMs()}ms");
                    //    }
                    //    //Thread.Yield();
                    //}

                    //var a = core.Burned;
                    //Thread.MemoryBarrier();
                    //Debug.Assert(core.Burned || a);
                    core.Reset((short)idx);
                    core.SetResult(value);
#if TRACE
                    Console.WriteLine("P");
#endif
                    primed = 1;
                    return true;
                    
                }
            }
            catch
            {
                // ignored
            }

            primed = 0;
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
            slowTaskCore = default;

            long idx;
            long cap;
            if ((idx = _b_tail.ZeroNextBounded(cap = _b_head + _capacity)) != cap)
            {
                Debug.Assert(idx < _b_head + _capacity);
                var slowCore = _blocking[idx %= ModCapacity];
                if(!slowCore.Burned)
                    LogManager.GetCurrentClassLogger().Error($"[{Thread.CurrentThread.ManagedThreadId}]: [FATAL] unbrunt core at idx = {idx} <- {_b_tail % ModCapacity} <- {_b_head % ModCapacity}: primed = {slowCore.Primed}, blocking = {slowCore.Blocking}, burned = {slowCore.Burned}, {slowCore.GetStatus((short)slowCore.Version)} - {Description}");
                slowCore.Reset((short)idx);
                slowTaskCore = new ValueTask<T>(slowCore, (short)idx);
                return true;

                //Debug.Assert(slowCore.Burned);
                //if (slowCore.Burned)
                //{
                //    slowCore.Reset((short)idx);
                //    slowTaskCore = new ValueTask<T>(slowCore, (short)idx);
                //    return true;
                //}
                //else
                //{
                //    Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}]: unbrunt core at idx = {idx} <- {_b_tail % ModCapacity} <- {_b_head % ModCapacity}: primed = {slowCore.Primed}, blocking = {slowCore.Blocking}, burned = {slowCore.Burned}, {slowCore.GetStatus((short)slowCore.Version)} - {Description}");
                //}

                Interlocked.Decrement(ref _b_tail);//TODO: Why is this happening?
            }

            return false;
        }

        /// <summary>
        /// Dequeue a primed core for non blocking
        /// </summary>
        /// <param name="fastTaskCore">The fast task core dequeued</param>
        /// <returns>True if a core could be dequeued, false otherwise</returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private bool ZeroFastPrime(out ValueTask<T> fastTaskCore)
        {
            fastTaskCore = default;

            if (ReadyCount == 0 || _zeroed > 0)
                return false;

            long idx;
            long cap;
            if ((idx = _n_head.ZeroNextBounded(cap = _n_tail)) != cap)
            {
                Debug.Assert(idx < _n_tail);
                var fastCore = _nonBlocking[idx % ModCapacity];
                while (fastCore.Burned)
                {
                    Console.Write(".");
                    //Thread.Yield();
                }
                Debug.Assert(!fastCore.Burned);
                if (!fastCore.Burned)
                {
                    try
                    {
                        //fastTaskCore = new ValueTask<T>(fastCore.GetResult((short)fastCore.Version));
                        fastTaskCore = new ValueTask<T>(fastCore, (short)fastCore.Version);
#if TRACE
                        Console.WriteLine("C");               
#endif
                        return true;
                    }
                    catch
                    {
                        // ignored
                    }
                }
                else
                {
                    Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}]: brunt primed core at idx = {idx} <- {_b_tail % ModCapacity} <- {_b_head % ModCapacity}: primed = {fastCore.Primed}, blocking = {fastCore.Blocking}, burned = {fastCore.Burned}, {fastCore.GetStatus((short)fastCore.Version)} - {Description}");
                }
            }
#if TRACE
            Console.WriteLine("E");
#endif
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
            if (WaitCount == 0 && ReadyCount >= _capacity)
                return 0;
            try
            {
                if (ZeroSet(value, out var release)) return release;

                if (WaitCount == 0 && ReadyCount >= _capacity)
                    return 0;

                if (ZeroPrime(value, out release)) return release;
            }
            catch
            {
                if (WaitCount == 0 && ReadyCount >= _capacity)
                    return 0;

                if (ZeroPrime(value, out var release)) return release;
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
            if (WaitCount == 0 && ZeroFastPrime(out var fastCore))
            {
#if TRACE
                Console.WriteLine("F");       
#endif
                return fastCore;
            }
                
            
            // => slow path
            if (ZeroBlock(out var slowCore))
            {
#if TRACE
                Console.WriteLine("S");       
#endif
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