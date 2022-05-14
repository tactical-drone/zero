﻿//#define TRACE
using System;
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
    /// <see cref="IoManualResetValueTaskSourceCore{TResult}"/> better;
    ///
    /// Status: Passes smoke tests but sometimes glitches out, reason unknown...
    /// </summary>
    /// <typeparam name="T">The type that this primitive marshals</typeparam>
    [StructLayout(LayoutKind.Sequential, Pack = 64)]
    public struct IoZeroSemCore<T>:IIoZeroSemaphoreBase<T>
    {
        public IoZeroSemCore(string description, int capacity, int ready = 0, bool zeroAsyncMode = false)
        {
            if(capacity > short.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(capacity));

            if(ready > capacity)
                throw new ArgumentOutOfRangeException(nameof(ready));

            _head = 0;
            _tail = 0;
            _zeroed = 0;
            _description = description;
            _capacity = capacity;
            ZeroAsyncMode = zeroAsyncMode;
            
            _manualResetValueTaskSourceCore = new IIoManualResetValueTaskSourceCore<T>[_capacity];

            for (int i = 0; i < _capacity; i++)
            {
                _manualResetValueTaskSourceCore[i] = new IoManualResetValueTaskSourceCore<T>{RunContinuationsAsynchronously = zeroAsyncMode};
                _manualResetValueTaskSourceCore[i].Reset((short)i);
            }

            _head = ready;

            _primeReady = _ => default;
            _primeContext = null;
        }

        public IIoZeroSemaphoreBase<T> ZeroRef(ref IIoZeroSemaphoreBase<T> @ref, Func<object, T> primeResult = default,
            object context = null)
        {
            _primeReady = primeResult;
            _primeContext = context;

            if (_head > 0 && primeResult == null)
                throw new ArgumentOutOfRangeException(nameof(primeResult));

            for (int i = 0; i < _head; i++)
                _manualResetValueTaskSourceCore[i].SetResult(_primeReady!(_primeContext));

            return @ref;
        }

        public void ZeroSem()
        {
            if (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return;

            //flush waiters
            while (_head < _tail)
                _manualResetValueTaskSourceCore[(Interlocked.Increment(ref _head) - 1) % _capacity].SetException(new TaskCanceledException($"{Description}"));
        }

        public bool Zeroed() => _zeroed > 0;

        #region Aligned
        private long _head;
        private long _tail;
        private readonly int _capacity;
        #endregion

        #region Properties
        //private IIoZeroSemaphoreBase<T> _zeroRef;
        private readonly string _description;
        private volatile int _zeroed;
        private readonly IIoManualResetValueTaskSourceCore<T>[] _manualResetValueTaskSourceCore;
        private Func<object, T> _primeReady;
        private object _primeContext;
        #endregion

        #region State
        public string Description => $"{nameof(IoZeroSemCore<T>)}: {_description}, r = {ReadyCount}, w = {WaitCount}, z = {_zeroed > 0}, H = {_head % _capacity}, T = {_tail % Capacity} h = {_head}, t = {_tail}";
        public int Capacity => _capacity;
        public int ReadyCount => (int)(_head - _tail);
        public int WaitCount => (int)(_head > _tail ? 0 : _tail - _head);
        public long Head => _head;
        public long Tail => _tail;
        public bool ZeroAsyncMode { get; }

        #endregion

        #region API
        public T GetResult(short token) => _manualResetValueTaskSourceCore[token].GetResult(token);

        public ValueTaskSourceStatus GetStatus(short token) => _manualResetValueTaskSourceCore[token].GetStatus(token);

        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => _manualResetValueTaskSourceCore[token].OnCompleted(continuation, state, token, flags);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T value, int releaseCount, bool forceAsync = false)
        {
            var released = 0;
            for (var i = 0; i < releaseCount; i++)
                released += Release(value, forceAsync);
            
            return released;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T value, bool forceAsync = false)
        {
            if (ReadyCount >= _capacity)
                return 0;

            long slot = -1;
            long latch;
            var cap = 0;

            //release a waiter
            reserve:
            while ((latch = _head) < _tail + cap && (slot = Interlocked.CompareExchange(ref _head, latch + 1, latch)) != latch)
            {
                if (Zeroed())
                    return default;

                slot = -1;
            }

            if (slot == latch)
            {
                try
                {
                    slot %= _capacity;
                    var core = _manualResetValueTaskSourceCore[slot];
                    if (core.Set(false))
                    {
                        core.RunContinuationsAsynchronously = forceAsync;
                        if(cap > 0)
                            core.Reset((short)slot);
                        core.SetResult(value);
                        return 1;
                    }
                }
                catch
                {
                    Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}] --> Releasing {slot} [FAILED] - {Description}");
                    return 0;
                }
            }

            //cog a release
            if (cap == 0)
            {
                cap = _capacity;
                goto reserve;
            }
            return 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T[] value, bool forceAsync = false)
        {
            var released = 0;
            foreach (var t in value)
                released += Release(t, forceAsync);
            return released;
        }

        public ValueTask<T> WaitAsync()
        {
            long slot = -1;
            long latch;

            //Check if there are cocked signals
            while ((latch = _head) < _tail &&
                   (slot = Interlocked.CompareExchange(ref _head, latch + 1, latch)) != latch)
            {
                if (Zeroed())
                    return default;

                slot = -1;
            }

            //>>> FAST PATH on cocked
            if (slot == latch)
                return new ValueTask<T>(GetResult((short)(slot % _capacity)));

            //<<< SLOW PATH on WAIT
            var tailIdx = Interlocked.Increment(ref _tail) - 1;
            Debug.Assert(tailIdx < _head + _capacity);
            var taskCore = _manualResetValueTaskSourceCore[slot = tailIdx % _capacity];
            taskCore.Reset((short)slot);
            
            return new ValueTask<T>(taskCore, (short)slot);
        }

        int IIoZeroSemaphoreBase<T>.ZeroDecAsyncCount()
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
