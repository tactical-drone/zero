//#define TRACE
using System;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog.LayoutRenderers;

namespace zero.core.patterns.semaphore.core
{
    /// <summary>
    /// A new core semaphore based on learnings from <see cref="IoZeroSemaphore{T}"/> that uses
    /// <see cref="IoManualResetValueTaskSourceCore{TResult}"/> better;
    ///
    /// Status: Passes smoke tests but sometimes glitches out, reason unknown...
    /// </summary>
    /// <typeparam name="T">The type that this primitive marshals</typeparam>
    [StructLayout(LayoutKind.Auto)]
    public struct IoZeroSemCore<T>:IIoZeroSemaphoreBase<T>
    {
        public IoZeroSemCore(string description, int capacity, int ready = 0, bool zeroAsyncMode = false)
        {
            if(capacity > short.MaxValue>>1)
                throw new ArgumentOutOfRangeException(nameof(capacity));

            if(ready > capacity)
                throw new ArgumentOutOfRangeException(nameof(ready));

            _head = 0;
            _tail = 0;
            _zeroed = 0;
            _description = description;
            _capacity = capacity;
            ZeroAsyncMode = zeroAsyncMode;
            
            _manualResetValueTaskSourceCore = new IIoManualResetValueTaskSourceCore<T>[_capacity<<1];

            for (short i = 0; i < _capacity<<1; i++)
            {
                _manualResetValueTaskSourceCore[i] = new IoManualResetValueTaskSourceCore<T>{RunContinuationsAsynchronouslyAlways = zeroAsyncMode, AutoReset = true};
                var core = _manualResetValueTaskSourceCore[i];
                core.Reset(i);
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
                _manualResetValueTaskSourceCore[(Interlocked.Increment(ref _head) - 1) % ModCapacity].SetException(new TaskCanceledException($"{Description}"));
        }

        public bool Zeroed() => _zeroed > 0;

        #region Aligned
        private long _head;
        private long _tail;
        private readonly IIoManualResetValueTaskSourceCore<T>[] _manualResetValueTaskSourceCore;
        private readonly int _capacity;
        #endregion

        #region Properties
        //private readonly int _modCapacity => _capacity;
        private readonly int ModCapacity => _capacity << 1;
        private readonly string _description;
        private volatile int _zeroed;
        private Func<object, T> _primeReady;
        private object _primeContext;
        #endregion

        #region State
        public string Description => $"{nameof(IoZeroSemCore<T>)}: {_description}, r = {ReadyCount}, w = {WaitCount}, z = {_zeroed > 0}, H = {_head % ModCapacity}, T = {_tail % ModCapacity} h = {_head}, t = {_tail}";
        public int Capacity => _capacity;
        public int ReadyCount => (int)(_tail < _head ? _head - _tail : 0);
        public int WaitCount => (int)(_head > _tail ? 0 : _tail - _head);
        public long Head => _head;
        public long Tail => _tail;
        public bool ZeroAsyncMode { get; }

        #endregion

        #region API
        public T GetResult(short token) => throw new NotImplementedException(nameof(GetResult));

        public ValueTaskSourceStatus GetStatus(short token) => throw new NotImplementedException(nameof(GetStatus));

        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => _manualResetValueTaskSourceCore[token].OnCompleted(continuation, state, token, flags);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T value, int releaseCount, bool forceAsync = false)
        {
            var released = 0;
            for (var i = 0; i < releaseCount; i++)
                released += Release(value, forceAsync);
            
            return released;
        }

        
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public int Release(T value, bool forceAsync = false)
        //{
        //    retry_unblock:

        //    if (_head == _tail)
        //        return 0;

        //    var head = _head;
        //    long slot = _head;
        //    if(slot < _tail)
        //    {
        //        race_unblock:
        //        var core = _manualResetValueTaskSourceCore[slot %= ModCapacity];
        //        if (core != null && Interlocked.CompareExchange(ref _manualResetValueTaskSourceCore[slot], null, core) == core)
        //        {
        //            try
        //            {
        //                //Console.WriteLine($"Sending core {core.Version} from {slot} that is {core.GetStatus((short)core.Version)}");
        //                if (core.IsBlocking(false))
        //                {
        //                    Interlocked.Increment(ref _head);
        //                    core.RunContinuationsAsynchronously = ZeroAsyncMode;
        //                    //Console.WriteLine($"Sending core {core.Version} from {slot} that is {core.GetStatus((short)core.Version)}");
        //                    core.SetResult(value);
        //                    return 1;
        //                }

        //                goto retry_unblock;
        //            }
        //            catch
        //            {
        //                Console.WriteLine($"[{Environment.CurrentManagedThreadId}] --> Unblock {slot} [FAILED] - {Description}");
        //                goto retry_unblock;
        //            }
        //            finally
        //            {
        //                //Interlocked.Exchange(ref _manualResetValueTaskSourceCore[slot], core);
        //                _manualResetValueTaskSourceCore[slot] = core;

        //                //Debug.Assert(_manualResetValueTaskSourceCore[slot] != null);
        //            }
        //        }
        //        goto race_unblock; //RACE
        //    }

        //    head = slot = _head;
        //    if(slot < _tail + _capacity)
        //    {
        //        race_reserve:
        //        var core = _manualResetValueTaskSourceCore[slot %= ModCapacity];
        //        if (core != null && Interlocked.CompareExchange(ref _manualResetValueTaskSourceCore[slot], null, core) == core)
        //        {
        //            try
        //            {
        //                if (head > _tail)
        //                {
        //                    var tailCore = _manualResetValueTaskSourceCore[_tail % ModCapacity];
        //                    if (tailCore == null || tailCore.IsBlocking(false))
        //                        goto retry_unblock;
        //                }

        //                Interlocked.Increment(ref _head);
        //                if (!core.IsBlocking(false))
        //                {
        //                    core.Reset((short)slot);
        //                }

        //                core.RunContinuationsAsynchronously = ZeroAsyncMode;
        //                core.SetResult(value);

        //                return 1;
        //            }
        //            catch
        //            {
        //                Console.WriteLine($"[{Environment.CurrentManagedThreadId}] --> Reserving {slot} [FAILED] - {Description}");
        //            }
        //            finally
        //            {
        //                //Interlocked.Exchange(ref _manualResetValueTaskSourceCore[slot], core);
        //                _manualResetValueTaskSourceCore[slot] = core;
        //            }

        //            //Debug.Assert(_manualResetValueTaskSourceCore[slot] != null);
        //        }
        //        goto race_reserve;
        //    }

        //    return 0;
        //}

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
            IIoManualResetValueTaskSourceCore<T> core = null;
            long latch = 0;
            //Race for a core
            try
            {
                
                //var ready = _manualResetValueTaskSourceCore[(_tail - 1) % ModCapacity].Ready;
                long tail;
                var retry = false;
                while ((retry || (latch = _manualResetValueTaskSourceCore[(tail = _tail-1) % ModCapacity].Blocking ? tail : _head) < _tail + _capacity && //take the head
                       (core = _manualResetValueTaskSourceCore[latch %= ModCapacity]) != null) && //race
                       Interlocked.CompareExchange(ref _manualResetValueTaskSourceCore[latch %= ModCapacity], null, core) != core) //reserve
                {
                    if (Zeroed())
                        return 0;
                    retry = true;
                    Thread.Yield();
                }

                if (core == null)
                    return 0;

                if (core.Blocking)
                {
                    try
                    {
                        core.SetResult(value);
                        Interlocked.Increment(ref _tail);
                        return 1;
                    }
                    catch
                    {
                        // ignored
                    }
                }
                else if(!core.Primed)
                    core.Reset((short)latch);
                Interlocked.Increment(ref _head);
            }
            finally
            {
                if(core != null)
                    _manualResetValueTaskSourceCore[latch] = core;
            }
            return 0;
        }

        public ValueTask<T> WaitAsync()
        {
            IIoManualResetValueTaskSourceCore<T> core = null;
            var latch = _tail;
            //Race for a core
            try
            {
                var retry = false;
                while ((retry || (core = _manualResetValueTaskSourceCore[latch %= ModCapacity]) != null) && //race
                       Interlocked.CompareExchange(ref _manualResetValueTaskSourceCore[latch %= ModCapacity], null, core) != core) //reserve)
                {
                    if (Zeroed())
                        return default;

                    //latch = _tail;
                    retry = true;
                }

                if (core == null)
                    return default;

                Interlocked.Increment(ref _tail);
                return new ValueTask<T>(core, (short)latch);
            }
            finally
            {
                if (core != null)
                    _manualResetValueTaskSourceCore[latch] = core;
            }
        }

        //public ValueTask<T> WaitAsync()
        //{
        //    retry:

        //    var slot = _tail;
        //    var slotIdx = slot % ModCapacity;
        //    var core = _manualResetValueTaskSourceCore[slotIdx];
        //    if (core != null &&
        //        Interlocked.CompareExchange(ref _manualResetValueTaskSourceCore[slotIdx], null, core) == core)
        //    {
        //        try
        //        {
        //            Interlocked.Increment(ref _tail);
        //            if (!core.IsBlocking(true))
        //            {
        //                //Interlocked.Increment(ref _head);
        //            }
                    
        //            //_manualResetValueTaskSourceCore[slotIdx] = core;
        //            return new ValueTask<T>(core, (short)slotIdx);
        //            //if (slot < head && !core.Set(false))
        //            //    return new ValueTask<T>(core.GetResult((short)slotIdx));
                    
        //            //else if (slot < head + _capacity)
        //            //{
        //            //    Interlocked.Increment(ref _tail);
        //            //    core.Reset((short)slotIdx);
        //            //    return new ValueTask<T>(core, (short)slotIdx);
        //            //}

        //            throw new InvalidOperationException($"Concurrency bug! {Description}");
        //        }
        //        catch
        //        {
        //            Console.WriteLine($"[{Environment.CurrentManagedThreadId}] --> Fastpath  {slotIdx} [RACE] - {Description}");
        //            //slowPath = true;
        //            //_manualResetValueTaskSourceCore[slotIdx] = core;
        //            goto retry;
        //        }
        //        finally
        //        {
        //            _manualResetValueTaskSourceCore[slotIdx] = core;
        //            //Interlocked.Exchange(ref _manualResetValueTaskSourceCore[slotIdx], core);
        //            //Debug.Assert(_manualResetValueTaskSourceCore[slotIdx] != null);
        //            //_manualResetValueTaskSourceCore[slotIdx] = core;
        //        }
        //    }

        //    goto retry;
        //    //slot = _tail;
        //    //if (slot < _head + _capacity)
        //    //{
        //    //    slot %= ModCapacity;
        //    //    core = _manualResetValueTaskSourceCore[slot];
        //    //    if (core != null &&
        //    //        Interlocked.CompareExchange(ref _manualResetValueTaskSourceCore[slot], null, core) == core)
        //    //    {
        //    //        try
        //    //        {
        //    //            Interlocked.Increment(ref _tail);
        //    //            //<<< SLOW PATH on WAIT
        //    //            Debug.Assert(slot < _head + _capacity);
        //    //            var taskCore = _manualResetValueTaskSourceCore[slot];
        //    //            taskCore.Reset((short)slot);

        //    //            return new ValueTask<T>(taskCore, (short)slot);
        //    //        }
        //    //        catch
        //    //        {
        //    //            goto retry;
        //    //        }
        //    //        finally
        //    //        {
        //    //            _manualResetValueTaskSourceCore[slot] = core;
        //    //        }
        //    //    }
        //    //}

        //    throw new InvalidOperationException($"Concurrency bug! {Description}");
        //}

        int IIoZeroSemaphoreBase<T>.ZeroDecAsyncCount()
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
