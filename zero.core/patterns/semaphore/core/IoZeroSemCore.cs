//#define TRACE
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog.LayoutRenderers;

namespace zero.core.patterns.semaphore.core
{
    /// <summary>
    ///
    /// DEPRECATED!!! (left here for a example of possible implementation of a single Q version of <see cref="IoZeroCore{T}"/>
    /// 
    /// A new core semaphore based on learning from <see cref="IoZeroSemaphore{T}"/> that uses
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
            
            _cores = new IIoManualResetValueTaskSourceCore<T>[_capacity<<1];

            for (short i = 0; i < _capacity<<1; i++)
            {
                _cores[i] = new IoManualResetValueTaskSourceCore<T>{RunContinuationsAsynchronouslyAlways = zeroAsyncMode, AutoReset = true};
                var core = _cores[i];
                core.Reset();
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
                _cores[i].SetResult(_primeReady!(_primeContext));

            return @ref;
        }

        public void ZeroSem()
        {
            if (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return;

            //flush waiters
            while (_head < _tail)
                _cores[(Interlocked.Increment(ref _head) - 1) % ModCapacity].SetException(new TaskCanceledException($"{Description}"));
        }

        public bool Zeroed() => _zeroed > 0;
        public double Cps(bool reset = false) => 0.0;
        

        #region Aligned
        private long _head;
        private long _tail;
        private readonly IIoManualResetValueTaskSourceCore<T>[] _cores;
        private readonly int _capacity;
        #endregion

        #region Properties
        //private readonly int _modCapacity => _capacity;
        private readonly int ModCapacity => _capacity << 1;
        private readonly string _description;
        private int _zeroed;
        private Func<object, T> _primeReady;
        private object _primeContext;
        #endregion

        #region State

        public long TotalOps => 0;
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

        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => _cores[token].OnCompleted(continuation, state, token, flags);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T value, int releaseCount, bool forceAsync = false, bool prime = true)
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
        //        var core = _cores[slot %= ModCapacity];
        //        if (core != null && Interlocked.CompareExchange(ref _cores[slot], null, core) == core)
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
        //                //Interlocked.Exchange(ref _cores[slot], core);
        //                _cores[slot] = core;

        //                //Debug.Assert(_cores[slot] != null);
        //            }
        //        }
        //        goto race_unblock; //RACE
        //    }

        //    head = slot = _head;
        //    if(slot < _tail + _capacity)
        //    {
        //        race_reserve:
        //        var core = _cores[slot %= ModCapacity];
        //        if (core != null && Interlocked.CompareExchange(ref _cores[slot], null, core) == core)
        //        {
        //            try
        //            {
        //                if (head > _tail)
        //                {
        //                    var tailCore = _cores[_tail % ModCapacity];
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
        //                //Interlocked.Exchange(ref _cores[slot], core);
        //                _cores[slot] = core;
        //            }

        //            //Debug.Assert(_cores[slot] != null);
        //        }
        //        goto race_reserve;
        //    }

        //    return 0;
        //}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T[] value, bool forceAsync = false, bool prime = true)
        {
            if (prime && ReadyCount == 0)
                return 0;

            var released = 0;
            foreach (var t in value)
            {
                var r = Release(t, forceAsync);

                if(prime && r == 0)
                    break;

                released += r;
            }
                
            return released;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T value, bool forceAsync = false, bool prime = true)
        {
            IIoManualResetValueTaskSourceCore<T> core = null;
            //Race for a core
            try
            {
                var spinWait = new SpinWait();
                //while ((retry || (latch = _cores[(tail = _tail-1) % ModCapacity].Blocking ? tail : _head) < _tail + _capacity && //take the head //TODO:return the favor
                var latch = 0L;
                while ((latch = _head) < _tail && //dequeue
                       (core = _cores[latch %= ModCapacity]) != null && //race
                       Interlocked.CompareExchange(ref _cores[latch %= ModCapacity], null, core) != core) //reserve
                {
                    if (Zeroed())
                        return 0;

                    spinWait.SpinOnce();
                }

                if (core == null)
                    return 0;

                if (core.Blocking || prime)
                {
                    Interlocked.Increment(ref _head);
                    Interlocked.Exchange(ref _cores[latch], core);
                    try
                    {
                        core.SetResult(value);
                        return 1;
                    }
                    catch
                    {
                        // ignored
                    }
                }
                else
                {
                    Interlocked.Exchange(ref _cores[latch], core);
                }
            }
            finally
            {
                
                    
            }
            return 0;
        }

        public ValueTask<T> WaitAsync()
        {
            IIoManualResetValueTaskSourceCore<T> core = null;
            var latch = 0L;
            //Race for a core
            try
            {
                var spinWait = new SpinWait();
                
                while ((core = _cores[latch = _tail % ModCapacity]) != null && //race
                       Interlocked.CompareExchange(ref _cores[latch], null, core) != core) //reserve)
                {
                    if (Zeroed())
                        return default;

                    spinWait.SpinOnce();
                }

                if (core == null)
                    return default;

                Interlocked.Increment(ref _tail);
                return new ValueTask<T>(core, (short)latch);
            }
            finally
            {
                if (core != null)
                    _cores[latch] = core;
            }
        }

        //public ValueTask<T> WaitAsync()
        //{
        //    retry:

        //    var slot = _tail;
        //    var slotIdx = slot % ModCapacity;
        //    var core = _cores[slotIdx];
        //    if (core != null &&
        //        Interlocked.CompareExchange(ref _cores[slotIdx], null, core) == core)
        //    {
        //        try
        //        {
        //            Interlocked.Increment(ref _tail);
        //            if (!core.IsBlocking(true))
        //            {
        //                //Interlocked.Increment(ref _head);
        //            }
                    
        //            //_cores[slotIdx] = core;
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
        //            //_cores[slotIdx] = core;
        //            goto retry;
        //        }
        //        finally
        //        {
        //            _cores[slotIdx] = core;
        //            //Interlocked.Exchange(ref _cores[slotIdx], core);
        //            //Debug.Assert(_cores[slotIdx] != null);
        //            //_cores[slotIdx] = core;
        //        }
        //    }

        //    goto retry;
        //    //slot = _tail;
        //    //if (slot < _head + _capacity)
        //    //{
        //    //    slot %= ModCapacity;
        //    //    core = _cores[slot];
        //    //    if (core != null &&
        //    //        Interlocked.CompareExchange(ref _cores[slot], null, core) == core)
        //    //    {
        //    //        try
        //    //        {
        //    //            Interlocked.Increment(ref _tail);
        //    //            //<<< SLOW PATH on WAIT
        //    //            Debug.Assert(slot < _head + _capacity);
        //    //            var taskCore = _cores[slot];
        //    //            taskCore.Reset((short)slot);

        //    //            return new ValueTask<T>(taskCore, (short)slot);
        //    //        }
        //    //        catch
        //    //        {
        //    //            goto retry;
        //    //        }
        //    //        finally
        //    //        {
        //    //            _cores[slot] = core;
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
