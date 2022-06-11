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
    /// Note: This struct does not need to implement <see cref="IIoZeroSemaphoreBase{T}"/>, it does not use the <see cref="IValueTaskSource"/> parts of the interface.
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

            _zeroed = 0;
            _description = description;
            _capacity = capacity;
            capacity *= 2;
            ZeroAsyncMode = zeroAsyncMode;

            _waiters = Channel.CreateBounded<IIoManualResetValueTaskSourceCore<T>>(new BoundedChannelOptions(capacity + 1)
            {
                SingleWriter = false,
                SingleReader = false,
            });

            _results = Channel.CreateBounded<IIoValueCore>(new BoundedChannelOptions(capacity + 1)
            {
                SingleWriter = false,
                SingleReader = false,
            });

            _heapCore = Channel.CreateBounded<IIoManualResetValueTaskSourceCore<T>>(capacity + 1);
            _heapValue = Channel.CreateBounded<IIoValueCore>(capacity + 1);

            _primeReady = _ => default;
            _primeContext = null;
            _asyncTasks = asyncTasks;
            
            _ensureCriticalRegion = (_ready = ready) == 1; //a mutex will always have a 1 here
            _waitersWriteLock = 0;
            _resultsWriteLock = 0;
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

            for (int i = 0; i < _ready; i++)
            {
                _results.Writer.TryWrite(new IoValueCore
                {
                    Kernel = _primeReady!(_primeContext)
                });
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
        private readonly Channel<IIoManualResetValueTaskSourceCore<T>> _waiters;
        private readonly Channel<IIoValueCore> _results;
        private readonly Channel<IIoManualResetValueTaskSourceCore<T>> _heapCore;
        private readonly Channel<IIoValueCore> _heapValue;
        private readonly CancellationTokenSource _asyncTasks;
        private Func<object, T> _primeReady;
        private object _primeContext;
        private readonly string _description;
        private readonly int _capacity;
        private volatile int _zeroed;
        private readonly bool _ensureCriticalRegion;
        private readonly int _ready;
        private volatile int _resultsWriteLock;
        private volatile int _waitersWriteLock;
        #endregion

        #region Properties
        private readonly int ModCapacity => _capacity * 2;

        private const int CoreReady = 0;
        private const int CoreWait  = 1;
        private const int CoreRace  = 2;

        public interface IIoValueCore
        {
            T Kernel { get; set; }

            bool Burned { get; }
            bool Burn();
            void Prime();
            bool Lock();
            IIoValueCore Free();
        }
        internal struct IoValueCore : IIoValueCore
        {
            public T Kernel { get; set; }

            private int _burned;
            public bool Burned => _burned > 0;
            public bool Burn() => Interlocked.CompareExchange(ref _burned, 1, 0) == 0;
            public void Prime() => Interlocked.Exchange(ref _burned, 0);

            private int _heapItemLock;
            public bool Lock() => Interlocked.CompareExchange(ref _heapItemLock, 1,0) == 0;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public IIoValueCore Free()
            {
                Interlocked.Exchange(ref _heapItemLock, 0);
                return this;
            }
        }

        //private IIoZeroSemaphoreBase<T> _zeroRef;
        #endregion

        #region State

        public string Description =>
            $"{nameof(IoZeroSemCore<T>)}: r = {ReadyCount}/{_capacity}, w = {WaitCount}/{_capacity}, z = {_zeroed > 0}, heap = {_heapCore.Reader.Count}, {_description}";
        public int Capacity => _capacity;
        public int WaitCount => _waiters.Reader.Count;
        public int ReadyCount => _results.Reader.Count;
        public bool ZeroAsyncMode { get; }
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
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static bool Unblock(IoZeroCore<T> @this, T value, out int released, bool forceAsync)
            {
                //unblock
                while (@this._waiters.Reader.TryRead(out var waiter))
                {
                    try
                    {
                        waiter.RunContinuationsAsynchronously = forceAsync || @this.ZeroAsyncMode;

                        //wait for the core to become ready
                        while (waiter.Relay == CoreWait)
                        {
                            if (@this.Zeroed())
                            {
                                released = 0;
                                return false;
                            }
                        }

                        overflow:
                        //use the core
                        if (waiter.Relay == CoreReady)
                        {
                            if (@this._results.Reader.Count >= @this.ModCapacity &&
                                @this._results.Reader.TryRead(out var swap))
                            {
                                if (swap.Burn())
                                {
                                    waiter.SetResult(swap.Kernel); //drop overflow
                                    @this._heapValue.Writer.TryWrite(swap.Free());
                                }
                                else
                                    goto overflow;
                            }
                            else
                            {
                                waiter.SetResult(value);
                            }
                            
                            released = 1;
                            return true;
                        }

                        //discard the core
                        waiter.Reset();
                    }
                    catch
                    {
                        // ignored
                    }
                }
                released = 0;
                return false;
            }

        retry:

            //fast track next result, incoming value is dropped
            if (_results.Reader.Count >= ModCapacity && _results.Reader.TryRead(out var fastTracked) && fastTracked.Burn())
            {
                try
                {
                    return Unblock(this, fastTracked.Kernel, out released, false);
                }
                finally
                {
                    _heapValue.Writer.TryWrite(fastTracked.Free());
                }
            }

            //fast path
            if (_results.Reader.Count == 0)
                if (Unblock(this, value, out released, forceAsync)) return true;
            
            //heap
            if (!_heapValue.Reader.TryRead(out var valueCore) || !valueCore.Lock())
                valueCore = new IoValueCore { Kernel = value };
            else
            {
                valueCore.Kernel = value;
                valueCore.Prime();
                Interlocked.MemoryBarrier();
            }

            //TODO: Is this a hack?
            //heap jit 
            if (_results.Reader.Count == 0)
            {
                if (Unblock(this, value, out released, forceAsync))
                {
                    _heapValue.Writer.TryWrite(valueCore.Free());
                    return true;
                }
            }

            ////prime
            while(Interlocked.CompareExchange(ref _resultsWriteLock, 1, 0) != 0 ){}

            try
            {
                if (!_results.Writer.TryWrite(valueCore))
                {
                    released = 0;
                    return false;
                }
            }
            finally
            {
                Interlocked.Exchange(ref _resultsWriteLock, 0);
            }

            //ensure critical region
            if (_ensureCriticalRegion && _waiters.Reader.Count > 0 && _results.Reader.Count > 0)
                goto retry;

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
            while (_waiters.Reader.Count == 0 && _results.Reader.TryRead(out var primedCore) && primedCore.Burn())
            {
                slowTaskCore = new ValueTask<T>(primedCore.Kernel);
                _heapValue.Writer.TryWrite(primedCore.Free());
                return true;
            }

            //heap
            if (!_heapCore.Reader.TryRead(out var waiter) || !waiter.Lock())
            {
                waiter = new IoManualResetValueTaskSourceCore<T> { AutoReset = false };
                waiter.Reset(static state =>
                {
                    var (@this, waiter) = (ValueTuple<IoZeroCore<T>, IIoManualResetValueTaskSourceCore<T>>)state;
                    @this._heapCore.Writer.TryWrite(waiter.Free());
                }, (this, waiter));
            }

            //fast jit
            while (_waiters.Reader.Count == 0 && _results.Reader.TryRead(out var jitCore) && jitCore.Burn())
            {
                slowTaskCore = new ValueTask<T>(jitCore.Kernel);
                _heapValue.Writer.TryWrite(jitCore.Free());
                waiter.Reset();
                return true;
            }

            //block
            waiter.Relay = CoreWait;
            while(Interlocked.CompareExchange(ref _waitersWriteLock, 0, 1) != 0){}

            try
            {
                if (_waiters.Writer.TryWrite(waiter))
                {
                    Interlocked.Exchange(ref _waitersWriteLock, 0);
                    //ensure critical region
                    while (_ensureCriticalRegion && _results.Reader.Count == 1 && _results.Reader.TryRead(out var racedCore) && racedCore.Burn())
                    {
                        waiter.Relay = CoreRace;
                        slowTaskCore = new ValueTask<T>(racedCore.Kernel);
                        _heapValue.Writer.TryWrite(racedCore.Free());
                        return true;
                    }

                    waiter.Relay = CoreReady;
                    slowTaskCore = new ValueTask<T>(waiter, 0);
                    return true;
                }
            }
            finally
            {
                Interlocked.Exchange(ref _waitersWriteLock, 0);
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
        public int Release(T value, bool forceAsync = false) => ZeroSetResult(value, out var release,forceAsync) ? release : 0;
        

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
