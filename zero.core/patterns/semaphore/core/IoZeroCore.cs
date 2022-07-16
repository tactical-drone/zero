//#define TRACE
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
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

            _waiters = new IoZeroQ<IIoManualResetValueTaskSourceCore<T>>(string.Empty, capacity, false, asyncTasks:null, capacity, false);

            _results = new IoZeroQ<T>(string.Empty, capacity, false, asyncTasks: null, capacity, false);
            //_heapCore = new IoZeroQ<IIoManualResetValueTaskSourceCore<T>>(string.Empty, capacity, false, asyncTasks: null, capacity, false);
            //_waiters = Channel.CreateBounded<IIoManualResetValueTaskSourceCore<T>>(new BoundedChannelOptions(capacity)
            //{
            //    SingleWriter = false,
            //    SingleReader = false,
            //    AllowSynchronousContinuations = true,
            //    FullMode = BoundedChannelFullMode.DropWrite
            //});

            //_results = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
            //{
            //    SingleWriter = false,
            //    SingleReader = false,
            //    AllowSynchronousContinuations = true,
            //    FullMode = BoundedChannelFullMode.DropWrite
            //});

            _heapCore = Channel.CreateBounded<IIoManualResetValueTaskSourceCore<T>>(new BoundedChannelOptions(capacity)
            {
                SingleWriter = false,
                SingleReader = false,
                AllowSynchronousContinuations = true,
                FullMode = BoundedChannelFullMode.DropWrite
            });

            _primeReady = _ => default;
            _primeContext = null;
            _asyncTasks = asyncTasks;
            
            _ensureCriticalRegion = (_ready = ready) == 1; //a mutex will always have a 1 here
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
                //_results.Writer.TryWrite(_primeReady!(_primeContext));
                _results.TryEnqueue(_primeReady!(_primeContext));
            }

            //return _zeroRef = @ref;
            return @ref;
        }

        public void ZeroSem()
        {
            if (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return;

            //while (_waiters.Reader.TryRead(out var cancelled))
            while (_waiters.TryDequeue(out var cancelled))
            {
                try
                {
                    cancelled.RunContinuationsAsynchronouslyAlways = false;
                    cancelled.RunContinuationsAsynchronously = false;
                    cancelled.SetException(new TaskCanceledException($"{nameof(ZeroSem)}: [TEARDOWN DIRECT] {Description}"));
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
        //private readonly Channel<IIoManualResetValueTaskSourceCore<T>> _waiters;
        //private readonly Channel<T> _results;
        private readonly Channel<IIoManualResetValueTaskSourceCore<T>> _heapCore;
        private readonly IoZeroQ<IIoManualResetValueTaskSourceCore<T>> _waiters;
        private readonly IoZeroQ<T> _results;
        //private readonly IoZeroQ<IIoManualResetValueTaskSourceCore<T>> _heapCore;
        private readonly CancellationTokenSource _asyncTasks;
        private Func<object, T> _primeReady;
        private object _primeContext;
        private readonly string _description;
        private readonly int _capacity;
        private volatile int _zeroed;
        private readonly int _ready;
        private readonly bool _ensureCriticalRegion;
        #endregion

        #region Properties
        private readonly int ModCapacity => _capacity * 2;

        private const int CoreReady = 0;
        private const int CoreWait  = 1;
        private const int CoreRace  = 2;

        //private IIoZeroSemaphoreBase<T> _zeroRef;
        #endregion

        #region State

        //public string Description => $"{nameof(IoZeroSemCore<T>)}: r = {ReadyCount}/{_capacity}, w = {WaitCount}/{_capacity}, z = {_zeroed > 0}, heap = {_heapCore.Reader.Count}, {_description}";
        //public int WaitCount => _waiters.Reader.Count;
        //public int ReadyCount => _results.Reader.Count;
        public string Description => $"{nameof(IoZeroSemCore<T>)}: r = {ReadyCount}/{_capacity}, w = {WaitCount}/{_capacity}, z = {_zeroed > 0}, heap = {_heapCore.Reader.Count}, {_description}";
        public int WaitCount => (int)_waiters.Count;
        public int ReadyCount => (int)_results.Count;

        public int Capacity => _capacity;
        public bool ZeroAsyncMode { get; }
        #endregion

        #region Core
        /// <summary>
        /// Unblocks a core
        /// </summary>
        /// <param name="value">The value to unblock with</param>
        /// <param name="forceAsync">To continue asynchronously</param>
        /// <returns>True on success</returns>
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private bool Unblock(T value, bool forceAsync)
        {
            retry:
            //unblock
            //if (!_waiters.Reader.TryRead(out var waiter)) return false;
            if (!_waiters.TryDequeue(out var waiter)) return false;

            waiter.RunContinuationsAsynchronously = forceAsync || ZeroAsyncMode;

            //wait for the core to become ready
            while (waiter.Relay == CoreWait)
            {
                if (!Zeroed()) continue;
                return false;
            }

            switch (waiter.Relay)
            {
                //use the core
                case CoreReady:
                    waiter.SetResult(value);
                    return true;
                case CoreRace:
                    goto retry;
            }
            return false;
        }

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
        private bool SetResult(T value, out int released, bool forceAsync = false)
        {
            bool banked;
            released = 0;

            //while ((banked = _results.Reader.Count < ModCapacity) && !_results.Writer.TryWrite(value)){}
            while ((banked = _results.Count < Capacity) && _results.TryEnqueue(value) < 0) { }

            //drain the Q
            //while (_waiters.Reader.Count > 0 && _results.Reader.TryRead(out var fastTracked))
            while (_waiters.Count > 0 && _results.TryDequeue(out var fastTracked))
            {
                if (Unblock(fastTracked, forceAsync))
                {
                    released++;
                    if(_waiters.Count < Capacity)
                        break;
                }
                else
                    _results.TryEnqueue(fastTracked);
            }

            //downstream mechanics require there be a 1 if either released or unblocked
            if (released == 0 && banked)
            {
                released = 1;
                return true;
            }
            
            return released > 0;
        }

        int iteration = 0;

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
        private bool Block(out ValueTask<T> slowTaskCore)
        {
            Debug.Assert(Zeroed() || WaitCount <= ModCapacity);

            slowTaskCore = default;
            //if (++iteration >= 2)
            //{
            //    Console.WriteLine($"{iteration}");
            //}
            //fast path
            if (_waiters.Count == 0 && _results.TryDequeue(out var primedCore))
            {
                slowTaskCore = new ValueTask<T>(primedCore);
                return true;
            }

            //heap
            if (!_heapCore.Reader.TryRead(out var waiter))
            {
                waiter = new IoManualResetValueTaskSourceCore<T> { AutoReset = false };
                waiter.Reset(static state =>
                {
                    var (@this, waiter) = (ValueTuple<IoZeroCore<T>, IIoManualResetValueTaskSourceCore<T>>)state;
                    @this._heapCore.Writer.TryWrite(waiter);
                }, (this, waiter));
            }

            //fast jit
            if (_waiters.Count == 0 && _results.TryDequeue(out var jitCore))
            {
                slowTaskCore = new ValueTask<T>(jitCore);
                _heapCore.Writer.TryWrite(waiter);
                return true;
            }

            waiter.Relay = _ensureCriticalRegion? CoreWait : CoreReady;

            //block
            if (_waiters.TryEnqueue(waiter) < 0) 
                return false;

            //ensure critical region
            if (_ensureCriticalRegion && _results.Count == 1 && _results.TryDequeue(out var racedCore))
            {
                waiter.Relay = CoreRace;
                slowTaskCore = new ValueTask<T>(racedCore);
                return true;
            }

            //prime blocking core
            waiter.Relay = CoreReady;
            slowTaskCore = new ValueTask<T>(waiter, 0);
            return true;
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
        public int Release(T value, bool forceAsync = false) => SetResult(value, out var release,forceAsync) ? release : 0;
        

        /// <summary>
        /// Wait for a signal
        /// </summary>
        /// <returns>A value task core that pumps values when signaled</returns>
        /// <exception cref="InvalidOperationException">When invalid concurrency levels are detected.</exception>
        public ValueTask<T> WaitAsync()
        {
            // => slow core
            if (!Zeroed() && Block(out var slowCore))
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
