//#define TRACE
using System;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
using NLog.Filters;
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
    /// <typeparam name="T">The type that this semaphore marshals</typeparam>
    [StructLayout(LayoutKind.Auto)]
    public struct IoZeroCore<T>:IIoZeroSemaphoreBase<T>
    {
        #region Memory management
        public IoZeroCore(string description, int capacity, CancellationTokenSource asyncTasks, int ready = 0, bool zeroAsyncMode = false)
        {
            if(ready > capacity)
                throw new ArgumentOutOfRangeException(nameof(ready));

            _zeroed = 0;
            _description = description;
            _capacity = capacity;
            ZeroAsyncMode = zeroAsyncMode;

            _blockingCores = new IoZeroQ<IIoManualResetValueTaskSourceCore<T>>(string.Empty, capacity + 1, false, asyncTasks:null, capacity);
            _results = new IoZeroQ<T>(string.Empty, capacity + 1, false, asyncTasks: null, capacity);
            _heapCore = new IoBag<IIoManualResetValueTaskSourceCore<T>>(string.Empty, capacity + 2, asyncTasks: null, capacity);

            _primeReady = _ => default;
            _primeContext = null;
            _asyncTasks = asyncTasks;
            
            _ensureMutex = (_ready = ready) == 1; //a mutex will always have a 1 here

            _lifoQ = default;
            _racedResultSyncRoot = 0;
        }

        public IIoZeroSemaphoreBase<T> ZeroRef(ref IIoZeroSemaphoreBase<T> @ref, Func<object, T> primeResult = default,
            object context = null)
        {
            if (@ref == null)
                throw new ArgumentNullException(nameof(@ref));

            _primeReady = primeResult;
            _primeContext = context;

            for (int i = 0; i < _ready; i++)
            {
                if (_results.TryEnqueue(_primeReady!(_primeContext)) < 0)
                    throw new OutOfMemoryException($"{nameof(ZeroRef)}: {_results.Description}");
            }

            return @ref;
        }

        public void ZeroSem()
        {
            if (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return;

            while (_blockingCores.TryDequeue(out var cancelled))
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
        private readonly IoZeroQ<IIoManualResetValueTaskSourceCore<T>> _blockingCores;
        private readonly IoZeroQ<T> _results;
        private int _racedResultSyncRoot;
        private T _lifoQ;
        private readonly IoBag<IIoManualResetValueTaskSourceCore<T>> _heapCore;
        private readonly CancellationTokenSource _asyncTasks;
        private Func<object, T> _primeReady;
        private object _primeContext;
        private readonly string _description;
        private readonly int _capacity;
        private int _zeroed;
        private readonly int _ready;
        private readonly bool _ensureMutex;
        #endregion

        #region Properties
        private const int SyncReady = 0;
        private const int SyncWait  = 1;
        private const int SyncRace  = 2;
        #endregion

        #region State
        public string Description => $"{nameof(IoZeroSemCore<T>)}: r = {ReadyCount}/{_capacity}, w = {WaitCount}/{_capacity}, z = {_zeroed > 0}, heap = {_heapCore.Count}, {_description}";
        public int WaitCount => _blockingCores.Count;
        public int ReadyCount => _results.Count;

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

            //fetch a blocking core from the Q
            var spinWait = new SpinWait();
            IIoManualResetValueTaskSourceCore<T> blockingCore;
            while (!_blockingCores.TryDequeue(out blockingCore))
            {
                if (Zeroed())
                    return false;

                if (_blockingCores.Count == 0)
                    return false;

                spinWait.SpinOnce();
            }

            //wait for the blocking core to synchronize
            while (blockingCore.SyncRoot == SyncWait)
            {
                if (Zeroed())
                    return false;
                spinWait.SpinOnce();
            }

            switch (blockingCore.SyncRoot)
            {
                case SyncReady://use the core
                    blockingCore.RunContinuationsAsynchronously = forceAsync;
                    blockingCore.SetResult(value);
                    return true;
                case SyncRace://discard the core (also from the heap)
                    goto retry;
            }

            return false;
        }


        /// <summary>
        /// Dequeue a slow core and unblock it using the <see cref="value"/> provided
        /// </summary>
        /// <param name="value">Send this value to the blocker</param>
        /// <param name="forceAsync"></param>
        /// <returns>If a waiter was unblocked, false otherwise</returns>
#if RELEASE
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private bool SetResult(T value, bool forceAsync = false)
        {
            var unBanked = false;

            //insane checks
            if (_results.Count >= Capacity && _blockingCores.Count == 0) //TODO: if the semaphore is full, but there are waiters hanging around... unblock one. Seems legit
                return false;

            //try fast path
            if (!(_results.Count == 0 && _racedResultSyncRoot == 0 && Unblock(value, forceAsync)))
            {
                //slow path
                unBanked = _results.TryEnqueue(value) < 0;

                //track overflow
                if (unBanked)
                {
                    if (Zeroed())
                        return false;

                    if (_blockingCores.Count == 0)
                        return false; //TODO: Overflow should we throw instead?
                }

                var spinWait = new SpinWait();
                var lifo = false;
                T nextResult = default;

                //poll LIFO Q - This Q allows us to split the result and blocking queues in 2, lessening the lock contention rates
                if (_racedResultSyncRoot > 0)
                {
                    while (Interlocked.CompareExchange(ref _racedResultSyncRoot, 0, 2) != 2)
                    {
                        if (Zeroed())
                            return false;

                        spinWait.SpinOnce();
                    }
                    lifo = true;
                    nextResult = _lifoQ;
                }

                //Attempt to unblock a waiter (using possible lifo Q result)
                retryOnRace:
                if (_blockingCores.Count > 0 && (lifo || _results.TryDequeue(out nextResult)))
                {
                    if (!Unblock(nextResult, forceAsync))
                    {
                        lifo = true;
                        goto retryOnRace;
                    }

                    lifo = false;
                }

                //did we race and loose? Our main Q cannot LIFO, so we create a small (size 1) LIFO Q here. 
                if (lifo)
                {
                    while (Interlocked.CompareExchange(ref _racedResultSyncRoot, 1, 0) != 0)
                    {
                        if (Zeroed())
                            return false;
                        spinWait.SpinOnce();
                    }

                    //fast track next result
                    _lifoQ = nextResult;
                    Interlocked.Exchange(ref _racedResultSyncRoot, 2);
                }
            }
            
            return !unBanked;
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
        private bool Block(out ValueTask<T> slowTaskCore)
        {
            //fast path
            if (_blockingCores.Count == 0 && _results.TryDequeue(out var readyResult))
            {
                slowTaskCore = new ValueTask<T>(readyResult);
                return true;
            }

            //prepare a core from the heap
            if (!_heapCore.TryDequeue(out var blockingCore))
            {
                blockingCore = new IoManualResetValueTaskSourceCore<T> { AutoReset = true, RunContinuationsAsynchronouslyAlways = ZeroAsyncMode};
                blockingCore.Reset(static state =>
                {
                    var (@this, blockingCore) = (ValueTuple<IoZeroCore<T>, IIoManualResetValueTaskSourceCore<T>>)state;
                    if (@this._heapCore.TryEnqueue(blockingCore) < 0)
                        throw new InvalidOperationException($"{nameof(@this.Block)}: unable to return memory to heap; {@this._heapCore.Description}");
                }, (this, blockingCore));
            }

            Debug.Assert(blockingCore != null);

            blockingCore.SyncRoot = _ensureMutex? SyncWait : SyncReady;

            //prepare to synchronize with release
            var spinWait = new SpinWait();
            while (_blockingCores.TryEnqueue(blockingCore) < 0)
            {
                Debug.Assert(Zeroed() || WaitCount <= Capacity);

                if (Zeroed() || WaitCount >= Capacity)
                {
                    slowTaskCore = default;
                    return false;
                }
                
                spinWait.SpinOnce();
            }

            if (_ensureMutex)
            {
                //ensure critical region - last ditched attempt to access the fast path on racing result
                if ((_results.Count <= 1 || _results.Count == Capacity) && _results.TryDequeue(out var racedResult))
                {
                    blockingCore.SyncRoot = SyncRace;
                    slowTaskCore = new ValueTask<T>(racedResult);
                    return true;
                }

                //block the core by synchronizing with release
                blockingCore.SyncRoot = SyncReady;
            }
            //else if(_results.Count >= Capacity && _blockingCores.Count >= Capacity && _results.TryDequeue(out var saturatedResult)) //unstuck full semaphore
            //{
            //    blockingCore.SyncRoot = SyncRace;
            //    slowTaskCore = new ValueTask<T>(saturatedResult);
            //    return true;
            //}
            
            //No results pending, we block
            slowTaskCore = new ValueTask<T>(blockingCore, 0);
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
        public int Release(T value, bool forceAsync = false) => SetResult(value,forceAsync) ? 1 : 0;
        

        /// <summary>
        /// Wait for a signal
        /// </summary>
        /// <returns>A value task core that pumps values when signaled</returns>
        /// <exception cref="InvalidOperationException">When invalid concurrency levels are detected.</exception>
        public ValueTask<T> WaitAsync()
        {
            // => slow core
            if (Block(out var slowCore))
                return slowCore;

            // => API implementation error
            if (!Zeroed())
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
