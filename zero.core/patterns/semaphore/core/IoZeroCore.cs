//#define TRACE
using System;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.ComTypes;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using System.Xml.Schema;
using zero.core.misc;
using zero.core.patterns.queue;

namespace zero.core.patterns.semaphore.core
{
    /// <summary>
    /// A new core semaphore based on findings from <see cref="IoZeroSemaphore{T}"/> that uses
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
                throw new ArgumentOutOfRangeException($"Initial count cannot be more than max blockers! {description}");

            _zeroed = 0;
            _description = description;
            _capacity = capacity;
            ZeroAsyncMode = zeroAsyncMode;

            _blockingCores = new IoZeroQ<IIoManualResetValueTaskSourceCore<T>>(string.Empty, capacity, false, asyncTasks:null, capacity);
            _results = new IoZeroQ<T>(string.Empty, capacity, false, asyncTasks: null, capacity);
            _heapCore = new IoBag<IIoManualResetValueTaskSourceCore<T>>(string.Empty, capacity, asyncTasks: null, capacity);

            _primeReady = _ => default;     
            _primeContext = null;
            _asyncTasks = asyncTasks;
            
            _curOps = 0;
            _totalOps = 0;
            _curOpsAnchor = 0;
            Interlocked.Exchange(ref _curOpsAnchor, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            _ready = ready;
        }

        public IIoZeroSemaphoreBase<T> ZeroRef(ref IIoZeroSemaphoreBase<T> @ref, Func<object, T> primeResult = default, object context = null)
        {
            if (@ref == null)
                throw new ArgumentNullException(nameof(@ref));

            _primeReady = primeResult;
            _primeContext = context;

            for (int i = 0; i < _ready; i++)
            {
                if(_primeReady == null)
                    throw new ArgumentNullException(nameof(_primeReady));

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
        private long _curOps;
        private long _totalOps;
        private long _curOpsAnchor;
        private readonly IoZeroQ<IIoManualResetValueTaskSourceCore<T>> _blockingCores;
        private readonly IoZeroQ<T> _results;
        private readonly IoBag<IIoManualResetValueTaskSourceCore<T>> _heapCore;
        private Func<object, T> _primeReady;
        private object _primeContext;
        private readonly string _description;
        private readonly int _capacity;
        private int _zeroed;
        private readonly int _ready;
        private readonly CancellationTokenSource _asyncTasks;
        private int _blocking;

        #endregion

        #region Properties
        private const int SyncReady = 0;
        private const int SyncWait  = 1;
        private const int SyncRace  = 2;
        #endregion

        #region State
        public string Description => $"{nameof(IoZeroSemCore<T>)} ([{_totalOps}] - {_curOps} @ {Cps(true):0.0} c/s): r = {ReadyCount}/{_capacity}, w = {WaitCount}/{_capacity}, z = {_zeroed > 0}, bag = {_heapCore.Count}/{_heapCore.Capacity}, {_description}";
        public readonly int WaitCount => _blockingCores.Count;
        public readonly int ReadyCount => _results.Count;

        public readonly int Capacity => _capacity;
        public bool ZeroAsyncMode { get; }
        public readonly long TotalOps => _totalOps;
        #endregion

        #region Core

        /// <summary>
        /// Unblocks a core
        /// </summary>
        /// <param name="value">The value to unblock with</param>
        /// <param name="forceAsync">To continue asynchronously</param>
        /// <param name="blockingCore">optional core to use instead of dequeuing one</param>
        /// <returns>True on success</returns>
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private bool Unblock(T value, bool forceAsync, IIoManualResetValueTaskSourceCore<T> blockingCore = null)
        {
            retry:
            //fetch a blocking core from the Q
            if(blockingCore == null)
                while (!_blockingCores.TryDequeue(out blockingCore))
                {
                    if (_blockingCores.Count == 0 || Zeroed())
                        return false;

                    Interlocked.MemoryBarrierProcessWide();
                }

            //wait for the blocking core to synchronize
            while (blockingCore.SyncRoot == SyncWait)
            {
                if (Zeroed())
                    return false;
            }

            switch (blockingCore.SyncRoot)
            {
                case SyncReady://use the core
                    blockingCore.SyncRoot = SyncRace;
                    blockingCore.RunContinuationsAsynchronously = forceAsync;
                    blockingCore.SetResult(value);
                    return true;
                case SyncRace://discard the core (also from the heap)
                    blockingCore = null; // SAFE_RELEASE
                    Interlocked.MemoryBarrierProcessWide();
                    goto retry;
            }

            return false;
        }

        /// <summary>
        /// Dequeue a slow core and unblock it using the <see cref="value"/> provided
        /// </summary>
        /// <param name="value">Send this value to the blocker</param>
        /// <param name="forceAsync">Set async continuation</param>
        /// <param name="prime">Prime a result</param>
        /// <returns>If a waiter was unblocked, false otherwise</returns>
#if RELEASE
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private int SetResult(T value, bool forceAsync = false, bool prime = true)
        {
            retry:
            //insane checks
            if (Zeroed() || _results.Count == Capacity || (!prime && _blockingCores.Count == 0))
                return -1;

            //unblock
            if (Unblock(value, forceAsync))
                return 1;

            //only set if there is a thread waiting
            if (!prime)
                return -1;

            //sloppy race detection that is probably good enough in worst case conditions
            //The other side does proper race detection since blockingCores have those bits attached to them
            if (_blockingCores.Count > 0 || _blocking > 0)
                goto retry;

            //Debug.Assert(_blockingCores.Count == 0 || b != 0);
            return (int)_results.TryEnqueue(value) != -1? 1:0; //TODO: Critical. This should be bigger than. Hacked for now with equals? 
            

        //TODO: For some reason this makes things worse... I don't know why.
        //TODO: From what I can tell from my telemetry, there is an old interlocked instruction that is resurrected that jams the _results Q with bogus values.
        //TODO: There might be a way to detect this with op-counts vs capacity trickery etc, but these are not foolproof. 
        //TODO: This has been the same issue since forever now. The GC is pausing an Interlocked instruction that resumes slow jamming the system with gunk. 
        //TODO: This might have something to do with runtime scheduler CPU banks etc. which I don't (cant) compensate for. (potential showstopper)
        bank:
            //queue result for future blocker
            var pos = _results.TryEnqueue(value);

            //saturated
            if (pos <= 0)
                return 0;

            ////race
            if (_blockingCores.TryDequeue(out var blockingCore))
            {
                Interlocked.MemoryBarrierProcessWide();
                if (_results.Drop(pos))
                {
                    if (!Unblock(value, forceAsync, blockingCore))
                    {
                        _blockingCores.TryEnqueue(blockingCore);
#if DEBUG
                        Console.WriteLine("x");
#endif
                        goto bank;
                    }

#if DEBUG
                    Console.WriteLine("."); //TODO: "duplicate un-blockers" appear when this msg triggers. Makes no sense 
#endif

                    return 1;
                }

                _blockingCores.TryEnqueue(blockingCore);
#if DEBUG
                Console.WriteLine("X");
#endif
            }

            return 1;
        }

        /// <summary>
        /// Creates a new blocking core and releases the current thread to the pool of there are no pending results
        /// else inline the return of a result from the Q
        /// </summary>
        /// <param name="slowTaskCore">The resulting core that will most likely result in a block</param>
        /// <returns>True if there was a core created, false if all <see cref="_capacity"/> cores are still blocked</returns>
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private bool Block(out ValueTask<T> slowTaskCore)
        {
            //Debug.Assert(_results.Count == 0 && _blockingCores.Count >= 0 || _results.Count >= 0 && _blockingCores.Count == 0);
            try
            {
                Interlocked.Increment(ref _blocking);

                IIoManualResetValueTaskSourceCore<T> blockingCore = null;
            
                retry:
                //insane checks
                if (_blockingCores.Count == Capacity || Zeroed())
                {
                    slowTaskCore = default;
                    return false;
                }
            
                int b;
                //fast path
                if ((b = _blockingCores.Count) == 0 && _results.TryDequeue(out var readyResult))
                {
                    Interlocked.Increment(ref _totalOps);
                    Interlocked.Increment(ref _curOps);
                    slowTaskCore = new ValueTask<T>(readyResult);
                    return true;
                }
            
                //prepare a core from the heap
                if (blockingCore == null && !_heapCore.TryDequeue(out blockingCore))
                {
                    blockingCore = new IoManualResetValueTaskSourceCore<T>
                    {
                        AutoReset = true, RunContinuationsAsynchronouslyAlways = ZeroAsyncMode
                    };
                    blockingCore.OnReset(static state =>
                    {
                        var (@this, blockingCore) = (ValueTuple<IoZeroCore<T>, IIoManualResetValueTaskSourceCore<T>>)state;

                        Interlocked.Increment(ref @this._totalOps);
                        Interlocked.Increment(ref @this._curOps);

                        if (@this._heapCore.TryEnqueue(blockingCore) < 0)
                            Console.WriteLine($"Core Heap Overflow - {@this._heapCore.Description}");
                    
                    }, (this, blockingCore));
                }

                //maybe a result got populated while preparing a blocking core from the heap....
                if (_results.Count > 0 && b == 0)
                    goto retry; //also, core is dropped
            
                //core waiting to sync
                blockingCore.SyncRoot = SyncWait;

                long pos;
                //Queue the core
                while ((pos = _blockingCores.TryEnqueue(blockingCore)) < 0)
                {
                    if (Zeroed() || _blockingCores.Count == Capacity)
                    {
                        slowTaskCore = default;
                        return false;
                    }
                }

                //race
                if (b == 0 && _results.TryDequeue(out var racedResult))
                {
                    //don't synchronize the core, it will be dropped
                    blockingCore.SyncRoot = SyncRace;
                    _blockingCores.Drop(pos);

                    //unblock
                    slowTaskCore = new ValueTask<T>(racedResult);

                    Interlocked.Increment(ref _totalOps);
                    Interlocked.Increment(ref _curOps);

                    return true;
                }

                //core ready
                slowTaskCore = new ValueTask<T>(blockingCore, 0);

                if (b == 0 && _results.Count > 0)
                {
                    blockingCore.SyncRoot = SyncRace;
                    _blockingCores.Drop(pos);
                    blockingCore = null;
                    goto retry;
                }

                blockingCore.SyncRoot = SyncReady;
            
                Interlocked.Increment(ref _totalOps);
                Interlocked.Increment(ref _curOps);

                return true;
            }
            finally
            {
                Interlocked.Decrement(ref _blocking);
            }
        }
#endregion

#region API
        public T GetResult(short token) => throw new NotImplementedException(nameof(GetResult));

        public ValueTaskSourceStatus GetStatus(short token) => throw new NotImplementedException(nameof(GetStatus));

        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => throw new NotImplementedException(nameof(OnCompleted));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T value, int releaseCount, bool forceAsync = false, bool prime = true)
        {
            var released = 0;
            for (var i = 0; i < releaseCount; i++)
                released += Release(value, forceAsync, prime);
            
            return released;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T[] value, bool forceAsync = false, bool prime = true)
        {
            var released = 0;
            foreach (var t in value)
                released += Release(t, forceAsync);

            return released;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T value, bool forceAsync = false, bool prime = true) => SetResult(value,forceAsync, prime);
        

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

        /// <summary>
        /// The number of cores processed per second
        /// </summary>
        /// <param name="reset">Whether to reset hysteresis</param>
        /// <returns>The current completions per second</returns>
        public double Cps(bool reset = false)
        {
            try
            {
                return _curOps * 1000.0 / _curOpsAnchor.ElapsedUtcMs();
            }
            finally
            {
                if (reset)
                {
                    Interlocked.Exchange(ref _curOpsAnchor, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                    Interlocked.Exchange(ref _curOps, 0);
                }
            }
        }
#endregion
    }
}
