//#define TOKEN //TODO this primitive does not work this way
using System;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore.core
{
    /// <summary>
    /// Zero alloc semaphore with strong order guarantees
    /// 
    /// Experimental auto capacity scaling (disabled by default), set max count manually instead for max performance.
    /// </summary>
    public struct IoZeroSemaphore : IIoZeroSemaphore
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="description">A description of this semaphore</param>
        /// <param name="maxBlockers">The maximum number of blockers this semaphore has capacity for</param>
        /// <param name="initialCount">The initial number of requests that will be non blocking</param>
        /// <param name="asyncWorkerCount">The maximum "not-inline" or concurrent workers that this semaphore executes.</param>
        /// <param name="enableAutoScale">Experimental/dev: Cope with real time concurrency demand changes at the cost of undefined behavior in high GC pressured environments. DISABLE if CPU usage and memory snowballs and set <see cref="maxBlockers"/> more accurately instead. Scaling down is not supported</param>
        /// <param name="enableFairQ">Enable fair queueing at the cost of performance. If not, sometimes <see cref="OnCompleted"/> might jump the queue to save queue performance and resources. Some continuations are effectively not queued in <see cref="OnCompleted"/> when set to true</param>
        /// <param name="enableDeadlockDetection">When <see cref="enableAutoScale"/> is enabled checks for deadlocks within a thread and throws when found</param>
        /// <param name="cancellationTokenSource">Optional cancellation source</param>
        public IoZeroSemaphore(
            string description, 
            int maxBlockers = 1, 
            int initialCount = 0,
            int asyncWorkerCount = 0,
            bool enableAutoScale = false, bool enableFairQ = true, bool enableDeadlockDetection = false, CancellationTokenSource cancellationTokenSource = default) : this()
        {
#if DEBUG
            _description = description;
#else
            _description = string.Empty;
#endif

#if TOKEN
            throw new InvalidOperationException(
                $"This implementation of {nameof(IValueTaskSource)} is highly specialized and cannot be retrofitted with the TOKEN checks because this primitive does not work that way. I inserted this #define TOKEN stuff to preemptively manage expectations of would be lurkers. You can for example also await this valuetask multiple times etc. Tokens prevent that");
#endif

            //validation
            if (maxBlockers < 1)
                throw new ZeroValidationException($"{_description}: invalid {nameof(maxBlockers)} = {maxBlockers} specified, value must be larger than 0");
            if(initialCount < 0)
                throw new ZeroValidationException($"{_description}: invalid {nameof(initialCount)} = {initialCount} specified, value may not be less than 0");
            if(asyncWorkerCount > maxBlockers)
                throw new ZeroValidationException($"{_description}: invalid {nameof(asyncWorkerCount)} = {asyncWorkerCount} specified, must less of equal to maxBlockers");

            _maxBlockers = maxBlockers;
            _maxAsyncWorkers = asyncWorkerCount;
            RunContinuationsAsynchronously = _maxAsyncWorkers > 0;
            _curSignalCount = initialCount;
            _zeroRef = null;
            _asyncTasks = cancellationTokenSource;
#if DEBUG
            _useMemoryBarrier = enableFairQ;
            _enableAutoScale = enableAutoScale;
            if(_enableAutoScale)
                _lock = new SpinLock(enableDeadlockDetection);
#endif

            _signalAwaiter = new Action<object>[_maxBlockers];
            _signalAwaiterState = new object[_maxBlockers];
            _signalExecutionState = new ExecutionContext[_maxBlockers];
            _signalCapturedContext = new object[_maxBlockers];

            _curAsyncWorkerCount = 0;
            _curWaitCount = 0;
            _zeroed = 0;
            _error = default;

            _tail = 0;
            _head = 0;
            ZeroSentinel = CompletionSentinel;
        }

        /// <summary>
        /// Locks a continuation in place so that it can be worked with
        /// </summary>
        /// <param name="_"></param>
        /// <exception cref="InvalidOperationException"></exception>
        private static void CompletionSentinel(object _)
        {
            throw new InvalidOperationException();
        }

        #region settings

#if DEBUG
        /// <summary>
        /// use memory barrier setting
        /// </summary>
        private readonly bool _useMemoryBarrier;
#endif
        #endregion

        #region properties

#if TOKEN
        /// <summary>
        /// The current token
        /// </summary>
        private volatile int _token;
#endif
        /// <summary>
        /// A semaphore description
        /// </summary>
        private readonly string _description;
        
        /// <summary>
        /// A semaphore description
        /// </summary>
        private string Description => $"{nameof(IoZeroSemaphore)}[{_description}]: z = {_zeroed > 0},  ready = {_curSignalCount}, wait = {_curWaitCount}/{_maxBlockers}, async = {_curAsyncWorkerCount}/{_maxAsyncWorkers}, head = {Head}/{Tail} (D:{Tail - Head})";

        /// <summary>
        /// The maximum threads that can be blocked by this semaphore. This blocking takes storage
        /// and cannot be dynamically adjusted without adding massive runtime costs. Knowing and
        /// controlling this value upfront is the key
        /// </summary>
#if DEBUG
        private int _maxBlockers;
#else
        private readonly int _maxBlockers;
#endif
        /// <summary>
        /// Max number of async workers
        /// </summary>
        private readonly int _maxAsyncWorkers;

        /// <summary>
        /// Whether we support async continuations 
        /// </summary>
        public bool RunContinuationsAsynchronously { get; }

        /// <summary>
        /// Current number of async workers
        /// </summary>
        private volatile int _curAsyncWorkerCount;

        /// <summary>
        /// The current available number of threads that can enter the semaphore without blocking 
        /// </summary>
        private volatile int _curSignalCount;

        /// <summary>
        /// The current number of threads blocking on this semaphore
        /// </summary>
        private volatile int _curWaitCount;

        /// <summary>
        /// The number of threads that can enter the semaphore without blocking 
        /// </summary>
        public int ReadyCount => _curSignalCount;

        /// <summary>
        /// Nr of threads currently waiting on this semaphore
        /// </summary>
        public int CurNrOfBlockers => _curWaitCount;

        /// <summary>
        /// Maximum allowed concurrent "not inline" continuations before
        /// they become inline.
        /// </summary>
        public int MaxAsyncWorkers => _maxAsyncWorkers;

        /// <summary>
        /// Maximum concurrent blockers this semaphore can accomodate. Each extra thread
        /// requires extra storage space for continuations.
        /// </summary>
        public int Capacity => _maxBlockers;

        /// <summary>
        /// Allows for zero alloc <see cref="ValueTask"/> to be emitted. 
        /// </summary>
        private volatile IIoZeroSemaphore _zeroRef;

        /// <summary>
        /// A queue of waiting continuations. The queue has strong order guarantees, FIFO
        /// </summary>
        private Action<object>[] _signalAwaiter;

        /// <summary>
        /// Holds the state of a queued item
        /// </summary>
        private object[] _signalAwaiterState;

        /// <summary>
        /// Holds the state of a queued item
        /// </summary>
        private ExecutionContext[] _signalExecutionState;

        /// <summary>
        /// Holds the state of a queued item
        /// </summary>
        private object[] _signalCapturedContext;

        /// <summary>
        /// A pointer to the head of the Q
        /// </summary>
        private long _tail; //Zero is a special start state;

        /// <summary>
        /// A pointer to the tail of the Q
        /// </summary>
        private long _head;

        public long Head => Interlocked.Read(ref _head);
        public long Tail => Interlocked.Read(ref _tail);

        /// <summary>
        /// Whether this semaphore has been cleared out
        /// </summary>
        private volatile int _zeroed;

        /// <summary>
        /// Used for locking
        /// </summary>
        internal static Action<object> ZeroSentinel;

        /// <summary>
        /// error info
        /// </summary>
        private ExceptionDispatchInfo _error;

        /// <summary>
        /// The cancellation token  
        /// </summary>
        private CancellationTokenSource _asyncTasks;

        #endregion

        #region core
        /// <summary>
        /// Validation failed exception
        /// </summary>
        private class ZeroValidationException : InvalidOperationException
        {
            public ZeroValidationException(string description) : base(description)
            {
            }
        }
        
        /// <summary>
        /// Set ref to this (struct address).
        ///
        /// A struct cannot create a ref pointer to itself in the constructor, because of copy logic. 
        /// 
        /// This means that the user needs to call this function manually from externally when the ref is available or the semaphore will error out.
        /// </summary>
        /// <param name="ref">The ref to this</param>
        public void ZeroRef(ref IIoZeroSemaphore @ref)
        {
            _zeroRef = @ref;
        }

        /// <summary>
        /// zeroes out this semaphore
        /// </summary>
        public void ZeroSem()
        {
            if(Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return;
            
            try
            {
#if NET6_0
                _asyncTokenReg.Unregister();
#endif

                if (_asyncTasks.Token.CanBeCanceled)
                    _asyncTasks.Cancel();
            }
            catch
            {
                // s
            }

            var i = 0;
            while (i < _signalAwaiter.Length)
            {
                var latch = _signalAwaiter[i];
                var waiter = Interlocked.CompareExchange(ref _signalAwaiter[i], ZeroSentinel, latch);
                if (waiter == latch && latch != null)
                {
                    try
                    {
                        ZeroComply(waiter, _signalAwaiterState[i], _signalExecutionState[i], _signalCapturedContext[i], true, true);
                        _signalAwaiter[i] = null;
                    }
                    catch
                    {
                        // ignored
                    }
                }
                i++;
            }

            Array.Clear(_signalAwaiter, 0, _maxBlockers);
            Array.Clear(_signalAwaiterState, 0, _maxBlockers);
            Array.Clear(_signalExecutionState, 0, _maxBlockers);
            Array.Clear(_signalCapturedContext, 0, _maxBlockers);

#if SAFE_RELEASE
            _signalAwaiter = null;
            _signalAwaiterState = null;
            _signalExecutionState = null;
            _signalCapturedContext = null;
            _asyncTasks = null;
            _zeroRef = null;
#endif
        }

#if DEBUG

        /// <summary>
        /// Lock
        /// </summary>s
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ZeroLock()
        {
            //Disable experimental features
            if(!_enableAutoScale)
                return;
            
            //acquire lock
            var _ = false;

            _lock.Enter(ref _);
        }

        /// <summary>
        /// Unlock
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ZeroUnlock()
        {
            //Disable experimental features
            if (!_enableAutoScale)
                return;
            
            _lock.Exit(_useMemoryBarrier);
        }

        /// <summary>
        /// Used for locking internally; when <see cref="_enableAutoScale"/> is enabled
        /// </summary>
        private SpinLock _lock;


        /// <summary>
        /// if auto scaling is enabled 
        /// </summary>
        private readonly bool _enableAutoScale;
#endif

#endregion

        /// <summary>
        /// Returns true if exit is clean, false otherwise
        /// </summary>
        /// <param name="token">Not used</param>
        /// <returns>True if exit was clean, false on <see cref="CancellationToken.IsCancellationRequested"/> </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetResult(short token)
        {
#if TOKEN
            if (_zeroRef.ZeroToken() != token)
                throw new ZeroValidationException($"{Description}: Invalid token: wants = {token}, has = {_zeroRef.ZeroToken()}");

            _zeroRef.ZeroTokenBump();
#endif
            try
            {
                ZeroThrow();
                return !Zeroed();
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Get the current status, which in this case will always be pending
        /// </summary>
        /// <param name="token">Not used</param>
        /// <returns><see cref="ValueTaskSourceStatus.Pending"/></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTaskSourceStatus GetStatus(short token)
        {
#if TOKEN
            if (_zeroRef.ZeroToken() != token)
                throw new ZeroValidationException($"{Description}: Invalid token: wants = {token}, has = {_token}");
#endif
            try
            {
                if (Zeroed())
                    return ValueTaskSourceStatus.Canceled;
            }
            catch
            {
                return ValueTaskSourceStatus.Canceled;
            }

            return ValueTaskSourceStatus.Pending;
        }

        /// <summary>
        /// Extract execution context
        /// </summary>
        /// <param name="ec">Execution context</param>
        /// <param name="cc">Captured context</param>
        /// <param name="flags">Completion flags</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ExtractContext(out ExecutionContext ec, out object cc, ValueTaskSourceOnCompletedFlags flags)
        {
            ec = null;
            cc = null;
            if ((flags & ValueTaskSourceOnCompletedFlags.FlowExecutionContext) != 0)
                ec = ExecutionContext.Capture();

            if ((flags & ValueTaskSourceOnCompletedFlags.UseSchedulingContext) == 0) return;

            var sc = SynchronizationContext.Current;
            if (sc != null && sc.GetType() != typeof(SynchronizationContext))
            {
                cc = sc;
            }
            else
            {
                var ts = TaskScheduler.Current;
                if (ts != TaskScheduler.Default)
                    cc = ts;
            }
        }

        /// <summary>
        /// Set signal handler
        /// </summary>
        /// <param name="continuation">The handler</param>
        /// <param name="state">The state</param>
        /// <param name="token">The safety token</param>
        /// <param name="flags">FLAGS</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
#if TOKEN
            if (_zeroRef.ZeroToken() != token)
                throw new ZeroValidationException($"{Description}: Invalid token: wants = {token}, has = {_zeroRef.ZeroToken()}");
#endif
            try
            {
#if DEBUG
                ZeroLock();
#endif
                //long tailIdx = 0;
                bool insaneOverflow = false;
                var c = 0;
                var slot = ZeroSentinel;
                //var tailMod = (tailIdx = Tail) % _maxBlockers;
                //Interlocked.MemoryBarrierProcessWide();
                Interlocked.MemoryBarrier();
                var tailMod = Tail % _maxBlockers;
                while (
                    _curWaitCount <= _maxBlockers &&
                    (
                        /*(insaneOverflow = tailIdx >= _head + _maxBlockers) ||*/
                        (slot = Interlocked.CompareExchange(ref _signalAwaiter[tailMod], ZeroSentinel, null)) != null
                    )
                )
                {
#if DEBUG
                    if (++c == 1000000)
                    {
                        Console.WriteLine($"[{c}] 1  OnComplete: bad latch[{tailMod}] = {slot != null}, isSentinel = ({slot == ZeroSentinel}), overflow = {insaneOverflow}, {Description}");
                        Console.WriteLine($"[{c}] 1  OnComplete: {_signalAwaiter[0]},{_signalAwaiter[1]},{_signalAwaiter[2]}, {_signalAwaiter[0] == ZeroSentinel},{_signalAwaiter[1] == ZeroSentinel},{_signalAwaiter[2] == ZeroSentinel}");
                    }
                    else if (c > 1000000)
                    {
                        //Thread.Yield();
                    }
#endif

                    if (_zeroed > 0)
                        break;

                    slot = ZeroSentinel;
                    //tailMod = (tailIdx = _tail) % _maxBlockers;
                    Interlocked.MemoryBarrierProcessWide();
                    tailMod = Tail % _maxBlockers;
                }

                if (slot == null)
                {
                    //fast path, RACES with SetResult 
                    if (_curSignalCount == 1 && _curWaitCount == 1)
                    {
                        if (Interlocked.CompareExchange(ref _curSignalCount, 0, 1) == 1)
                        {
                            TaskScheduler cc = null;
                            if (TaskScheduler.Current != TaskScheduler.Default)
                                cc = TaskScheduler.Current;

                            Interlocked.Exchange(ref _signalAwaiter[tailMod], null);
                            InvokeContinuation(continuation, state, cc, false);
                            return;
                        }
                    }

#if DEBUG
                    if (c > 1000000)
                        Console.WriteLine(
                            $"[{c}] 1 [RECOVER] OnComplete: bad latch[{tailMod}] = {slot != null}({slot != ZeroSentinel}), {Description}");
#endif

                    _signalAwaiterState[tailMod] = state;
                    ExtractContext(out _signalExecutionState[tailMod], out _signalCapturedContext[tailMod], flags);
                    _signalAwaiter[tailMod] = continuation;
                    Interlocked.MemoryBarrier();
                    Interlocked.Increment(ref _tail);
                    //Interlocked.MemoryBarrier();
                    return;
                }

#if DEBUG
                if (c > 1000000)
                    Console.WriteLine(
                        $"[{c}] 1 [RECOVER] OnComplete: bad latch[{tailMod}] = {slot != null}({slot != ZeroSentinel}), {Description}");
#endif

#if !DEBUG
                if (slot == ZeroSentinel)
                    return;
#endif

#if DEBUG
                if (!_enableAutoScale && slot == ZeroSentinel)
                    return;

                ZeroUnlock();

                if (_enableAutoScale) //EXPERIMENTAL: double concurrent capacity
                {
                    //release lock

                    ZeroUnlock();


                    //Scale
                    if (_enableAutoScale)
                    {
                        ZeroScale();
                        OnCompleted(continuation, state, token, flags);
                    }
                    else
                    {
                        throw new ZeroValidationException(
                            $"{_description}: FATAL!, {nameof(_curWaitCount)} = {_curWaitCount}/{_maxBlockers}, {nameof(_curAsyncWorkerCount)} = {_curAsyncWorkerCount}/{_maxAsyncWorkers}");
                    }

                }

                throw new ZeroValidationException(
                    $"{nameof(OnCompleted)}: Invalid state! Concurrency bug. Too many blockers... {Description}");
#endif
            }
            catch when (Zeroed())
            {
            }
            catch (Exception e) when (!Zeroed())
            {
                LogManager.GetCurrentClassLogger().Error(e, $"{nameof(OnCompleted)}:");
            }
        }

        /// <summary>
        /// Executes a worker
        /// </summary>
        /// <param name="callback">The callback</param>
        /// <param name="state">The state</param>
        /// <param name="capturedContext"></param>
        /// <param name="zeroed">If we are zeroed</param>
        /// <param name="executionContext"></param>
        /// <param name="forceAsync">Forces async execution</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ZeroComply(Action<object> callback, object state, ExecutionContext executionContext, object capturedContext, bool zeroed = false, bool forceAsync = false)
        {
#if DEBUG
            //validate
            if (callback == null || state == null)
                throw new ArgumentNullException($"-> {nameof(callback)} = {callback}, {nameof(state)} = {state}");
#endif
            try
            {
                //Execute with captured context
                if (executionContext != null)
                {
                    ExecutionContext.Run(
                        executionContext,
                        static s =>
                        {
                            var (@this, callback, state, capturedContext) =
                                (ValueTuple<IoZeroSemaphore, Action<object>, object, object>)s;

                            try
                            {
                                @this.InvokeContinuation(callback, state, capturedContext, false);
                            }
                            catch (Exception e)
                            {
                                LogManager.GetCurrentClassLogger().Trace(e, $"{nameof(ExecutionContext)}: ]");
                            }
                        }, (_zeroRef, callback, state, capturedContext));

                    return true;
                }

                InvokeContinuation(callback, state, capturedContext, forceAsync);
                return true;
            }
            catch (TaskCanceledException)
            {
                return true;
            }
            catch (Exception) when (zeroed)
            {
            }
            catch (Exception e) when (!zeroed)
            {
                //LogManager.GetCurrentClassLogger().Error(e, $"{_description}: {nameof(ThreadPool.QueueUserWorkItem)}, {nameof(worker.Continuation)} = {worker.Continuation}, {nameof(worker.State)} = {worker.State}");
                throw IoNanoprobe.ZeroException.ErrorReport($"{nameof(ThreadPool.QueueUserWorkItem)}",
                    $"{nameof(callback)} = {callback}, " +
                    $"{nameof(state)} = {state}", e);
            }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InvokeContinuation(Action<object> callback, object state, object capturedContext, bool forceAsync)
        {
            Interlocked.Decrement(ref _curWaitCount);
            switch (capturedContext)
            {
                case null:
                    if (RunContinuationsAsynchronously || forceAsync)
                    {
                        if (forceAsync)
                        {
                            _ = Task.Factory.StartNew(callback, state, CancellationToken.None,
                                TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
                            break;
                        }

                        if (Interlocked.Increment(ref _curAsyncWorkerCount) <= _maxAsyncWorkers)
                        {
                            _ = Task.Factory.StartNew(callback, state, CancellationToken.None, 
                                TaskCreationOptions.DenyChildAttach, TaskScheduler.Default)
                                .ContinueWith(static (_, zeroRef) => { ((IIoZeroSemaphore)zeroRef).ZeroDecAsyncCount(); }, _zeroRef);
                            break;
                        }

                        Interlocked.Decrement(ref _curAsyncWorkerCount);
                        //race condition
                        try
                        {
                            callback(state);
                        }
                        catch (Exception e)
                        {
                            LogManager.GetCurrentClassLogger().Error(e, "InvokeContinuation.callback(): ");
                        }
                    }
                    else
                    {
                        try
                        {
                            callback(state);
                        }
                        catch (Exception e)
                        {
                            LogManager.GetCurrentClassLogger().Error(e, "InvokeContinuation.callback(): ");
                        }
                    }
                    break;
                case SynchronizationContext sc:
                    sc.Post(static s =>
                    {
                        var tuple = ((Action<object>, object))s!;
                        try
                        {
                            tuple.Item1(tuple.Item2);
                        }
                        catch (Exception e)
                        {
                            LogManager.GetCurrentClassLogger().Error(e, "InvokeContinuation.callback(): ");
                        }
                    }, (callback, state));
                    break;

                case TaskScheduler ts:
                    _ = Task.Factory.StartNew(callback, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ts);
                    break;
            }
        }

#if DEBUG
        /// <summary>
        /// Attempts to scale the semaphore to handle higher volumes of concurrency experienced. (for example if worker counts were tied to F(#CPUs))
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ZeroScale() 
        {
            var acquiredLock = false;

            try
            {
                //Acquire lock and disable GC
                while (!acquiredLock)
                {
                    _lock.Enter(ref acquiredLock);

                    try
                    {
                        GC.TryStartNoGCRegion(250 / 2 * 1024 * 1024, true);
                    }
                    catch
                    {
                        // ignored
                    }
                }

                //double the q
                var prevZeroQ = _signalAwaiter;
                var prevZeroState = _signalAwaiterState;
                var prevExecutionState = _signalExecutionState;
                var prevCapturedContext = _signalCapturedContext;

                //allocate memory
                _maxBlockers *= 2;
                _signalAwaiter = new Action<object>[_maxBlockers];
                _signalAwaiterState = new object[_maxBlockers];
                _signalExecutionState = new ExecutionContext[_maxBlockers];
                _signalCapturedContext = new object[_maxBlockers];

                //copy Q

                var j = 0;
                //special zero case
                if (Head % _maxBlockers != Tail % _maxBlockers || prevZeroState[_head % _maxBlockers] != null && prevZeroQ.Length == 1)
                {
                    _signalAwaiter[0] = prevZeroQ[0];
                    _signalAwaiterState[0] = prevZeroState[0];
                    _signalExecutionState[0] = prevExecutionState[0];
                    _signalCapturedContext[0] = prevCapturedContext[0];
                    j = 1;
                }
                else
                {
                    for (var i = Head % _maxBlockers; i != Tail % _maxBlockers || prevZeroState[i] != null && j < _maxBlockers; i = (i + 1) % prevZeroQ.Length)
                    {
                        _signalAwaiter[j] = prevZeroQ[i];
                        _signalAwaiterState[j] = prevZeroState[i];
                        _signalExecutionState[j] = prevExecutionState[i];
                        _signalCapturedContext[j] = prevCapturedContext[i];
                        j++;
                    }
                }

                //reset queue pointers
                _head = 0;
                _tail = j;
            }
            finally
            {
                //release the lock
                ZeroUnlock();

                //Enable GC
                try
                {
                    GC.EndNoGCRegion();
                }
                catch
                {
                    // ignored
                }
            }
            
        }
#endif

        /// <summary>
        /// Allow waiter(s) to enter the semaphore
        /// </summary>
        /// <param name="releaseCount">The number of waiters to enter</param>
        /// <param name="bestEffort">Whether this originates from <see cref="OnCompleted"/></param>
        /// <returns>The number of waiters released, -1 on failure</returns>
        /// <exception cref="SemaphoreFullException">Fails when maximum waiters reached</exception>
        public int Release(int releaseCount = 1, bool bestEffort = false)
        {
            //preconditions that reject overflow because every overflowing signal will spin seeking its waiter
            if (Zeroed() || !bestEffort && (releaseCount < 1 || releaseCount + _curSignalCount > _maxBlockers))
                return -1;

            //lock in return value
            var released = 0;

            var c = 0;
            //release waiters
            while (released < releaseCount && _curWaitCount > 0)
            {
                IoZeroWorker worker = default;
                worker.Continuation = ZeroSentinel;
                //Lock
#if DEBUG
                ZeroLock();
#endif
                //latch a chosen head
                long headLatch = 0;
                long headMod = 0;
                c = 0;

                //Interlocked.MemoryBarrierProcessWide();
                Interlocked.MemoryBarrier();
                var latch = _signalAwaiter[headMod = (headLatch = Head) % _maxBlockers];
                while (
                    _curWaitCount > 0 &&
                    (
                       /*headLatch >= _tail || */
                       latch == null || 
                       latch == ZeroSentinel || 
                       (worker.Continuation = Interlocked.CompareExchange(ref _signalAwaiter[headMod], ZeroSentinel, latch)) != latch)
                    )
                {
                    if (++c == 1000000)
                    {
                        Console.WriteLine($"[{c}] 2 Release: bad latch[{headMod}] = {worker.Continuation != latch}(null = {latch == null}), overflow = {headLatch >= Tail}, locked = {headLatch != Head}, {Description}");

                        //TODO: wtf?
                        Interlocked.Increment(ref _head);
                        //Interlocked.Increment(ref _tail);
                        Interlocked.MemoryBarrierProcessWide();
                        c /= 2;
                    }
                    else if (c > 1000000)
                    {
                        //Thread.Yield();
                    }


                    worker.Continuation = ZeroSentinel;

                    if (_zeroed > 0)
                        break;

                    if (_curSignalCount == 0)
                        bestEffort = true;

                    Interlocked.MemoryBarrierProcessWide();
                    latch = _signalAwaiter[headMod = (headLatch = Head) % _maxBlockers];
                }

                if (worker.Continuation != latch || worker.Continuation == null || worker.Continuation == ZeroSentinel || latch == null || latch == ZeroSentinel )
                {
                    if (Zeroed())
                    {
                        if (c > 1000000)
                            Console.WriteLine($"[{c}] 2 [RECOVER] Release: bad latch[{headMod}] = {worker.Continuation != latch}(null = {latch == null}), overflow = {headLatch > Tail}, locked = {headLatch != Head}, {Description}");
                        break;
                    }
                    
                    continue;
                }

#if DEBUG
                if (c > 1000000)
                    Console.WriteLine($"[{c}] 2 [RECOVER] Release: bad latch[{headMod}] = {worker.Continuation != latch}(null = {latch == null}), overflow = {headLatch > Tail}, locked = {headLatch != Head}, {Description}");
#endif

                worker.State = Interlocked.Exchange(ref _signalAwaiterState[headMod], null);
                worker.ExecutionContext = Interlocked.Exchange(ref _signalExecutionState[headMod], null);
                worker.CapturedContext = Interlocked.Exchange(ref _signalCapturedContext[headMod], null);
                _ = Interlocked.Exchange(ref _signalAwaiter[headMod], null);

                Interlocked.MemoryBarrier();
                Interlocked.Increment(ref _head);
#if DEBUG
                //unlock
                ZeroUnlock();
#endif
                if (!ZeroComply(worker.Continuation, worker.State, worker.ExecutionContext, worker.CapturedContext, Zeroed() || worker.State is IIoNanite nanite && nanite.Zeroed(),bestEffort))
                {
                    if (!bestEffort)
                        Interlocked.Add(ref _curSignalCount, releaseCount - released);

                    return -1;
                }

                //count the number of waiters released
                released++;
            }

            if (!bestEffort)
                Interlocked.Add(ref _curSignalCount, releaseCount - released);

            if (c > 1000000)
                Console.WriteLine($"[{c}] 2 [RECOVER] Release: {Description}");

            //return the number of waiters released
            return released;
        }
        
        /// <summary>
        /// Waits on this semaphore
        /// </summary>
        /// <returns>True if waiting, false otherwise. If the semaphore is awaited on more than <see cref="_maxBlockers"/>, false is returned</returns>
        public ValueTask<bool> WaitAsync()
        {
            //insane checks
            if (Zeroed())
                return new ValueTask<bool>(false);

            var slot = -1;
            int latch;

            //grab a signal slot
            while ((latch = _curSignalCount) > 0 && _curWaitCount == 0 && (slot = Interlocked.CompareExchange(ref _curSignalCount, latch - 1, latch)) != latch)
                slot = -1;

            //fast path
            if (slot == latch && _curWaitCount == 0)//There is a race here with _cureWaitCount going > 0, but I don't think this is an ordering issue as the race was won in the spin lock therefor this waiter must have been first anyway.
                return new ValueTask<bool>(true);

            //grab a wait slot
            slot = -1;
            while ((latch = _curWaitCount) < _maxBlockers && (slot = Interlocked.CompareExchange(ref _curWaitCount, latch + 1, latch)) != latch)
                slot = -1;

            return slot == latch ? new ValueTask<bool>(_zeroRef, 23) : new ValueTask<bool>(false);
        }

        /// <summary>Completes with an error.</summary>
        /// <param name="error">The exception.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetException(Exception error)
        {
            _error = ExceptionDispatchInfo.Capture(error);
            Release();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IIoZeroSemaphore.ZeroDecAsyncCount()
        {
            return Interlocked.Decrement(ref _curAsyncWorkerCount);
        }

        /// <summary>
        /// Are we zeroed out?
        /// </summary>
        /// <returns>True if zeroed, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Zeroed()
        {
            return _zeroed > 0 || _asyncTasks.IsCancellationRequested;
        }

        /// <summary>
        /// Throw on error
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ZeroThrow()
        {
            _error?.Throw();
        }

        /// <summary>
        /// Friendly output
        /// </summary>
        /// <returns>A description of the primitive</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override string ToString()
        {
            return $"{_description}: sync = {_curWaitCount}/{_maxBlockers}, async = {_curAsyncWorkerCount}/{_maxAsyncWorkers}, ready = {_curSignalCount}";
        }

        /// <summary>
        /// Worker info
        /// </summary>
        private struct IoZeroWorker 
        {
            public Action<object> Continuation;
            public object State;
            public ExecutionContext ExecutionContext;
            public object CapturedContext;
        }
    }
}