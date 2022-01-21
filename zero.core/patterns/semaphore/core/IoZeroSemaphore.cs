//#define TOKEN //TODO this primitive does not work this way
using System;
using System.Diagnostics;
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
        private string Description => $"{nameof(IoZeroSemaphore)}[{_description}]:  ready = {_curSignalCount}, wait = {_curWaitCount}/{_maxBlockers}, async = {_curAsyncWorkerCount}/{_maxAsyncWorkers}";

        /// <summary>
        /// The maximum threads that can be blocked by this semaphore. This blocking takes storage
        /// and cannot be dynamically adjusted without adding massive runtime costs. Knowing and
        /// controlling this value upfront is the key
        /// </summary>
        private int _maxBlockers;

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
        private IIoZeroSemaphore _zeroRef;

        /// <summary>
        /// The cancellation token
        /// </summary>
        private CancellationTokenSource _asyncTasks;

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
        private long _tail;

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
        /// Validation failed exception
        /// </summary>
        public class ZeroSemaphoreFullException : SemaphoreFullException
        {
            public ZeroSemaphoreFullException(string description) : base(description)
            {
            }
        }
        
        /// <summary>
        /// Set ref to this (struct address) and register cancellation.
        ///
        /// One would have liked to do this in the constructor, but C# currently does not support this.
        /// This means that the user needs to call this function manually or the semaphore will error out.
        /// </summary>
        /// <param name="ref">The ref to this</param>
        /// <param name="asyncTokenSource">The cancellation token</param>
        public void ZeroRef(ref IIoZeroSemaphore @ref, CancellationTokenSource asyncTokenSource)
        {
            _zeroRef = @ref;
            _asyncTasks = asyncTokenSource;
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
                //fast path, RACES with SetResult 
                if (_curSignalCount == 1 && _curWaitCount == 1)
                {
                    if (Interlocked.Decrement(ref _curSignalCount) == 0)
                    {
                        TaskScheduler cc = null;
                        if (TaskScheduler.Current != TaskScheduler.Default)
                            cc = TaskScheduler.Current;

                        InvokeContinuation(continuation, state, cc, false);
                        return;
                    }

                    Interlocked.Increment(ref _curSignalCount);
                }
#if DEBUG
                ZeroLock();
#endif
                var headMod = (Interlocked.Increment(ref _tail) - 1) % _maxBlockers;
#if DEBUG
                Action<object> slot;
                while ((slot = Interlocked.CompareExchange(ref _signalAwaiter[headMod], ZeroSentinel, null)) != null && _zeroed == 0)
#else
                while (Interlocked.CompareExchange(ref _signalAwaiter[headMod], ZeroSentinel, null) != null && _zeroed == 0)
#endif
                { }
#if DEBUG
                if (slot == null)
#endif
                {
                    _signalAwaiterState[headMod] = state;
                    ExtractContext(out _signalExecutionState[headMod], out _signalCapturedContext[headMod], flags);
                    Thread.MemoryBarrier();
                    _signalAwaiter[headMod] = continuation;
#if DEBUG
                    return;
#endif
                }
#if DEBUG
                ZeroUnlock();
                Interlocked.Decrement(ref _tail);
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
                        throw new ZeroSemaphoreFullException(
                            $"{_description}: FATAL!, {nameof(_curWaitCount)} = {_curWaitCount}/{_maxBlockers}, {nameof(_curAsyncWorkerCount)} = {_curAsyncWorkerCount}/{_maxAsyncWorkers}");
                    }

                }

                throw new ZeroValidationException($"{nameof(OnCompleted)}: Invalid state! Concurrency bug. Too many blockers...");
#endif
            }
            catch when (Zeroed()) { }
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
                            Task.Factory.StartNew(callback, state, CancellationToken.None,
                                TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
                        }
                        else if (Interlocked.Increment(ref _curAsyncWorkerCount) - 1 < _maxAsyncWorkers)
                        {
                            Task.Factory.StartNew(callback, state, CancellationToken.None, 
                                TaskCreationOptions.DenyChildAttach, TaskScheduler.Default)
                                .ContinueWith((_, zeroRef) => { ((IIoZeroSemaphore)zeroRef).ZeroDecAsyncCount(); }, _zeroRef);
                        }
                        else
                        {
                            //race condition
                            try
                            {
                                callback(state);
                            }
                            catch (Exception e)
                            {
                                LogManager.GetCurrentClassLogger().Error(e, $"InvokeContinuation.callback(): ");
                            }
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
                            LogManager.GetCurrentClassLogger().Error(e, $"InvokeContinuation.callback(): ");
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
                            LogManager.GetCurrentClassLogger().Error(e, $"InvokeContinuation.callback(): ");
                        }
                    }, (callback, state));
                    break;

                case TaskScheduler ts:
                    Task.Factory.StartNew(callback, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ts);
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
                if (_head % _maxBlockers != _tail % _maxBlockers || prevZeroState[_head % _maxBlockers] != null && prevZeroQ.Length == 1)
                {
                    _signalAwaiter[0] = prevZeroQ[0];
                    _signalAwaiterState[0] = prevZeroState[0];
                    _signalExecutionState[0] = prevExecutionState[0];
                    _signalCapturedContext[0] = prevCapturedContext[0];
                    j = 1;
                }
                else
                {
                    for (var i = _head % _maxBlockers; i != _tail % _maxBlockers || prevZeroState[i] != null && j < _maxBlockers; i = (i + 1) % prevZeroQ.Length)
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
            //fail fast on cancellation token
            if (Zeroed())
                return -1;
            
            //preconditions
            if (!bestEffort && (releaseCount < 1 || releaseCount + _curSignalCount > _maxBlockers))
            {
                throw new SemaphoreFullException($"{Description}: Invalid {nameof(releaseCount)} = {releaseCount} < 0 or  {nameof(_curSignalCount)}({releaseCount + _curSignalCount}) > {nameof(_maxBlockers)} = {_maxBlockers}");
            }
            
            //lock in return value
            var released = 0;

            //awaiter entries
            while (released < releaseCount && _curWaitCount > 0)
            {
                IoZeroWorker worker = default;
                //Lock
#if DEBUG
                ZeroLock();
#endif
                var headIdx = Interlocked.Increment(ref _head) - 1;
                var latchMod = headIdx % _maxBlockers;
                var latch = _signalAwaiter[latchMod];

                //latch a chosen head
                while (_curWaitCount > 0 && (headIdx > _tail || latch == null || latch == ZeroSentinel || (worker.Continuation = Interlocked.CompareExchange(ref _signalAwaiter[latchMod], ZeroSentinel, latch)) != latch))
                {
                    Interlocked.Decrement(ref _head);

                    if (Zeroed())
                        break;

                    headIdx = Interlocked.Increment(ref _head) - 1;
                    latchMod = headIdx % _maxBlockers;

                    latch = _signalAwaiter[latchMod];
                }

                if (worker.Continuation != latch || latch == ZeroSentinel || latch == null)
                {
                    Interlocked.Decrement(ref _head);

                    if (Zeroed())
                        break;

                    continue;
                }

                worker.State = _signalAwaiterState[latchMod];
                _signalAwaiterState[latchMod] = null;
                worker.ExecutionContext = _signalExecutionState[latchMod];
                _signalExecutionState[latchMod] = null;
                worker.CapturedContext = _signalCapturedContext[latchMod];
                _signalCapturedContext[latchMod] = null;
                Thread.MemoryBarrier();
                _signalAwaiter[latchMod] = null;
#if DEBUG
                //unlock
                ZeroUnlock();
#endif
                if (!ZeroComply(worker.Continuation, worker.State, worker.ExecutionContext, worker.CapturedContext, Zeroed() || Zeroed() || worker.State is IIoNanite nanite && nanite.Zeroed(),bestEffort))
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

            //fast path
            if (_curSignalCount > 0 && _curWaitCount == 0)
            {
                if (Interlocked.Decrement(ref _curSignalCount) >= 0)
                    return new ValueTask<bool>(true);

                Interlocked.Increment(ref _curSignalCount);
            }

            if (_curWaitCount < _maxBlockers && Interlocked.Increment(ref _curWaitCount) <= _maxBlockers)
                return new ValueTask<bool>(_zeroRef, 23);

            Interlocked.Decrement(ref _curWaitCount);
            return new ValueTask<bool>(false);
        }

        /// <summary>Completes with an error.</summary>
        /// <param name="error">The exception.</param>
        public void SetException(Exception error)
        {
            _error = ExceptionDispatchInfo.Capture(error);
            Release();
        }

        //        /// <summary>
        //        /// returns the next tail
        //        /// </summary>
        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        long IIoZeroSemaphore.ZeroNextTail()
        //        {
        //            return Interlocked.Increment(ref _head);
        //        }

        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        long IIoZeroSemaphore.ZeroNextHead()
        //        {
        //            return Interlocked.Increment(ref _tail);
        //        }

        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        long IIoZeroSemaphore.ZeroPrevTail()
        //        {
        //            return Interlocked.Decrement(ref _head);
        //        }

        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        long IIoZeroSemaphore.ZeroPrevHead()
        //        {
        //            return Interlocked.Decrement(ref _tail);
        //        }

        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        int IIoZeroSemaphore.ZeroIncWait()
        //        {
        //            return Interlocked.Increment(ref _curWaitCount);
        //        }

        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        int IIoZeroSemaphore.ZeroDecWait()
        //        {
        //            return Interlocked.Decrement(ref _curWaitCount);
        //        }

        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        int IIoZeroSemaphore.ZeroWaitCount()
        //        {
        //            return _curWaitCount;
        //        }

        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        int IIoZeroSemaphore.ZeroIncAsyncCount()
        //        {
        //            return Interlocked.Increment(ref _curAsyncWorkerCount);
        //        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IIoZeroSemaphore.ZeroDecAsyncCount()
        {
            return Interlocked.Decrement(ref _curAsyncWorkerCount);
        }

        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        int IIoZeroSemaphore.ZeroAsyncCount()
        //        {
        //            return _curAsyncWorkerCount;
        //        }

        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        int IIoZeroSemaphore.ZeroCount()
        //        {
        //            return _curSignalCount;
        //        }

        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        int IIoZeroSemaphore.ZeroIncCount()
        //        {
        //            return Interlocked.Increment(ref _curSignalCount);
        //        }

        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        int IIoZeroSemaphore.ZeroDecCount()
        //        {
        //            return Interlocked.Decrement(ref _curSignalCount);
        //        }

        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        int IIoZeroSemaphore.ZeroEnter()
        //        {
        //            var slot = -1;
        //            var latch = _curSignalCount;

        //            while (latch > 0 && (slot = Interlocked.CompareExchange(ref _curSignalCount, latch - 1, latch)) != latch)
        //            {
        //                if (slot == 0)
        //                    return -1;

        //                latch = _curSignalCount;
        //            }

        //            if (slot > 0 && slot == latch)
        //                return latch;

        //            return -1;
        //        }

        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        int IIoZeroSemaphore.ZeroAddCount(int value)
        //        {
        //            return Interlocked.Add(ref _curSignalCount, value);
        //        }

        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        long IIoZeroSemaphore.ZeroHead()
        //        {
        //            return _tail;
        //        }

        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        long IIoZeroSemaphore.ZeroTail()
        //        {
        //            return _head;
        //        }


        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        short IIoZeroSemaphore.ZeroToken()
        //        {
        //#if TOKEN
        //            return (short)(_token % ushort.MaxValue);
        //#else
        //            return default;
        //#endif
        //        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        short IIoZeroSemaphore.ZeroTokenBump()
        {
#if TOKEN
            return (short)(Interlocked.Increment(ref _token) % ushort.MaxValue);
#else
            return default;
#endif
        }
        
        /// <summary>
        /// Are we zeroed out?
        /// </summary>
        /// <returns>True if zeroed, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Zeroed()
        {
            return _zeroRef == null || _zeroed > 0 || _asyncTasks.IsCancellationRequested;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsCancellationRequested()
        {
            return _zeroed > 0 || (_asyncTasks?.IsCancellationRequested?? true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ZeroThrow()
        {
            _error?.Throw();
        }

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