//#define TOKEN //TODO this primitive does not work this way
using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore.core
{
    /// <summary>
    /// ZeroAsync Semaphore with strong order guarantees
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
        public IoZeroSemaphore(
            string description, 
            int maxBlockers = 1, 
            int initialCount = 0,
            int asyncWorkerCount = 0,
            bool enableAutoScale = false, bool enableFairQ = true, bool enableDeadlockDetection = false) : this()
        {
            _description = description;
            
            //validation
            if(maxBlockers < 1)
                throw new ZeroValidationException($"{_description}: invalid {nameof(maxBlockers)} = {maxBlockers} specified, value must be larger than 0");
            if(initialCount < 0)
                throw new ZeroValidationException($"{_description}: invalid {nameof(initialCount)} = {initialCount} specified, value may not be less than 0");
            if(asyncWorkerCount > maxBlockers)
                throw new ZeroValidationException($"{_description}: invalid {nameof(asyncWorkerCount)} = {asyncWorkerCount} specified, must less of equal to maxBlockers");

            _maxBlockers = maxBlockers;
            _useMemoryBarrier = enableFairQ;
            _maxAsyncWorkers = asyncWorkerCount;
            RunContinuationsAsynchronously = _maxAsyncWorkers > 0;
            _curSignalCount = initialCount;
            _zeroRef = null;
            _asyncTasks = default;
            _asyncTokenReg = default;
            _enableAutoScale = enableAutoScale;
            
            if(_enableAutoScale)
                _lock = new SpinLock(enableDeadlockDetection);
            
            _signalAwaiter = new Action<object>[_maxBlockers];
            _signalAwaiterState = new object[_maxBlockers];
            _signalExecutionState = new ExecutionContext[_maxBlockers];
            _signalCapturedContext = new object[_maxBlockers];

            _head = 0;
            _tail = 0;
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

        /// <summary>
        /// use memory barrier setting
        /// </summary>
        private readonly bool _useMemoryBarrier;

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
        private string Description => $"{nameof(IoZeroSemaphore)}[{_description}]:  ready = {_zeroRef.ZeroCount()}, wait = {_zeroRef.ZeroWaitCount()}/{_maxBlockers}, async = {_zeroRef.ZeroAsyncCount()}/{_maxAsyncWorkers}";

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
        internal readonly bool RunContinuationsAsynchronously;

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
        public int ReadyCount => _zeroRef.ZeroCount();

        /// <summary>
        /// Nr of threads currently waiting on this semaphore
        /// </summary>
        public int CurNrOfBlockers => _zeroRef.ZeroWaitCount();

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
        /// The cancellation token registration
        /// </summary>
        private CancellationTokenRegistration _asyncTokenReg;
        
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
        private long _head;

        /// <summary>
        /// A pointer to the tail of the Q
        /// </summary>
        private long _tail;

        /// <summary>
        /// Whether this semaphore has been cleared out
        /// </summary>
        private volatile int _zeroed;

        /// <summary>
        /// Used for locking
        /// </summary>
        internal static Action<object> ZeroSentinel;

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

            _asyncTokenReg = asyncTokenSource.Token.Register(s =>
            {
                var (z, _, r) = (ValueTuple<IIoZeroSemaphore,CancellationTokenSource,CancellationTokenRegistration>)s;
                z.ZeroSem();
#if NET6_0
                r.Unregister();
#endif
                r.Dispose();
            }, ValueTuple.Create(_zeroRef,_asyncTasks, _asyncTokenReg));
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
                if(_asyncTasks.Token.CanBeCanceled)
                    _asyncTasks.Cancel();
#if NET6_0
                _asyncTokenReg.Unregister();
#endif
                _asyncTokenReg.Dispose();
                _asyncTasks = null;
            }
            catch
            {
                // s
            }

            var i = 0;
            while (i < _signalAwaiter.Length)
            {
                var waiter = Interlocked.CompareExchange(ref _signalAwaiter[i], ZeroSentinel, _signalAwaiter[i]);
                if (waiter != null)
                {
                    var state = _signalAwaiterState[i];
                    var executionState = _signalExecutionState[i];
                    var context = _signalCapturedContext[i];

                    _signalAwaiterState[i] = null;
                    _signalExecutionState[i] = null;
                    _signalCapturedContext[i] = null;
                    Thread.MemoryBarrier();
                    _signalAwaiter[i] = null;

                    try
                    {
                        ZeroComply(waiter, state, executionState, context);
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

            _signalAwaiter = null;
            _signalAwaiterState = null;
            _signalExecutionState = null;
            _signalCapturedContext = null;

            _zeroRef = null;
        }

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
                return !(_zeroRef == null || _zeroRef.IsCancellationRequested() || _zeroRef.Zeroed());
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
                if (_zeroRef == null || _zeroRef.IsCancellationRequested() || _zeroRef.Zeroed())
                    return ValueTaskSourceStatus.Canceled;
            }
            catch
            {
                return ValueTaskSourceStatus.Canceled;
            }

            return ValueTaskSourceStatus.Pending;
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

#if DEBUG
            if (state == null)
                throw new ArgumentNullException(nameof(state));

            if (continuation == null)
                throw new ArgumentNullException(nameof(continuation));
#endif
            if (state == null)
                throw new ArgumentNullException(nameof(state));

            if (continuation == null)
                throw new ArgumentNullException(nameof(continuation));

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            void ExtractStack(out ExecutionContext ec, out object cc)
            {
                ec = null;
                cc = null;
                if ((flags & ValueTaskSourceOnCompletedFlags.FlowExecutionContext) != 0)
                    ec = ExecutionContext.Capture();

                if ((flags & ValueTaskSourceOnCompletedFlags.UseSchedulingContext) == 0) return;

                var sc  = SynchronizationContext.Current;
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

            //fast path, RACES with SetResult through _zeroRef. 
            int latch;
            if ((latch = _zeroRef.ZeroEnter()) > 0)
            {
                if (latch == 1 || Release(1, bestEffort: true) != 1)
                {
                    TaskScheduler cc = null;
                    if (TaskScheduler.Current != TaskScheduler.Default)
                        cc = TaskScheduler.Current;

                    InvokeContinuation(continuation, state, cc, false);

                    return;
                }
            }

            //lock
            ZeroLock();

            //choose a head index for blocking state
            var headIdx = (_zeroRef.ZeroNextHead() - 1) % _maxBlockers;
            Action<object> slot = null;

            //Did we win?
            while ( _zeroRef.ZeroWaitCount() < _maxBlockers && (slot = Interlocked.CompareExchange(ref _signalAwaiter[headIdx], ZeroSentinel, null)) != null)
            {
                if (slot == ZeroSentinel) continue;

                _zeroRef.ZeroPrevHead();
                headIdx = (_zeroRef.ZeroNextHead() - 1) % _maxBlockers;
            }
            
            if (slot == null)
            {
                ExtractStack(out var executionContext, out var capturedContext);
                Volatile.Write(ref _signalExecutionState[headIdx], executionContext);
                Volatile.Write(ref _signalCapturedContext[headIdx], capturedContext);
                Volatile.Write(ref _signalAwaiterState[headIdx], state);
                Volatile.Write(ref _signalAwaiter[headIdx], continuation);
                //_signalExecutionState[headIdx] = executionContext;
                //_signalCapturedContext[headIdx] = capturedContext;
                //_signalAwaiterState[headIdx] = state;
                //Thread.MemoryBarrier();
                //_signalAwaiter[headIdx] = continuation;

                _zeroRef.ZeroIncWait();

                ZeroUnlock();
                return;
            }

            _zeroRef.ZeroPrevHead();
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
                    throw new ZeroSemaphoreFullException($"{_description}: FATAL!, {nameof(_curWaitCount)} = {_zeroRef.ZeroWaitCount()}/{_maxBlockers}, {nameof(_curAsyncWorkerCount)} = {_zeroRef.ZeroAsyncCount()}/{_maxAsyncWorkers}");
                }
            }
        }

        /// <summary>
        /// Executes a worker
        /// </summary>
        /// <param name="callback">The callback</param>
        /// <param name="state">The state</param>
        /// <param name="capturedContext"></param>
        /// <param name="onComplete">Whether this call originates from <see cref="OnCompleted"/></param>
        /// <param name="zeroed">If we are zeroed</param>
        /// <param name="executionContext"></param>
        /// <param name="forceAsync">Forces async execution</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ZeroComply(Action<object> callback, object state, ExecutionContext executionContext, object capturedContext, bool zeroed = false)
        {
#if DEBUG
            //validate
            if (state == null)
                throw new ArgumentNullException($"-> {nameof(state)}");
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
                        var (@this, callback, state, capturedContext) = (ValueTuple<IoZeroSemaphore,Action<object>, object, object>)s;

                        @this.InvokeContinuation(callback, state, capturedContext, false);

                    }, ValueTuple.Create(_zeroRef, callback, state, capturedContext));
                    
                    return true;
                }

                InvokeContinuation(callback, state, capturedContext, false);
                return true;
            }
            catch (TaskCanceledException) {return true;}
            catch (Exception) when (zeroed){}
            catch (Exception e) when (!zeroed)
            {
                //LogManager.GetCurrentClassLogger().Error(e, $"{_description}: {nameof(ThreadPool.QueueUserWorkItem)}, {nameof(worker.Continuation)} = {worker.Continuation}, {nameof(worker.State)} = {worker.State}");
                throw IoNanoprobe.ZeroException.ErrorReport($"{nameof(ThreadPool.QueueUserWorkItem)}", 
                    $"{nameof(callback)} = {callback}, " +
                    $"{nameof(state)} = {state}",e);
            }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InvokeContinuation(Action<object> callback, object state, object capturedContext, bool forceAsync)
        {
            //TODO?
            if(callback == null || state == null)
                return;

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
                        else if (_zeroRef.ZeroIncAsyncCount() - 1 < _maxAsyncWorkers)
                        {
                            var asyncContinue = Task.Factory.StartNew(callback, state, CancellationToken.None,
                                TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);

                                asyncContinue.ContinueWith((_, zeroRef) => { ((IIoZeroSemaphore)zeroRef).ZeroDecAsyncCount(); }, _zeroRef);
                        }
                        else
                        {
                            _zeroRef.ZeroDecAsyncCount();
                            callback(state);
                        }
                    }
                    else
                    {
                        callback(state);
                    }

                    break;
                case SynchronizationContext sc:
                    sc.Post(static s =>
                    {
                        var tuple = (ValueTuple<Action<object>, object>)s;
                        tuple.Item1(tuple.Item2);
                    }, new ValueTuple<Action<object>, object>(callback, state));
                    break;

                case TaskScheduler ts:
                    Task.Factory.StartNew(callback, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ts);
                    break;
            }
        }

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
                if (_zeroRef.ZeroTail() != _zeroRef.ZeroHead() || prevZeroState[_zeroRef.ZeroTail()] != null && prevZeroQ.Length == 1)
                {
                    _signalAwaiter[0] = prevZeroQ[0];
                    _signalAwaiterState[0] = prevZeroState[0];
                    _signalExecutionState[0] = prevExecutionState[0];
                    _signalCapturedContext[0] = prevCapturedContext[0];
                    j = 1;
                }
                else
                {
                    for (var i = _zeroRef.ZeroTail(); i != _zeroRef.ZeroHead() || prevZeroState[i] != null && j < _maxBlockers; i = (i + 1) % prevZeroQ.Length)
                    {
                        _signalAwaiter[j] = prevZeroQ[i];
                        _signalAwaiterState[j] = prevZeroState[i];
                        _signalExecutionState[j] = prevExecutionState[i];
                        _signalCapturedContext[j] = prevCapturedContext[i];
                        j++;
                    }
                }

                //reset queue pointers
                _tail = 0;
                _head = j;

                
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

        /// <summary>
        /// Allow waiter(s) to enter the semaphore
        /// </summary>
        /// <param name="releaseCount">The number of waiters to enter</param>
        /// <param name="bestEffort">Whether this originates from <see cref="OnCompleted"/></param>
        /// <returns>The number of signals sent, before this one, -1 on failure</returns>
        /// <exception cref="ZeroValidationException">Fails on preconditions</exception>
        public int Release(int releaseCount = 1, bool bestEffort = false)
        {
            //fail fast on cancellation token
            if (_zeroRef == null || _zeroRef.IsCancellationRequested() || _zeroRef.Zeroed())
            {                
                return -1;
            }

            //preconditions
            if(releaseCount < 1 || releaseCount + _zeroRef.ZeroCount() > _maxBlockers)
                throw new SemaphoreFullException($"{Description}: Invalid {nameof(releaseCount)} = {releaseCount} < 0 or  {nameof(_curSignalCount)}({releaseCount + _zeroRef.ZeroCount()}) > {nameof(_maxBlockers)} = {_maxBlockers}");

            //lock in return value
            var released = 0;

            IoZeroWorker worker = default;
            worker.Semaphore = _zeroRef;

            //awaiter entries
            while (released < releaseCount && _zeroRef.ZeroWaitCount() > 0)
            {
                //Lock
                ZeroLock();

                var nextTail = _zeroRef.ZeroNextTail();

                if (nextTail < 0)
                    nextTail = 0;

                var latchIdx = nextTail - 1;
                var latchMod = latchIdx % _maxBlockers;
                var latch = Volatile.Read(ref _signalAwaiter[latchMod]);

                //latch a chosen head
                while (
                    _zeroRef.ZeroWaitCount() > 0 && latchIdx < _head &&
                    (latch == ZeroSentinel || latch != null && (worker.Continuation = Interlocked.CompareExchange(ref _signalAwaiter[latchMod], ZeroSentinel, latch)) != latch)
                )
                {
                    if (latch != ZeroSentinel)
                    {
                        _zeroRef.ZeroPrevTail();
                        nextTail = _zeroRef.ZeroNextTail();
                        latchIdx = nextTail - 1;
                        latchMod = latchIdx % _maxBlockers;
                    }
                    
                    latch = Volatile.Read(ref _signalAwaiter[latchMod]);
                }

                if (worker.Continuation == null || worker.Continuation == ZeroSentinel)
                {
                    _zeroRef.ZeroPrevTail();
                    continue;
                }

                _zeroRef.ZeroDecWait();

                //worker.State = Volatile.Read(ref _signalAwaiterState[latchMod]);
                //worker.ExecutionContext = Volatile.Read(ref _signalExecutionState[latchMod]);
                //worker.CapturedContext = Volatile.Read(ref _signalCapturedContext[latchMod]);

                Volatile.Write(ref worker.State, Volatile.Read(ref _signalAwaiterState[latchMod]));
                Volatile.Write(ref worker.ExecutionContext, Volatile.Read(ref _signalExecutionState[latchMod]));
                Volatile.Write(ref worker.CapturedContext, Volatile.Read(ref _signalCapturedContext[latchMod]));

                Volatile.Write(ref _signalAwaiterState[latchMod], null);
                Volatile.Write(ref _signalExecutionState[latchMod], null);
                Volatile.Write(ref _signalCapturedContext[latchMod], null);
                Volatile.Write(ref _signalAwaiter[latchMod], null);

                //_signalAwaiterState[latchIdx] = null;
                //_signalExecutionState[latchIdx] = null;
                //_signalCapturedContext[latchIdx] = null;
                //Thread.MemoryBarrier();
                //_signalAwaiter[latchIdx] = null;

                //unlock
                ZeroUnlock();

                if (!ZeroComply(worker.Continuation, worker.State, worker.ExecutionContext, worker.CapturedContext, Zeroed() || worker.Semaphore.Zeroed() || worker.State is IIoNanite nanite && nanite.Zeroed()))
                {
                    if(!bestEffort)
                        _zeroRef.ZeroAddCount(releaseCount - released);
                    
                    return -1;
                }

                //count the number of waiters released
                released++;
            }

            //update current count
            var delta = releaseCount - released;

            if(delta > 0 && !bestEffort)
                _zeroRef.ZeroAddCount(delta);
            
            //return the number of waiters released
            return released;
        }
        
        /// <summary>
        /// Waits on this semaphore
        /// </summary>
        /// <returns>The version number</returns>
        public ValueTask<bool> WaitAsync()
        {
            //insane checks
            if (_zeroRef == null || _zeroRef.ZeroWaitCount() >= _maxBlockers || _zeroRef.Zeroed())
                return new ValueTask<bool>(false);

            //fast path
            return _zeroRef.ZeroEnter() > 0 ? new ValueTask<bool>( true) : new ValueTask<bool>(_zeroRef, 23);
        }

        /// <summary>
        /// returns the next tail
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long IIoZeroSemaphore.ZeroNextTail()
        {
            return Interlocked.Increment(ref _tail);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long IIoZeroSemaphore.ZeroNextHead()
        {
            return Interlocked.Increment(ref _head);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long IIoZeroSemaphore.ZeroPrevTail()
        {
            return Interlocked.Decrement(ref _tail);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long IIoZeroSemaphore.ZeroPrevHead()
        {
            return Interlocked.Decrement(ref _head);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IIoZeroSemaphore.ZeroIncWait()
        {
            return Interlocked.Increment(ref _curWaitCount);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IIoZeroSemaphore.ZeroDecWait()
        {
            return Interlocked.Decrement(ref _curWaitCount);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IIoZeroSemaphore.ZeroWaitCount()
        {
            return _curWaitCount;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IIoZeroSemaphore.ZeroIncAsyncCount()
        {
            return Interlocked.Increment(ref _curAsyncWorkerCount);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IIoZeroSemaphore.ZeroDecAsyncCount()
        {
            return Interlocked.Decrement(ref _curAsyncWorkerCount);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IIoZeroSemaphore.ZeroAsyncCount()
        {
            return _curAsyncWorkerCount;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IIoZeroSemaphore.ZeroCount()
        {
            return _curSignalCount;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IIoZeroSemaphore.ZeroIncCount()
        {
            return Interlocked.Increment(ref _curSignalCount);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IIoZeroSemaphore.ZeroDecCount()
        {
            return Interlocked.Decrement(ref _curSignalCount);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IIoZeroSemaphore.ZeroEnter()
        {
            var slot = -1;
            var latch = _curSignalCount;

            while (latch > 0 && (slot = Interlocked.CompareExchange(ref _curSignalCount, latch - 1, latch)) != latch)
            {
                if (slot == 0)
                    return -1;

                latch = _curSignalCount;
            }

            if (slot > 0 && slot == latch)
                return latch;
            
            return -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IIoZeroSemaphore.ZeroAddCount(int value)
        {
            return Interlocked.Add(ref _curSignalCount, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long IIoZeroSemaphore.ZeroHead()
        {
            return _head % _maxBlockers;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long IIoZeroSemaphore.ZeroTail()
        {
            return _tail % _maxBlockers;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        short IIoZeroSemaphore.ZeroToken()
        {
#if TOKEN
            return (short)(_token % ushort.MaxValue);
#else
            return default;
#endif
        }
        
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
            return _zeroed > 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsCancellationRequested()
        {
            return _zeroed > 0 || (_asyncTasks?.IsCancellationRequested?? true);
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
            public IIoZeroSemaphore Semaphore;
            public Action<object> Continuation;
            public object State;
            public ExecutionContext ExecutionContext;
            public object CapturedContext;
        }
    }
}