
using System;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
using zero.core.misc;
using zero.core.patterns.misc;
using zero.core.runtime.scheduler;

namespace zero.core.patterns.semaphore.core
{
    /// <summary>
    /// DisposeAsync alloc semaphore with strong order guarantees
    /// 
    /// Experimental auto capacity scaling (disabled by default), set max count manually instead for max performance.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 64)]
    public struct IoZeroSemaphore<T> : IIoZeroSemaphoreBase<T>
    {
        static IoZeroSemaphore()
        {
            ZeroSentinel = CompletionSentinel;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="description">A description of this semaphore</param>
        /// <param name="maxBlockers">The maximum number of blockers this semaphore has capacity for</param>
        /// <param name="initialCount">The initial number of requests that will be non blocking</param>
        /// <param name="runContinuationsAsynchronously"></param>
        /// <param name="enableAutoScale">Experimental/dev: Cope with real time concurrency demand changes at the cost of undefined behavior in high GC pressured environments. DISABLE if CPU usage and memory snowballs and set <see cref="maxBlockers"/> more accurately instead. Scaling down is not supported</param>
        /// <param name="cancellationTokenSource">Optional cancellation source</param>
        public IoZeroSemaphore(string description,
            int maxBlockers = 1,
            int initialCount = 0,
            bool runContinuationsAsynchronously = false,
            bool enableAutoScale = false,
            CancellationTokenSource cancellationTokenSource = default) : this()
        {
#if DEBUG
            _description = description;
#else
            _description = string.Empty;
#endif

            //validation
            if (maxBlockers < 1)
                throw new ZeroValidationException($"{_description}: invalid {nameof(maxBlockers)} = {maxBlockers} specified, value must be larger than 0");

            if (maxBlockers > short.MaxValue)
                throw new ZeroValidationException($"{_description}: invalid {nameof(maxBlockers)} = {maxBlockers} specified, value must be smaller than {short.MaxValue}. see OnComplete framework limitation");

            if (initialCount < 0)
                throw new ZeroValidationException($"{_description}: invalid {nameof(initialCount)} = {initialCount} specified, value may not be less than 0");

            _maxBlockers = maxBlockers + 1;
            RunContinuationsAsynchronously = runContinuationsAsynchronously;
            _curSignalCount = initialCount;
            _zeroRef = null;
            _asyncTasks = cancellationTokenSource;
#if DEBUG
            _enableAutoScale = enableAutoScale;
            _lock = new SpinLock(true);
#endif
            _signalAwaiter = new Action<object>[_maxBlockers];
            _signalAwaiterState = new object[_maxBlockers];
            _signalExecutionState = new ExecutionContext[_maxBlockers];
            _signalCapturedContext = new object[_maxBlockers];
            _result = new T[_maxBlockers<<1];

            _curAsyncWorkerCount = 0;
            _curWaitCount = 0;
            _zeroed = 0;
            _error = default;

            _tail = 0;
            _head = 0;
            _ingressToken = 0;
            _egressToken = 0;
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
#endif
        #endregion

        #region properties

        //place this region fist for alignment
        #region aligned
        /// <summary>
        /// A pointer to the head of the Q
        /// </summary>
        private long _head;

        /// <summary>
        /// A pointer to the tail of the Q, separate from _head
        /// </summary>
        private long _tail;

        /// <summary>
        /// Token registry
        /// </summary>
        private long _ingressToken;

        public long IngressToken => _ingressToken % (_maxBlockers << 1);

        /// <summary>
        /// Token registry
        /// </summary>
        private long _egressToken;

        public long EgressToken => _egressToken % (_maxBlockers << 1);

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
        /// Whether this semaphore has been cleared out
        /// </summary>
        private volatile int _zeroed;

#if DEBUG
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

#else
        /// <summary>
        /// A queue of waiting continuations. The queue has strong order guarantees, FIFO
        /// </summary>
        private readonly Action<object>[] _signalAwaiter;

        /// <summary>
        /// Holds the state of a queued item
        /// </summary>
        private readonly object[] _signalAwaiterState;

        /// <summary>
        /// Holds the state of a queued item
        /// </summary>
        private readonly ExecutionContext[] _signalExecutionState;

        /// <summary>
        /// Holds the state of a queued item
        /// </summary>
        private readonly object[] _signalCapturedContext;

#endif


        #endregion

        /// <summary>
        /// A semaphore description
        /// </summary>
        private readonly string _description;
        
        /// <summary>
        /// A semaphore description
        /// </summary>
        public string Description => $"{nameof(IoZeroSemaphore<T>)}[{_description}]: z = {_zeroed > 0},  ready = {_curSignalCount}, wait = {_curWaitCount}/{_maxBlockers}, async = {_curAsyncWorkerCount}/{RunContinuationsAsynchronously}, head = {Head}/{Tail} (D:{Tail - Head})";

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
        public bool RunContinuationsAsynchronously { get; }


        /// <summary>
        /// The number of threads that can enter the semaphore without blocking 
        /// </summary>
        public int ReadyCount => _curSignalCount;

        /// <summary>
        /// Nr of threads currently waiting on this semaphore
        /// </summary>
        public int WaitCount => _curWaitCount;

        /// <summary>
        /// Maximum allowed concurrent "not inline" continuations before
        /// they become inline.
        /// </summary>
        public bool ZeroAsyncMode => RunContinuationsAsynchronously;

        /// <summary>
        /// Maximum concurrent blockers this semaphore can accomodate. Each extra thread
        /// requires extra storage space for continuations.
        /// </summary>
        public int Capacity => _maxBlockers;

        /// <summary>
        /// Allows for zero alloc <see cref="ValueTask"/> to be emitted. 
        /// </summary>
        private IIoZeroSemaphoreBase<T> _zeroRef;

        /// <summary>
        /// Where results are stored
        /// </summary>
        private readonly T[] _result;
        public long Tail => _tail;
        public long Head => _head;
        public long EgressCount => _egressToken % (_maxBlockers<<1);

        /// <summary>
        /// Used for locking
        /// </summary>
        internal static readonly Action<object> ZeroSentinel;

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
        /// <param name="primeResult"></param>
        /// <param name="context"></param>
        public IIoZeroSemaphoreBase<T> ZeroRef(ref IIoZeroSemaphoreBase<T> @ref, Func<object, T> primeResult,
            object context = null)
        {
            Interlocked.Exchange(ref _ingressToken, Math.Min(_curSignalCount, _maxBlockers << 1));

            for (var i = 0; i < _ingressToken; i++)
            {
                _result[i] = primeResult(context);
            }
            
            return _zeroRef = @ref;
        }

        /// <summary>
        /// zeroes out this semaphore
        /// </summary>
        public void ZeroSem()
        {
            if(Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return;

            Interlocked.Exchange(ref _curSignalCount, int.MinValue);
            Interlocked.Exchange(ref _curWaitCount, int.MinValue);
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
                if (waiter == latch && latch != null && latch != ZeroSentinel)
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
            
            _lock.Exit(true);
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
        public T GetResult(short token)
        {
            long idx = 0;
            try
            {
                idx = (Interlocked.Increment(ref _egressToken) - 1) % (_maxBlockers << 1);
                ZeroThrow();

                return _result[idx];
            }
            catch
            {
                return default;
            }
            finally
            {
                _result[idx] = default;
            }
        }

        /// <summary>
        /// Get the current status, which in this case will always be pending
        /// </summary>
        /// <param name="token">Not used</param>
        /// <returns><see cref="ValueTaskSourceStatus.Pending"/></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTaskSourceStatus GetStatus(short token) => ValueTaskSourceStatus.Pending;
        

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
            int r = 3;
            while (r-->0)
            {
                try
                {
#if DEBUG
                    ZeroLock();
#endif
                    Action<object> slot;

#if DEBUG
                    int c = 0;
#endif
                    long tailMod;
                    var race = false;

                    while ((slot = Interlocked.CompareExchange(ref _signalAwaiter[tailMod = Tail % _maxBlockers], ZeroSentinel, null)) != null || (race = tailMod != Tail % _maxBlockers))
                    {
                        if (race)
                        {
                            if (Interlocked.CompareExchange(ref _signalAwaiter[tailMod], null, ZeroSentinel) != ZeroSentinel)
                            {
                                LogManager.GetCurrentClassLogger().Fatal($"{nameof(OnCompleted)}: Unable to restore lock at head = {tailMod}, too {null}, cur = {_signalAwaiter[tailMod]}");
                            }
                        }
#if DEBUG
                        if (c++ > 500000)
                        {
                            Debug.Fail($"OnComplete lock spinning!!! {Description}");
                            throw new InternalBufferOverflowException($"OnComplete lock spinning!!! {Description}");
                        }
                            
#endif
                        if (_zeroed > 0) break;

                        //TODO: What is this?
                        //if (Tail % 2 == 0)
                        //    Interlocked.MemoryBarrierProcessWide();
                        //else
                        //    Interlocked.MemoryBarrier();

                        race = false;
                    }

                    if (slot == null)
                    {
                        //fast path, RACES with SetResult
                        if (_curWaitCount == 1)
                        {
                            //race for a waiter
                            var latch = _curSignalCount;
                            var s = -1;
                            while (latch > 0 && (s = Interlocked.CompareExchange(ref _curSignalCount, latch - 1, latch)) != latch)
                            {
                                if (_curSignalCount == 0)
                                {
                                    s = -1;
                                    break;
                                }

                                latch = _curSignalCount;
                            }

                            if (s >= 0)
                            {
                                if(Interlocked.CompareExchange(ref _curWaitCount, 0, 1) == 1)
                                {
                                    TaskScheduler cc = null;
                                    if (TaskScheduler.Current != TaskScheduler.Default) cc = TaskScheduler.Current;
                                    _signalAwaiter[tailMod] = null;
                                    InvokeContinuation(continuation, state, cc);
                                    return;
                                }
                                Interlocked.Increment(ref _curSignalCount); //dine on deadlock
                            }
                        }

                        _signalAwaiter[tailMod] = continuation;
                        _signalAwaiterState[tailMod] = state;
                        ExtractContext(out _signalExecutionState[tailMod], out _signalCapturedContext[tailMod], flags);
                        Interlocked.MemoryBarrier();
                        Interlocked.Increment(ref _tail);

                        return;
                    }
#if DEBUG
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
                            return;
                        }

                        throw new ZeroValidationException($"{_description}: FATAL!, {nameof(_curWaitCount)} = {_curWaitCount}/{_maxBlockers}, {nameof(_curAsyncWorkerCount)} = {_curAsyncWorkerCount}");
                    }
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

            if(!Zeroed())
                throw new ZeroValidationException($"{nameof(OnCompleted)}: Invalid state! Concurrency bug. Too many blockers... {Description}");
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
                            var (@this, callback, state, capturedContext, forceAsync) =
                                (ValueTuple<IoZeroSemaphore<T>, Action<object>, object, object, bool>)s;
                            @this.InvokeContinuation(callback, state, capturedContext,forceAsync);
                        }, (_zeroRef, callback, state, capturedContext, forceAsync));

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
        private void InvokeContinuation(Action<object> callback, object state, object capturedContext, bool forceAsync = false)
        {
            Debug.Assert(callback != null && state != null);
            switch (capturedContext)
            {
                case null:
                    if (forceAsync | RunContinuationsAsynchronously)
                    {
#if ZERO_CORE
                        _ = Task.Factory.StartNew(callback, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
#else
                        if (!ThreadPool.UnsafeQueueUserWorkItem(static delegate (object s)
                            {
                                var (callback, state) = (ValueTuple<Action<object>, object>)s;
                                try
                                {
                                    callback(state);
                                }
                                catch(Exception e)
                                {
                                    LogManager.GetCurrentClassLogger().Trace(e);
                                }
                            }, (callback, state)))
                        {
                            _ = Task.Factory.StartNew(callback, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
                        };
#endif
                    }
                    else
                    {
                        callback(state);
                    }

                    break;
                case SynchronizationContext sc:
#pragma warning disable VSTHRD001 // Avoid legacy thread switching APIs
                    sc.Post(static s =>
                    {
                        var tuple = ((Action<object>, object))s!;
                        try
                        {
                            tuple.Item1(tuple.Item2);
                        }
                        catch (Exception e)
                        {
                            LogManager.GetCurrentClassLogger().Error(e, $"InvokeContinuation.callback(): {tuple.Item2} ");
                        }
                    }, (callback, state));
#pragma warning restore VSTHRD001 // Avoid legacy thread switching APIs
                    break;
                case IoZeroScheduler zs:
                    //async
                    if (forceAsync | RunContinuationsAsynchronously)
                    {
                        IoZeroScheduler.Zero.QueueCallback(callback, state);
                    }
                    else //sync
                    {
                        callback(state);
                    }
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
                Volatile.Write(ref _signalAwaiter, new Action<object>[_maxBlockers]);
                Volatile.Write(ref _signalAwaiterState, new object[_maxBlockers]);
                Volatile.Write(ref _signalExecutionState , new ExecutionContext[_maxBlockers]);
                Volatile.Write(ref _signalCapturedContext, new object[_maxBlockers]); 

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
        /// <param name="forceAsync">non blocking if true</param>
        /// <returns>The number of waiters released, -1 on failure</returns>
        /// <exception cref="SemaphoreFullException">Fails when maximum waiters reached</exception>
        public int Release(int releaseCount = 1, bool forceAsync = false)
        {
            //lock in return value
            var released = 0;

            //release waiters
            while (released < releaseCount && _curWaitCount > 0 && !Zeroed())
            {
                IoZeroWorker worker = default;
                worker.Continuation = null;
#if DEBUG
                ZeroLock();
#endif
                long head;
                long headMod = -1;
                Action<object> latched;
#if DEBUG
                var c = 0;
#endif
                while ((head = Head) == Tail || //race
                       (latched = _signalAwaiter[headMod = head % _maxBlockers]) == null || 
                       latched == ZeroSentinel || //or bad latches
                       head != Head || //head has moved since
                       (worker.Continuation = Interlocked.CompareExchange(ref _signalAwaiter[headMod], ZeroSentinel, latched)) != latched|| //or race for latch failed
                        worker.Continuation == ZeroSentinel) // or race
                {
                    worker.Continuation = null;//retry
#if DEBUG
                    if (c++ > 500000)
                        throw new InternalBufferOverflowException($"spin fail {Description}");
#endif
                    if (Zeroed() || Head == Tail && _signalAwaiter[Head % _maxBlockers] == null)
                        break;
                }

                //nothing to release, possible teardown state
                if (worker.Continuation == null)
                    continue;
                
                worker.State = _signalAwaiterState[headMod];
                _signalAwaiterState[headMod] = null;
                worker.ExecutionContext = _signalExecutionState[headMod];
                _signalExecutionState[headMod] = null;
                worker.CapturedContext = _signalCapturedContext[headMod];
                _signalCapturedContext[headMod] = null;
                _signalAwaiter[headMod] = null;

                Interlocked.Decrement(ref _curWaitCount);
                Interlocked.MemoryBarrier();
                Interlocked.Increment(ref _head);
#if DEBUG
                //unlock
                ZeroUnlock();
#endif
                //execute continuation
                if (!ZeroComply(worker.Continuation, worker.State, worker.ExecutionContext, worker.CapturedContext,
                        worker.State is IIoNanite nanite && nanite.Zeroed(), forceAsync))
                {
                    Interlocked.Add(ref _curSignalCount, releaseCount - released);
                    return -1;
                }

                released++;
            }

            Interlocked.Add(ref _curSignalCount, releaseCount - released);

            return released;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T value, int releaseCount, bool forceAsync = false)
        {
            Debug.Assert(value != null);
            //overfull semaphore exit here
            if (_curSignalCount + releaseCount > _maxBlockers && _ingressToken - _egressToken > _maxBlockers)
                return Release(releaseCount, true);

            for (var i = 0; i < releaseCount; i++)
            {
                if (_curSignalCount > _maxBlockers << 1)
                    break;

                _result[(Interlocked.Increment(ref _ingressToken) - 1) % (_maxBlockers << 1)] = value;
            }
            return Release(releaseCount);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T value, bool forceAsync = false)
        {
            Debug.Assert(value != null);
            //overfull semaphore exit here
            if (_curSignalCount + 1 > _maxBlockers && _ingressToken - _egressToken > _maxBlockers)
                return Release(1, true);

            _result[(Interlocked.Increment(ref _ingressToken) - 1) % (_maxBlockers << 1)] = value;
            return Release(1,forceAsync);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(T[] value, bool forceAsync = false)
        {
            Debug.Assert(value is { Length: > 0 });
            var releaseCount = value.Length;

            //overfull semaphore exit here
            if (_curSignalCount + releaseCount > _maxBlockers && _ingressToken - _egressToken > _maxBlockers)
                return Release(releaseCount, true);

            for (int i = 0; i < releaseCount; i++)
            {
                if (_curSignalCount > _maxBlockers << 1)
                    break;

                _result[(Interlocked.Increment(ref _ingressToken) - 1) % (_maxBlockers << 1)] = value[i];
            }
            return Release(releaseCount, forceAsync);
        }

        /// <summary>
        /// Waits on this semaphore
        /// </summary>
        /// <returns>True if waiting, false otherwise. If the semaphore is awaited on more than <see cref="_maxBlockers"/>, false is returned</returns>
        public ValueTask<T> WaitAsync()
        {
            var slot = -1;
            int latch = 0;
            //reserve a signal if set
            while (_curWaitCount == 0 && (latch = _curSignalCount) > 0 &&
                   (slot = Interlocked.CompareExchange(ref _curSignalCount, latch - 1, latch)) != latch)
            {
                if (Zeroed())
                    return default;

                slot = -1;
            }

            //>>> FAST PATH on set
            if (slot == latch)
                return new ValueTask<T>(GetResult(0));
            
            if(_curWaitCount < _maxBlockers && Interlocked.Increment(ref _curWaitCount) <= _maxBlockers)
                return new ValueTask<T>(_zeroRef, 0);

            throw new ZeroValidationException($"WAIT: Concurrency bug, Semaphore full! {Description}");
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
        int IIoZeroSemaphoreBase<T>.ZeroDecAsyncCount()
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
            //return _zeroed > 0 || _asyncTasks.IsCancellationRequested;
            return _zeroed > 0;
        }

        public void DecWaitCount()
        {
            throw new NotImplementedException();
        }

        public void IncWaitCount()
        {
            throw new NotImplementedException();
        }

        public void IncReadyCount()
        {
            throw new NotImplementedException();
        }

        public void DecReadyCount()
        {
            throw new NotImplementedException();
        }

        public void IncCur()
        {
            throw new NotImplementedException();
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
            return $"{_description}: sync = {_curWaitCount}/{_maxBlockers}, async = {_curAsyncWorkerCount}/{RunContinuationsAsynchronously}, ready = {_curSignalCount}";
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