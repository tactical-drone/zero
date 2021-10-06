using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore.core
{
    /// <summary>
    /// Zero Semaphore with experimental auto capacity scaling (disabled by default), set max count manually instead.
    ///
    /// When scaling is enabled there is a slight performance hit
    /// </summary>
    public struct IoZeroSemaphore : IIoZeroSemaphore
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="description">A description of this mutex</param>
        /// <param name="maxBlockers"> The maximum number of requests for the semaphore that can be granted concurrently.</param>
        /// <param name="initialCount">The initial number non blocking requests</param>
        /// <param name="maxAsyncWork"></param>
        /// <param name="enableAutoScale">Experimental: Cope with real time concurrency demands at the cost of undefined behavior in high GC pressured environments. DISABLE if CPU usage snowballs and set <see cref="maxBlockers"/> more accurately to compensate</param>
        /// <param name="enableFairQ">Enable fair queueing at the cost of performance</param>
        /// <param name="enableDeadlockDetection">Checks for deadlocks within a thread and throws when found</param>
        public IoZeroSemaphore(
            string description = "", 
            int maxBlockers = 1, 
            int initialCount = 0,
            int maxAsyncWork = 0,
            bool enableAutoScale = false, bool enableFairQ = false, bool enableDeadlockDetection = false) : this()
        {
            _description = description;
            
            //validation
            if(maxBlockers < 1)
                throw new ZeroValidationException($"{Description}: invalid {nameof(maxBlockers)} = {maxBlockers} specified, value must be larger than 0");
            if(initialCount < 0)
                throw new ZeroValidationException($"{Description}: invalid {nameof(initialCount)} = {initialCount} specified, value may not be less than 0");

            //if(initialCount > maxBlockers)
            //    throw new ZeroValidationException($"{Description}: invalid {nameof(initialCount)} = {initialCount} specified, larger than {nameof(maxBlockers)} = {maxBlockers}");

            _maxBlockers = maxBlockers;
            _useMemoryBarrier = enableFairQ;
            _maxAsyncWorkers = maxAsyncWork;
            _signalCount = initialCount;
            _zeroRef = null;
            _asyncToken = default;
            _asyncTokenReg = default;
            _enableAutoScale = enableAutoScale;
            
            if(_enableAutoScale)
                _lock = new SpinLock(enableDeadlockDetection);

            //_latched = o => { };
            
            _signalAwaiter = new Action<object>[_maxBlockers];
            _signalAwaiterState = new object[_maxBlockers];
            _head = 0;
            _tail = 0;
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
        private short _currentToken;
#endif

        /// <summary>
        /// A semaphore description
        /// </summary>
        private readonly string _description;
        
        /// <summary>
        /// A semaphore description
        /// </summary>
        private string Description => $"{nameof(IoZeroSemaphore)}[{_description}]:  current = {_signalCount}, qSize = {_maxBlockers}, h = {_head}, t = {_tail}";

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
        /// Current number of async workers
        /// </summary>
        private volatile int _asyncWorkerCount;

        /// <summary>
        /// The number of threads that can enter the semaphore without blocking 
        /// </summary>
        private volatile int _signalCount;

        /// <summary>
        /// The number of threads that can enter the semaphore without blocking 
        /// </summary>
        public int ReadyCount => _signalCount;

        /// <summary>
        /// Nr of threads currently blocking on this
        /// </summary>
        public uint NrOfBlockers => _waitCount;
        
        /// <summary>
        /// Allows for zero alloc <see cref="ValueTask"/> to be emitted
        /// </summary>
        private IIoZeroSemaphore _zeroRef;

        /// <summary>
        /// The cancellation token
        /// </summary>
        private CancellationToken _asyncToken;
        
        /// <summary>
        /// The cancellation token registration
        /// </summary>
        private CancellationTokenRegistration _asyncTokenReg;
        
        /// <summary>
        /// A "queue" of waiting continuations. The queue has weak order guarantees, but is mostly FIFO 
        /// </summary>
        private Action<object>[] _signalAwaiter;

        /// <summary>
        /// Holds the state of a queued item
        /// </summary>
        private object[] _signalAwaiterState;
        
        /// <summary>
        /// A pointer to the head of the Q
        /// </summary>
        private volatile uint _head;

        /// <summary>
        /// A pointer to the tail of the Q
        /// </summary>
        private volatile uint _tail;

        /// <summary>
        /// Whether this semaphore has been cleared out
        /// </summary>
        private volatile int _zeroed;
        
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
        /// Set ref to this and register cancellation
        /// </summary>
        /// <param name="ref">The ref to this</param>
        /// <param name="asyncToken">The cancellation token</param>
        public void ZeroRef(ref IIoZeroSemaphore @ref, CancellationToken asyncToken)
        {
            _zeroRef = @ref;
            _zeroWait = new ValueTask<bool>(_zeroRef, 23);
            _asyncToken = asyncToken;

            _asyncTokenReg = asyncToken.Register(s =>
            {
                var (z, t, r) = (Tuple<IIoZeroSemaphore,CancellationToken,CancellationTokenRegistration>)s;
                z.Zero();
                r.Unregister();
                r.Dispose();
            }, Tuple.Create(_zeroRef,_asyncToken, _asyncTokenReg));
        }

        /// <summary>
        /// zeroes out this semaphore
        /// </summary>
        public void Zero()
        {
            if(Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return;
            
            try
            {
                _asyncTokenReg.Unregister();
            }
            catch
            {
                // s
            }

            var i = 0;
            while (i < _signalAwaiter.Length)
            {
                var state = Interlocked.CompareExchange(ref _signalAwaiterState[i], null, _signalAwaiterState[i]);
                var waiter = Interlocked.CompareExchange(ref _signalAwaiter[i], null, _signalAwaiter[i]);
                if (waiter != null)
                {
                    try
                    {
                        waiter(state);
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

            //tweak thread priority
            if (_lock.IsHeld)
                Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;

            _lock.Enter(ref _);

            //tweak thread priority
            if (Thread.CurrentThread.Priority != ThreadPriority.Normal)
                Thread.CurrentThread.Priority = ThreadPriority.Normal;
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

        /// <summary>
        /// A wait sentinel
        /// </summary>
        private ValueTask<bool> _zeroWait;

        /// <summary>
        /// Number of threads currently waiting on this semaphore
        /// </summary>
        private volatile uint _waitCount;

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
            if (_currentToken != token)
                throw new ZeroValidationException($"{Description}: Invalid token: wants = {token}, has = {_currentToken}");

            _currentToken++;
#endif
            var res = !_asyncToken.IsCancellationRequested && _zeroed == 0;
            return res;
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
            if (_currentToken != token)
                throw new ZeroValidationException($"{Description}: Invalid token: wants = {token}, has = {_currentToken}");
#endif

            if (_asyncToken.IsCancellationRequested || _zeroed > 0)
                return ValueTaskSourceStatus.Canceled;

            return ValueTaskSourceStatus.Pending;
        }

        /// <summary>
        /// Determines if the semaphore can be entered without blocking
        /// </summary>
        /// <returns>true if fast path is available, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool Signalled()
        {
            //signal on cancel
            if (_asyncToken.IsCancellationRequested || _zeroed > 0)
            {
                return true;
            }

            if (_signalCount > 0)
            {
                if (Interlocked.Decrement(ref _signalCount) >= 0)
                    return true;
                Interlocked.Increment(ref _signalCount);
            }
            return false;

            //race for fast path
            //var released = 0;
            //ulong spinCount = 0;
            //var lockedCount = _signalCount;
            //while (lockedCount > 0 &&
            //       (released = Interlocked.CompareExchange(ref _signalCount, lockedCount - 1, lockedCount)) != lockedCount)
            //{
            //    //Will this ever happen?
            //    if (spinCount > MaxSpin)
            //        Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;

            //    lockedCount = _signalCount;
            //}

            ////restore thread priority
            //if (Thread.CurrentThread.Priority != ThreadPriority.Normal)
            //    Thread.CurrentThread.Priority = ThreadPriority.Normal;

            ////Did we win?
            //return lockedCount > 0 && released == lockedCount;
        }

        /// <summary>
        /// Set signal handler
        /// </summary>
        /// <param name="continuation">The handler</param>
        /// <param name="state">The state</param>
        /// <param name="token">The safety token</param>
        /// <param name="flags">FLAGS</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted(Action<object> continuation, object state, short token,
            ValueTaskSourceOnCompletedFlags flags)
        {
#if TOKEN
            if (_currentToken != token)
                throw new ZeroValidationException($"{Description}: Invalid token: wants = {token}, has = {_currentToken}");
#endif

            //fast path
            if (Signalled() || _zeroed == 1 || _asyncToken.IsCancellationRequested)
            {
                continuation(state);
                return;
            }
            
            //lock
            ZeroLock();
            
            //choose a head
            
            
            var c = 0;
            //while (Interlocked.CompareExchange(ref _signalAwaiter[head], _latched, null) != null && c++ < _maxBlockers)
            //{
            //    head = (Interlocked.Increment(ref _head) - 1) % _maxBlockers;
            //}

            var headIdx = (Interlocked.Increment(ref _head) - 1) % _maxBlockers;
            var slot = Interlocked.CompareExchange(ref _signalAwaiter[headIdx], continuation, null);

            while (slot != null && c < _maxBlockers)
            {
                Volatile.Write(ref _signalAwaiter[headIdx], slot);
                Interlocked.Decrement(ref _head);
                headIdx = (Interlocked.Increment(ref _head) - 1) % _maxBlockers;
                slot = Interlocked.CompareExchange(ref _signalAwaiter[headIdx], continuation, null);
                c++;
            }

            //Did we win?
            if(slot == null) 
            {
                while (Interlocked.CompareExchange(ref _signalAwaiterState[headIdx], state, null) != null)
                {
                    //keep trying, the tail will prevent deadlock
                }

                //release lock
                ZeroUnlock();
            }
            else //if(_enableAutoScale) //EXPERIMENTAL: double concurrent capacity
            {
                Volatile.Write(ref _signalAwaiter[headIdx], slot);
                Interlocked.Decrement(ref _head);

                //release lock
                ZeroUnlock();
                
                //Scale
                if (_enableAutoScale)
                {
                    ZeroScale();
                    OnCompleted(continuation, state, token, flags);
                }
                // else if (Interlocked.Decrement(ref _manifold) > 0) 
                // {
                //     Thread.Yield();
                //     OnCompleted(continuation, state, token, flags);
                // }
                else
                {
                    //throw new ZeroValidationException($"{Description}: semaphore DRAINED!!! {_waitCount}/{_maxBlockers} w[{headIdx % _maxBlockers}] = {_signalAwaiter[headIdx]} : {_signalAwaiterState[headIdx]}");
                    throw new ZeroSemaphoreFullException($"{_description}: FATAL!, {nameof(_waitCount)} = {_waitCount}/{_maxBlockers}, {nameof(_asyncWorkerCount)} = {_asyncWorkerCount}/{_maxAsyncWorkers}");
                }
            }
        }
        
        /// <summary>
        /// Attempts to scale the semaphore to handle higher volumes of concurrency experienced. (for example if worker counts were tied to F(#CPUs))
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ZeroScale()
        {
            var acquiredLock = false;
            
            //Acquire lock and disable GC
            while (!acquiredLock)
            {
                _lock.Enter(ref acquiredLock);

                try
                {
                    GC.TryStartNoGCRegion(250 / 2 * 1024 * 1024, false);
                }
                catch
                {
                    // ignored
                }

                if (Thread.CurrentThread.Priority == ThreadPriority.Normal)
                    Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
            }

            Thread.CurrentThread.Priority = ThreadPriority.Highest;

            //double the q
            var prevZeroQ = _signalAwaiter;
            var prevZeroState = _signalAwaiterState;
            
            //allocate memory
            _maxBlockers *= 2;
            _signalAwaiter = new Action<object>[_maxBlockers];
            _signalAwaiterState = new object[_maxBlockers];
            
            //copy Q
            
            var j = (uint)0;
            //special zero case
            if (_tail != _head || prevZeroState[_tail] != null && prevZeroQ.Length == 1)
            {
                _signalAwaiter[0] = prevZeroQ[0];
                _signalAwaiterState[0] = prevZeroState[0];
                j = 1;
            }
            else
            {
                for (var i = _tail; i != _head || prevZeroState[i] != null && j < _maxBlockers; i = (i + 1) % (uint)prevZeroQ.Length)
                {
                    _signalAwaiter[j] = prevZeroQ[i];
                    _signalAwaiterState[j] = prevZeroState[i];
                    j++;
                }    
            }
            
            //reset queue pointers
            _tail = 0;
            _head = j;

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

            Thread.CurrentThread.Priority = ThreadPriority.Normal;

            Console.WriteLine($"Scaled {Description}");
        }

        /// <summary>
        /// Allow waiter(s) to enter the semaphore
        /// </summary>
        /// <param name="releaseCount">The number of waiters to enter</param>
        /// <param name="async"></param>
        /// <returns>The number of signals sent, before this one, -1 on failure</returns>
        /// <exception cref="ZeroValidationException">Fails on preconditions</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(int releaseCount = 1, bool async = false)
        {
#if DEBUG
            if(_zeroRef == null)
                throw new NullReferenceException($"{nameof(_zeroRef)}: BUG!!! Value cannot be null, {nameof(ZeroRef)} was not set!!!");
#endif
            //preconditions
            if(releaseCount < 1 || releaseCount + _signalCount > _maxBlockers)
                throw new ZeroValidationException($"{Description}: Invalid {nameof(releaseCount)} = {releaseCount} < 0 or  {nameof(_signalCount)}({releaseCount + _signalCount}) > {nameof(_maxBlockers)}({_maxBlockers})");

            //if(releaseCount < 1 )
            //    throw new ZeroValidationException($"{Description}: Invalid {nameof(releaseCount)} = {releaseCount} < 0");

            //fail fast on cancellation token
            if (_asyncToken.IsCancellationRequested || _zeroed > 0)
                return -1;
            
            //lock in return value
            var released = 0;
            var c = 0;

            IoZeroWorker worker;
            worker.Semaphore = _zeroRef;

            //awaiter entries
            while (released < releaseCount && c++ < _maxBlockers)
            {
                //Lock
                ZeroLock();

                //choose a tail index
                var origTail = Interlocked.Increment(ref _tail);
                var tailIdx = (origTail - 1) % _maxBlockers;

                ////latch a chosen tail state
                worker.State = Interlocked.CompareExchange(ref _signalAwaiterState[tailIdx], null, _signalAwaiterState[tailIdx]);

                //Did we loose?
                if (worker.State == null)
                {
                    //restore the latch
                    var reset =  origTail == Interlocked.Decrement(ref _tail);

                    ZeroUnlock();

                    //abort
                    if (reset)
                        break;

                    //try again
                    continue;
                }

                //latch a chosen tail
                //var targetWaiter = _signalAwaiter[tailIdx];
                worker.Continuation = Interlocked.CompareExchange(ref _signalAwaiter[tailIdx], null, _signalAwaiter[tailIdx]);
                if (worker.Continuation == null)
                {
                    //prevent deadlock
                    Volatile.Write(ref _signalAwaiterState[tailIdx], worker.State);

                    //restore the latch
                    Interlocked.Decrement(ref _tail);

                    ZeroUnlock();

                    //try again
                    continue;
                }
                
                Volatile.Write(ref _signalAwaiter[tailIdx], null);
                //_signalAwaiter[tailIdx] = null;
#if DEBUG
                //validate
                if (worker.State == null)
                    throw new ArgumentNullException($"-> {nameof(worker.State)}");
#endif
                
                //release the lock
                ZeroUnlock();
                
                //count the number of overall waiters
                Interlocked.Decrement(ref _waitCount);

                //async workers
                var parallelized = false;
                if (async && Interlocked.Increment(ref _asyncWorkerCount) < _maxAsyncWorkers)
                {
                    ThreadPool.QueueUserWorkItem(ioZeroWorker =>
                    {
                        var nanite = ioZeroWorker.Continuation.Target as IoNanoprobe;
                        try
                        {
                            //execute continuation
                            if(nanite!=null && !nanite.Zeroed())
                                ioZeroWorker.Continuation(ioZeroWorker.State);
                            else
                                ioZeroWorker.Continuation(ioZeroWorker.State);
                        }
                        catch (NullReferenceException e) when (nanite == null && !ioZeroWorker.Semaphore.Zeroed() ||
                                                               nanite != null && nanite.Zeroed())
                        {
                            throw IoNanoprobe.ZeroException.ErrorReport($"{nameof(ThreadPool.QueueUserWorkItem)}", 
                                $"{nameof(ioZeroWorker.Continuation)} = {ioZeroWorker.Continuation}, " +
                                $"{nameof(ioZeroWorker.State)} = {ioZeroWorker.State}", e);
                        }
                        catch (Exception e) when (nanite == null && !ioZeroWorker.Semaphore.Zeroed() ||
                                                 nanite != null && nanite.Zeroed())
                        {
                            throw IoNanoprobe.ZeroException.ErrorReport($"{nameof(ThreadPool.QueueUserWorkItem)}", 
                                $"{nameof(ioZeroWorker.Continuation)} = {ioZeroWorker.Continuation}, " +
                                $"{nameof(ioZeroWorker.State)} = {ioZeroWorker.State}",e);
                        }
                    }, worker, false);
                    parallelized = true;
                }
                else
                {
                    if( async) Interlocked.Decrement(ref _asyncWorkerCount);
                }
                
                //sync workers
                if(!parallelized)
                {
                    var nanite = worker.Continuation.Target as IoNanoprobe;
                    try
                    {
                        //execute continuation
                        if(nanite!=null && !nanite.Zeroed())
                            worker.Continuation(worker.State);
                        else
                            worker.Continuation(worker.State);
                    }
                    catch (NullReferenceException e) when (nanite == null && _zeroed == 0 ||
                                                           nanite != null && nanite.Zeroed())
                    {
                        throw IoNanoprobe.ZeroException.ErrorReport(this, 
                                $"{nameof(worker.Continuation)} = {worker.Continuation}, " +
                                $"{nameof(worker.State)} = {worker.State}", e);
                    }
                    catch (Exception e) when (nanite == null && _zeroed == 0 ||
                                             nanite != null && nanite.Zeroed())
                    {
                        throw IoNanoprobe.ZeroException.ErrorReport(this, 
                            $"{nameof(worker.Continuation)} = {worker.Continuation}, " +
                            $"{nameof(worker.State)} = {worker.State}",e);
                    }
                }

                //count the number of waiters released
                released++;
            }

            //update current count
            Interlocked.Add(ref _signalCount, releaseCount - released);
            
            //return previous number of waiters
            return _signalCount;
        }
        
        /// <summary>
        /// Waits on this semaphore
        /// </summary>
        /// <returns>The version number</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> WaitAsync()
        { 
            //fail fast 
            if (_asyncToken.IsCancellationRequested || _zeroed > 0)
                return ValueTask.FromResult(false);

            if (_waitCount >= _maxBlockers)
                throw new ZeroSemaphoreFullException($"{_description}: FATAL!, {nameof(_waitCount)} = {_waitCount}/{_maxBlockers}, {nameof(_asyncWorkerCount)} = {_asyncWorkerCount}/{_maxAsyncWorkers}");

            if (Signalled())
            {
                var ret = !_asyncToken.IsCancellationRequested && _zeroed == 0;
                if (ret == false)
                {
                    return new ValueTask<bool>(ret);
                }
                return new ValueTask<bool>(ret);
            }
            
            Interlocked.Increment(ref _waitCount);
            return _zeroWait;
        }
        
        /// <summary>
        /// Increment free worker threads by 1
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SignalWorker()
        {
            Interlocked.Decrement(ref _asyncWorkerCount);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Zeroed()
        {
            return _zeroed > 0;
        }

        /// <summary>
        /// Worker info
        /// </summary>
        struct IoZeroWorker 
        {
            public IIoZeroSemaphore Semaphore;
            public Action<object> Continuation;
            public object State;
        }
    }
}