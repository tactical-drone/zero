//#define TOKEN

using System;
using System.Diagnostics;
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
            string description, 
            int maxBlockers = 1, 
            int initialCount = 0,
            uint maxAsyncWork = 0,
            bool enableAutoScale = false, bool enableFairQ = false, bool enableDeadlockDetection = false) : this()
        {
            _description = description;
            
            //validation
            if(maxBlockers < 1)
                throw new ZeroValidationException($"{Description}: invalid {nameof(maxBlockers)} = {maxBlockers} specified, value must be larger than 0");
            if(initialCount < 0)
                throw new ZeroValidationException($"{Description}: invalid {nameof(initialCount)} = {initialCount} specified, value may not be less than 0");

            if(initialCount > maxBlockers * 2)
                throw new ZeroValidationException($"{Description}: invalid {nameof(initialCount)} = {initialCount} specified, larger than {nameof(maxBlockers)} * 2 = {maxBlockers * 2}");

            _maxBlockers = maxBlockers;
            _useMemoryBarrier = enableFairQ;
            _maxAsyncWorkers = maxAsyncWork;
            _curSignalCount = initialCount;
            _zeroRef = null;
            _asyncToken = default;
            _asyncTokenReg = default;
            _enableAutoScale = enableAutoScale;
            
            if(_enableAutoScale)
                _lock = new SpinLock(enableDeadlockDetection);
            
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
        private uint _token;
#endif
        /// <summary>
        /// A semaphore description
        /// </summary>
        private readonly string _description;
        
        /// <summary>
        /// A semaphore description
        /// </summary>
        private string Description => $"{nameof(IoZeroSemaphore)}[{_description}]:  current = {_zeroRef.ZeroCount()}, qStatus = {_zeroRef.ZeroWaitCount()}/{_maxBlockers}";

        /// <summary>
        /// The maximum threads that can be blocked by this semaphore. This blocking takes storage
        /// and cannot be dynamically adjusted without adding massive runtime costs. Knowing and
        /// controlling this value upfront is the key
        /// </summary>
        private int _maxBlockers;

        /// <summary>
        /// Max number of async workers
        /// </summary>
        private readonly uint _maxAsyncWorkers;

        /// <summary>
        /// Current number of async workers
        /// </summary>
        private uint _curAsyncWorkerCount;

        /// <summary>
        /// The current available number of threads that can enter the semaphore without blocking 
        /// </summary>
        private int _curSignalCount;

        /// <summary>
        /// The current number of threads blocking on this semaphore
        /// </summary>
        private uint _curWaitCount;

        /// <summary>
        /// The number of threads that can enter the semaphore without blocking 
        /// </summary>
        public int ReadyCount => _zeroRef.ZeroCount();

        /// <summary>
        /// Nr of threads currently waiting on this semaphore
        /// </summary>
        public uint CurNrOfBlockers => _zeroRef.ZeroWaitCount();

        /// <summary>
        /// Maximum allowed concurrent "not inline" continuations before
        /// they become inline.
        /// </summary>
        public uint MaxAsyncWorkers => _maxAsyncWorkers;

        /// <summary>
        /// Maximum concurrent blockers this semaphore can accomodate. Each extra thread
        /// requires extra storage space for continuations.
        /// </summary>
        public int Capacity => _maxBlockers;

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
        private uint _head;

        /// <summary>
        /// A pointer to the tail of the Q
        /// </summary>
        private uint _tail;

        /// <summary>
        /// Whether this semaphore has been cleared out
        /// </summary>
        private int _zeroed;

        /// <summary>
        /// A wait sentinel
        /// </summary>
        private ValueTask<bool> _zeroWait;
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
            return  !_asyncToken.IsCancellationRequested && _zeroed == 0;
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
            //signal true on cancel so that threads can unwind
            if (_asyncToken.IsCancellationRequested || _zeroed > 0)
            {
                return true;
            }

            //enter or block?
            if (_zeroRef.ZeroCount() > 0)
            {
                if (_zeroRef.ZeroDecCount() >= 0)
                    return true;
                _zeroRef.ZeroIncCount();
            }
            return false;
        }

        /// <summary>
        /// Set signal handler
        /// </summary>
        /// <param name="continuation">The handler</param>
        /// <param name="state">The state</param>
        /// <param name="token">The safety token</param>
        /// <param name="flags">FLAGS</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public void OnCompleted(Action<object> continuation, object state, short token,
            ValueTaskSourceOnCompletedFlags flags)
        {
#if TOKEN
            if (_zeroRef.ZeroToken() != token)
                throw new ZeroValidationException($"{Description}: Invalid token: wants = {token}, has = {_zeroRef.ZeroToken()}");
#endif

            //fast path
            if (Signalled())
            {
                ZeroComply(continuation, state);
                return;
            }
            
            //lock
            ZeroLock();

            //choose a head index for blocking state
            var headIdx = (_zeroRef.ZeroNextHead() - 1) % _maxBlockers;
            Action<object> slot = null;

            //Did we win?
            while ( _zeroRef.ZeroWaitCount() < _maxBlockers && (slot = Interlocked.CompareExchange(ref _signalAwaiter[headIdx], continuation, null)) != null )
            {
                _signalAwaiter[headIdx] = slot;
                Thread.Yield();//The continuation executer wants this slot that we just put back, give it time to take it.

                headIdx = (_zeroRef.ZeroNextHead() - 1) % _maxBlockers;
            }
            
            if (slot == null)
            {
                _zeroRef.ZeroIncWait();
                _signalAwaiterState[headIdx] = state;

                ZeroUnlock();
            }
            else if (_enableAutoScale) //EXPERIMENTAL: double concurrent capacity
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
        /// <param name="zeroed">If we are zeroed</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining|MethodImplOptions.AggressiveOptimization)]
        private bool ZeroComply(Action<object> callback, object state, bool zeroed = false)
        {
            try
            {
                callback(state);
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

        /// <summary>
        /// Attempts to scale the semaphore to handle higher volumes of concurrency experienced. (for example if worker counts were tied to F(#CPUs))
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ZeroScale() //TODO, does this still work?
        {
            var acquiredLock = false;
            
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
            if (_zeroRef.ZeroTail() != _zeroRef.ZeroHead() || prevZeroState[_zeroRef.ZeroTail()] != null && prevZeroQ.Length == 1)
            {
                _signalAwaiter[0] = prevZeroQ[0];
                _signalAwaiterState[0] = prevZeroState[0];
                j = 1;
            }
            else
            {
                for (var i = _zeroRef.ZeroTail(); i != _zeroRef.ZeroHead() || prevZeroState[i] != null && j < _maxBlockers; i = (i + 1) % (uint)prevZeroQ.Length)
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
        }

        /// <summary>
        /// Allow waiter(s) to enter the semaphore
        /// </summary>
        /// <param name="releaseCount">The number of waiters to enter</param>
        /// <param name="async"></param>
        /// <returns>The number of signals sent, before this one, -1 on failure</returns>
        /// <exception cref="ZeroValidationException">Fails on preconditions</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<int> ReleaseAsync(int releaseCount = 1, bool async = false)
        {
            Debug.Assert(_zeroRef != null);

            //preconditions
            if(releaseCount < 1 || releaseCount - _zeroRef.ZeroCount() > _maxBlockers)
                throw new SemaphoreFullException($"{Description}: Invalid {nameof(releaseCount)} = {releaseCount} < 0 or  {nameof(_curSignalCount)}({releaseCount + _curSignalCount}) > {nameof(_maxBlockers)} = {_maxBlockers}");

            //fail fast on cancellation token
            if (_asyncToken.IsCancellationRequested || _zeroed > 0)
                return -1;
            
            //lock in return value
            var released = 0;

            IoZeroWorker worker = default;
            worker.Semaphore = _zeroRef;

            //awaiter entries
            while (released < releaseCount && _zeroRef.ZeroWaitCount() > 0)
            {
                //Lock
                ZeroLock();

                //choose a tail index to release
                var latchIdx = (_zeroRef.ZeroNextTail() -1) % _maxBlockers;
                var latch = _signalAwaiterState[latchIdx];

                ////latch a chosen tail state
                worker.State = Interlocked.CompareExchange(ref _signalAwaiterState[latchIdx], null, latch);
                
                //Did we loose?
                if (worker.State == null)
                {
                    //restore the tail
                    _zeroRef.ZeroPrevTail(); 

                    ZeroUnlock();

                    //try again
                    continue;
                }
                
                //count the number of overall waiters
                _zeroRef.ZeroDecWait();

                //latch a chosen tail
                while ((worker.Continuation =
                    Interlocked.CompareExchange(ref _signalAwaiter[latchIdx], null, _signalAwaiter[latchIdx])) == null) {}

                //unlock
                ZeroUnlock();
                
#if DEBUG
                //validate
                if (worker.State == null)
                    throw new ArgumentNullException($"-> {nameof(worker.State)}");
#endif

                //async workers
                var parallelized = false;
                switch (async && _zeroRef.ZeroAsyncCount() < _maxAsyncWorkers)
                {
                    case true when _zeroRef.ZeroAsyncCount() < _maxAsyncWorkers:
                        await Task.Factory.StartNew(static state =>
                            {
                                var (@this, worker) = (ValueTuple<IoZeroSemaphore, IoZeroWorker>)state;

                                try
                                {
                                    @this.ZeroComply(worker.Continuation, worker.State,
                                        worker.Semaphore.Zeroed() ||
                                        worker.State is IIoNanite nanite && nanite.Zeroed());
                                }
                                catch (Exception e)
                                {
                                    LogManager.GetCurrentClassLogger().Error(e,
                                        $@"{@this._description}: async callback failed!!! worker = {worker}, state = {worker.State}");
                                    return ValueTask.FromException(e);
                                }
                                finally
                                {
                                    worker.Semaphore.ZeroDecAsyncWait();
                                }

                                return ValueTask.CompletedTask;
                            }, ValueTuple.Create(this, worker),_asyncToken,
                            TaskCreationOptions.AttachedToParent | TaskCreationOptions.DenyChildAttach | TaskCreationOptions.PreferFairness,TaskScheduler.Default);

                        _zeroRef.ZeroIncAsyncWait();
                        parallelized = true;
                        break;
                    case true when (_maxAsyncWorkers > 0):
                        _zeroRef.ZeroDecAsyncWait();
                        break;
                }

                //synced workers
                if (!parallelized)
                {
                    if (!ZeroComply(worker.Continuation, worker.State, Zeroed() || worker.Semaphore.Zeroed() || worker.State is IIoNanite nanite && nanite.Zeroed()))
                    {
                        //update current count
                        _zeroRef.ZeroAddCount(releaseCount - released);
                        return -1;
                    }
                }

                //count the number of waiters released
                released++;
            }

            //update current count
            var delta = releaseCount - released;
            if(delta > 0)
                _zeroRef.ZeroAddCount(delta);
            
            //return previous number of waiters
            return released;
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

            //fast path
            if (Signalled())
                return new ValueTask<bool>(!_asyncToken.IsCancellationRequested && _zeroed == 0);
            
            //insane checks
            if (_zeroRef.ZeroWaitCount() >= _maxBlockers)
                return ValueTask.FromResult(false);
            
            //thread will attempt to block
            return _zeroWait;
        }

        /// <summary>
        /// returns the next tail
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        uint IIoZeroSemaphore.ZeroNextTail()
        {
            return _tail++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        uint IIoZeroSemaphore.ZeroNextHead()
        {
            return _head++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        uint IIoZeroSemaphore.ZeroPrevTail()
        {
            return _tail--;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        uint IIoZeroSemaphore.ZeroPrevHead()
        {
            return _head--;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        uint IIoZeroSemaphore.ZeroIncWait()
        {
            return _curWaitCount++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        uint IIoZeroSemaphore.ZeroDecWait()
        {

            return _curWaitCount--; 
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        uint IIoZeroSemaphore.ZeroWaitCount()
        {
            return _curWaitCount;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        uint IIoZeroSemaphore.ZeroIncAsyncWait()
        {
            return _curAsyncWorkerCount++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        uint IIoZeroSemaphore.ZeroDecAsyncWait()
        {
            return _curAsyncWorkerCount--;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        uint IIoZeroSemaphore.ZeroAsyncCount()
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
            return _curSignalCount++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IIoZeroSemaphore.ZeroDecCount()
        {
            return _curSignalCount--;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IIoZeroSemaphore.ZeroAddCount(int value)
        {
            return _curSignalCount += value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        uint IIoZeroSemaphore.ZeroHead()
        {
            return (uint)(_head % _maxBlockers);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        uint IIoZeroSemaphore.ZeroTail()
        {
            return (uint)(_tail % _maxBlockers);
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