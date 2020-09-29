using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore.core
{
    /// <summary>
    /// Zero Semaphore with experimental auto capacity scaling (disabled by default), set capacity manually instead.
    /// </summary>
    public struct IoZeroSemaphore : IIoZeroSemaphore
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="description">A description of this mutex</param>
        /// <param name="maxCount">The maximum number of requests for the semaphore that can be granted concurrently.</param>
        /// <param name="initialCount">The initial number of requests for the semaphore that can be granted concurrently.</param>
        /// <param name="enableAutoScale">Experimental: Cope with real time concurrency demands at the cost of undefined behavior in high GC pressured environments. DISABLE if CPU usage snowballs and set <see cref="maxCount"/> more accurately to compensate</param>
        /// <param name="version">Initial zero state</param>
        /// <param name="enableFairQ">Enable fair queueing at the cost of performance</param>
        /// <param name="enableDeadlockDetection">Checks for deadlocks within a thread and throws when found</param>
        public IoZeroSemaphore(string description = "", int maxCount = 1, int initialCount = 0, 
            bool enableAutoScale = false, uint version = 1, bool enableFairQ = false, bool enableDeadlockDetection = false) : this()
        {
            _description = description;
            
            //validation
            if(maxCount < 1)
                throw new ZeroValidationException($"{Description}: invalid {nameof(maxCount)} = {maxCount} specified, value must be larger than 0");
            if(initialCount < 0)
                throw new ZeroValidationException($"{Description}: invalid {nameof(initialCount)} = {initialCount} specified, value may not be less than 0");
            if(initialCount > maxCount)
                throw new ZeroValidationException($"{Description}: invalid {nameof(initialCount)} = {initialCount} specified, larger than {nameof(maxCount)} = {maxCount}");
            if(version == 0 || version % ZeroDomain == 0) //zero not allowed because modulo domain of (short)token causes a duplication of state zero
                throw new ZeroValidationException($"{Description}: Validation failed: {nameof(version)} % {nameof(ZeroDomain)} = {version % ZeroDomain} may not equal 0");

            _maxCount = maxCount;
            _useMemoryBarrier = enableFairQ;
            _currentCount = initialCount;
            _zeroRef = null;
            _asyncToken = default;
            _asyncTokenReg = default;
            _lock = new SpinLock(enableDeadlockDetection);
            _enableAutoScale = enableAutoScale;
            _continuationAction = new Action<object>[_maxCount];
            _continuationState = new object[_maxCount];
            _head = 0;
            _tail = 0;
        }
        
        #region constants
        /// <summary>
        /// The domain of the tokens issues 
        /// </summary>
        private const uint ZeroDomain = ushort.MaxValue + 1;
        private const int MaxFastSpins = 5;
        #endregion

        #region settings

        /// <summary>
        /// use memory barrier setting
        /// </summary>
        private readonly bool _useMemoryBarrier;

        #endregion

        #region properties

        /// <summary>
        /// A semaphore description
        /// </summary>
        private readonly string _description;
        
        /// <summary>
        /// A semaphore description
        /// </summary>
        public string Description => $"{nameof(IoZeroSemaphore)}[{_description}]: qSize = {_maxCount}, h = {_head}, t = {_tail}, c = {_currentCount}, m = {_maxCount}";

        /// <summary>
        /// The semaphore capacity 
        /// </summary>
        private int _maxCount;
        
        /// <summary>
        /// The number of continuations that can enter the semaphore without blocking 
        /// </summary>
        private int _currentCount;

        /// <summary>
        /// The number of continuations that can enter the semaphore without blocking 
        /// </summary>
        public int CurrentCount => _currentCount;

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
        /// A "queue" of waiting continuations. The queue has weak order guarantees, but us mostly FIFO 
        /// </summary>
        private Action<object>[] _continuationAction;
        
        /// <summary>
        /// Holds the state of a queued item
        /// </summary>
        private object[] _continuationState;
        
        /// <summary>
        /// A pointer to the head of the Q
        /// </summary>
        private volatile int _head;
        
        /// <summary>
        /// A pointer to the tail of the Q
        /// </summary>
        private volatile int _tail;

        #endregion

        #region core

        /// <summary>
        /// Validation failed exception
        /// </summary>
        public class ZeroValidationException : InvalidOperationException
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
            ZeroWait = new ValueTask<bool>(_zeroRef, 305);
            _asyncToken = asyncToken;
            _asyncTokenReg = asyncToken.Register(s =>
            {
                ((IIoZeroSemaphore) s).Zero();
            }, _zeroRef);
        }

        /// <summary>
        /// zeroes out this semaphore
        /// </summary>
        public void Zero()
        {
            for (var i = _tail; i != _head || _continuationState[i] != null; i = (i + 1) % _continuationAction.Length)
            {
                if (_continuationState[i] != null)
                {
                    _continuationAction[i](_continuationState[i]);
                    _continuationState[i] = null;
                }
                    
            }

            //TODO: is this ok?
            _asyncTokenReg.Unregister();
        }

        /// <summary>
        /// Lock
        /// </summary>s
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ZeroLock(ThreadPriority enterPriority = ThreadPriority.Highest)
        {
            var acquiredLock = false;
            ulong spinCounter = 0;
            
            // Thread.CurrentThread.Priority = ThreadPriority.Lowest;
            // _lock.Enter(ref acquiredLock);
            
            //sacquire lock
             while (!acquiredLock)
             {
                 _lock.TryEnter( 1, ref acquiredLock);
                 if (spinCounter++ >= MaxFastSpins && !acquiredLock)
                 {
                     Thread.CurrentThread.Priority = ThreadPriority.Lowest;
                     _lock.Enter(ref acquiredLock);
                 }
             }

            Thread.CurrentThread.Priority = enterPriority;
        }

        /// <summary>
        /// Unlock
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ZeroUnlock(ThreadPriority exitPriority = ThreadPriority.Normal)
        {
            _lock.Exit(_useMemoryBarrier);
            Thread.CurrentThread.Priority = exitPriority;
        }
        
        /// <summary>
        /// Used for locking internally;
        /// </summary>
        private SpinLock _lock;
        
        /// <summary>
        /// if auto scaling is enabled 
        /// </summary>
        private readonly bool _enableAutoScale;
        
        /// <summary>
        /// A true sentinel
        /// </summary>
        private static readonly ValueTask<bool> ZeroTrue = new ValueTask<bool>(true);

        /// <summary>
        /// A wait sentinel
        /// </summary>
        private ValueTask<bool> ZeroWait;
        

        #endregion

        /// <summary>
        /// Returns true if exit is clean, false otherwise
        /// </summary>
        /// <param name="token">Not used</param>
        /// <returns>True if exit was clean, false on <see cref="CancellationToken.IsCancellationRequested"/> </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetResult(short token)
        {
            return !_asyncToken.IsCancellationRequested;
        }

        /// <summary>
        /// Get the current status, which in this case will always be pending
        /// </summary>
        /// <param name="token">Not used</param>
        /// <returns><see cref="ValueTaskSourceStatus.Pending"/></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTaskSourceStatus GetStatus(short token)
        {
            return ValueTaskSourceStatus.Pending;
        }

        /// <summary>
        /// Determines if the semaphore can be entered without blocking
        /// </summary>
        /// <returns>true if fast path is available, false otherwise</returns>
        private bool Signalled()
        {
            //fail fast
            if (_asyncToken.IsCancellationRequested)
            {
                return false;
            }

            //race for fast path
            var released = 0;
            ulong spinCount = 0;
            var lockedCount = _currentCount;
            while (lockedCount > 0 &&
                   (released = Interlocked.CompareExchange(ref _currentCount, lockedCount - 1, lockedCount)) != lockedCount)
            {
                if (spinCount > MaxFastSpins)
                    Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
                lockedCount = _currentCount;
            }
            
            if (Thread.CurrentThread.Priority != ThreadPriority.Normal)
                Thread.CurrentThread.Priority = ThreadPriority.Normal;

            //if we won release
            return lockedCount > 0 && released == lockedCount;
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
            //fast path
            if (Signalled())
            {
                continuation(state);
                return;
            }
            
            //lock
            ZeroLock();
            
            //check for space
            if (_continuationState[_head] == null)
            {
                //store the continuation
                _continuationAction[_head] = continuation;
                _continuationState[_head] = state;

                //advance queue head
                _head = (_head + 1) % _maxCount;

                //release lock
                ZeroUnlock();
            }
            else //if(_enableAutoScale) //EXPERIMENTAL: double concurrent capacity
            {
                //release lock
                ZeroUnlock();
                
                //Scale
                if (_enableAutoScale)
                {
                    ZeroScale();
                    OnCompleted(continuation, state, token, flags);//TODO unwind?
                }
                else
                {
                    throw new ZeroValidationException(
                        $"{Description}: Unable to handle concurrent call, enable auto scale or increase expectedNrOfWaiters");
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
                try
                {
                    GC.TryStartNoGCRegion(250 / 2 * 1024 * 1024, false);
                }
                catch
                {
                    // ignored
                }

                _lock.Enter(ref acquiredLock);
            }
            Thread.CurrentThread.Priority = ThreadPriority.Highest;

            //double the q
            var prevZeroQ = _continuationAction;
            var prevZeroState = _continuationState;
            
            _maxCount *= 2;
            
            _continuationAction = new Action<object>[_maxCount];
            _continuationState = new object[_maxCount];
            
            var j = 0;
            //special zero case
            if (_tail != _head || prevZeroState[_tail] != null && prevZeroQ.Length == 1)
            {
                _continuationAction[0] = prevZeroQ[0];
                _continuationState[0] = prevZeroState[0];
                j = 1;
            }
            else
            {
                //copy the Q
                for (var i = _tail; i != _head || prevZeroState[i] != null && j < _maxCount; i = (i + 1) % prevZeroQ.Length)
                {
                    _continuationAction[j] = prevZeroQ[i];
                    _continuationState[j] = prevZeroState[i];
                    j++;
                }    
            }
            
            //recalibrate the queue
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
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(int releaseCount = 1)
        {
            //preconditions
            if(releaseCount < 1)
                throw new ZeroValidationException($"{Description}: Invalid set {nameof(releaseCount)} = {releaseCount}, which is bigger than {nameof(_maxCount)} = {_maxCount}");

            //fail fast on cancel
            if (_asyncToken.IsCancellationRequested)
                throw new TaskCanceledException(Description);
            
            //Lock
            ZeroLock();
            
            //lock in return value
            var returnCount = _currentCount;

            //release lock
            ZeroUnlock();

            //var lwmTail = -1;
            var released = 0;
            //service count continuations
            for (var i = 0; i < releaseCount ; i++)
            {
                //Lock
                ZeroLock();

                //Are there any waiters?
                if (_continuationState[_tail] == null)
                {
                    ZeroUnlock();
                    break;
                }

                //grab a continuation
                var continuation = _continuationAction[_tail];
                var state = _continuationState[_tail];

                //mark as handled (saves 4 bytess)
                _continuationState[_tail] = null;

                //advance tail position
                _tail = (_tail + 1) % _maxCount;

                //release the lock
                ZeroUnlock();

                //release a thread
                continuation(state);

                //count the number of waiters released
                released++;
            }
            
            //validate releaseCount
            if (_maxCount - (_currentCount) < releaseCount - released)
            {
                Interlocked.Exchange(ref _currentCount, _maxCount);
                //throw
                throw new SemaphoreFullException($"${Description}, {nameof(_currentCount)} = {_currentCount}, {nameof(_maxCount)} = {_maxCount}, rem/> {nameof(releaseCount)} = {releaseCount - released}");
            }
            else
            {
                Interlocked.Add(ref _currentCount, releaseCount - released);   
            }
            
            //return previous number of waiters
            return returnCount;
        }
        
        /// <summary>
        /// Waits on this semaphore
        /// </summary>
        /// <returns>The version number</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> WaitAsync()
        { 
            //fail fast 
            if (_asyncToken.IsCancellationRequested)
                return ValueTask.FromResult(false);

            //Fast path if signalled or block
            return Signalled() ? ZeroTrue : new ValueTask<bool>(_zeroRef, 0);
        }
    }
}