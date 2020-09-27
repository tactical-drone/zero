using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore.core
{
    /// <summary>
    /// Zero Semaphore with experimental concurrency scaling (disabled by default), use expectedNrOfWaiters instead.
    /// </summary>
    public struct IoZeroSemaphore : IIoZeroSemaphore
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="description">A description of this mutex</param>
        /// <param name="maxCount">The maximum number of requests for the semaphore that can be granted concurrently.</param>
        /// <param name="initialCount">The initial number of requests for the semaphore that can be granted concurrently.</param>
        /// <param name="expectedNrOfWaiters">The number of expected waiters to wait on this semaphore concurrently</param>
        /// <param name="enableAutoScale">Experimental: Cope with real time concurrency demands at the cost of undefined behavior in high GC pressured environments. DISABLE if CPU usage snowballs and set <see cref="expectedNrOfWaiters"/> more accurately to compensate</param>
        /// <param name="version">Initial zero state</param>
        /// <param name="enableFairQ">Enable fair queueing at the cost of performance</param>
        /// <param name="enableDeadlockDetection">Checks for deadlocks within a thread and throws when found</param>
        public IoZeroSemaphore(string description = "", int maxCount = 1, int initialCount = 0, int expectedNrOfWaiters = 1,
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
            if(expectedNrOfWaiters < 1)
                throw new ZeroValidationException($"{Description}: invalid {nameof(expectedNrOfWaiters)} specified, value must be larger than 0");
            
            _maxCount = maxCount;
            _useMemoryBarrier = enableFairQ;
            _currentCount = initialCount;
            _version = version; 
            _zeroRef = null;
            _asyncToken = default;
            _asyncTokenReg = default;
            _lock = new SpinLock(enableDeadlockDetection);
            _enableAutoScale = enableAutoScale;
            _continuationAction = new Action<object>[expectedNrOfWaiters];
            _continuationState = new object[expectedNrOfWaiters];
            _continuationToken = new short[expectedNrOfWaiters];
            _head = 0;
            _tail = 0;
        }
        
        #region constants
        /// <summary>
        /// The domain of the tokens issues 
        /// </summary>
        private const int ZeroDomain = ushort.MaxValue + 1;
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
        public string Description => $"{nameof(IoZeroSemaphore)}[{_description}]: Z = {_version}";

        /// <summary>
        /// The semaphore capacity 
        /// </summary>
        private int _maxCount;
        
        /// <summary>
        /// The number semaphore releases held in reserve 
        /// </summary>
        private int _currentCount;

        /// <summary>
        /// 
        /// </summary>
        public int CurrentCount => _currentCount;

        /// <summary>
        /// Primary safety property
        /// </summary>
        private volatile uint _version;
        
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
        /// Holds the safety version of a queued item
        /// </summary>
        private short[] _continuationToken;
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
        
        private SpinLock _lock;
        private readonly bool _enableAutoScale;
        private static readonly ValueTask<bool> ZeroTrue = new ValueTask<bool>(true);

        #endregion

        /// <summary>
        /// Get the result
        /// </summary>
        /// <param name="token">The safety token</param>
        /// <returns>The current result</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetResult(short token)
        {
            return GetStatus(token) == ValueTaskSourceStatus.Succeeded;
        }

        /// <summary>
        /// Get the current status
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTaskSourceStatus GetStatus(short token)
        {
            //fail fast
            if (_asyncToken.IsCancellationRequested)
                return ValueTaskSourceStatus.Canceled;
            
            //return status
            return token == (short) (_version % ZeroDomain)
                ? ValueTaskSourceStatus.Pending
                : ValueTaskSourceStatus.Succeeded;
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
            //fast path, late continuations win, zeroQueue is not a Q and order is not guaranteed
            // Care: _zeroVersion must change inside a lock
            if ((ushort) token < _version % ZeroDomain)
            {
                continuation(state);
                return;
            }
            
            var acquiredLock = false;
            //acquire lock
            Thread.CurrentThread.Priority = ThreadPriority.Lowest;
            while (!acquiredLock)
                _lock.Enter(ref acquiredLock);
            Thread.CurrentThread.Priority = ThreadPriority.AboveNormal;
            
            //check for space
            if (_continuationState[_head] == null) //TODO: does this assumption fail at concurrent waiters at ZERODOMAIN?
            {
                //store the continuation
                _continuationAction[_head] = continuation;
                _continuationState[_head] = state;
                _continuationToken[_head] = token;

                //advance queue head
                _head = (_head + 1) % _continuationAction.Length;
                
                _lock.Exit(_useMemoryBarrier);
                Thread.CurrentThread.Priority = ThreadPriority.Normal;
            }
            else //EXPERIMENTAL: double concurrent capacity
            {
                _lock.Exit(_useMemoryBarrier);
                Thread.CurrentThread.Priority = ThreadPriority.Normal;
                
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
            
            Thread.CurrentThread.Priority = ThreadPriority.Lowest;
            while (!acquiredLock)
            {
                try
                {
                    GC.TryStartNoGCRegion((_continuationAction.Length) * 2 * 8 * 2, false);
                }
                catch 
                {
                }
                _lock.Enter(ref acquiredLock);
            }
            Thread.CurrentThread.Priority = ThreadPriority.Highest;

            //double the q
            var prevZeroQ = _continuationAction;
            var prevZeroState = _continuationState;
            var prevZeroSafetyVersion = _continuationToken;
            _continuationAction = new Action<object>[_continuationAction.Length * 2];
            _continuationState = new object[_continuationState.Length * 2];
            _continuationToken = new short[_continuationToken.Length * 2];

            var j = 0;
            //special zero case
            if (_tail != _head || prevZeroState[_tail] != null && prevZeroQ.Length == 1)
            {
                _continuationAction[0] = prevZeroQ[0];
                _continuationState[0] = prevZeroState[0];
                _continuationToken[0] = prevZeroSafetyVersion[0];
                j = 1;
            }
            else
            {
                //copy the Q
                for (var i = _tail; i != _head || prevZeroState[i] != null; i = (i + 1) % prevZeroQ.Length)
                {
                    _continuationAction[j] = prevZeroQ[i];
                    _continuationState[j] = prevZeroState[i];
                    _continuationToken[j] = prevZeroSafetyVersion[i];
                    j++;
                }    
            }
            
            //recalibrate the queue
            _tail = 0;
            _head = j;

            //Release the lock
            _lock.Exit(_useMemoryBarrier);
            
            //Enable GC

            try
            {
                GC.EndNoGCRegion();
            }
            catch 
            {
            }
            
            //restore thread priority
            Thread.CurrentThread.Priority = ThreadPriority.Normal;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(int releaseCount = 1)
        {
            //preconditions
            #if DEBUG
            if(releaseCount < 1)
                throw new ZeroValidationException($"{Description}: Invalid set {nameof(releaseCount)} = {releaseCount}, which is bigger than {nameof(_maxCount)} = {_maxCount}");
            #endif

            //fail fast on cancel
            if (_asyncToken.IsCancellationRequested)
                throw new TaskCanceledException(Description);
            
            var acquiredLock = false;
            
            #region atomic verion bump
            //acquire lock
            Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
            while(!acquiredLock)
                _lock.Enter(ref acquiredLock);
            Thread.CurrentThread.Priority = ThreadPriority.AboveNormal;
            
            
            // validate releaseCount
            if (_maxCount - _currentCount < releaseCount)
            {
                //release lock
                _lock.Exit(_useMemoryBarrier);
                Thread.CurrentThread.Priority = ThreadPriority.Normal;
                
                //throw
                throw new ZeroSemaphoreFullException(Description);
            }
            
            var returnCount = _currentCount;

            //bump version 
            var bump = Interlocked.Increment(ref _version);
            uint safety;
            if (bump % ZeroDomain == 0) 
            {
                safety = Interlocked.Increment(ref _version) - 1;
            }
            else
            {
                safety = bump - 1;
            }
            
            //release lock
            _lock.Exit(_useMemoryBarrier);
            Thread.CurrentThread.Priority = ThreadPriority.Normal;
            #endregion
            
            var lwmTail = -1;
            var released = 0;
            //service count continuations
            for (var i = 0; i < releaseCount ; i++)
            {
                //Deque continuation
                #region atomic dequeue
                //acquire lock
                acquiredLock = false;
                Thread.CurrentThread.Priority = ThreadPriority.Lowest;
                while (!acquiredLock)
                    _lock.Enter(ref acquiredLock);
                Thread.CurrentThread.Priority = ThreadPriority.AboveNormal;

                //Check if there is a continuation
                if (_continuationState[_tail] == null) //TODO: Does this wrap around on high concurrency? Q
                {
                    _lock.Exit(_useMemoryBarrier);
#if DEBUG
                    //TODO do we throw? 
#endif
                    //NO continuations
                    break;
                }
                
                //validate handler safety, skip on fail up until the head and reset back to lwmTail
                if ((ushort)_continuationToken[_tail] > safety % ZeroDomain && _tail <= _head)
                {
                    //capture the lowest water mark tail
                    if(lwmTail < 0)
                        lwmTail = _tail;
                    
                    //Release lock
                    _lock.Exit(_useMemoryBarrier);

                    //skip continuations part of next version
                    continue;
                }
                
                //grab a continuation
                var continuation = _continuationAction[_tail];
                var state = _continuationState[_tail];
                
                //mark as handled (saves 4 bytes)
                _continuationState[_tail] = null;
                
                //advance tail position
                _tail = (_tail + 1) % _continuationAction.Length;
                
                //Has this continuation been serviced?
                if (state == null)
                {
                    //release lock
                    _lock.Exit(_useMemoryBarrier);

                    //skip already serviced continuations
                    continue;
                }
                
                //release the lock
                _lock.Exit(_useMemoryBarrier);
                #endregion
                
                //release a thread
                continuation(state);

                released++;
            }

            Interlocked.Add(ref _currentCount, releaseCount - released);
            
            //set low water mark tail if set
            if (lwmTail != -1)
                _tail = lwmTail;

            Thread.CurrentThread.Priority = ThreadPriority.Normal;
            
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

            //state zero is not allowed 
            var zeroLock = _version % ZeroDomain;
            if (zeroLock == 0)
                zeroLock = 1;
            
            //race for fast path
            var released = 0;
            var lockedCount = _currentCount;
            Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
            while (lockedCount > 0 &&
                   (released = Interlocked.CompareExchange(ref _currentCount, lockedCount - 1, lockedCount)) != lockedCount)
            {
                lockedCount = _currentCount;
            }
            Thread.CurrentThread.Priority = ThreadPriority.Normal;
            
            //if we won release
            if (lockedCount > 0 && released == lockedCount)
            {
                return ZeroTrue;
            }

            //else we wait
            return new ValueTask<bool>(_zeroRef, (short)(zeroLock));
        }
    }
}