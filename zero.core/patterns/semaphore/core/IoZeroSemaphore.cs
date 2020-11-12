using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Microsoft.AspNetCore.Razor.TagHelpers;

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
        /// <param name="maxCount">The maximum number of requests for the semaphore that can be granted concurrently.</param>
        /// <param name="initialCount">The initial number of requests for the semaphore that can be granted concurrently.</param>
        /// <param name="enableAutoScale">Experimental: Cope with real time concurrency demands at the cost of undefined behavior in high GC pressured environments. DISABLE if CPU usage snowballs and set <see cref="maxCount"/> more accurately to compensate</param>
        /// <param name="enableFairQ">Enable fair queueing at the cost of performance</param>
        /// <param name="enableDeadlockDetection">Checks for deadlocks within a thread and throws when found</param>
        public IoZeroSemaphore(string description = "", int maxCount = 1, int initialCount = 0, 
            bool enableAutoScale = false, bool enableFairQ = false, bool enableDeadlockDetection = false) : this()
        {
            _description = description;
            
            //validation
            if(maxCount < 1)
                throw new ZeroValidationException($"{Description}: invalid {nameof(maxCount)} = {maxCount} specified, value must be larger than 0");
            if(initialCount < 0)
                throw new ZeroValidationException($"{Description}: invalid {nameof(initialCount)} = {initialCount} specified, value may not be less than 0");
            if(initialCount > maxCount)
                throw new ZeroValidationException($"{Description}: invalid {nameof(initialCount)} = {initialCount} specified, larger than {nameof(maxCount)} = {maxCount}");

            _maxCount = maxCount;
            _useMemoryBarrier = enableFairQ;
            _currentCount = initialCount;
            _zeroRef = null;
            _asyncToken = default;
            _asyncTokenReg = default;
            _enableAutoScale = enableAutoScale;
            
            if(_enableAutoScale)
                _lock = new SpinLock(enableDeadlockDetection);
            
            _signalAwaiter = new Action<object>[_maxCount];
            _signalAwaiterState = new object[_maxCount];
            _head = 0;
            _tail = 0;
            _manifold = MaxTolerance;
        }

        #region settings

        /// <summary>
        /// use memory barrier setting
        /// </summary>
        private readonly bool _useMemoryBarrier;

        /// <summary>
        /// Max number of times a spinlock can be tested before throttling 
        /// </summary>
        private const int MaxSpin = 5;

        #endregion

        #region properties

        /// <summary>
        /// A semaphore description
        /// </summary>
        private readonly string _description;
        
        /// <summary>
        /// A semaphore description
        /// </summary>
        public string Description => $"{nameof(IoZeroSemaphore)}[{_description}]:  current = {_currentCount}, qSize = {_maxCount}, h = {_head}, t = {_tail}";

        /// <summary>
        /// The semaphore capacity 
        /// </summary>
        private int _maxCount;
        
        /// <summary>
        /// The number of waiters that can enter the semaphore without blocking 
        /// </summary>
        private int _currentCount;

        /// <summary>
        /// The number of waiters that can enter the semaphore without blocking 
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
        private int _head;

        /// <summary>
        /// A pointer to the tail of the Q
        /// </summary>
        private int _tail;

        /// <summary>
        /// Whether this semaphore has been cleared out
        /// </summary>
        private volatile int _zeroed;

        /// <summary>
        /// Max error tolerance, should be set > 1
        /// </summary>
        private const int MaxTolerance = 2;

        /// <summary>
        /// Organic Manifold
        /// </summary>
        /// <returns></returns>
        private volatile int _manifold; 

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
            _zeroWait = new ValueTask<bool>(_zeroRef, 305);
            _asyncToken = asyncToken;

            _asyncTokenReg = asyncToken.Register(s =>
            {
                ((IIoZeroSemaphore)s).Zero();
            }, _zeroRef);
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
            Action<object> waiter = null;
            while (waiter != null || i < _signalAwaiter.Length)
            {
                waiter = Interlocked.CompareExchange(ref _signalAwaiter[i], null, _signalAwaiter[i]);
                if (waiter != null)
                {
                    try
                    {
                        waiter(_signalAwaiterState[i]);
                    }
                    catch
                    {
                        // ignored
                    }
                    finally
                    {
                        waiter = null;
                        _signalAwaiterState[i] = null;
                    }
                }

                i++;
            }

            Array.Clear(_signalAwaiter, 0, _maxCount);
            Array.Clear(_signalAwaiterState, 0, _maxCount);
        }

        /// <summary>
        /// Lock
        /// </summary>s
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ZeroLock()
        {
            //Disable experimental features
            if(_enableAutoScale)
                return;
            
            var acquiredLock = false;

            //acquire lock
            _lock.Enter(ref acquiredLock);
        }

        /// <summary>
        /// Unlock
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ZeroUnlock()
        {
            //Disable experimental features
            if(_enableAutoScale)
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
        /// A enter sentinel
        /// </summary>
        private static readonly ValueTask<bool> ZeroEnter = new ValueTask<bool>(true);

        /// <summary>
        /// A wait sentinel
        /// </summary>
        private ValueTask<bool> _zeroWait;
        
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
                //Will this ever happen?
                if (spinCount > MaxSpin)
                    Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
                
                lockedCount = _currentCount;
            }
            
            //restore thread priority
            if (Thread.CurrentThread.Priority != ThreadPriority.Normal)
                Thread.CurrentThread.Priority = ThreadPriority.Normal;

            //Did we win?
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
            if (Signalled() || _zeroed == 1)
            {
                continuation(state);
                return;
            }
            
            //lock
            ZeroLock();
            
            //choose a head
            var head = Interlocked.Increment(ref _head) % _maxCount;

            //Did we get it?
            if ( Interlocked.CompareExchange(ref _signalAwaiter[head], continuation, null) == null)
            {
                //set the state as well
                _signalAwaiterState[head] = state;

                //reset scan range
                if(_manifold == 0)
                    _manifold = MaxTolerance;

                //release lock
                ZeroUnlock();
            }
            else //if(_enableAutoScale) //EXPERIMENTAL: double concurrent capacity
            {
                Interlocked.Decrement(ref _head);

                //release lock
                ZeroUnlock();
                
                //Scale
                if (_enableAutoScale)
                {
                    ZeroScale();
                    OnCompleted(continuation, state, token, flags);
                }
                else if (Interlocked.Decrement(ref _manifold) > 0) //add organic manifold
                {
                    Thread.Yield();
                    OnCompleted(continuation, state, token, flags);
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
            var prevZeroQ = _signalAwaiter;
            var prevZeroState = _signalAwaiterState;
            
            //allocate memory
            _maxCount *= 2;
            _signalAwaiter = new Action<object>[_maxCount];
            _signalAwaiterState = new object[_maxCount];
            
            //copy Q
            
            var j = 0;
            //special zero case
            if (_tail != _head || prevZeroState[_tail] != null && prevZeroQ.Length == 1)
            {
                _signalAwaiter[0] = prevZeroQ[0];
                _signalAwaiterState[0] = prevZeroState[0];
                j = 1;
            }
            else
            {
                for (var i = _tail; i != _head || prevZeroState[i] != null && j < _maxCount; i = (i + 1) % prevZeroQ.Length)
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
        }
        
        /// <summary>
        /// Allow waiter(s) to enter the semaphore
        /// </summary>
        /// <param name="releaseCount">The number of waiters to enter</param>
        /// <returns>The current count, -1 on failure</returns>
        /// <exception cref="ZeroValidationException">Fails on preconditions</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(int releaseCount = 1)
        {
            //preconditions
            if(releaseCount < 1 || releaseCount > _maxCount)
                throw new ZeroValidationException($"{Description}: Invalid {nameof(releaseCount)} = {releaseCount}, must be bigger than zero and smaller than {nameof(_maxCount)} = {_maxCount}");

            //fail fast on cancellation token
            if (_asyncToken.IsCancellationRequested || _zeroed == 1)
                return -1;
            
            //Lock
            ZeroLock();
            
            //lock in return value
            var returnCount = _currentCount;

            //release lock
            ZeroUnlock();
            
            var released = 0;
            var count = 0;
            
            //awaiter entries
            while (released < releaseCount && count++ < _maxCount)
            {
                //Lock
                ZeroLock();

                //choose a tail
                var tail = Interlocked.Increment(ref _tail) % _maxCount;

                //latch the chosen tail
                var latchedWaiter = _signalAwaiter[tail];

                //did we get a waiter to latch on?
                if (latchedWaiter == null)
                {
                    Interlocked.Decrement(ref _tail);
                        
                    ZeroUnlock();
                    
                    //try again
                    continue;
                }
                
                //latch onto the waiter
                Action<object> waiter;
                if ((waiter = Interlocked.CompareExchange(ref _signalAwaiter[tail], null, latchedWaiter)) == latchedWaiter)
                {
                    //grab the state
                    var state = _signalAwaiterState[tail];
                    
                    //release the lock
                    ZeroUnlock();

                    //release a waiter
                    waiter(state);

                    //count the number of waiters released
                    released++;   
                }
                else
                {
                    //advance the tail
                    Interlocked.Decrement(ref _tail);
                }
            }
            
            //validate releaseCount
            if (_maxCount - (_currentCount) < releaseCount - released)
            {
                Interlocked.Exchange(ref _currentCount, _maxCount);
                
                //throw when the semaphore runs out of capacity
                throw new ZeroSemaphoreFullException($"${Description}, {nameof(_currentCount)} = {_currentCount}, {nameof(_maxCount)} = {_maxCount}, rem/> {nameof(releaseCount)} = {releaseCount - released}");
            }

            //update current count
            Interlocked.Add(ref _currentCount, releaseCount - released);

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
            if (_asyncToken.IsCancellationRequested || _zeroed == 1)
                return ValueTask.FromResult(false);

            //Enter on fast path if signalled or wait
            return Signalled() ? ZeroEnter : _zeroWait;
        }
    }
}