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

            //if(initialCount > maxCount)
            //    throw new ZeroValidationException($"{Description}: invalid {nameof(initialCount)} = {initialCount} specified, larger than {nameof(maxCount)} = {maxCount}");

            _maxCount = maxCount;
            _useMemoryBarrier = enableFairQ;
            _signalCount = initialCount;
            _zeroRef = null;
            _asyncToken = default;
            _asyncTokenReg = default;
            _enableAutoScale = enableAutoScale;
            
            if(_enableAutoScale)
                _lock = new SpinLock(enableDeadlockDetection);

            //_latched = o => { };
            
            _signalAwaiter = new Action<object>[_maxCount];
            _signalAwaiterState = new object[_maxCount];
            _head = 0;
            _tail = 0;
            //_manifold = MaxTolerance;
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
        public string Description => $"{nameof(IoZeroSemaphore)}[{_description}]:  current = {_signalCount}, qSize = {_maxCount}, h = {_head}, t = {_tail}";

        /// <summary>
        /// The semaphore capacity 
        /// </summary>
        private int _maxCount;
        
        /// <summary>
        /// The number of waiters that can enter the semaphore without blocking 
        /// </summary>
        private volatile int _signalCount;

        /// <summary>
        /// The number of waiters that can enter the semaphore without blocking 
        /// </summary>
        public int ReadyCount => _signalCount;

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

        /// <summary>
        /// Organic Manifold
        /// </summary>
        //private volatile int _manifold; 

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
            _zeroWait = new ValueTask<bool>(_zeroRef, 23);
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
        /// A enter sentinel
        /// </summary>
        private static readonly ValueTask<bool> ZeroEnter = new ValueTask<bool>(true);

        /// <summary>
        /// A wait sentinel
        /// </summary>
        private ValueTask<bool> _zeroWait;

        /// <summary>
        /// Used for latching 
        /// </summary>
        //private readonly Action<object> _latched;

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

            return !_asyncToken.IsCancellationRequested && _zeroed == 0;
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
            //while (Interlocked.CompareExchange(ref _signalAwaiter[head], _latched, null) != null && c++ < _maxCount)
            //{
            //    head = (Interlocked.Increment(ref _head) - 1) % _maxCount;
            //}

            var headIdx = (Interlocked.Increment(ref _head) - 1) % _maxCount;
            var slot = Interlocked.CompareExchange(ref _signalAwaiter[headIdx], continuation, null);

            while (slot != null && c < _maxCount)
            {
                Volatile.Write(ref _signalAwaiter[headIdx], slot);
                Interlocked.Decrement(ref _head);
                headIdx = (Interlocked.Increment(ref _head) - 1) % _maxCount;
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
                    throw new ZeroValidationException($"{Description}: w[{headIdx % _maxCount}] = {_signalAwaiter[headIdx]} : {_signalAwaiterState[headIdx]}, Unable to handle concurrent call, enable auto scale or increase expectedNrOfWaiters");
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
            _maxCount *= 2;
            _signalAwaiter = new Action<object>[_maxCount];
            _signalAwaiterState = new object[_maxCount];
            
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
                for (var i = _tail; i != _head || prevZeroState[i] != null && j < _maxCount; i = (i + 1) % (uint)prevZeroQ.Length)
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
        /// <returns>The number of signals sent, before this one, -1 on failure</returns>
        /// <exception cref="ZeroValidationException">Fails on preconditions</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Release(int releaseCount = 1)
        {
            //preconditions
            //if(releaseCount < 1 || releaseCount + _signalCount > _maxCount)
            //    throw new ZeroValidationException($"{Description}: Invalid {nameof(releaseCount)} = {releaseCount} < 0 or  {nameof(_signalCount)}({releaseCount + _signalCount}) > {nameof(_maxCount)}({_maxCount})");

            if(releaseCount < 1 )
                throw new ZeroValidationException($"{Description}: Invalid {nameof(releaseCount)} = {releaseCount} < 0");

            //fail fast on cancellation token
            if (_asyncToken.IsCancellationRequested || _zeroed > 0)
                return -1;
            
            //lock in return value
            var returnCount = _signalCount;
            var released = 0;
            var c = 0;
            //awaiter entries
            while (released < releaseCount && c++ < _maxCount)
            {
                //Lock
                ZeroLock();

                //choose a tail index
                var origTail = Interlocked.Increment(ref _tail);
                var tailIdx = (origTail - 1) % _maxCount;

                ////latch a chosen tail state
                var state = Interlocked.CompareExchange(ref _signalAwaiterState[tailIdx], null, _signalAwaiterState[tailIdx]);

                //Did we loose?
                if (state == null)
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
                var targetWaiter = Interlocked.CompareExchange(ref _signalAwaiter[tailIdx], null, _signalAwaiter[tailIdx]);
                if (targetWaiter == null)
                {
                    //prevent deadlock
                    Volatile.Write(ref _signalAwaiterState[tailIdx], state);

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
                if (state == null)
                    throw new ArgumentNullException($"-> {nameof(state)}");
#endif
                
                //release the lock
                ZeroUnlock();

                //release a waiter
                targetWaiter(state);

                //count the number of waiters released
                released++;
            }
            
            //update current count
            Interlocked.Add(ref _signalCount, releaseCount - released);

            ////validate releaseCount
            //if (_maxCount - _signalCount < 0)
            //{
            //    //throw when the semaphore runs out of capacity
            //    throw new ZeroSemaphoreFullException($"${Description}, {nameof(_signalCount)} = {_signalCount}, {nameof(_maxCount)} = {_maxCount}, rem/> {nameof(releaseCount)} = {releaseCount - released}");
            //}

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
            if (_asyncToken.IsCancellationRequested || _zeroed > 0)
                return ValueTask.FromResult(false);

            //Enter on fast path if signalled or wait
            return Signalled() ? ZeroEnter : _zeroWait;
        }
    }
}