﻿//#define SUPER_SYNC
//#define SYNC
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.misc;
using zero.core.patterns.misc;
using zero.core.patterns.queue.enumerator;
using zero.core.patterns.semaphore;

namespace zero.core.patterns.queue
{
    /// <summary>
    /// A lighter concurrent round robin Q
    /// </summary>
    public class IoZeroQ<T> : IEnumerable<T>
        //where T : class
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="description">A description</param>
        /// <param name="capacity">The initial capacity</param>
        /// <param name="autoScale">This is pseudo scaling: If set, allows the internal buffers to grow (amortized) if buffer pressure drops below 50% after exceeding it, otherwise scaling is not possible</param>
        /// <param name="asyncTasks">When used as async blocking collection</param>
        /// <param name="concurrencyLevel">Max expected concurrency</param>
        /// <param name="zeroAsyncMode"></param>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public IoZeroQ(string description, int capacity, bool autoScale = false, CancellationTokenSource asyncTasks = null, int concurrencyLevel = 1, bool zeroAsyncMode = false)
        {
#if DEBUG
            _description = description;
#else
            _description = string.Empty;
#endif
            if(autoScale && (capacity & -capacity) != capacity || capacity == 0)
                throw new ArgumentOutOfRangeException($"{nameof(capacity)} = {capacity} must be a power of 2 when {nameof(autoScale)} is set true");

            _autoScale = autoScale;
            _blockingCollection = asyncTasks != null;

#if DEBUG
            _fastStorageTime = new int[16384];
#endif

            //if scaling is enabled
            if (autoScale)
            {
                //TODO: tuning
                _capacity = 1;
                capacity = Math.Max(4, capacity);
                _storage = new T[32][];
                _storage[0] = _fastStorage = new T[1];
                _bloom = new int[32][];
                _bloom[0] = _fastBloom = new int[1];

                var v = Log2(capacity + 1);
                var scaled = false;
                for (var i = 0; i < v; i++)
                {
                    scaled = Scale(true);
                }

                if (!scaled) 
                    Scale(true);
            }
            else
            {
                //_hwm = _capacity = capacity;
                _capacity = capacity++;
                _storage = new T[1][];
                _storage[0] = _fastStorage = new T[capacity];
                _bloom = new int[1][];
                _bloom[0] = _fastBloom = new int[capacity];
            }

            if (_blockingCollection)
            {
                _fanSync = new IoZeroSemaphoreSlim(asyncTasks, $"fan {description}", concurrencyLevel, zeroAsyncMode: zeroAsyncMode); //TODO: tuning
                _balanceSync = new IoZeroSemaphoreSlim(asyncTasks, $"balance {description}", concurrencyLevel, zeroAsyncMode: zeroAsyncMode); //TODO: tuning
                _zeroSync = new IoZeroSemaphoreChannel<T>($"pump  {description}", concurrencyLevel, zeroAsyncMode: zeroAsyncMode); //TODO: tuning

                _fanSyncs = Enumerable.Repeat<AsyncDelegate>(BlockOnConsumeAsync, concurrencyLevel).ToArray();
                _balanceSyncs = Enumerable.Repeat<AsyncDelegate>(BalanceOnConsumeAsync, concurrencyLevel).ToArray();
                _zeroSyncs = Enumerable.Repeat<AsyncDelegate>(PumpOnConsumeAsync, concurrencyLevel).ToArray();
            }
            
            //_curEnumerator = new IoQEnumerator<T>(this);
        }

        #region packed
        private long _head;
        //private long _hwm;

        private readonly string _description;

        private readonly T[][]   _storage;
        private readonly T[]     _fastStorage;
#if DEBUG
        private readonly int[] _fastStorageTime;
#endif
        private readonly int[][] _bloom;
        private readonly int[]   _fastBloom;

        private readonly object _syncRoot = new();

        //private readonly ReaderWriterLockSlim _rwLock = new(LockRecursionPolicy.NoRecursion);
        //private const int _readTo = 100;
        //private const int _writeTo = 50;
        
        //private volatile IoQEnumerator<T> _curEnumerator;
        private readonly IoZeroSemaphoreSlim _fanSync;
        private readonly IoZeroSemaphoreChannel<T> _zeroSync;
        private readonly IoZeroSemaphoreSlim _balanceSync;
        private readonly AsyncDelegate[] _fanSyncs;
        private readonly AsyncDelegate[] _balanceSyncs;
        private readonly AsyncDelegate[] _zeroSyncs;
        private long _tail;
        private delegate IAsyncEnumerable<T> AsyncDelegate();
        private readonly bool _autoScale;
        private readonly bool _blockingCollection;

        private int _capacity;
        private int _zeroed;
        private int _clearing;
        private int _virility;
        private int _blockingConsumers;
        private int _sharingConsumers;
        private int _pumpingConsumers;
        private int _primedForScale;
        private int _timeSinceLastScale = Environment.TickCount;
        private int _count;
        private long _lastInsertIndex = -1;
#if DEBUG
        private long _lastRemoveIndex = -1;
        private int _opCounter;
#endif

        #endregion

        private const int _zero = 0;
        private const int _one = 1;
        private const int _set = 2;
        private const int _reset = 3;
        private const int YieldRetryCount = 4;

        public long Tail => Interlocked.Read(ref _tail);
        public long Head => Interlocked.Read(ref _head);

        /// <summary>
        /// ZeroAsync status
        /// </summary>
        public bool Zeroed => _zeroed > 0;

        /// <summary>
        /// Description
        /// </summary>
        public string Description => $"{nameof(IoZeroQ<T>)}: z = {_zeroed > 0}, {nameof(Count)} = {_count}/{Capacity}, s = {IsAutoScaling}({_timeSinceLastScale.ElapsedMs()/1000} sec), h = {Head}/{Tail}({Head%Capacity}/{Tail % Capacity}) (max: {Capacity}) (d:{Tail - Head}), desc = {_description}";

        /// <summary>
        /// Current number of items in the bag
        /// </summary>
        public int Count => _count;

        /// <summary>
        /// Capacity
        /// </summary>
        public long Capacity => _capacity;

        /// <summary>
        /// Whether we are auto scaling
        /// </summary>
        public bool IsAutoScaling => _autoScale;

        /// <summary>
        /// Q item by index
        /// </summary>
        /// <param name="idx">index</param>
        /// <returns>Object stored at index</returns>

#if DEBUG
        private const int _CASerror = 32;
#endif
        public T this[long idx]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(idx >= 0);
                if (!IsAutoScaling) return _fastStorage[idx % _capacity];

                idx %= Capacity;

                var i = Log2(idx + 1);
                return _storage[i][idx - ((1 << i) - 1)];
               
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            protected set
            {
                Debug.Assert(idx >= 0);

                if (!IsAutoScaling)
                {
                    _fastStorage[idx % _capacity] = value;
                    return;
                }

                idx %= Capacity;
                var i = Log2(idx + 1);
                _storage[i][idx - ((1 << i) - 1)] = value;
            }
        }

        /// <summary>
        /// Horizontal scale
        /// </summary>
        /// <returns>True if scaling happened, false on race or otherwise.</returns>
        private bool Scale(bool force = false)
        {
            if (!IsAutoScaling || Zeroed)
                return false;

            lock (_syncRoot)
            {
                var threshold = Capacity >> 1;

                //prime for a scale
                if (_primedForScale == 0 && Count >= threshold)
                    Interlocked.Exchange(ref _primedForScale, 1);

                //Only allow scaling to happen only when the Q dips under 50% capacity & some other factors, otherwise the indexes will corrupt.
                if (_primedForScale == 1 &&
                    _storage[_virility][0] == null && _storage[_virility][^1] == null
                    && Interlocked.CompareExchange(ref _primedForScale, 2, 1) == 1 || force)
                {
                    var hwm = 1 << (_virility + 1);
                    _storage[_virility + 1] = new T[hwm];
                    _bloom[_virility + 1] = new int[hwm];
                    _tail %= _capacity;
                    _head %= _capacity;
                    Interlocked.Add(ref _capacity, hwm);
                    Interlocked.Increment(ref _virility);
                    Interlocked.Exchange(ref _primedForScale, 0);
                    Interlocked.Exchange(ref _timeSinceLastScale, Environment.TickCount);
                    Interlocked.MemoryBarrierProcessWide();
                    return true;
                }
                return false;
            }
        }

        /// <summary>
        /// Wraps Interlocked.CompareExchange that copes with horizontal scaling
        /// </summary>
        /// <param name="value">The new value</param>
        /// <param name="index">index to work with</param>
        /// <returns>False on race, true otherwise</returns>
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private long AtomicAdd(T value)
        {
#if DEBUG
            var ts = Environment.TickCount;
#endif
            try
            {
                var state = -1;
                long latch;
                var modIdx = (latch = Tail) % Capacity;

                SpinWait sw = new();
                if (!IsAutoScaling)
                {
                    ref var fastBloomPtr = ref _fastBloom[modIdx];
                    retry:
                    if (Tail != latch || (state = Interlocked.CompareExchange(ref fastBloomPtr, _reset, _zero)) != _zero)
                    {
                        if (state == _one && Interlocked.CompareExchange(ref fastBloomPtr, _reset, _one) == _one)
                        {
                            Interlocked.Decrement(ref _count);
                        }
                        else
                        {
                            if (Tail != latch || state != _reset || sw.Count > short.MaxValue)
                                return -1;

                            state = -1;
                            sw.SpinOnce();
                            goto retry;
                        }
                    }

                    if (Tail != latch)
                    {
                        Interlocked.MemoryBarrierProcessWide();
                        //TAIL is racing towards this _one... set it back to _zero and hope for the best. So far it checks out. 
                        if (Interlocked.CompareExchange(ref fastBloomPtr, _zero, _reset) != _reset)
                        {
                            LogManager.GetCurrentClassLogger()
                                .Fatal($"add-f: Unable to restore lock at {latch}, diff = {Tail - latch}, bloom = {fastBloomPtr}, state = {state} - {Description}");
                        }

                        return -1;
                    }

                    Interlocked.Increment(ref _count);

                    _lastInsertIndex = Interlocked.Increment(ref _tail) - 1;
                    _fastStorage[modIdx] = value;
#if DEBUG
                    _fastStorageTime[modIdx] = Interlocked.Increment(ref _opCounter) - 1;
#endif
                    Interlocked.Exchange(ref fastBloomPtr, _set);
                    
#if SUPER_SYNC
                    Interlocked.MemoryBarrierProcessWide();
#elif SYNC
                    Thread.MemoryBarrier();
#endif

                    return _lastInsertIndex;
                }

                var i = Log2(modIdx + 1);
                var i2 = modIdx - ((1 << i) - 1);
                ref var bloomPtr = ref _bloom[i][i2];

                sw.Reset();
                retry2:
                if (Tail != latch || (state = Interlocked.CompareExchange(ref bloomPtr, _reset, _zero)) != _zero)
                {
                    if (state == _one &&
                        Interlocked.CompareExchange(ref bloomPtr, _reset, _one) == _one)
                    {
                        
                    }
                    else
                    {
                        if (Tail != latch || state != _reset)
                            return -1;

                        state = -1;
                        sw.SpinOnce();
                        goto retry2;
                    }
                }

                if (Tail != latch)
                {
                    Interlocked.MemoryBarrierProcessWide();
                    if (Interlocked.CompareExchange(ref bloomPtr, _zero, _reset) != _reset)
                        LogManager.GetCurrentClassLogger().Fatal($"add: Unable to restore lock at {latch}, bloom = {{fastBloomPtr}}  - {Description}");
                    return -1;
                }

                Interlocked.Increment(ref _count);

                _lastInsertIndex = Interlocked.Increment(ref _tail) - 1;

                _storage[i][i2] = value;
                Interlocked.Exchange(ref bloomPtr, _set);
#if SUPER_SYNC
                Interlocked.MemoryBarrierProcessWide();


#elif SYNC
                    Thread.MemoryBarrier();
#endif

                return _lastInsertIndex;
            }
            finally
            {
#if DEBUG
                if (ts.ElapsedMs() > _CASerror)
                {
                    LogManager.GetCurrentClassLogger().Fatal($"{nameof(AtomicAdd)}: CAS took => {ts.ElapsedMs()} ms");
                }
#endif
            }
        }

        /// <summary>
        /// Wraps Interlocked.CompareExchange that copes with horizontal scaling
        /// </summary>
        /// <param name="value">The new value</param>
        /// <returns>False on race, true otherwise</returns>
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private bool AtomicRemove(out T value)
        {
#if DEBUG
            var ts = Environment.TickCount;
#endif
            SpinWait sw = new();
            try
            {
#if DEBUG
                var latchOp = _opCounter;
#endif
                var state = -1;

                var latch = Head;
                var modIdx = latch % Capacity;

                if (!IsAutoScaling)
                {
                    ref var fastBloomPtr = ref _fastBloom[modIdx];
                    
                    retry:
                    if (Head != latch || (state = Interlocked.CompareExchange(ref fastBloomPtr, _reset, _set)) != _set)
                    {
                        if (fastBloomPtr == _one && Interlocked.CompareExchange(ref fastBloomPtr, _zero, _one) == _one)
                        {
#if !DEBUG
                            Interlocked.Increment(ref _head);
#else
                            _lastRemoveIndex = Interlocked.Increment(ref _head) - 1;
#endif
                        }
                        else
                        {
                            if (Head != latch || state != _reset || sw.Count > short.MaxValue)
                            {
                                value = default;
                                return false;
                            }
                        }

                        sw.SpinOnce();
                        goto retry;
                    }

                    if (Head != latch)
                    {
                        Interlocked.MemoryBarrierProcessWide();
                        if (Interlocked.CompareExchange(ref fastBloomPtr, _set, _reset) != _reset)
                            LogManager.GetCurrentClassLogger().Fatal($"Rf> Unable to restore lock at {latch}, bloom = {fastBloomPtr}  - {Description}");

                        value = default;
                        return false;
                    }
                    
#if !DEBUG
                    Interlocked.Increment(ref _head);
#else
                    _lastRemoveIndex = Interlocked.Increment(ref _head) - 1;
#endif
                    Interlocked.Decrement(ref _count);

                    value = _fastStorage[modIdx];
                    _fastStorage[modIdx] = default;

#if DEBUG
                    _fastStorageTime[modIdx] = -(Interlocked.Increment(ref _opCounter) - 1);

                    if (_fastStorageTime[modIdx] - latchOp > 3)
                    {
                        Console.WriteLine($"-------> & zero skew ({_opCounter - latchOp}/{Capacity}) == ({(_opCounter - latchOp) / (float)Capacity * 100:0.0}%)");
                    }
#endif
                    Interlocked.Exchange(ref fastBloomPtr, _zero);

                    
#if SUPER_SYNC
                    Interlocked.MemoryBarrierProcessWide();


#elif SYNC
                    Thread.MemoryBarrier();
#endif


                    return true;
                }

                var i = Log2(modIdx + 1);
                var i2 = modIdx - ((1 << i) - 1);
                ref var bloomPtr = ref _bloom[i][i2];
                sw.Reset();
                retry2:
                if (bloomPtr != _one && (Head != latch || (state = Interlocked.CompareExchange(ref bloomPtr, _reset, _set)) != _set))
                {
                    if (bloomPtr == _one && Interlocked.CompareExchange(ref bloomPtr, _zero, _one) == _one)
                    {
#if !DEBUG
                        Interlocked.Increment(ref _head);
#else
                        _lastRemoveIndex = Interlocked.Increment(ref _head) - 1;
#endif
                    }
                    else
                    {
                        if (Head != latch || state != _reset)
                        {
                            value = default;
                            return false;
                        }
                    }

                    sw.SpinOnce();
                    goto retry2;
                }

                if (Head != latch)
                {
                    Interlocked.MemoryBarrierProcessWide();
                    if (Interlocked.CompareExchange(ref bloomPtr, _set, _reset) != _reset)
                        LogManager.GetCurrentClassLogger().Fatal($"R> Unable to restore lock at {latch}, bloom = {bloomPtr}  - {Description}");

                    value = default;
                    return false;
                }
                
                Interlocked.Increment(ref _head);
                Interlocked.Decrement(ref _count);

                value = _storage[i][i2];
                _storage[i][i2] = default;
                Interlocked.Exchange(ref bloomPtr, _zero);
#if SUPER_SYNC
                Interlocked.MemoryBarrierProcessWide();


#elif SYNC
                    Thread.MemoryBarrier();
#endif

                return true;
            }
            finally
            {
#if DEBUG
                if (ts.ElapsedMs() > _CASerror)
                {
                    LogManager.GetCurrentClassLogger().Fatal($"{nameof(AtomicRemove)}: CAS[{sw.Count}] took => {ts.ElapsedMs()} ms");
                }
#endif
            }
        }


#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private bool AtomicDrop(long index)
        {
#if DEBUG
            var ts = Environment.TickCount;
#endif
            
            try
            {
                var modIdx = index % Capacity;
                if (!IsAutoScaling)
                {
                    if (Interlocked.CompareExchange(ref _fastBloom[modIdx], _one, _set) != _set) return false;
                    Interlocked.Decrement(ref _count);
                    return true;
                }
                    

                var i = Log2(modIdx + 1);
                var i2 = modIdx - ((1 << i) - 1);

                if (Interlocked.CompareExchange(ref _bloom[i][i2], _one, _set) != _set) return false;
                Interlocked.Decrement(ref _count);
                return true;
            }
            finally
            {
#if SUPER_SYNC
                Interlocked.MemoryBarrierProcessWide();


#elif SYNC
                    Thread.MemoryBarrier();
#endif
#if DEBUG
                if (ts.ElapsedMs() > _CASerror)
                {
                    LogManager.GetCurrentClassLogger().Fatal($"{nameof(AtomicRemove)}: CAS took => {ts.ElapsedMs()} ms");
                }
#endif
            }
        }

        /// <summary>
        /// Add item to the bag
        /// </summary>
        /// <param name="item">The item to be added</param>
        /// <param name="deDup">Whether to de-dup this item from the bag</param>
        /// <param name="onAtomicAdd">Action to execute on add success</param>
        /// <param name="context">Action context</param>
        /// <exception cref="OutOfMemoryException">Thrown if we are internally OOM</exception>
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public long TryEnqueue<TC>(T item, bool deDup = false, Action<TC> onAtomicAdd = null, TC context = default)
        {
            Debug.Assert(Zeroed || item != null);

            if (Zeroed || _clearing > 0 || _count >= Capacity && _autoScale == false)
                return -1;

            //auto scale
            if (_autoScale && (_primedForScale > 0 || Count >= Capacity >> 1))
            {
                if (!Scale() && Count >= Capacity)
                {
                    //continue to drain the queues on dropped inserts...
                    if (_blockingCollection && _sharingConsumers > 0)
                    {
                        try
                        {
                            return _balanceSync.Release(Environment.TickCount)? 1:0;
                        }
                        catch
                        {
                            // ignored
                        }
                    } 
                    else if (_blockingCollection && _blockingConsumers > 0)
                    {
                        try
                        {
                            return _fanSync.Release(Environment.TickCount, _blockingConsumers);
                        }
                        catch
                        {
                            // ignored
                        }
                    }
                    else if (_blockingCollection && _pumpingConsumers > 0)
                    {
                        try
                        {
                            return _zeroSync.Release(item)? 1:0;
                        }
                        catch
                        {
                            // ignored
                        }
                    }

                    if (IsAutoScaling)
                        Scale();
                    else
                        return -1;
                }
            }

            if (deDup)
            {
                if (Contains(item))
                    return -1;
            }

            try
            {
                //message pump
                if (_blockingCollection && _pumpingConsumers > 0)
                {
                    try
                    { 
                        return _zeroSync.Release(item)? 1:0;
                    }
                    catch
                    {
                        // ignored
                    }
                }

                long cap, idx;
                while (Tail >= Head + (cap = Capacity) || _count >= cap || (idx = AtomicAdd(item)) < 0)
                {
                    if (_count == cap)
                    {
                        if (IsAutoScaling)
                            Scale();
                        else
                            return -1;
                    }

                    if (Zeroed)
                        return -1;
                }

                //execute atomic action on success
                onAtomicAdd?.Invoke(context);
                
                //service async blockers
                if (_blockingCollection && _sharingConsumers > 0)
                {
                    try
                    {
                        _balanceSync.Release(Environment.TickCount);
                    }
                    catch
                    {
                        // ignored
                    }
                }

                if (_blockingCollection && _blockingConsumers > 0)
                {
                    try
                    {
                        _fanSync.Release(Environment.TickCount, _blockingConsumers);
                    }
                    catch
                    {
                        // ignored
                    }
                }

                //_curEnumerator.IncIteratorCount(); //TODO: is this a good idea?

                return idx;
            }
            catch when (Zeroed)
            {
            }
            catch (Exception e) when (!Zeroed)
            {
                LogManager.GetCurrentClassLogger().Error(e);
            }

            return -1;
        }
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public long TryEnqueue(T item, bool deDup = false) => TryEnqueue<object>(item, deDup);

        /// <summary>
        /// Try take from the Q, round robin
        /// </summary>
        /// <param name="slot">The item to be fetched</param>
        /// <returns>True if an item was found and returned, false otherwise</returns>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryDequeue([MaybeNullWhen(false)] out T slot)
        {
            try
            {
                if (Count == 0)
                {
                    slot = default;
                    return false;
                }

                while (Head >= Tail || !AtomicRemove(out slot))
                {
                    if (Count == 0 || Zeroed)
                    {
                        slot = default;
                        return false;
                    }
                }

                return true;
            }
            catch (Exception e)
            {
                LogManager.LogFactory.GetCurrentClassLogger().Error(e, $"{nameof(TryDequeue)} failed!");
            }
            
            slot = default;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Drop(long index) => AtomicDrop(index);
        
        /// <summary>
        /// Peeks the head of the queue
        /// </summary>
        /// <param name="result">Returns the head of the Q</param>
        /// <returns>True if the head was not null, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPeek([MaybeNullWhen(false)] out T result)
        {
            return (result = this[Head]) != null;
        }

        /// <summary>
        /// ZeroAsync managed cleanup
        /// </summary>
        /// <param name="op">Optional callback to execute on all items in the bag</param>
        /// <param name="nanite">Callback context</param>
        /// <param name="zero">Whether the bag is assumed to contain <see cref="IIoNanite"/>s and should only be zeroed out</param>
        /// <typeparam name="TC">The callback context type</typeparam>
        /// <returns>True if successful, false if something went wrong</returns>
        /// <exception cref="ArgumentException">When zero is true but <see cref="nanite"/> is not of type <see cref="IIoNanite"/></exception>
        public async ValueTask<bool> ZeroManagedAsync<TC>(Func<T, TC, ValueTask> op = null, TC nanite = default, bool zero = false)
        {
            if (zero && Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return true;

            if (Interlocked.CompareExchange(ref _clearing, 1, 0) != 0)
                return true;

            try
            {
                for (long i = 0; i < Capacity; i++)
                {
                    var item = this[i];
                    try
                    {
                        if (item is not null)
                        {
                            if (op != null)
                                await op(item, nanite).FastPath();

                            if (item is IIoNanite ioNanite)
                            {
                                if (!ioNanite.Zeroed())
                                    await ioNanite.DisposeAsync((IIoNanite)nanite, string.Empty).FastPath();
                            }
                            else if (item is IAsyncDisposable asyncDisposable)
                                await asyncDisposable.DisposeAsync().FastPath();
                            else if (item is IDisposable disposable)
                                disposable.Dispose();
                        }
                    }
                    catch (InvalidCastException) { }
                    catch (Exception) when (Zeroed) { }
                    catch (Exception e) when (!Zeroed)
                    {
                        LogManager.GetCurrentClassLogger().Trace(e, $"{_description}: {op}, {item}, {nanite}");
                    }
                    finally
                    {
                        this[i] = default;

                        if (!IsAutoScaling)
                            _fastBloom[i % _capacity] = 0;
                        else
                        {
                            var idx = i % Capacity;
                            var i2 = Log2(idx + 1);
                            Interlocked.Exchange(ref _bloom[i2][idx - ((1 << i2) - 1)], 0);
                        }
                    }
                }

            }
            catch
            {
                return false;
            }
            finally
            {
                _count = (int)(_head = _tail = 0);
#if DEBUG
                if (IsAutoScaling)
                {
                    for (var i = 0; i < _bloom.Length && _bloom[i] != null; i++)
                    {
                        for (var j = 0; j < _bloom[i].Length; j++)
                        {
                            if (_bloom != null && _bloom[i][j] != 0)
                            {
                                LogManager.GetCurrentClassLogger().Fatal($"{nameof(ZeroManagedAsync)}: Tainted bloom filter at [{i}][{j}] = {_bloom[i][j]} ({_storage[i][j]})");
                            }
                        }
                    }
                }
#endif
                Interlocked.Exchange(ref _clearing, 0);

                if(zero)
                    _balanceSync.ZeroSem();
            }

            return true;
        }

        /// <summary>
        /// Contains
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Contains(T item)
        {
            for (var i = 0; i < _virility + 1; i++)
            {
                if (_storage[i].Contains(item))
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Returns the bag enumerator
        /// </summary>
        /// <returns>The bag enumerator</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<T> GetEnumerator()
        {
            //_curEnumerator = (IoQEnumerator<T>)_curEnumerator.Reuse(this, b => new IoQEnumerator<T>((IoZeroQ<T>)b));
            //return _curEnumerator;
            //return _curEnumerator = new IoQEnumerator<T>(this);
            return new IoQEnumerator<T>(this);
        }

        /// <summary>
        /// Async blocking consumer support
        /// </summary>
        /// <returns>The next inserted item</returns>
        protected async IAsyncEnumerable<T> BlockOnConsumeAsync()
        {
            if (!_blockingCollection)
                yield return default;

            try
            {
                Interlocked.Increment(ref _blockingConsumers);
                var cur = Head;
                while (!_fanSync.Zeroed())
                {
                    if (cur >= Tail && (await _fanSync.WaitAsync().FastPath()).ElapsedMs() > 0x7ffffff)
                        break;
                    
                    var newItem = this[cur];
                    if (newItem != null)
                        yield return newItem;

                    cur++;
                }
            }
            finally
            {
                Interlocked.Decrement(ref _blockingConsumers);
            }
        }

        /// <summary>
        /// Async balancing consumer support
        /// </summary>
        /// <returns>The next inserted item</returns>
        protected async IAsyncEnumerable<T> BalanceOnConsumeAsync()
        {
            if (!_blockingCollection)
                yield return default;

            try
            {
                Interlocked.Increment(ref _sharingConsumers);

                //follow the tail
                while (!_balanceSync.Zeroed())
                {
                    try
                    {
                        if (Count == 0 && (await _balanceSync.WaitAsync().FastPath()).ElapsedMs() > 0x7ffffff)
                            break;
                    }
                    catch (Exception e)
                    {
                        LogManager.GetCurrentClassLogger().Error(e, Description);
                    }

                    while (TryDequeue(out var next))
                        yield return next;
                }
            }
            finally
            {
                Interlocked.Decrement(ref _sharingConsumers);
            }
        }

        /// <summary>
        /// Async pump consumer support
        /// </summary>
        /// <returns>The next inserted item</returns>
        protected async IAsyncEnumerable<T> PumpOnConsumeAsync()
        {
            if (!_blockingCollection)
                yield return default;

            try
            {
                Interlocked.Increment(ref _pumpingConsumers);

                //follow the tail
                while (!_zeroSync.Zeroed())
                {
                    T next = default;
                    try
                    {
                        if (Count == 0 || !TryDequeue(out next))
                        {
                            if((next = await _zeroSync.WaitAsync().FastPath()) == null)
                                break;
                        }
                    }
                    catch (Exception e)
                    {
                        LogManager.GetCurrentClassLogger().Error(e,Description);
                    }
                    yield return next;
                }
            }
            finally
            {
                Interlocked.Decrement(ref _pumpingConsumers);
            }
        }
        public IAsyncEnumerable<T> BlockOnConsumeAsync(int threadIndex) => _fanSyncs[threadIndex]();
        public IAsyncEnumerable<T> BalanceOnConsumeAsync(int threadIndex) =>_balanceSyncs[threadIndex]();
        public IAsyncEnumerable<T> PumpOnConsumeAsync(int threadIndex) => _zeroSyncs[threadIndex]();

        /// <summary>
        /// Returns the bag enumerator
        /// </summary>
        /// <returns>The bag enumerator</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Log2(long n)
        {
            var count = 0; 
            while (n > 1) 
            {
                n >>= 1;
                count++;
            }

            return count;
        }
    }
}