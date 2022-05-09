using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.misc;
using zero.core.patterns.queue.enumerator;
using zero.core.patterns.semaphore;
using zero.@unsafe.core.math;

namespace zero.core.patterns.queue
{
    /// <summary>
    /// A lighter concurrent round robin Q
    /// </summary>
    public class IoZeroQ<T> : IEnumerable<T>
    where T : class
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

            //if scaling is enabled
            if (autoScale)
            {
                //TODO: tuning
                capacity = Math.Max(4, capacity);
                _hwm = _capacity = 1;
                _storage = new T[32][];
                _storage[0] = _fastStorage = new T[_capacity];

                var v = IoMath.Log2((ulong)capacity) - 1;
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
                _hwm = _capacity = capacity;
                _storage = new T[1][];
                _storage[0] = _fastStorage = new T[_capacity];
            }

            if (_blockingCollection)
            {
                _fanSync = new IoZeroSemaphoreSlim(asyncTasks, $"fan {description}", concurrencyLevel, zeroAsyncMode: zeroAsyncMode); //TODO: tuning
                _balanceSync = new IoZeroSemaphoreSlim(asyncTasks, $"balance {description}", concurrencyLevel, zeroAsyncMode: zeroAsyncMode); //TODO: tuning
                _zeroSync = new IoZeroSemaphoreChannel<T>(asyncTasks, $"pump  {description}", concurrencyLevel, zeroAsyncMode: zeroAsyncMode); //TODO: tuning
            }
            
            _curEnumerator = new IoQEnumerator<T>(this);

            _sentinel = Unsafe.As<T>(new object());
        }

        #region packed
        private long _head; 
        private long _tail;
        private long _hwm;

        private readonly string _description;

        private readonly T[][] _storage;
        private readonly T[] _fastStorage;

        private readonly T _sentinel;
        private readonly object _syncRoot = new();
        
        private volatile IoQEnumerator<T> _curEnumerator;
        private readonly IoZeroSemaphoreSlim _fanSync;
        private readonly IoZeroSemaphoreChannel<T> _zeroSync;
        private readonly IoZeroSemaphoreSlim _balanceSync;

        private volatile int _zeroed;
        private volatile int _capacity;
        private volatile int _virility;
        private volatile int _count;
        private volatile int _blockingConsumers;
        private volatile int _sharingConsumers;
        private volatile int _pumpingConsumers;
        private volatile int _primedForScale;
        private readonly bool _autoScale;
        private readonly bool _blockingCollection;
        #endregion

        public long Tail => _tail;
        public long Head => _head;

        /// <summary>
        /// ZeroAsync status
        /// </summary>
        public bool Zeroed => _zeroed > 0;

        /// <summary>
        /// Description
        /// </summary>
        public string Description => $"{nameof(IoZeroQ<T>)}: z = {_zeroed > 0}, {nameof(Count)} = {_count}/{Capacity}, s = {IsAutoScaling}, h = {Head}/{Tail} (d:{Tail - Head}), desc = {_description}";

        /// <summary>
        /// Current number of items in the bag
        /// </summary>
        public int Count => _count;

        /// <summary>
        /// Capacity
        /// </summary>
        public long Capacity => IsAutoScaling ? unchecked(_capacity * ((1 << (_virility + 1)) - 1)) : _capacity;

        /// <summary>
        /// Whether we are auto scaling
        /// </summary>
        public bool IsAutoScaling => _autoScale;

        /// <summary>
        /// Q item by index
        /// </summary>
        /// <param name="idx">index</param>
        /// <returns>Object stored at index</returns>
        public T this[long idx]
        {
            get
            {
                Debug.Assert(idx >= 0);

                //if (!IsAutoScaling) return Volatile.Read(ref _fastStorage[idx % _capacity]);
                if (!IsAutoScaling) return _fastStorage[idx % _capacity];

                idx %= Capacity;
                var i = IoMath.Log2(unchecked((ulong)idx + 1));
                //return Volatile.Read(ref _storage[i][idx - ((1 << i) - 1)]);
                return _storage[i][idx - ((1 << i) - 1)];
            }
            protected set
            {
                Debug.Assert(idx >= 0);

                if (!IsAutoScaling)
                {
                    _fastStorage[idx % _capacity] = value;
                    //Interlocked.Exchange(ref _fastStorage[idx % _capacity], value);
                    return;
                }

                idx %= Capacity;
                var i = IoMath.Log2(unchecked((ulong)idx + 1));
                _storage[i][idx - ((1 << i) - 1)] = value;
                //Interlocked.Exchange(ref _storage[i][idx - ((1 << i) - 1)], value);
            }
        }

        /// <summary>
        /// Horizontal scale
        /// </summary>
        /// <returns>True if scaling happened, false on race or otherwise.</returns>
        private bool Scale(bool force = false)
        {
            if (!IsAutoScaling)
                return false;

            lock (_syncRoot)
            {
                var cap2 = Capacity >> 1;

                if (_primedForScale == 0)
                {
                    if (_count >= cap2)
                        Interlocked.CompareExchange(ref _primedForScale, 1, 0);
                }

                //Only allow scaling to happen only when the Q dips under 50% capacity & some other factors, otherwise the indexes will corrupt.
                if (_primedForScale == 1 && Head <= cap2 && Tail <= cap2 && (Tail > Head || Tail == Head && _count < cap2) && Interlocked.CompareExchange(ref _primedForScale, 2, 1) == 1 || force)
                {
                    var hwm = 1 << (_virility + 1);
                    _storage[_virility + 1] = new T[hwm];
                    Interlocked.Add(ref _hwm, hwm);
                    Interlocked.Increment(ref _virility);
                    Interlocked.Exchange(ref _primedForScale, 0);
                    Interlocked.MemoryBarrierProcessWide();
                    return true;
                }
                return false;
            }
        }

        /// <summary>
        /// Wraps Interlocked.CompareExchange that copes with horizontal scaling
        /// </summary>
        /// <param name="idx">index to work with</param>
        /// <param name="value">The new value</param>
        /// <param name="compare">The compare value</param>
        /// <returns>The previous value</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private T CompareExchange(long idx, T value, T compare)
        {
            if (!IsAutoScaling) return Interlocked.CompareExchange(ref _fastStorage[idx % _capacity], value, compare);

            idx %= Capacity;
            var i = IoMath.Log2(unchecked((ulong)idx + 1));
            return Interlocked.CompareExchange(ref _storage[i][idx - ((1 << i) - 1)], value, compare);
        }

        /// <summary>
        /// Add item to the bag
        /// </summary>
        /// <param name="item">The item to be added</param>
        /// <param name="deDup">Whether to de-dup this item from the bag</param>
        /// <param name="onAtomicAdd">Action to execute on add success</param>
        /// <param name="context">Action context</param>
        /// <exception cref="OutOfMemoryException">Thrown if we are internally OOM</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long TryEnqueue<TC>(T item, bool deDup = false, Action<TC> onAtomicAdd = null, TC context = default)
        {
            Debug.Assert(Zeroed || item != null);

            if (_autoScale && (_primedForScale == 1 || _count >= Capacity >> 1))
            {
                if (!Scale() && _count >= Capacity)
                    return -1;
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
                        return _zeroSync.Release(item, 1, false);
                    }
                    catch
                    {
                        // ignored
                    }
                }


                long tail;
                T slot = null;
                long cap;
                var race = false;

                while ((tail = Tail) >= Head + (cap = Capacity) || _count >= cap || this[tail] != null || tail != Tail ||
                       (slot = CompareExchange(tail, _sentinel, null)) != null || (race = tail != Tail))
                {
                    if (race)
                    {
                        if (CompareExchange(tail, null, _sentinel) != _sentinel && !Zeroed)
                        {
                            LogManager.GetCurrentClassLogger()
                                .Fatal(
                                    $"{nameof(TryEnqueue)}: Unable to restore lock at tail = {tail} != {Tail}, slot = `{null}', cur = `{this[tail]}'");
                        }
                    }

                    if (Zeroed || !_autoScale && _count >= cap)
                        return -1;

                    if (slot != null)
                        Interlocked.MemoryBarrierProcessWide();
                    else
                        Interlocked.MemoryBarrier();

                    race = false;
                }
#if DEBUG
                Debug.Assert(Zeroed || slot == null);
                Debug.Assert(Zeroed || this[tail] == _sentinel);
                Debug.Assert(Zeroed || tail == Tail);
                Interlocked.MemoryBarrier();
#endif
                //execute atomic action on success
                onAtomicAdd?.Invoke(context);
                    
                Interlocked.Increment(ref _count);
                this[tail] = item;
                Interlocked.MemoryBarrier();
                Interlocked.Increment(ref _tail);

                //service async blockers
                if (_blockingCollection && _sharingConsumers > 0)
                {
                    try
                    {
                        _balanceSync.Release( true, false);
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
                        _fanSync.Release(true,_blockingConsumers, false);
                    }
                    catch
                    {
                        // ignored
                    }
                }

                //_curEnumerator.IncIteratorCount(); //TODO: is this a good idea?

                return tail;
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long TryEnqueue(T item, bool deDup = false) => TryEnqueue<object>(item, deDup);

        /// <summary>
        /// Try take from the Q, round robin
        /// </summary>
        /// <param name="returnValue">The item to be fetched</param>
        /// <returns>True if an item was found and returned, false otherwise</returns>
#if DEBUG
    [MethodImpl(MethodImplOptions.NoInlining)]
#else
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public bool TryDequeue([MaybeNullWhen(false)] out T returnValue)
        {
            try
            {
                if (_count == 0)
                {
                    returnValue = null;
                    return false;
                }

                long head;
                T slot = null;
                T latch;
                //var race = false;

                while ((head = Head) >= Tail || (latch = this[head]) == _sentinel || latch == null || head != Head ||
                       (slot = CompareExchange(head, _sentinel, latch)) != latch) //|| (race = head != Head))
                {
                    //if (race)
                    //{
                    //    if ((slot = CompareExchange(head, slot, _sentinel)) != _sentinel)
                    //    {
                    //        LogManager.GetCurrentClassLogger().Fatal($"{nameof(TryEnqueue)}: Unable to restore lock at head = {head}, too {slot}, cur = {this[head]}");
                    //    }
                    //}

                    //if (slot != null)
                    //    Interlocked.MemoryBarrierProcessWide();
                    //else
                    //    Interlocked.MemoryBarrier();

                    if (_count == 0 || Zeroed)
                    {
                        returnValue = null;
                        return false;
                    }

                    slot = null;
                    //race = false;
                }


#if DEBUG
                Debug.Assert(Zeroed || this[head] == _sentinel);
                Debug.Assert(Zeroed || slot != null);
                Debug.Assert(Zeroed || _count > 0);
                Debug.Assert(Zeroed || head == Head);
                Debug.Assert(Zeroed || head != Tail);
                Interlocked.MemoryBarrier();
#endif
                this[head] = null;//restore the sentinel
                Interlocked.Decrement(ref _count);
                Interlocked.MemoryBarrier();
                Interlocked.Increment(ref _head);
                
                returnValue = slot;
                return true;
            }
            catch (Exception e)
            {
                LogManager.LogFactory.GetCurrentClassLogger().Error(e, $"{nameof(TryDequeue)} failed!");
            }
            
            returnValue = null;
            return false;
        }

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

            try
            {
                for (var i = 0; i < Capacity; i++)
                {
                    var item = this[i];
                    try
                    {
                        if (item == default)
                            continue;

                        if (op != null)
                            await op(item, nanite).FastPath();

                        if (item is IIoNanite ioNanite)
                        {
                            if (!ioNanite.Zeroed())
                                await ioNanite.Zero((IIoNanite)nanite, string.Empty).FastPath();
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
                    }
                }

            }
            catch
            {
                return false;
            }
            finally
            {
                _count = 0;
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
            return _curEnumerator = new IoQEnumerator<T>(this);
        }

        /// <summary>
        /// Async blocking consumer support
        /// </summary>
        /// <returns>The next inserted item</returns>
        public async IAsyncEnumerable<T> BlockOnConsumeAsync()
        {
            if (!_blockingCollection)
                yield return null;

            try
            {
                Interlocked.Increment(ref _blockingConsumers);
                var cur = Head;
                while (!_fanSync.Zeroed())
                {
                    if (cur >= Tail && !await _fanSync.WaitAsync().FastPath())
                        break;

                    var newItem = this[cur];
                    if (newItem != _sentinel && newItem != null)
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
        public async IAsyncEnumerable<T> BalanceOnConsumeAsync()
        {
            if (!_blockingCollection)
                yield return null;

            try
            {
                Interlocked.Increment(ref _sharingConsumers);

                //drain the head
                while (!_balanceSync.Zeroed() && TryDequeue(out var next))
                    yield return next;

                //follow the tail
                while (!_balanceSync.Zeroed())
                {
                    try
                    {
                        if (!await _balanceSync.WaitAsync().FastPath())
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
        public async IAsyncEnumerable<T> PumpOnConsumeAsync()
        {
            if (!_blockingCollection)
                yield return null;

            try
            {
                Interlocked.Increment(ref _pumpingConsumers);

                //drain the head
                while (!_zeroSync.Zeroed() && TryDequeue(out var next))
                    yield return next;

                //follow the tail
                while (!_zeroSync.Zeroed())
                {
                    T next = null;
                    try
                    {
                        if ((next = await _zeroSync.WaitAsync().FastPath()) == null)
                            break;
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

        /// <summary>
        /// Returns the bag enumerator
        /// </summary>
        /// <returns>The bag enumerator</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}