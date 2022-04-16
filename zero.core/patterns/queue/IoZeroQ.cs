using System;
using System.Collections;
using System.Collections.Concurrent;
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
using zero.core.patterns.semaphore.core;
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
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public IoZeroQ(string description, int capacity, bool autoScale = false, CancellationTokenSource asyncTasks = null, int concurrencyLevel = 0)
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
                _blockSync = new IoZeroSemaphoreSlim(asyncTasks, $"{description}", concurrencyLevel, maxAsyncWork: 0);

            _curEnumerator = new IoQEnumerator<T>(this);

            _sentinel = Unsafe.As<T>(new object());
        }

        private long _head; 
        private long _tail;

        private volatile int _zeroed;
        private readonly string _description;

        private T[][] _storage;
        private readonly T[] _fastStorage;

        private readonly T _sentinel;
        //private readonly object _syncRoot = new();
        private volatile int _capacity;
        private volatile int _virility;
        private long _hwm;
        public long Tail => Interlocked.Read(ref _tail);
        public long Head => Interlocked.Read(ref _head);
        //public long Tail => _tail;
        //public long Head => _head;
        private volatile IoQEnumerator<T> _curEnumerator;

        private volatile int _count;
        private readonly bool _autoScale;
        private readonly bool _blockingCollection;
        private volatile int _blockingConsumers;
        private volatile int _primedForScale;
        private readonly IoZeroSemaphoreSlim _blockSync;

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

                if (!IsAutoScaling) return Volatile.Read(ref _fastStorage[idx % _capacity]);

                idx %= Capacity;
                var i = IoMath.Log2(unchecked((ulong)idx + 1));
                return Volatile.Read(ref _storage[i][idx - ((1 << i) - 1)]);
            }
            protected set
            {
                Debug.Assert(idx >= 0);

                if (!IsAutoScaling)
                {
                    _fastStorage[idx % _capacity] = value;
                    Interlocked.MemoryBarrier();
                    return;
                }

                idx %= Capacity;
                var i = IoMath.Log2(unchecked((ulong)idx + 1));
                _storage[i][idx - ((1 << i) - 1)] = value;
                Interlocked.MemoryBarrier();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Get(long idx, T next = null)
        {
            if (!IsAutoScaling) return Interlocked.Exchange(ref _storage[0][idx % _capacity], next);
            idx %= Capacity;
            var i = IoMath.Log2(unchecked((ulong)idx + 1));
            return Interlocked.Exchange(ref _storage[i][idx - ((1 << i) - 1)], next);
        }
        
        /// <summary>
        /// Horizontal scale
        /// </summary>
        /// <returns>True if scaling happened, false on race or otherwise.</returns>
        private bool Scale(bool force = false)
        {
            if (!IsAutoScaling)
                return false;

            //lock (_syncRoot)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private T Exchange(long idx, T value)
        {
            if (!IsAutoScaling) return Interlocked.Exchange(ref _fastStorage[idx % _capacity], value);

            idx %= Capacity;
            var i = IoMath.Log2(unchecked((ulong)idx + 1));
            
            return Interlocked.Exchange(ref _storage[i][idx - ((1 << i) - 1)], value);
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
        /// <exception cref="OutOfMemoryException">Thrown if we are internally OOM</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long TryEnqueue(T item, bool deDup = false)
        {
            if (Zeroed)
                return -1;

            Debug.Assert(Zeroed || item != null);
            
            if (_autoScale && (_primedForScale == 1 || _count >= Capacity>>1 ))
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
                long tail;
#if DEBUG
                int c = 0;
#endif

                T slot = null;
                long cap;
                bool blocked, race = false;

                while ((blocked = (tail = Tail) >= Head + (cap = Capacity)) || _count >= cap || tail != Tail ||
                       (slot = CompareExchange(tail, item, null)) != null || (race = tail != Tail))
                {
                    if (race)
                    {
                        if ((slot = CompareExchange(tail, null, item)) != item)
                        {
                            LogManager.GetCurrentClassLogger()
                                .Fatal(
                                    $"{nameof(TryEnqueue)}: Unable to restore lock at tail = {tail}, too {slot}, cur = {this[tail]}");
                        }
                    }
#if DEBUG
                    if (!blocked && c++ > 100000)
                        throw new InternalBufferOverflowException($"{Description}");
#endif

                    if (Zeroed || !_autoScale && _count >= cap)
                        return -1;

                    if (slot != null)
                        Interlocked.MemoryBarrierProcessWide();
                    else
                        Interlocked.MemoryBarrier();

                    race = false;
                    Thread.Yield();
                }
#if DEBUG
                Debug.Assert(Zeroed || slot == null);
                Debug.Assert(Zeroed || this[tail] == item);
                Debug.Assert(Zeroed || tail == Tail);
                Interlocked.MemoryBarrier();
#endif
                Interlocked.Increment(ref _count);
                Interlocked.MemoryBarrier();
                Interlocked.Increment(ref _tail);

                //service async blockers
                if (_blockingCollection && _blockingConsumers > 0)
                    _blockSync.Release(_blockingConsumers, bestCase: Head != Tail);

                _curEnumerator.IncIteratorCount(); //TODO: is this a good idea?

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

        /// <summary>
        /// Try take from the Q, round robin
        /// </summary>
        /// <param name="returnValue">The item to be fetched</param>
        /// <returns>True if an item was found and returned, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryDequeue([MaybeNullWhen(false)] out T returnValue)
        {
            try
            {
                if (_count == 0 || Zeroed)
                {
                    returnValue = null;
                    return false;
                }

                long head;
                T slot = null;
#if DEBUG
                int c = 0;
#endif
                T latch = null;
                //bool race = false;
                while ((head = Head) >= Tail || (latch = this[head]) == _sentinel || latch == null || head != Head ||
                       (slot = CompareExchange(head, _sentinel, latch)) != latch) //|| (race = head != Head))
                {
                    //TODO: I don't think this one is needed, overruns can handle because it looks for non null values
                    //if (race)
                    //{
                    //    if ((slot = CompareExchange(head, latch, _sentinel)) != _sentinel)
                    //    {
                    //        LogManager.GetCurrentClassLogger().Fatal($"{nameof(TryEnqueue)}: Unable to restore lock at head = {head}, too {latch}, cur = {this[head]}");
                    //    }
                    //}
#if DEBUG
                    if (c++ > 50000)
                        throw new InternalBufferOverflowException($"{Description}");
#endif
                    if(slot != null)
                        Interlocked.MemoryBarrierProcessWide();
                    else
                        Interlocked.MemoryBarrier();

                    if (_count == 0 || Zeroed)
                    {
                        returnValue = null;
                        return false;
                    }

                    slot = null;
                    //race = false;
                    Thread.Yield();
                }


#if DEBUG
                Debug.Assert(Zeroed || this[head] == _sentinel);
                Debug.Assert(Zeroed || slot != null);
                Debug.Assert(Zeroed || _count > 0);
                Debug.Assert(Zeroed || head == Head);
                Debug.Assert(Zeroed || head != Tail);
                Interlocked.MemoryBarrier();
#endif
                Interlocked.Decrement(ref _count);
                this[head] = null;//restore the sentinel
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
            return (result = this[Head % Capacity]) != null;
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

                if (zero)
                {
                    _storage = null;
                }
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
            _curEnumerator = (IoQEnumerator<T>)_curEnumerator.Reuse(this, b => new IoQEnumerator<T>((IoZeroQ<T>)b));
            return _curEnumerator;
            //return _curEnumerator = new IoQEnumerator<T>(this);
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
                while (!_blockSync.Zeroed())
                {
                    if (cur == Tail && !await _blockSync.WaitAsync().FastPath())
                        break;

                    var newItem = this[cur];
                    if (newItem != _sentinel && newItem != null)
                    {
                        cur++;
                        yield return newItem;
                    }
                }
            }
            finally
            {
                Interlocked.Decrement(ref _blockingConsumers);
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