using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.misc;
using zero.core.patterns.misc;
using zero.core.patterns.queue.enumerator;
using zero.core.runtime.scheduler;
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
        public IoZeroQ(string description, int capacity, T sentinel, bool autoScale = false)
        {
#if DEBUG
            _description = description;
#else
            _description = string.Empty;
#endif
            if(autoScale && (capacity & -capacity) != capacity || capacity == 0)
                throw new ArgumentOutOfRangeException($"{nameof(capacity)} = {capacity} must be a power of 2 when {nameof(autoScale)} is set true");

            _sentinel = sentinel;
            _autoScale = autoScale;
            if (autoScale)
            {
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

            _curEnumerator = new IoQEnumerator<T>(this);
        }

        private volatile int _zeroed;
        private readonly bool _zc = IoNanoprobe.ContinueOnCapturedContext;
        private readonly string _description;

        private T[][] _storage;
        private readonly T[] _fastStorage;

        private readonly object _syncRoot = new();
        // ReSharper disable once StaticMemberInGenericType
        private volatile T _sentinel;
        private volatile int _capacity;
        private volatile int _virility;
        private long _hwm;
        
        public long Tail => Interlocked.Read(ref _tail);
        private long _tail;
        public long Head => Interlocked.Read(ref _head);
        private long _head;

        private volatile IoQEnumerator<T> _curEnumerator;

        private volatile int _count;
        private volatile bool _autoScale;

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
                if (idx < _capacity) return Volatile.Read(ref _fastStorage[idx]);

                idx %= Capacity;
                var i = IoMath.Log2(unchecked((ulong) idx + 1));

                return Volatile.Read(ref _storage[i][idx - ((1 << i) - 1)]);
            }
            protected set
            {
                Debug.Assert(idx >= 0);
                if (!IsAutoScaling)
                {
                    _fastStorage[idx % _capacity] = value;
                    return;
                }

                if (idx < _capacity)
                {
                    _fastStorage[idx] = value;
                    return;
                }
                idx %= Capacity;
                var i = IoMath.Log2(unchecked((ulong)(idx) + 1));
                Volatile.Write(ref _storage[i][idx - ((1 << i) - 1)], value);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Get(long idx, T next = null)
        {
            if (!IsAutoScaling) return Interlocked.Exchange(ref _storage[0][idx % _capacity], next);
            if (idx < _capacity) return Interlocked.Exchange(ref _storage[0][idx], next);
            idx %= Capacity;
            var i = IoMath.Log2((ulong)idx + 1);

            return Interlocked.Exchange(ref _storage[i][idx - ((1 << i) - 1)], next);
        }
        //public T Set(long idx)
        //{

        //    Debug.Assert(idx >= 0);
        //    if (!IsAutoScaling)
        //    {
        //        _storage[0][idx % _capacity] = value;
        //        return;
        //    }

        //    if (idx < _capacity)
        //    {
        //        _storage[0][idx] = value;
        //        return;
        //    }
        //    idx %= Capacity;
        //    var i = IoMath.Log2((ulong)(idx) + 1);
        //    Volatile.Write(ref _storage[i][idx - ((1 << i) - 1)], value);
        //    //_storage[i][idx - ((1 << i) - 1)] = value;
        //}

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
                if (_count >= Capacity || force)
                {
                    var hwm = 1 << (_virility + 1);
                    _storage[_virility + 1] = new T[hwm];
                    Interlocked.Add(ref _hwm, hwm);
                    Interlocked.Increment(ref _virility);
                    return true;
                }
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private T Exchange(long idx, T value)
        {
            var i = 0;
            try
            {
                if (!IsAutoScaling) return Interlocked.Exchange(ref _fastStorage[idx % _capacity], value);

                if (idx < _capacity)
                    return Interlocked.Exchange(ref _fastStorage[idx], value);

                idx %= Capacity;
                i = IoMath.Log2(unchecked((ulong)idx + 1));
                return Interlocked.Exchange(ref _storage[i][idx - ((1 << i) - 1)], value);
            }
            finally
            {
                if (!IsAutoScaling)
                    Debug.Assert(Volatile.Read(ref _fastStorage[idx % _capacity]) == value);
                else
                    Debug.Assert(Volatile.Read(ref _storage[i][idx - ((1 << i) - 1)]) == value);
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
            if (idx < _capacity)
                return Interlocked.CompareExchange(ref _fastStorage[idx], value, compare);

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
        [MethodImpl(MethodImplOptions.NoInlining)]
        public long TryEnqueue(T item, bool deDup = false)
        {
            if (Zeroed)
                return -1;

            Debug.Assert(Zeroed || item != null);
            if (_count >= Capacity)
            {
                if (!_autoScale)
                {
                    //throw new OutOfMemoryException($"{_description}: Ran out of storage space, count = {_count}/{Capacity}");
                    return -1;
                }

                Scale();
            }

            if (deDup)
            {
                if (Contains(item))
                    return -1;
            }

            try
            {
                var c = 0;
                var slot = _sentinel;
                long tail;
                long insaneScale;
                while ((tail = Tail) == Head + (insaneScale = Capacity) || _count < insaneScale &&
                       (slot = CompareExchange(tail, item, null)) != null)
                {
#if DEBUG
                    if (++c == 5000000)
                    {
                        Console.WriteLine($"[{c}] eq 3 latch[{tail % Capacity}] ~ [{Tail % insaneScale}] ({slot != _sentinel}), overflow = {tail >= Head + insaneScale}, has space = {_count < insaneScale}, scale failure = {insaneScale != Capacity}, {Description}");
                    }
                    else if (c > 5000000)
                    {
                        Console.WriteLine(slot);
                        try
                        {
                            var t = Unsafe.As<Tuple<byte, byte>>(slot);
                            Console.WriteLine("{0},{1}", t.Item1, t.Item2);
                        }
                        catch 
                        {
                            
                        }
                        Debug.Assert(false);
                    }
#endif

                    slot = _sentinel;

                    if (Zeroed)
                        break;

                    Interlocked.MemoryBarrierProcessWide();
                }

                //retry on scaling
                if (insaneScale != Capacity)
                {
                    if (slot == null)
                    {
                        if (CompareExchange(tail, null, item) != item)
                            LogManager.GetCurrentClassLogger()
                                .Error($"{nameof(TryEnqueue)}: Could not restore latch state!");
                    }

                    return TryEnqueue(item, deDup);
                }

                if (slot == null)
                {
#if DEBUG
                    Debug.Assert(Zeroed || this[tail] == item);
                    Debug.Assert(Zeroed || tail == Tail);
                    Interlocked.MemoryBarrier();
#endif
                    Interlocked.Increment(ref _tail);
                    Interlocked.MemoryBarrier();
                    Interlocked.Increment(ref _count);
                    _curEnumerator.IncIteratorCount(); //TODO: is this a good idea?
                    return tail;
                }

                if (Zeroed)
                    return -1;

                if (IsAutoScaling)
                {
                    Scale();
                    return TryEnqueue(item, deDup);
                }

                if (!Zeroed)
                {
                    throw new OutOfMemoryException(
                        $"{_description}: Ran out of storage space, count = {_count}/{_capacity}:\n {Environment.StackTrace}");
                }
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
        [MethodImpl(MethodImplOptions.NoInlining)]
        public bool TryDequeue([MaybeNullWhen(false)] out T returnValue)
        {
            try
            {
                if (_count == 0 || Zeroed)
                {
                    returnValue = null;
                    return false;
                }

                var insaneScale = Capacity;
                long head;
                T result;
                if ((head = Head) == Tail || (result = Exchange(head, null)) == null)
                {
                    returnValue = null;
                    Interlocked.MemoryBarrierProcessWide();
                    return false;
                }

                if (insaneScale != Capacity)
                {
                    if (CompareExchange(head, result, null) != null)
                        LogManager.GetCurrentClassLogger()
                            .Error($"{nameof(TryDequeue)}: Could not restore latch state!");

                    return TryDequeue(out returnValue);
                }

#if DEBUG
                Debug.Assert(Zeroed || result != null);
                Debug.Assert(Zeroed || _count > 0);
                Debug.Assert(Zeroed || this[head] == null);
                Debug.Assert(Zeroed || head == Head);
                Interlocked.MemoryBarrier();
#endif
                Interlocked.Increment(ref _head);
                Interlocked.MemoryBarrier();
                Interlocked.Decrement(ref _count);

                returnValue = result;
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
                            await op(item, nanite).FastPath().ConfigureAwait(_zc);

                        if (item is IIoNanite ioNanite)
                        {
                            if (!ioNanite.Zeroed())
                                await ioNanite.Zero((IIoNanite)nanite, string.Empty).FastPath().ConfigureAwait(_zc);
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