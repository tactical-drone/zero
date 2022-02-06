using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.misc;
using zero.core.patterns.queue.enumerator;
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

            Sentinel = sentinel;
            _autoScale = autoScale;
            if (autoScale)
            {
                _hwm = _capacity = 1;
                _storage = new T[32][];
                _storage[0] = new T[_capacity];

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
                _storage[0] = new T[_capacity];
            }

            _curEnumerator = new IoQEnumerator<T>(this);
        }

        private volatile int _zeroed;
        private readonly bool _zc = IoNanoprobe.ContinueOnCapturedContext;
        private readonly string _description;

        private volatile T[][] _storage;

        private readonly object _syncRoot = new();
        // ReSharper disable once StaticMemberInGenericType
        private volatile T Sentinel;
        private volatile int _capacity;
        private volatile int _virility;
        private long _hwm;
        private volatile int _count;
        public long Tail => Interlocked.Read(ref _tail);
        private long _tail;
        public long Head => Interlocked.Read(ref _head);
        private long _head;

        private volatile IoQEnumerator<T> _curEnumerator;

        private volatile bool _autoScale;

        /// <summary>
        /// ZeroAsync status
        /// </summary>
        public bool Zeroed => _zeroed > 0;

        /// <summary>
        /// Description
        /// </summary>
        public string Description => $"{nameof(IoZeroQ<T>)}: {nameof(Count)} = {_count}/{Capacity}, s = {IsAutoScaling}, h = {Head}/{Tail} (d:{Tail - Head}), desc = {_description}";

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
                if (!IsAutoScaling || idx < _capacity) return Volatile.Read(ref _storage[0][idx]);

                var i = IoMath.Log2((ulong)idx + 1);
                return Volatile.Read(ref _storage[i][idx - ((1 << i) - 1)]);
            }
            protected set
            {
                Debug.Assert(idx >= 0);
                if (!IsAutoScaling || idx < _capacity)
                {
                    _storage[0][idx] = value;
                    return;
                }

                var i = IoMath.Log2((ulong)idx + 1);
                _storage[i][idx - ((1 << i) - 1)] = value;
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
            if (!IsAutoScaling || idx < _capacity)
                return Interlocked.CompareExchange(ref _storage[0][idx], value, compare);

            var i = IoMath.Log2((ulong)idx + 1);
            return Interlocked.CompareExchange(ref _storage[i][idx - ((1 << i) - 1)], value, compare);
        }

#if DEBUG
        private volatile int _addsAttempted;
        private volatile int _adds;
#endif
        /// <summary>
        /// Add item to the bag
        /// </summary>
        /// <param name="item">The item to be added</param>
        /// <param name="deDup">Whether to de-dup this item from the bag</param>
        /// <exception cref="OutOfMemoryException">Thrown if we are internally OOM</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int TryEnqueue(T item, bool deDup = false)
        {
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
                    return 0;
            }

            try
            {
#if DEBUG
                Interlocked.Increment(ref _addsAttempted);
#endif
                long insaneScale;
                long tailIdx;

                var result = Sentinel;
                var c = 0;
                var tailMod = (tailIdx = Tail) % (insaneScale = Capacity);
                while (
                    _count < insaneScale && 
                    (tailIdx > Head + insaneScale || insaneScale == Capacity && (result = CompareExchange(tailMod, item, null)) != null))
                {
                    if (++c == 100000)
                    {
                        Console.WriteLine($"[{c}] 3 latch[{tailMod}]~[{Tail % insaneScale}] bad = {result != null}({result != Sentinel}), overflow = {tailIdx > Head + insaneScale}, has space = {_count < insaneScale}, scale failure = {insaneScale != Capacity}, {Description}");
                    }
                    else if (c > 100000)
                    {
                        //TODO: bugs? Attempt to recover...
                        if (_count == Tail - Head && result != Sentinel)
                        {
                            //Interlocked.Exchange(ref _head, tailIdx);
                            //Interlocked.Decrement(ref _tail);
                            
                            Interlocked.Increment(ref _tail);
                            Interlocked.Increment(ref _count);
                        }
                        else//TODO: unrecoverable bug?
                        {
                            Console.WriteLine($"[{c}] 3 [CRITICAL LOCK FAILURE!!!] latch[{tailMod}]~[{Tail % insaneScale}] bad = {result != null}({result != Sentinel}), overflow = {tailIdx >= Head + insaneScale}, has space = {_count < insaneScale}, scale failure = {insaneScale != Capacity}, {Description}");
                            break;
                        }

                        Thread.Yield();
                    }

                    if (Zeroed)
                        break;

                    result = Sentinel;
                    Thread.Yield();
                    tailMod = (tailIdx = Tail) % (insaneScale = Capacity);
                }
                
                //retry on scaling
                if (insaneScale != Capacity)
                {
                    if (result == null)
                    {
                        if (CompareExchange(tailMod, null, result) != result)
                            LogManager.GetCurrentClassLogger().Error($"{nameof(TryEnqueue)}: Could not restore latch state!");
                    }
                    return TryEnqueue(item, deDup);
                }

                //if success
                if (result == null)
                {
                    Interlocked.Increment(ref _count);//count first
                    Interlocked.Increment(ref _tail);
                    _curEnumerator.IncIteratorCount(); //TODO: is this a good idea?
#if DEBUG
                    Interlocked.Increment(ref _adds);
#endif
                    return (int)tailIdx;
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
            catch (IndexOutOfRangeException)
            {
                TryEnqueue(item, deDup);
            }
            catch when (Zeroed)
            {
            }
            catch (Exception e) when (!Zeroed)
            {
                LogManager.GetCurrentClassLogger().Error(e);
            }
            finally
            {
#if DEBUG
                Interlocked.Decrement(ref _addsAttempted);
#endif
            }

            return -1;
        }

#if DEBUG
        private volatile int _takesAttempted;
        private volatile int _takes;
#endif

        /// <summary>
        /// Try take from the Q, round robin
        /// </summary>
        /// <param name="result">The item to be fetched</param>
        /// <returns>True if an item was found and returned, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryDequeue([MaybeNullWhen(false)] out T result)
        {
            result = Sentinel;

            try
            {
                if (_count <= 0)
                {
                    result = null;
                    return false;
                }
                    
#if DEBUG
                Interlocked.Increment(ref _takesAttempted);
#endif
                long insaneScale;
                long headLatch;
                long latchMod;
                
                var c = 0;
                var latch = this[latchMod = (headLatch = Head) % (insaneScale = Capacity)];
                while (_count > 0 && 
                    insaneScale == Capacity &&
                    (headLatch > Tail || latch == null || latch == Sentinel || (result = CompareExchange(latchMod, null, latch)) != latch))
                {
                    if (++c == 100000)
                    {
                        Console.WriteLine($"[{c}] 4  latch[{latchMod}] bad = {latch != result}({latch == null}), overflow = {headLatch > Tail}, scale failure = {insaneScale != Capacity}, {Description}");
                    }
                    else if (c > 100000)
                    {
                        //TODO: bugs? Attempt to recover...
                        if (_count == Tail - Head)
                        {
                            Interlocked.Increment(ref _head);
                            if (_count > 0 && Interlocked.Decrement(ref _count) < 0)
                            {
                                Thread.Yield();
                                if (_count < 0)
                                    Interlocked.Increment(ref _count);
                            }
                        }
                        else//TODO: unrecoverable bug?
                        {
                            Console.WriteLine($"[{c}] 4 [CRITICAL LOCK FAILURE!!!] latch[{latchMod}] bad = {latch != result}({latch == null}), overflow = {headLatch > Tail}, scale failure = {insaneScale != Capacity}, {Description}");
                            break;
                        }
                            
                        Thread.Yield();
                    }

                    if (Zeroed)
                        break;
                    result = Sentinel;

                    Thread.Yield();
                    latch = this[latchMod = (headLatch = Head) % (insaneScale = Capacity)];
                }
                
                if (insaneScale != Capacity)
                {
                    if (result == latch && result != Sentinel && result != null)
                    {
                        if (CompareExchange(latchMod, result, null) != null)
                            LogManager.GetCurrentClassLogger().Error($"{nameof(TryDequeue)}: Could not restore latch state!");
                    }
                    return TryDequeue(out result);
                }

                if (result != latch || result == null || result == Sentinel)
                {
                    if (c > 100000)
                        Console.WriteLine($"[{c}] 4 [RECOVERED!!!] latch[{latchMod}] bad = {latch != result}, overflow = {headLatch > Tail}, scale = {insaneScale != Capacity}, {Description}");
                    result = null;
                    return false;
                }

                Interlocked.Decrement(ref _count); //count first
                Interlocked.Increment(ref _head);
#if DEBUG
                Interlocked.Increment(ref _takes);       
#endif

                if (c > 100000)
                    Console.WriteLine($"[{c}] 4  (R) latch[{latchMod}] bad = {latch != result}, overflow = {headLatch > Tail}, scale = {insaneScale != Capacity}, {Description}");

                Debug.Assert(result != null);
                return true;
            }
            finally
            {
#if DEBUG
                Interlocked.Decrement(ref _takesAttempted);
#endif
            }
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