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
using zero.core.patterns.misc;
using zero.core.patterns.queue.enumerator;

namespace zero.core.patterns.queue
{
    /// <summary>
    /// A lighter concurrent bag implementation
    /// </summary>
    public class IoBag<T>:IEnumerable<T>
    where T:class
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public IoBag(string description, int capacity, bool autoScale = false)
        {
#if DEBUG
            _description = description;
#else
            _description = string.Empty;
#endif
            //_hwm = _capacity = Math.Max(capacity,4);
            _hwm = _capacity = capacity;
            _storage = new T[32][];
            _storage[0] = _fastStorage = new T[_capacity];
            _autoScale = autoScale;
            _curEnumerator = new IoBagEnumerator<T>(this);
        }

        private volatile int _zeroed;
        private readonly bool Zc = IoNanoprobe.ContinueOnCapturedContext;
        private readonly string _description;

        private volatile T[] _fastStorage;
        private volatile T[][] _storage;

        private volatile object _syncRoot = new();
        private volatile int _capacity;
        private volatile int _virility;
        private long _hwm;
        private volatile int _count;        
        public long Tail => _tail;
        private long _tail; 
        public long Head => _head;
        private long _head;

        private volatile IoBagEnumerator<T> _curEnumerator;

        private volatile bool _autoScale;

        /// <summary>
        /// ZeroAsync status
        /// </summary>
        public bool Zeroed => _zeroed > 0;

        /// <summary>
        /// Description
        /// </summary>
        public string Description => $"{nameof(IoBag<T>)}: {nameof(Count)} = {_count}/{_capacity}, s = {IsAutoScaling}, h = {_head}/{_tail} (d:{_tail - _head}), desc = {_description}";

        /// <summary>
        /// Current number of items in the bag
        /// </summary>
        public int Count => _count;

        /// <summary>
        /// Capacity
        /// </summary>
        //public int Capacity => IsAutoScaling? _capacity * ((1 << (_virility + 1)) - 1): _capacity;
        public int Capacity => _capacity;

        /// <summary>
        /// Whether we are auto scaling
        /// </summary>
        public bool IsAutoScaling => _autoScale;

        /// <summary>
        /// Bag item by index
        /// </summary>
        /// <param name="idx">index</param>
        /// <returns>Object stored at index</returns>
        public T this[long idx]
        {
            get
            {
                if (!IsAutoScaling || idx < _capacity) return _fastStorage[idx];

                var i = Log2((ulong)(idx / _capacity + 1));
                return _storage[i][idx - ((1 << i) - 1) * _capacity];
            }
            set
            {
                if (!IsAutoScaling || idx < _capacity)
                {
                    _fastStorage[idx] = value;
                    return;
                }

                var i = Log2((ulong)(idx / _capacity + 1));
                _storage[i][idx - ((1 << i) - 1) * _capacity] = value;
            }
        }
        
        /// <summary>
        /// Horizontal scale
        /// </summary>
        /// <returns>True if scaling happened, false on race or otherwise.</returns>
        private bool Scale()
        {
            if(!IsAutoScaling)
                return false;
            
            lock (_syncRoot)
            {
                if (_count >= Capacity)
                {
                    var hwm = (1 << _virility + 1) * _capacity;
                    _storage[_virility + 1] = new T[hwm];
                    Interlocked.Increment(ref _virility);
                    Interlocked.Add(ref _hwm, hwm);
                    return true;
                }
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Log2(ulong value)
        {
            int i;
            for (i = -1; value != 0; i++)
                value >>= 1;

            return (i == -1) ? 0 : i;
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
                return Interlocked.CompareExchange(ref _fastStorage[idx], value, compare);

            var f = idx / _capacity;
            var i = Log2((ulong)f + 1);
            return Interlocked.CompareExchange(ref _storage[i][idx - ((1 << i) - 1) * _capacity], value, compare);
        }

        private volatile int _addsAttempted = 0;
        private volatile int _adds = 0;
        /// <summary>
        /// Add item to the bag
        /// </summary>
        /// <param name="item">The item to be added</param>
        /// <param name="deDup">Whether to de-dup this item from the bag</param>
        /// <exception cref="OutOfMemoryException">Thrown if we are internally OOM</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Add(T item, bool deDup = false)
        {
            if (_count >= Capacity)
            {
                if(!_autoScale)
                    throw new OutOfMemoryException($"{_description}: Ran out of storage space, count = {_count}/{_capacity}");

                Scale();
            }
            
            if(deDup)
            {
                if (Contains(item))
                    return 0;
            }

            try
            {
                Interlocked.Increment(ref _addsAttempted);

                var insaneScale = 0;
                T slot = null;
                var tailIdx = (Interlocked.Increment(ref _tail) - 1);
                var tailMod = tailIdx % (insaneScale = Capacity);
                
                //lock (_syncRoot)
                {
                    var c = 0;
                    while (_count < insaneScale && (tailIdx > _head + insaneScale || insaneScale == Capacity &&
                               (slot = Interlocked.CompareExchange(ref _fastStorage[tailMod], item, null)) != null))
                    //(slot = CompareExchange(tailMod, item, null)) != null))
                    {
                        //foolproof hack
                        if (slot == null)
                            Interlocked.Decrement(ref _tail);

                        if (++c == 10000000)
                        {
                            Console.WriteLine(
                                $"[{c}] 3 latch[{tailMod}]~[{_tail % insaneScale}] bad = {slot != null}, overflow = {tailIdx >= _head + insaneScale}, has space = {_count < insaneScale}, {Description}");
                        }
                        else if (c > 10000000)
                        {
                            Thread.Yield();
                        }

                        tailIdx = Interlocked.Increment(ref _tail) - 1;
                        tailMod = tailIdx % (insaneScale = Capacity);

                        if (Zeroed)
                            break;

                        slot = null;
                    }
                }

                //retry on scaling
                if (insaneScale != Capacity)
                {
                    Console.WriteLine("!!!!!!!!!!!!!!!!!!!! Bag Rescale reload !!!!!!!!!!!!!!!!!!!!!!!!!");
                    Interlocked.Decrement(ref _tail);
                    return Add(item, deDup);
                }

                if (slot == null)
                {
                    Debug.Assert(tailIdx < _tail);
                    _curEnumerator.IncIteratorCount();
                    Interlocked.Increment(ref _count);
                    Interlocked.Increment(ref _adds);
                    return (int)tailIdx;
                }

                Interlocked.Decrement(ref _tail);

                if (Zeroed)
                    return -1;

                if (IsAutoScaling)
                {
                    Scale();
                    return Add(item, deDup);
                }

                if (!Zeroed)
                {
                    throw new OutOfMemoryException(
                        $"{_description}: Ran out of storage space, count = {_count}/{_capacity}:\n {Environment.StackTrace}");
                }
            }
            catch (IndexOutOfRangeException)
            {
                Add(item, deDup);
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
                Interlocked.Decrement(ref _addsAttempted);
            }

            return -1;
        }

        private volatile int _takesAttempted = 0;
        private volatile int _takes = 0;
        /// <summary>
        /// Try take from the bag, round robin
        /// </summary>
        /// <param name="result">The item to be fetched</param>
        /// <returns>True if an item was found and returned, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryTake([MaybeNullWhen(false)] out T result)
        {
            var success = false;
            result = null;

            try
            {
                if (_count <= 0)
                    return false;

                Interlocked.Increment(ref _takesAttempted);

                var insaneScale = 0;
                var headIdx = Interlocked.Increment(ref _head) - 1;
                var latchMod = headIdx % (insaneScale = Capacity);

                T latch = default;
                if(!IsAutoScaling || latchMod < _capacity)
                    latch = this[latchMod];//TODO: bug
                else
                    latch = _fastStorage[latchMod];//TODO: bug
                var c = 0;
                //lock (_syncRoot)
                {
                    while (_count > 0 && 
                           headIdx < _tail && 
                           insaneScale == Capacity && 
                           (headIdx > _tail || latch == null ||
                            //(result = CompareExchange(latchMod, null, latch)) != latch))
                            (result = Interlocked.CompareExchange(ref _fastStorage[latchMod], null, latch)) != latch))
                    {
                        //foolproof hack
                        if (result != null)
                        {
                            Interlocked.Decrement(ref _head);
                        }
                        else if (_count > 0) //This still races
                        {
                            Interlocked.Decrement(ref _count);
                            Thread.Yield();
                            if (_count < 0)
                                Interlocked.Increment(ref _count);
                        }

                        if (++c == 10000000)
                        {
                            Console.WriteLine($"[{c}] 4  latch[{latchMod}] bad = {latch != result}({latch == null}), overflow = {headIdx > _tail}, scale = {insaneScale != Capacity}, {Description}");
                        }
                        else if (c > 10000000)
                        {
                            Thread.Yield();
                        }

                        headIdx = Interlocked.Increment(ref _head) - 1;
                        latchMod = headIdx % (insaneScale = Capacity);

                        if (!IsAutoScaling || latchMod < _capacity)
                            latch = _fastStorage[latchMod];
                        else
                            latch = this[latchMod];

                        if (Zeroed)
                            break;
                        result = null;
                    }
                }

                if (insaneScale != Capacity)
                {
                    Interlocked.Decrement(ref _head);
                    Console.WriteLine("INSANE SCALE TAKE RETAKE!!!!!!!");
                    return TryTake(out result);
                }
                
                if (result != latch || result == null)
                {
                    if(latch != null)
                        Interlocked.Decrement(ref _head);

                    result = null;
                    return false;
                }

                if (!IsAutoScaling || latchMod < _capacity)
                    _fastStorage[latchMod] = null;
                else
                    this[latchMod] = null;

                Interlocked.Decrement(ref _count);

                Interlocked.Increment(ref _takes);
                if(c > 10000000)
                    Console.WriteLine($"[{c}] 4  (R) latch[{latchMod}] bad = {latch != result}, overflow = {headIdx > _tail}, scale = {insaneScale != Capacity}, {Description}");
                return success = true;
            }
            finally
            {
                Interlocked.Decrement(ref _takesAttempted);
                if (success)//TODO: what is going on here?
                    Debug.Assert(result != null); //TODO: Why does this assert fail?
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
            var idx = _head % Capacity;
            if (!IsAutoScaling || idx < _capacity)
                return (result = _fastStorage[idx]) != null;

            return (result = this[idx]) != null;
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
        public async ValueTask<bool> ZeroManagedAsync<TC>(Func<T,TC, ValueTask> op = null, TC nanite = default, bool zero = false)
        {
            if (zero && Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return true;

            try
            {
                for(var i = 0; i < Capacity; i++)
                {
                    var item = this[i];
                    try
                    {
                        if(item == default)
                            continue;
                        
                        if (op != null)
                            await op(item, nanite).FastPath().ConfigureAwait(Zc);

                        if (item is IIoNanite ioNanite)
                        {
                            if (!ioNanite.Zeroed())
                                await ioNanite.Zero((IIoNanite)nanite, string.Empty).FastPath().ConfigureAwait(Zc);
                        }                        
                    }
                    catch (InvalidCastException){}
                    catch (Exception) when(Zeroed){}
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
            //_curEnumerator = (IoBagEnumerator<T>)_curEnumerator.Reuse(this, b => new IoBagEnumerator<T>((IoBag<T>)b));
            //_curEnumerator.Reset();

            //return _curEnumerator;
            return new IoBagEnumerator<T>(this);
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