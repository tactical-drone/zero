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
            _capacity = capacity;
            _storage = new T[_capacity];
            _autoScale = autoScale;
            _curEnumerator = new IoBagEnumerator<T>(this);
        }

        private volatile int _zeroed;
        private readonly bool Zc = IoNanoprobe.ContinueOnCapturedContext;
        private readonly string _description;

        private T[] _storage;
        private readonly object _syncRoot = new();
        private volatile int _capacity;
        private volatile int _count;        
        public long Tail => _tail;
        private long _tail;
        public long Head => _head;
        private long _head;

        private IoBagEnumerator<T> _curEnumerator;

        private volatile bool _autoScale;

        /// <summary>
        /// ZeroAsync status
        /// </summary>
        public bool Zeroed => _zeroed > 0;

        /// <summary>
        /// Description
        /// </summary>
        public string Description => $"{nameof(IoBag<T>)}: {nameof(Count)} = {_count}, desc = {_description}";

        /// <summary>
        /// Current number of items in the bag
        /// </summary>
        public int Count => _count;

        /// <summary>
        /// Capacity
        /// </summary>
        public int Capacity => _capacity;

        /// <summary>
        /// Whether we are auto scaling
        /// </summary>
        public bool IsAutoScaling => _autoScale;

        /// <summary>
        /// Bag item by index
        /// </summary>
        /// <param name="i">index</param>
        /// <returns>Object stored at index</returns>
        public T this[int i]
        {
            get => _storage[i];
            set => _storage[i] = value;
        }
        
        bool Scale()
        {
            if(!IsAutoScaling)
                return false;
            
            lock (_syncRoot)
            {
                if (_count >= _capacity)
                {
                    Interlocked.Exchange(ref _capacity, _capacity * 2);
                    var newStorage = new T[_capacity];
                    Array.Copy(_storage, 0, newStorage, 0, _capacity>>1);
                    _storage = newStorage;
                    Thread.MemoryBarrier(); //in case Exchanges does not do this
                    
                    return true;
                }
                return false;
            }
        }

        /// <summary>
        /// Add item to the bag
        /// </summary>
        /// <param name="item">The item to be added</param>
        /// <param name="deDup">Whether to de-dup this item from the bag</param>
        /// <exception cref="OutOfMemoryException">Thrown if we are internally OOM</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Add(T item, bool deDup = false)
        {
            if (_count >= _capacity)
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
                var insaneScale = 0;
                var tailIdx = (Interlocked.Increment(ref _tail) - 1);
                var tailMod = tailIdx % (insaneScale = _capacity);
                T latched = null;
                while ( tailIdx >= _head + insaneScale || _count < insaneScale && insaneScale == _capacity && 
                       (latched = Interlocked.CompareExchange(ref _storage[tailMod], item, null)) != null)
                {
                    Interlocked.Decrement(ref _tail);

                    tailIdx = (Interlocked.Increment(ref _tail) - 1) % (insaneScale = _capacity);
                    tailMod = tailIdx % (insaneScale = _capacity);

                    if (Zeroed)
                        break;
                    
                    latched = null;
                }

                //retry on scaling
                if (insaneScale != _capacity)
                {
                    Console.WriteLine("!!!!!!!!!!!!!!!!!!!! INSANE BAG RESCALE READ!!!!!!!!!!!!!!!!!!!!!!!!!");
                    Interlocked.Decrement(ref _tail);
                    return Add(item, deDup);
                }

                if (latched == null)
                {
                    _curEnumerator.IncIteratorCount();
                    Interlocked.Increment(ref _count);
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
            catch when (Zeroed){}
            catch (Exception e) when(!Zeroed)
            {
                LogManager.GetCurrentClassLogger().Error(e);
            }

            return -1;
        }
        
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
                if (_count == 0)
                    return false;

                var insaneScale = 0;
                var headIdx = Interlocked.Increment(ref _head) - 1;
                var latchMod = headIdx % (insaneScale = _capacity);
                var latch = _storage[latchMod];

                bool insaneOverflow;
                while ((insaneOverflow = headIdx >= _tail) || _count > 0 && insaneScale == _capacity && (result = Interlocked.CompareExchange(ref _storage[latchMod], null, latch)) != latch || latch == null)
                {
                    //skip over empty slots
                    if (latch != null || insaneOverflow)
                        Interlocked.Decrement(ref _head);
                    else
                        Interlocked.Decrement(ref _count);

                    headIdx = Interlocked.Increment(ref _head) - 1;
                    latchMod = headIdx % (insaneScale = _capacity);
                    latch = _storage[latchMod];
                    if (Zeroed)
                        break;
                    result = null;
                }

                if (insaneScale != _capacity)
                    return TryTake(out result);
                
                if (result != latch || result == null)
                {
                    Interlocked.Decrement(ref _head);
                    result = null;
                    return false;
                }
                _storage[latchMod] = null;

                Interlocked.Decrement(ref _count);

                return success = true;
            }
            finally
            {
                if(success)//TODO: what is going on here?
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
            return (result = _storage[(_tail - 1) % _capacity]) != null;
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
                for(var i = 0; i < _capacity; i++)
                {
                    var item = _storage[i];
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
                        _storage[i] = default;                        
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
            return _storage.Contains(item);
        }

        /// <summary>
        /// Returns the bag enumerator
        /// </summary>
        /// <returns>The bag enumerator</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<T> GetEnumerator()
        {
            _curEnumerator = (IoBagEnumerator<T>)_curEnumerator.Reuse(this, b => new IoBagEnumerator<T>((IoBag<T>)b));
            _curEnumerator.Reset();
            return _curEnumerator;
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