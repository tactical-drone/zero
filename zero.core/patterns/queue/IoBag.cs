using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.misc;

namespace zero.core.patterns.queue
{
    /// <summary>
    /// A lighter concurrent bag implementation
    /// </summary>
    public class IoBag<T>:IEnumerator<T>, IEnumerable<T>
    where T:class
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public IoBag(string description, uint capacity)
        {
            _description = description;
            _capacity = capacity;
            _storage = new T[_capacity + 1];
#if DEBUG
            _zeroSentinel = new IoNanoprobe($"{nameof(IoBag<T>)}: {description}");
#else
            _zeroSentinel = new IoNanoprobe(null);
#endif
        }

        private volatile int _zeroed;
        private readonly bool Zc = true;
        private readonly string _description;
        private T[] _storage;
        private readonly uint _capacity;
        private volatile uint _count;        
        private uint Head => _head % _capacity;
        private volatile uint _head = 0;
        private uint Tail => _tail % _capacity;
        private volatile uint _tail = 0;
        
        private volatile uint _iteratorIdx;
        private IoNanoprobe _zeroSentinel;

        /// <summary>
        /// Zero status
        /// </summary>
        public bool Zeroed => _zeroed > 0;

        /// <summary>
        /// Description
        /// </summary>
        public string Description => $"{nameof(IoBag<T>)}: {nameof(Count)} = {_count}, desc = {_description}";

        /// <summary>
        /// Current number of items in the bag
        /// </summary>
        public uint Count => _count;
        
        /// <summary>
        /// Add item to the bag
        /// </summary>
        /// <param name="item">The item to be added</param>
        /// <exception cref="OutOfMemoryException">Thrown if we are internally OOM</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public void Add(T item)
        {
            if (_count == _capacity)
                throw new OutOfMemoryException($"{_description}: Ran out of storage space, count = {_count}/{_capacity}");

            try
            {
                var latch = (Interlocked.Increment(ref _head) - 1) % _capacity;
                while (Interlocked.CompareExchange(ref _storage[latch], item, null) != null)
                {
                    Interlocked.Decrement(ref _head);
                    if (_count == _capacity)
                        throw new OutOfMemoryException($"{_description}: Ran out of storage space, count = {_count}/{_capacity}");

                    latch = (Interlocked.Increment(ref _head) - 1) % _capacity;
                }

                Interlocked.Increment(ref _count);
            }
            catch (Exception e) when(!Zeroed)
            {
                LogManager.GetCurrentClassLogger().Error(e);
            }
        }
        
        /// <summary>
        /// Try take from the bag, round robin
        /// </summary>
        /// <param name="result">The item to be fetched</param>
        /// <returns>True if an item was found and returned, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public bool TryTake([MaybeNullWhen(false)] out T result)
        {
            result = null;
            var latch = (Interlocked.Increment(ref _tail) - 1) % _capacity;
            var target = _storage[latch];
            while (_count > 0 && (result = Interlocked.CompareExchange(ref _storage[latch], null, target)) != target)
            {
                Interlocked.Decrement(ref _tail);
                latch = (Interlocked.Increment(ref _tail) - 1) % _capacity;
                target = _storage[latch];
            }

            if (result != target || result == null) return false;

            Interlocked.Decrement(ref _count);
            return true;
        }

        /// <summary>
        /// Peeks the head of the queue
        /// </summary>
        /// <param name="result">Returns the head of the Q</param>
        /// <returns>True if the head was not null, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPeek([MaybeNullWhen(false)] out T result)
        {
            return (result = _storage[Head - 1]) != null;
        }

        /// <summary>
        /// Zero managed cleanup
        /// </summary>
        /// <param name="op">Optional callback to execute on all items in the bag</param>
        /// <param name="nanite">Callback context</param>
        /// <param name="zero">Whether the bag is assumed to contain <see cref="IIoNanite"/>s and should only be zeroed out</param>
        /// <typeparam name="TC">The callback context type</typeparam>
        /// <returns>True if successful, false if something went wrong</returns>
        /// <exception cref="ArgumentException">When zero is true but <see cref="nanite"/> is not of type <see cref="IIoNanite"/></exception>
        public async ValueTask<bool> ZeroManagedAsync<TC>(Func<T,TC, ValueTask> op = null, TC nanite = default, bool zero = false)
        {
            try
            {

                if (zero && Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                    return true;
                
                //foreach (var item in _storage)
                for(int i = 0; i < _capacity; i++)
                {
                    var item = _storage[i];
                    try
                    {
                        //TODO is this a good idea?
                        if(item == null)
                            continue;
                        
                        if (!zero && op != null)
                            await op(item, nanite).FastPath().ConfigureAwait(Zc);
                        else if(zero)
                        {
                            if (!((IIoNanite)item)!.Zeroed())
                                await ((IIoNanite)item).ZeroAsync((IIoNanite)nanite ?? _zeroSentinel)
                                    .FastPath()
                                    .ConfigureAwait(Zc);
                        }                        
                    }
                    catch (Exception) when(Zeroed){}
                    catch (Exception e) when (!Zeroed)
                    {
                        LogManager.GetCurrentClassLogger().Trace(e, $"{_description}: {op}, {item}, {nanite}");
                    }
                    finally
                    {
                        _storage[i] = null;                        
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
                    await _zeroSentinel.ZeroAsync(_zeroSentinel).FastPath().ConfigureAwait(Zc);
                    _zeroSentinel = null;
                    _storage = null;
                }                
            }

            return true;
        }

        /// <summary>
        /// Move to next item
        /// </summary>
        /// <returns>True if the iterator could be advanced by 1</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public bool MoveNext()
        {            
            if (_iteratorIdx == 0)
                return false;

            var tmpIdx = _iteratorIdx;

            while ( _storage[Interlocked.Decrement(ref _iteratorIdx)] == null && _iteratorIdx != 0) { }

            return _storage[_iteratorIdx] != null;
        }

        /// <summary>
        /// Reset iterator
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            Interlocked.Exchange(ref _iteratorIdx, _capacity);
        }

        /// <summary>
        /// Return the current element in the iterator
        /// </summary>
        public T Current => _storage[_iteratorIdx];

        /// <summary>
        /// Return the current element in the iterator
        /// </summary>
        object IEnumerator.Current => Current;

        /// <summary>
        /// Returns the bag enumerator
        /// </summary>
        /// <returns>The bag enumerator</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<T> GetEnumerator()
        {
            return this;
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

        /// <summary>
        /// Not used
        /// </summary>
        public void Dispose()
        {
            
        }
    }
}