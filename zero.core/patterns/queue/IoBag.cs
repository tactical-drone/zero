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
    /// A concurrent bag
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
            _zeroSentinel = new IoNanoprobe($"{nameof(IoBag<T>)}: {description}");
        }
            
        private volatile int _zeroed;
        private readonly string _description;
        private T[] _storage;
        private readonly uint _capacity;
        private volatile uint _count;
        private volatile uint _next;
        private volatile uint _iteratorIdx = UInt32.MaxValue;
        private readonly IoNanoprobe _zeroSentinel;

        public bool Zeroed => _zeroed > 0;
        
        public uint Count => _count;
        private uint Next => _next % _capacity;
        private uint Prev => (_next - 1) % _capacity;
        
        /// <summary>
        /// Add item to the bag
        /// </summary>
        /// <param name="item">The item to be added</param>
        /// <exception cref="OutOfMemoryException">Thrown if we are internally OOM</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public void Add(T item)
        {
            var latch = Interlocked.Increment(ref _next) - 1;
            T fail = null;
            while (_count < _capacity && (fail = Interlocked.CompareExchange(ref _storage[latch], item, null)) != null)
            {
                Interlocked.Decrement(ref _next);
                latch = Interlocked.Increment(ref _next) - 1;
            }
            
            Interlocked.Increment(ref _count);
            
            if (fail != null)
                throw new OutOfMemoryException($"{_description}: Ran out of storage space, count = {_count}/{_capacity}");
        }
        

        /// <summary>
        /// Peeks the head of the queue
        /// </summary>
        /// <param name="result">Returns the head of the Q</param>
        /// <returns>True if the head was not null, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPeek([MaybeNullWhen(false)] out T result)
        {
            return (result = _storage[Next]) != null;
        }
        
        /// <summary>
        /// Try take from the bag, normally the bag is LIFO 
        /// </summary>
        /// <param name="result">The item to be fetched</param>
        /// <returns>True if an item was found and returned, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public bool TryTake([MaybeNullWhen(false)] out T result)
        {
            result = null;
            
            //fail fast
            if (_count == 0)
                return false;

            while (_count > 0)
            {
                var latch = Prev;
                var target = _storage[latch];
                if ((result = Interlocked.CompareExchange(ref _storage[latch], null, target)) == target)
                {
                    Interlocked.Decrement(ref _count);
                    Interlocked.Decrement(ref _next);    
                    break;    
                }
            }
            
            return (result != null);
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
                if (Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                    return true;
                
                foreach (var item in _storage)
                {
                    try
                    {
                        //TODO is this a good idea?
                        if(item == null)
                            continue;
                        
                        if (!zero && op != null)
                            await op(item, nanite).FastPath().ConfigureAwait(false);
                        else if(zero)
                        {
                            if (!((IIoNanite)item)!.Zeroed())
                                await ((IIoNanite)item).ZeroAsync((IIoNanite)nanite ?? _zeroSentinel)
                                    .FastPath()
                                    .ConfigureAwait(false);
                        }
                    }
                    catch (Exception) when(Zeroed){}
                    catch (Exception e) when (!Zeroed)
                    {
                        LogManager.GetCurrentClassLogger().Trace(e, $"{_description}: {op}, {item}, {nanite}");
                    }
                }
            }
            catch (Exception)
            {
                return false;
            }
            finally
            {
                _storage = null;
            }

            return true;
        }

        /// <summary>
        /// Move to next item
        /// </summary>
        /// <returns>True if the iterator could be advanced by 1</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            return _storage[Interlocked.Increment(ref _iteratorIdx)] != null;
        }

        /// <summary>
        /// Reset iterator
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            Interlocked.Exchange(ref _iteratorIdx, 0);
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

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}