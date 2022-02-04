using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using NLog;
using zero.core.patterns.misc;
using zero.core.patterns.queue.enumerator;

namespace zero.core.patterns.queue
{
    /// <summary>
    /// A lighter concurrent bag implementation
    /// </summary>
    public class IoHashCodes : IEnumerable<int>    
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public IoHashCodes(string description, int capacity, bool hotReload = false)
        {
            _description = description;
            _capacity = capacity;
            _storage = new int[_capacity];
            _curEnumerator = new IoHashCodeEnum(this);
        }
        
        private readonly string _description;
        private int[] _storage;
        private readonly int _capacity;
        private volatile int _count;
        public int Head => _head;
        private volatile int _head;
        public int Tail => _tail;
        private volatile int _tail;
        private IoHashCodeEnum _curEnumerator;
        private volatile int _zeroed;

        /// <summary>
        /// ZeroAsync status
        /// </summary>
        public bool Zeroed => _zeroed > 0;

        /// <summary>
        /// Description
        /// </summary>
        public string Description => $"{nameof(IoHashCodes)}: {nameof(Count)} = {_count}, desc = {_description}";

        /// <summary>
        /// Current number of items in the bag
        /// </summary>
        public int Count => _count;


        /// <summary>
        /// Capacity
        /// </summary>
        public int Capacity => _capacity;

        /// <summary>
        /// Bag item by index
        /// </summary>
        /// <param name="i">index</param>
        /// <returns>Object stored at index</returns>
        public int this[int i] => _storage[i];

        /// <summary>
        /// Add item to the bag
        /// </summary>
        /// <param name="item">The item to be added</param>
        /// <param name="unique">Ensure set property</param>
        /// <exception cref="OutOfMemoryException">Thrown if we are internally OOM</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(int item, bool unique = false)
        {
            if (_count == _capacity)
                throw new OutOfMemoryException($"{_description}: Ran out of storage space, count = {_count}/{_capacity}");

            //We don't allow zero hashes
            if (item == 0)
                item++;

            if (unique)
            {
                if (Contains(item))
                    return;
            }

            try
            {
                var latch = (Interlocked.Increment(ref _head) - 1) % _capacity;
                int latched = default;
                while (_count < _capacity && (latched = Interlocked.CompareExchange(ref _storage[latch], item, default)) != default)
                {
                    Interlocked.Decrement(ref _head);
                    latch = (Interlocked.Increment(ref _head) - 1) % _capacity;
                }

                if (latched == default)
                {
                    _curEnumerator.IncIteratorCount();
                    Interlocked.Increment(ref _count);
                }
                else
                    throw new OutOfMemoryException($"{_description}: Ran out of storage space, count = {_count}/{_capacity}");
            }
            catch (Exception e)
            {
                LogManager.GetCurrentClassLogger().Error(e);
            }
        }

        /// <summary>
        /// Try take from the bag, round robin
        /// </summary>
        /// <param name="result">The item to be fetched</param>
        /// <returns>True if an item was found and returned, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryTake([MaybeNullWhen(false)] out int result)
        {
            result = default;
            var latch = (Interlocked.Increment(ref _tail) - 1) % _capacity;
            var target = _storage[latch];

            while (_count > 0 && (result = Interlocked.CompareExchange(ref _storage[latch], default, target)) != target)
            {
                Interlocked.Decrement(ref _tail);
                latch = (Interlocked.Increment(ref _tail) - 1) % _capacity;
                target = _storage[latch];
            }

            if (result != target || result == default) return false;

            Interlocked.Decrement(ref _count);

            return true;
        }

        /// <summary>
        /// Peeks the head of the queue
        /// </summary>
        /// <param name="result">Returns the head of the Q</param>
        /// <returns>True if the head was not null, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPeek([MaybeNullWhen(false)] out int result)
        {
            return (result = _storage[(_head - 1) % (int)_capacity]) != default;
        }
        
        /// <summary>
        /// ZeroAsync managed cleanup
        /// </summary>
        /// <param name="op">Optional callback to execute on all items in the bag</param>
        /// <param name="zero">Whether the bag is assumed to contain <see cref="IIoNanite"/>s and should only be zeroed out</param>
        /// <returns>True if successful, false if something went wrong</returns>
        public void ZeroManaged(bool zero = false)
        {
            if (zero && Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return;

            _count = 0;
            
            if (!zero)                            
                Array.Clear(_storage, 0, _storage.Length);                            
            else
                _storage = null;                        
        }

        /// <summary>
        /// Contains
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Contains(int item)
        {
            return _storage.Contains(item);
        }

        /// <summary>
        /// Returns the bag enumerator
        /// </summary>
        /// <returns>The bag enumerator</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<int> GetEnumerator()
        {
            return _curEnumerator = (IoHashCodeEnum)_curEnumerator.Reuse(this, b => new IoHashCodeEnum((IoHashCodes)b));
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