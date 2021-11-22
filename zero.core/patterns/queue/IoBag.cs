﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
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
        public IoBag(string description, int capacity, bool hotReload = false)
        {
            _description = description;
            _capacity = capacity;
            _storage = new T[_capacity];
            _hotReload = hotReload;
            Reset();
        }

        private volatile int _zeroed;
        private readonly bool Zc = IoNanoprobe.ContinueOnCapturedContext;
        private readonly string _description;
        private T[] _storage;        
        private int _capacity;
        private volatile int _count;        
        private int Head => _head % _capacity;
        private volatile int _head = 0;
        private int Tail => _tail % _capacity;
        private volatile int _tail = 0;
        
        private volatile int _iteratorIdx = - 1;
        private volatile int _iteratorCount;
        private readonly bool _hotReload;

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
        /// Add item to the bag
        /// </summary>
        /// <param name="item">The item to be added</param>
        /// <exception cref="OutOfMemoryException">Thrown if we are internally OOM</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(T item, bool deDup = false)
        {
            if (_count == _capacity)
            {
                if(!_hotReload)
                    throw new OutOfMemoryException($"{_description}: Ran out of storage space, count = {_count}/{_capacity}");

                var tmp = _storage;
                var tmpCapacity = _capacity;
                _capacity *= 2;
                _storage = new T[_capacity];
                Array.Copy(tmp,0,_storage, 0, tmpCapacity);
            }
            
            if(deDup)
            {
                if (Contains(item))
                    return;
            }

            try
            {
                var latch = (Interlocked.Increment(ref _head) - 1) % _capacity;
                T latched = default;
                while (_count < _capacity && (latched = Interlocked.CompareExchange(ref _storage[latch], item, default)) != default)
                {                    
                    Interlocked.Decrement(ref _head);                    
                    latch = (Interlocked.Increment(ref _head) - 1) % _capacity;
                }

                if (latched == default)
                {
                    Interlocked.Increment(ref _iteratorCount);
                    Interlocked.Increment(ref _count);
                }                    
                else
                    throw new OutOfMemoryException($"{_description}: Ran out of storage space, count = {_count}/{_capacity}");
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryTake([MaybeNullWhen(false)] out T result)
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

            if (result != target || result == default)
            {
                Interlocked.Decrement(ref _tail);
                return false;
            }


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
            return (result = _storage[(_head - 1) % _capacity]) != default;
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
            try
            {
                if (zero && Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                    return true;
                
                for(int i = 0; i < _capacity; i++)
                {
                    var item = _storage[i];
                    try
                    {
                        //TODO is this a good idea?
                        if(item == default)
                            continue;
                        
                        if (!zero && op != null)
                            await op(item, nanite).FastPath().ConfigureAwait(Zc);
                        else if(zero)
                        {
                            if (!((IIoNanite)item)!.Zeroed())
                                ((IIoNanite)item).Zero((IIoNanite)nanite);
                        }                        
                    }
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
        /// Move to next item
        /// </summary>
        /// <returns>True if the iterator could be advanced by 1</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            if (Zeroed || _iteratorCount <= 0)
                return false;

            var idx = Interlocked.Increment(ref _iteratorIdx) % _capacity;

            while (Interlocked.Decrement(ref _iteratorCount) > 0 && _storage[idx] == default)
                idx = Interlocked.Increment(ref _iteratorIdx) % _capacity;
            
            return _iteratorCount >= 0 && _storage[idx] != default;
        }

        /// <summary>
        /// Contains
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized | MethodImplOptions.AggressiveInlining)]
        public bool Contains(T item)
        {            
            return _storage.Contains(item);
        }

        /// <summary>
        /// Reset iterator
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized | MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            Interlocked.Exchange(ref _iteratorIdx, (_tail - 1) % _capacity);
            Interlocked.Exchange(ref _iteratorCount, _count);
        }

        /// <summary>
        /// Return the current element in the iterator
        /// </summary>
        public T Current => _storage[_iteratorIdx % _capacity];

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
            Reset();
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