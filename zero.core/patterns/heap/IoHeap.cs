﻿using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.misc;
using zero.core.patterns.queue;

namespace zero.core.patterns.heap
{
    /// <summary>
    /// Basic heap construct
    /// </summary>
    /// <typeparam name="TItem">The type of item managed</typeparam>
    /// <typeparam name="TContext">The context of this heap</typeparam>
    public class IoHeap<TItem,TContext>
        where TItem : class
        where TContext : class
    {
        /// <summary>
        /// ctor
        /// </summary>
        static IoHeap()
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// Construct a heap with capacity
        /// </summary>
        /// <param name="description">A description</param>
        /// <param name="context">dev context</param>
        /// <param name="capacity">Heap  capacity</param>
        /// <param name="autoScale">Whether this heap capacity grows (exponentially) with demand</param>
        public IoHeap(string description, int capacity, bool autoScale = false, TContext context = null)
        {
            _description = description;
            _ioHeapBuf = new IoBag<TItem>($"{nameof(_ioHeapBuf)}: {description}", capacity, autoScale);
            Context = context;
            Malloc = default;
        }

        /// <summary>
        /// Logger
        /// </summary>
        private static Logger _logger;

        /// <summary>
        /// Description
        /// </summary>
        private readonly string _description;

        /// <summary>
        /// 
        /// </summary>
        public string Description => $"{nameof(IoHeap<TItem,TContext>)}: {nameof(Count)} = {Count}, capacity = {Capacity}, refs = {_refCount}, desc = {_description}, bag ~> {_ioHeapBuf.Description}";

        /// <summary>
        /// Config await
        /// </summary>
        protected bool Zc = IoNanoprobe.ContinueOnCapturedContext;

        /// <summary>
        /// Whether this object has been cleaned up
        /// </summary>
        private volatile int _zeroed;

        /// <summary>
        /// If we are in zero state
        /// </summary>
        public bool Zeroed => _zeroed > 0;

        /// <summary>
        /// The heap buffer space
        /// </summary>
        private IoBag<TItem> _ioHeapBuf;

        /// <summary>
        /// The current WorkHeap size
        /// </summary>
        public int Count => _ioHeapBuf.Count;
        

        /// <summary>
        /// The maximum heap size
        /// </summary>
        public int Capacity => _ioHeapBuf.Capacity;

        /// <summary>
        /// The number of outstanding references
        /// </summary>
        private volatile int _refCount;

        /// <summary>
        /// The number of outstanding references
        /// </summary>
        public int ReferenceCount => _refCount; //TODO refactor

        /// <summary>
        /// Whether we are auto scaling
        /// </summary>
        public bool IsAutoScaling => _ioHeapBuf.IsAutoScaling;

        /// <summary>
        /// zero unmanaged
        /// </summary>  
        public void ZeroUnmanaged()
        {
#if SAFE_RELEASE
            _logger = null;
            _ioHeapBuf = default;
            Context = null;
            Malloc = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public async ValueTask ZeroManagedAsync<TC>(Func<TItem,TC,ValueTask> zeroAction = null, TC nanite = default)
        {
            if (Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0 )
                return;
            
            if (zeroAction != null)
                await _ioHeapBuf.ZeroManagedAsync(zeroAction, nanite, true).FastPath().ConfigureAwait(Zc);
            else
                await _ioHeapBuf.ZeroManagedAsync<object>(zero:true).FastPath().ConfigureAwait(Zc);

            _refCount = 0;
        }

        /// <summary>
        /// Checks the heap for a free item and returns it. If no free items exists make a new one if we have capacity for it.
        /// </summary>
        /// <exception cref="InternalBufferOverflowException">Thrown when the max heap size is breached</exception>
        /// <returns>True if the item required malloc, false if popped from the heap otherwise<see cref="TItem"/></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TItem Take(object userData = null)
        {
            try
            {
                //If the heap is empty
                if (!_ioHeapBuf.TryTake(out var heapItem))
                {
                    if (_refCount == _ioHeapBuf.Capacity && !IsAutoScaling)
                    {
                        Console.WriteLine($"{_refCount} - {_ioHeapBuf.Capacity}");
                        throw new OutOfMemoryException($"{nameof(_ioHeapBuf)}: {_ioHeapBuf.Description}");
                    }
                    
                    Interlocked.Increment(ref _refCount);
                    heapItem = Malloc(userData, Context);
                    Constructor?.Invoke(heapItem, userData);
                    PopAction?.Invoke(heapItem, userData);

                    return heapItem;
                }
                else //take the item from the heap
                {
                    Interlocked.Increment(ref _refCount);
                    PopAction?.Invoke(heapItem, userData);
                    return heapItem;
                }
            }
            catch when (_zeroed > 0) {}
            catch (Exception) when (_ioHeapBuf.Zeroed) { }
            catch (Exception e) when(!Zeroed)
            {
                _logger.Error(e, $"{GetType().Name}: Failed to new up {typeof(TItem)}");
            }
            
            return default;
        }

        private static readonly ValueTask<TItem> FromNullTask = new ValueTask<TItem>((TItem) default);

        /// <summary>
        /// Returns an item to the heap
        /// </summary>
        /// <param name="item">The item to be returned to the heap</param>
        /// <param name="zero">Whether to destroy this object</param>
        /// <param name="deDup"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void Return(TItem item, bool zero = false, bool deDup = false)
        {
#if DEBUG
             if (item == null)
                 throw new ArgumentException($"{nameof(item)} cannot be null");
#else
            if (item == null)
                 return;
#endif
            try
            {
                if (!zero)
                    _ioHeapBuf.Add(item, deDup);
                Interlocked.Decrement(ref _refCount);
            }
            catch (Exception) when(_zeroed > 0){ }
            catch (Exception) when(_ioHeapBuf.Zeroed){ }
            catch (Exception e) when (!Zeroed)
            {
                _logger.Error(e, $"{GetType().Name}: Failed to new up {typeof(TItem)}");
            }
        }

        /// <summary>
        /// Makes a new item
        /// </summary>
        public Func<object,TContext,TItem> Malloc;

        /// <summary>
        /// Dev context that aids with heap setup when lambda's are involved
        /// </summary>
        public TContext Context { get; set; }

        /// <summary>
        /// Prepares an item from the stack
        /// </summary>
        public Action<TItem, object> Constructor;

        /// <summary>
        /// Prepares an item from the stack
        /// </summary>
        public Action<TItem, object> PopAction;

        /// <summary>
        /// Returns the amount of space left in the buffer
        /// </summary>
        /// <returns>The number of free slots</returns>
        public long AvailableCapacity => Capacity - _refCount;

        /// <summary>
        /// Cache size.
        /// </summary>
        /// <returns>The number of unused heap items</returns>
        public long CacheSize => _ioHeapBuf.Count;

        /// <summary>
        /// Clear
        /// </summary>
        public async ValueTask ClearAsync(bool zero = false)
        {
            await _ioHeapBuf.ZeroManagedAsync<object>(zero: zero).FastPath().ConfigureAwait(Zc);
        }

        public override string ToString()
        {
            return $"{GetType()}"; //TODO
        }
    }

    /// <summary>
    /// Legacy implementation
    /// </summary>
    /// <typeparam name="T2"></typeparam>
    public class IoHeap<T2> : IoHeap<T2, object> 
        where T2 : class
    {
        public IoHeap(string description, int capacity, bool autoScale = false, object context = null) : base(description, capacity, autoScale, context)
        {
        }
    }

}
