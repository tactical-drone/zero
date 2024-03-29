﻿using System;
using System.IO;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore.core;
using zero.core.runtime.scheduler;

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
        /// <param name="capacity">Heap capacity</param>
        /// <param name="malloc"></param>
        /// <param name="autoScale">Whether this heap capacity grows (exponentially) with demand</param>
        /// <param name="context">dev context</param>
        public IoHeap(string description, int capacity, Func<object, TContext, TItem> malloc, bool autoScale = false, TContext context = null)
        {
            _description = description;
            Malloc = malloc;

            _heap = new IoBag<TItem>($"{nameof(_heap)}: {description}", capacity);

            Context = context;
            _capacity = capacity;
            _autoScale = autoScale;
        }

        /// <summary>
        /// Logger
        /// </summary>
        private static readonly Logger _logger;

        /// <summary>
        /// Description
        /// </summary>
        private readonly string _description;

        /// <summary>
        /// 
        /// </summary>
        public string Description => $"#{GetHashCode()}:{nameof(IoHeap<TItem,TContext>)}: ops = {_hit+_miss} [ratio = {CacheHitRatio * 100:0.0}%] {nameof(Count)} = {Count}, capacity = {Capacity}, refs = {_refCount}, desc = {_description}, bag ~> _heap.Description";

        /// <summary>
        /// The heap buffer space
        /// </summary>
        //private Channel<TItem> _heap;
        private IoBag<TItem> _heap;

        /// <summary>
        /// Whether this object has been cleaned up
        /// </summary>
        private int _zeroed;

        /// <summary>
        /// The current WorkHeap size
        /// </summary>
        //public int Count => _heap.Reader.Count;
        public int Count => _heap.Count;


        /// <summary>
        /// The maximum heap size
        /// </summary>
        public int Capacity => _capacity;

        /// <summary>
        /// The number of outstanding references
        /// </summary>
        private int _refCount;

        private long _hit;
        private long _miss;

        /// <summary>
        /// Effectiveness of this cache
        /// </summary>
        public double CacheHitRatio => (double)_hit / (_hit + _miss);

        /// <summary>
        /// If we are in zero state
        /// </summary>
        public bool Zeroed => _zeroed > 0 || _heap.Zeroed;

        /// <summary>
        /// The number of outstanding references
        /// </summary>
        public int ReferenceCount => _refCount; //TODO refactor

        /// <summary>
        /// Whether we are auto scaling
        /// </summary>
        public bool IsAutoScaling => _autoScale;

        /// <summary>
        /// zero unmanaged
        /// </summary>  
        public void ZeroUnmanaged()
        {
#if SAFE_RELEASE
            _heap = default;
            Context = null;
            Malloc = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public async ValueTask ZeroManagedAsync<TC>(Func<TItem,TC,ValueTask> zeroAction = null, TC context = default)
        {
            if (Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0 )
                return;

            await _heap.ZeroManagedAsync<object>(zero: true).FastPath();

            Volatile.Write(ref _refCount, 0);
        }

        /// <summary>
        /// Checks the heap for a free item and returns it. If no free items exists make a new one if we have capacity for it.
        /// </summary>
        /// <exception cref="InternalBufferOverflowException">Thrown when the max heap size is breached</exception>
        /// <returns>True if the item required malloc, false if popped from the heap otherwise<see cref="TItem"/></returns>
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public TItem Take(object userData = null, Action<TItem, object> customConstructor = null) => Make(userData, customConstructor).item;
        

        /// <summary>
        /// Checks the heap for a free item and returns it. If no free items exists make a new one if we have capacity for it.
        /// </summary>
        /// <exception cref="InternalBufferOverflowException">Thrown when the max heap size is breached</exception>
        /// <returns>True if the item required malloc, false if popped from the heap otherwise<see cref="TItem"/></returns>
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public (TItem item,bool malloc) Make(object userData = null, Action<TItem, object> customConstructor = null)
        {
            try
            {
                var sw = new SpinWait();
                race:
                if (_refCount.ZeroNext(Capacity) < 0)
                {
                    if (!IsAutoScaling)
                    {
                        Interlocked.MemoryBarrierProcessWide();
                        if (Volatile.Read(ref _refCount) < Capacity)
                        {
                            sw.SpinOnce();
                            goto race;
                        }
#if DEBUG
                        _logger.Error($"{nameof(_heap)}: LEAK DETECTED!!! Heap -> {Description}");
#endif
                        Interlocked.Exchange(ref _refCount, _refCount >> 1);
                    }
                    else
                    {
                        if (_capacity > int.MaxValue>>2)
                            throw new OutOfMemoryException($"{nameof(Make)}: Unable to grow further than {_capacity}");

                        var prev = _heap;
                        if(prev.Capacity == Capacity && Interlocked.CompareExchange(ref _heap, new IoBag<TItem>(Description,  _capacity << 1), prev) == prev)
                        {
                            Interlocked.Exchange(ref _capacity, _capacity << 1);
                        }
                        else if(sw.Count < byte.MaxValue)
                        {
                            sw.SpinOnce();
                            goto race;
                        }

                        IoZeroScheduler.Zero.QueueAsyncFunction(static async state =>
                        {
                            try
                            {
                                //TODO, transfer memory to the new heap
                                await ((IoBag<TItem>)state).ZeroManagedAsync<object>();
                            }
                            catch
                            {
                                // ignored
                            }
                        }, prev);
                    }
                }

                sw.Reset();
                retry:
                if (!_heap.TryDequeue(out var heapItem))
                {
                    if (_heap.Count > 0)
                    {
                        if (_heap.Head != _heap.Tail && sw.Count < byte.MaxValue)
                        {
                            sw.SpinOnce();
                            goto retry; //TODO: hack    
                        }
                    }

                    heapItem = Malloc(userData, Context);

                    customConstructor?.Invoke(heapItem, userData);
                    Constructor?.Invoke(heapItem, userData);
                    PopAction?.Invoke(heapItem, userData);

                    Interlocked.Increment(ref _miss);
                    return (heapItem, true);
                }
                else
                {
                    Interlocked.Increment(ref _hit);

                    PopAction?.Invoke(heapItem, userData);
                    return (heapItem, false);
                }
            }
            catch (Exception) when (Zeroed)
            {
            }
            catch (Exception e) when (!Zeroed)
            {
                _logger.Error(e, $"{GetType().Name}: Failed to malloc {typeof(TItem)}");
            }
            finally
            {
#if DEBUG
                var t = _hit + _miss;
                if (t > Capacity << 1)
                {
                    var r = (double)_miss / (t);
                    if (r is > 0.75 && _hit > 0)
                    {
                        _logger.Warn($"{nameof(Make)}: Bad cache miss ratio of {r * 100:0.0}%, hit = {_hit}, miss = {_miss}, {Description}");
                    }
                }
#endif
            }
            return default;
        }

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
                 throw new ArgumentNullException(nameof(item));
#endif
            try
            {
                retry:
                if (!zero)
                {
                    PushAction?.Invoke(item);
                    if (_heap.TryEnqueue(item, deDup) < 0 && !Zeroed)
                    {
                        _logger.Warn($"{nameof(Return)}: Unable to return {item} to the heap, {Description}");
                        zero = true;
                        goto retry;
                    }
                    Interlocked.Decrement(ref _refCount);
                }
                else
                    Destroy(item);
            }
            catch when (_zeroed > 0)
            {
            }
            catch (Exception e) when (!Zeroed)
            {
                _logger.Error(e, $"{GetType().Name}: Failed malloc {typeof(TItem)}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Destroy(TItem item)
        {
            Interlocked.Decrement(ref _refCount);
            if (item is not IIoHeapItem)
            {
                if (item is IDisposable disposable)
                    disposable.Dispose();
                if (item is IAsyncDisposable asyncDisposable)
                {
                    IoZeroScheduler.Zero.QueueAsyncFunction(static async state =>
                    {
                        var item = (IAsyncDisposable)state;
                        await item.DisposeAsync().FastPath();
                    }, asyncDisposable);
                }
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
        /// Prepares an item from the stack
        /// </summary>
        public Action<TItem> PushAction;

        private int _capacity;
        private readonly bool _autoScale;

        /// <summary>
        /// Returns the amount of space left in the buffer
        /// </summary>
        /// <returns>The number of free slots</returns>
        public long AvailableCapacity => Capacity - _refCount;

        /// <summary>
        /// Cache size.
        /// </summary>
        /// <returns>The number of unused heap items</returns>
        public long CacheSize => _capacity;

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
        public IoHeap(string description, int capacity, Func<object, object, T2> malloc, bool autoScale = false,
            object context = null) : base(description, capacity, malloc, autoScale, context)
        {

        }
    }

}
