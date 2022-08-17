using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
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

            _ioHeapBuf = new IoBag<TItem>($"{nameof(_ioHeapBuf)}: {description}", capacity);

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
        public string Description => $"#{GetHashCode()}:{nameof(IoHeap<TItem,TContext>)}: [ratio = {CacheHitRatio * 100:0.0}%] {nameof(Count)} = {Count}, capacity = {Capacity}, refs = {_refCount}, desc = {_description}, bag ~> _ioHeapBuf.Description";

        /// <summary>
        /// The heap buffer space
        /// </summary>
        //private Channel<TItem> _ioHeapBuf;
        private IoBag<TItem> _ioHeapBuf;

        /// <summary>
        /// Whether this object has been cleaned up
        /// </summary>
        private volatile int _zeroed;

        /// <summary>
        /// The current WorkHeap size
        /// </summary>
        //public int Count => _ioHeapBuf.Reader.Count;
        public int Count => _ioHeapBuf.Count;


        /// <summary>
        /// The maximum heap size
        /// </summary>
        public int Capacity => _capacity;

        /// <summary>
        /// The number of outstanding references
        /// </summary>
        private volatile int _refCount;

        private long _hit;
        private long _miss;

        /// <summary>
        /// Effectiveness of this cache
        /// </summary>
        public double CacheHitRatio => (double)_hit / (_hit + _miss);

        /// <summary>
        /// If we are in zero state
        /// </summary>
        public bool Zeroed => _zeroed > 0 || _ioHeapBuf.Zeroed;

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
            _ioHeapBuf = default;
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

            //_ioHeapBuf.Writer.Complete();

            //if (zeroAction != null)
            //{
            //    await foreach (var item in _ioHeapBuf.Reader.ReadAllAsync())
            //    {
            //        try
            //        {
            //            await zeroAction(item, context).FastPath();
            //        }
            //        catch (Exception e)
            //        {
            //            _logger.Error(e, Description);
            //        }
            //    }
            //}

            await _ioHeapBuf.ZeroManagedAsync<object>(zero: true).FastPath();

            _refCount = 0;
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
                retry:
                //If the heap is empty
                if (!_ioHeapBuf.TryDequeue(out var heapItem))
                {
                    if (_ioHeapBuf.Count > 0)
                        goto retry; //TODO: hack

                    //TODO: Leak
                    if (_refCount >= Capacity)
                    {
                        if (!IsAutoScaling)
                        {
#if DEBUG
                            _logger.Error($"{nameof(_ioHeapBuf)}: LEAK DETECTED!!! Heap -> {Description}: Q -> _ioHeapBuf.Description");
#endif
                            Thread.Yield();
                            if (_refCount >= Capacity && !IsAutoScaling) //TODO: what is going on here? The same check insta fails with huge state differences;
                                throw new OutOfMemoryException($"{nameof(_ioHeapBuf)}: Heap -> {Description}: Q -> _ioHeapBuf.Description");
                        }
                        else
                        {
                            _capacity *= 2;
                            var prev = _ioHeapBuf;
                            _ioHeapBuf = new IoBag<TItem>(Description, _capacity);
                            IoZeroScheduler.Zero.LoadAsyncContext(static async state =>
                            {
                                try
                                {
                                    await ((IoHeap<TItem>)state).ZeroManagedAsync<object>();
                                }
                                catch
                                {
                                    // ignored
                                }
                            }, prev);
                            
                        }
                    }

                    heapItem = Malloc(userData, Context);

                    customConstructor?.Invoke(heapItem, userData);
                    Constructor?.Invoke(heapItem, userData);
                    PopAction?.Invoke(heapItem, userData);

                    Interlocked.Increment(ref _refCount);
                    Interlocked.Increment(ref _miss);
                    return (heapItem, true);
                }
                else //take the item from the heap
                {
                    PopAction?.Invoke(heapItem, userData);
                    Interlocked.Increment(ref _refCount);
                    Interlocked.Increment(ref _hit);
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
                    var r = (double)_hit / (t);
                    if (r is < 0.25 and > 0)
                    {
                        _logger.Warn($"{nameof(Make)}: Bad cache hit ratio of {r * 100:0.0}%, hit = {_hit}, miss = {_miss}, {Description}");
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
            if (item == null)
                 return;

            Interlocked.Decrement(ref _refCount);

            PushAction?.Invoke(item);

            try
            {
                if (!zero)
                {
                    if (_ioHeapBuf.TryEnqueue(item) < 0 && !Zeroed)
                        _logger.Warn($"{nameof(Return)}: Unable to return {item} to the heap, {Description}");
                }
                else
                {
                    if (item is not IIoHeapItem)
                    {
                        if(item is IDisposable disposable)
                            disposable.Dispose();
                        if (item is IAsyncDisposable asyncDisposable)
                        {
                            IoZeroScheduler.Zero.LoadAsyncContext(static async state =>
                            {
                                var item = (IAsyncDisposable)state;
                                await item.DisposeAsync().FastPath();
                            }, asyncDisposable);
                        }
                    }
                    Interlocked.Increment(ref _hit);
                }
                    
            }
            catch when (_zeroed > 0)
            {
            }
            catch (Exception e) when (!Zeroed)
            {
                _logger.Error(e, $"{GetType().Name}: Failed malloc {typeof(TItem)}");
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
