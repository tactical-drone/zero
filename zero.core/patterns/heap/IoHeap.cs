using System;
using System.IO;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
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
        /// <param name="capacity">Heap  capacity</param>
        /// <param name="malloc"></param>
        /// <param name="autoScale">Whether this heap capacity grows (exponentially) with demand</param>
        /// <param name="context">dev context</param>
        public IoHeap(string description, int capacity, Func<object, TContext, TItem> malloc, bool autoScale = false, TContext context = null)
        {
            _description = description;
            Malloc = malloc;
            //_ioHeapBuf = new IoZeroQ<TItem>($"{nameof(_ioHeapBuf)}: {description}", capacity,autoScale);
            _ioHeapBuf = Channel.CreateBounded<TItem>(capacity);
            Context = context;
            _capacity = capacity;
            _autoScale = autoScale;
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
        public string Description => $"#{GetHashCode()}:{nameof(IoHeap<TItem,TContext>)}: {nameof(Count)} = {Count}, capacity = {Capacity}, refs = {_refCount}, desc = {_description}, bag ~> _ioHeapBuf.Description";


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
        private Channel<TItem> _ioHeapBuf;

        /// <summary>
        /// The current WorkHeap size
        /// </summary>
        public int Count => _ioHeapBuf.Reader.Count;
        

        /// <summary>
        /// The maximum heap size
        /// </summary>
        public int Capacity => _capacity;

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
        public bool IsAutoScaling => _autoScale;

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
            
            _ioHeapBuf.Writer.Complete();
            //if (zeroAction != null)
            //    await _ioHeapBuf.ZeroManagedAsync(zeroAction, nanite, true).FastPath();
            //else
            //    await _ioHeapBuf.ZeroManagedAsync<object>(zero:true).FastPath();

            _refCount = 0;
        }

        /// <summary>
        /// Checks the heap for a free item and returns it. If no free items exists make a new one if we have capacity for it.
        /// </summary>
        /// <exception cref="InternalBufferOverflowException">Thrown when the max heap size is breached</exception>
        /// <returns>True if the item required malloc, false if popped from the heap otherwise<see cref="TItem"/></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TItem Take(object userData = null, Action<TItem, object> customConstructor = null) => Make(userData, customConstructor).item;
        

        /// <summary>
        /// Checks the heap for a free item and returns it. If no free items exists make a new one if we have capacity for it.
        /// </summary>
        /// <exception cref="InternalBufferOverflowException">Thrown when the max heap size is breached</exception>
        /// <returns>True if the item required malloc, false if popped from the heap otherwise<see cref="TItem"/></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (TItem item,bool malloc) Make(object userData = null, Action<TItem, object> customConstructor = null)
        {
            try
            {
                retry:
                //If the heap is empty
                if (!_ioHeapBuf.Reader.TryRead(out var heapItem))
                {
                    if (_ioHeapBuf.Reader.Count > 0)
                        goto retry;//TODO: hack

                    //TODO: Leak
                    if (_refCount == Capacity && !IsAutoScaling)
                    {
                        _logger.Debug($"{nameof(_ioHeapBuf)}: LEAK DETECTED!!! Heap -> {Description}: Q -> _ioHeapBuf.Description");
                        //throw new OutOfMemoryException($"{nameof(_ioHeapBuf)}: Heap -> {Description}: Q -> _ioHeapBuf.Description");
                    }
                    
                    heapItem = Malloc(userData, Context);

                    customConstructor?.Invoke(heapItem, userData);
                    Constructor?.Invoke(heapItem, userData);
                    PopAction?.Invoke(heapItem, userData);

                    Interlocked.Increment(ref _refCount);
                    return (heapItem, true);
                }
                else //take the item from the heap
                {
                    PopAction?.Invoke(heapItem, userData);
                    Interlocked.Increment(ref _refCount);
                    return (heapItem, false);
                }
            }
            catch when (_zeroed > 0) { }
            catch (Exception) when (Zeroed) { }
            catch (Exception e) when(!Zeroed)
            {
                _logger.Error(e, $"{GetType().Name}: Failed to malloc {typeof(TItem)}");
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

            try
            {
                if (!zero)
                    _ioHeapBuf.Writer.TryWrite(item);
            }
            catch when(_zeroed > 0){ }
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

        private readonly int _capacity;
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
