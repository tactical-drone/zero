using System;
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
    /// <typeparam name="T">The type of item managed</typeparam>
    public class IoHeap<T>
        where T : class
    {
        /// <summary>
        /// ctor
        /// </summary>
        static IoHeap()
        {
            _logger = LogManager.GetCurrentClassLogger();
        }
        
        /// <summary>
        /// Constructor a heap that has a maximum capacity of <see cref="maxSize"/>
        /// </summary>
        /// <param name="maxSize">The maximum capacity of this heap</param>
        /// 
        public IoHeap(uint maxSize, int concurrencyLevel)
        {
            _maxSize = maxSize;
            _ioHeapBuf = new IoBag<T>($"{nameof(_ioHeapBuf)}", _maxSize);
            _count = 0;
            _refCount = 0;
            //IoFpsCounter = new IoFpsCounter(500, 5000);
            Make = default;
        }

        /// <summary>
        /// Logger
        /// </summary>
        private static Logger _logger;

        /// <summary>
        /// The heap buffer space
        /// </summary>
        private IoBag<T> _ioHeapBuf;

        /// <summary>
        /// The current WorkHeap size
        /// </summary>
        protected volatile uint _count;
        
        /// <summary>
        /// The current WorkHeap size
        /// </summary>
        public uint Count => _count;

        /// <summary>
        /// The maximum heap size
        /// </summary>
        private uint _maxSize;

        /// <summary>
        /// The number of outstanding references
        /// </summary>
        private volatile uint _refCount;
        /// <summary>
        /// The number of outstanding references
        /// </summary>
        public uint ReferenceCount =>_refCount; //TODO refactor

        /// <summary>
        /// The maximum heap size allowed. Configurable, collects & compacts on shrinks
        /// </summary>
        public uint MaxSize
        {
            get => _maxSize;

            set
            {
                _maxSize = value;

                //Clear some memory and it does not need to be the exact amount
                while (value > Count && _ioHeapBuf.TryTake(out var _))
                {
                    Interlocked.Decrement(ref _count);
                }
            }
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>  
        public void ZeroUnmanaged()
        {
#if SAFE_RELEASE
            _logger = null;
            _ioHeapBuf = null;
            Make = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public async ValueTask ZeroManagedAsync<TC>(Func<T,TC,ValueTask> zeroAction = null, TC nanite = default)
        {
            if (zeroAction != null)
                await _ioHeapBuf.ZeroManagedAsync(zeroAction, nanite).FastPath().ConfigureAwait(false);

            await _ioHeapBuf.ZeroManagedAsync<object>(nanite:nanite, zero:true).FastPath().ConfigureAwait(false);
        }

        /// <summary>
        /// Checks the heap for a free item and returns it. If no free items exists make a new one if we have capacity for it.
        /// </summary>
        /// <exception cref="InternalBufferOverflowException">Thrown when the max heap size is breached</exception>
        /// <returns>True if the item required malloc, false if popped from the heap otherwise<see cref="T"/></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> TakeAsync(object userData = null)
        {
            try
            {
                //If the heap is empty
                if (!_ioHeapBuf.TryTake(out var heapItem))
                {
                    //And we can allocate more heap space
                    if (Count < _maxSize)
                    {
                        //Allocate and return
                        Interlocked.Increment(ref _count);
                        Interlocked.Increment(ref _refCount);
                        return ValueTask.FromResult(Make(userData));
                    }
                    else //we have run out of capacity
                    {
                        return default;
                    }
                }
                else //take the item from the heap
                {
                    Interlocked.Increment(ref _refCount);
                    Prep?.Invoke(heapItem, userData);
                    return ValueTask.FromResult(heapItem);
                }
            }
            catch (NullReferenceException e) //TODO IIoNanite
            {
                _logger.Trace(e);
            }
            catch (Exception e)
            {
                _logger.Error(e, $"{GetType().Name}: Failed to new up {typeof(T)}");
            }
            
            return default;
        }

        private static readonly ValueTask<T> FromNullTask = new ValueTask<T>((T) default);

        /// <summary>
        /// Returns an item to the heap
        /// </summary>
        /// <param name="item">The item to be returned to the heap</param>
        /// <param name="zero">Whether to destroy this object</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual ValueTask<T> ReturnAsync(T item, bool zero = false)
        {
#if DEBUG
             if (item == null)
                 throw new ArgumentException($"{nameof(item)} cannot be null");
#endif
            try
            {
                //IoFpsCounter.TickAsync().ConfigureAwait(false).GetAwaiter();
                if (!zero)
                    _ioHeapBuf.Add(item);
                else
                    Interlocked.Decrement(ref _count);
                
                Interlocked.Decrement(ref _refCount);
            }
            catch (NullReferenceException)
            {
                _logger.Fatal($"Unexpected error while returning item to the heap! ");
            }

            return ValueTask.FromResult(item);
        }

        /// <summary>
        /// Makes a new item
        /// </summary>
        public Func<object,T> Make;

        /// <summary>
        /// Prepares an item from the stack
        /// </summary>
        public Action<T, object> Prep;

        /// <summary>
        /// Returns the amount of space left in the buffer
        /// </summary>
        /// <returns>The number of free slots</returns>
        public long FreeCapacity() =>_maxSize - Count;

        /// <summary>
        /// Cache size.
        /// </summary>
        /// <returns>The number of unused heap items</returns>
        public long CacheSize() => _ioHeapBuf.Count;

        /// <summary>
        /// Clear
        /// </summary>
        public async ValueTask ClearAsync()
        {
            await _ioHeapBuf.ZeroManagedAsync<object>().FastPath().ConfigureAwait(false);
        }
    }
}
