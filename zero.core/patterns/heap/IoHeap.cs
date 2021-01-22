using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.misc;
using zero.core.patterns.misc;

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
        public IoHeap(long maxSize)
        {
            _maxSize = maxSize;
            _buffer = new ConcurrentBag<T>();
            CurrentHeapSize = 0;
            ReferenceCount = 0;
            IoFpsCounter = new IoFpsCounter(500, 5000);
            Make = default;
        }

        /// <summary>
        /// Logger
        /// </summary>
        private static readonly Logger _logger;

        /// <summary>
        /// The heap buffer space
        /// </summary>
        private ConcurrentBag<T> _buffer;

        /// <summary>
        /// The current WorkHeap size
        /// </summary>
        public long CurrentHeapSize;

        /// <summary>
        /// The maximum heap size
        /// </summary>
        private long _maxSize;

        /// <summary>
        /// The number of outstanding references
        /// </summary>
        public long ReferenceCount; //TODO refactor

        /// <summary>
        /// Jobs per second
        /// </summary>
        public IoFpsCounter IoFpsCounter;

        /// <summary>
        /// The maximum heap size allowed. Configurable, collects & compacts on shrinks
        /// </summary>
        public long MaxSize
        {
            get => _maxSize;

            set
            {
                var collect = value < CurrentHeapSize;

                _maxSize = value;

                //Clear some memory and it does not need to be the exact amount
                while (value < CurrentHeapSize )
                {
                    if(!_buffer.TryTake(out var flushed))
                        break;
                    Interlocked.Decrement(ref CurrentHeapSize);
                }

                //Do some compaction
                if (collect)
                {
                    GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
                    GC.Collect(GC.MaxGeneration);
                }                
            }
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>  
        public void ZeroUnmanaged()
        {
#if SAFE_RELEASE
            _buffer = null;
            Make = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public void ZeroManaged(Action<T> zeroAction = null)
        {

            try
            {
                //Do we need to zero here? It is slowing teardown and jobs are supposed to be volatile
                // But maybe sometime jobs are expected to zero? We leave that to the IDisposable pattern
                // to zero eventually?
                _buffer.ToList().ForEach(h=>
                {
                    if (zeroAction != null) zeroAction(h);
                });
                _buffer.Clear();
            }
            catch { }

            //await base.ZeroManaged().ConfigureAwait(false);
        }

        /// <summary>
        /// Checks the heap for a free item and returns it. If no free items exists make a new one if we have capacity for it.
        /// </summary>
        /// <exception cref="InternalBufferOverflowException">Thrown when the max heap size is breached</exception>
        /// <returns>True if the item required malloc, false if popped from the heap otherwise<see cref="T"/></returns>
        public bool Take(out T item, object userData = null)
        {
            try
            {
                //If the heap is empty
                if (!_buffer.TryTake(out var newItem))
                {
                    //And we can allocate more heap space
                    if (Interlocked.Read(ref CurrentHeapSize) < _maxSize)
                    {
                        //Allocate and return
                        Interlocked.Increment(ref CurrentHeapSize);
                        Interlocked.Increment(ref ReferenceCount);
                        item = Make(userData);
                        return true;
                    }
                    else //we have run out of capacity
                    {
                        item = null;
                        return false;
                    }
                }
                else //take the item from the heap
                {
                    Interlocked.Increment(ref ReferenceCount);
                    item = newItem; 
                    return false;
                }
            }
            catch (NullReferenceException) { }
            catch (Exception e)
            {
                _logger.Error(e, $"{GetType().Name}: Failed to new up {typeof(T)}");
            }

            item = null;
            return false;
        }

        private static readonly ValueTask<T> FromNullTask = new ValueTask<T>((T) default);

        /// <summary>
        /// Returns an item to the heap
        /// </summary>
        /// <param name="item">The item to be returned to the heap</param>
        public void Return(T item)
        {
#if DEBUG
            // if (item == null)
            //     throw new ArgumentException($"{nameof(item)} cannot be null");
#endif
            try
            {
                IoFpsCounter.Tick();
                _buffer.Add(item);
                Interlocked.Add(ref ReferenceCount, -1);
            }
            catch (NullReferenceException)
            {
                //await item.ZeroAsync(this).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Makes a new item
        /// </summary>
        public Func<object,T> Make;

        /// <summary>
        /// Returns the amount of space left in the buffer
        /// </summary>
        /// <returns>The number of free slots</returns>
        public long FreeCapacity() =>_maxSize - Interlocked.Read(ref CurrentHeapSize) ;

        /// <summary>
        /// Cache size.
        /// </summary>
        /// <returns>The number of unused heap items</returns>
        public long CacheSize() => _buffer.Count;
    }
}
