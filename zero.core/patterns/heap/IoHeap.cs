using System;
using System.Collections.Concurrent;
using System.IO;
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
        where T:class,IIoNanoprobe
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
            IoFpsCounter = new IoFpsCounter();
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
        public ValueTask ZeroManagedAsync()
        {

            try
            {
                //Do we need to zero here? It is slowing teardown and jobs are supposed to be volatile
                // But maybe sometime jobs are expected to zero? We leave that to the IDisposable pattern
                // to zero eventually?
                //_buffer.ToList().ForEach(h=>h.ZeroAsync(this));
                _buffer.Clear();
            }
            catch { }

            //await base.ZeroManagedAsync().ConfigureAwait(false);
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Checks the heap for a free item and returns it. If no free items exists make a new one if we have capacity for it.
        /// </summary>
        /// <exception cref="InternalBufferOverflowException">Thrown when the max heap size is breached</exception>
        /// <returns>The next initialized item from the heap.<see cref="T"/></returns>
        public ValueTask<T> TakeAsync(object userData = null)
        {
            try
            {
//If the heap is empty
                if (!_buffer.TryTake(out var item))
                {
                    //And we can allocate more heap space
                    if (Interlocked.Read(ref CurrentHeapSize) < _maxSize)
                    {
                        //Allocate and return
                        Interlocked.Increment(ref CurrentHeapSize);
                        Interlocked.Increment(ref ReferenceCount);
                        return ValueTask.FromResult(Make(userData));
                    }
                    else //we have run out of capacity
                    {
                        return FromNullTask;
                    }
                }
                else //take the item from the heap
                {
                    Interlocked.Increment(ref ReferenceCount);
                    return ValueTask.FromResult(item);
                }
            }
            catch (NullReferenceException) { }
            catch (Exception e)
            {
                _logger.Error(e, $"{GetType().Name}: Failed to new up {typeof(T)}");
            }

            return FromNullTask;
        }

        private static readonly ValueTask<T> FromNullTask = new ValueTask<T>((T) default);

        /// <summary>
        /// Returns an item to the heap
        /// </summary>
        /// <param name="item">The item to be returned to the heap</param>
        public ValueTask ReturnAsync(T item)
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
            return ValueTask.CompletedTask;
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
