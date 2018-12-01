using System;
using System.Collections.Concurrent;
using System.IO;
using System.Runtime;
using System.Threading;
using NLog;

namespace zero.core.patterns.heap
{
    /// <summary>
    /// Basic heap construct
    /// </summary>
    /// <typeparam name="T">The type of item managed</typeparam>
    public class IoHeap<T> where T:class
    {               
        /// <summary>
        /// Constructor a heap that has a maximum capacity of <see cref="maxSize"/>
        /// </summary>
        /// <param name="maxSize">The maximum capacity of this heap</param>
        public IoHeap(long maxSize)
        {
            _maxSize = maxSize;
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// Logger
        /// </summary>
        private Logger _logger;

        /// <summary>
        /// The heap buffer space
        /// </summary>
        private readonly ConcurrentBag<T> _buffer = new ConcurrentBag<T>();

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
        public volatile int ReferenceCount; //TODO refactor

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

                //Flush some memory and it does not need to be the exact amount
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
        /// Checks the heap for a free item and returns it. If no free items exists make a new one if we have capacity for it.
        /// </summary>
        /// <exception cref="InternalBufferOverflowException">Thrown when the max heap size is breached</exception>
        /// <returns>The next initialized item from the heap.<see cref="T"/></returns>
        public virtual T Take(Func<T,T> parms = null, object userData = null)
        {
            if (parms == null)
                parms = arg => arg;

            //If the heap is empty
            if (!_buffer.TryTake(out var item))
            {
                //And we can allocate more heap space
                if (Interlocked.Read(ref CurrentHeapSize) < _maxSize)
                {
                    //Allocate and return
                    Interlocked.Increment(ref CurrentHeapSize);
                    Interlocked.Increment(ref ReferenceCount);
                    return parms(Make(userData));
                }
                else //we have run out of capacity
                {
                    return null;
                }
            }
            else //take the item from the heap
            {
                Interlocked.Increment(ref ReferenceCount);
                return item;
            }
        }

        /// <summary>
        /// Returns an item to the heap
        /// </summary>
        /// <param name="item">The item to be returned to the heap</param>
        public void Return(T item)
        {
            _buffer.Add(item);
            Interlocked.Add(ref ReferenceCount, -1);
        }

        /// <summary>
        /// Makes a new item
        /// </summary>
        public Func<object,T> Make;

        /// <summary>
        /// Flushes the buffer
        /// </summary>
        public void Flush()
        {
            _buffer.Clear();
        }

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
