using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
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
        //public IoFpsCounter IoFpsCounter;

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
            }
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>  
        public void ZeroUnmanaged()
        {
#if SAFE_RELEASE
            _logger = null;
            _buffer = null;
            Make = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public async ValueTask ZeroManaged(Func<T,ValueTask> zeroAction = null)
        {
            if (zeroAction != null)
            {
                foreach (var h in _buffer)
                {
                    try
                    {
                        //Do we need to zero here? It is slowing teardown and jobs are supposed to be volatile
                        // But maybe sometime jobs are expected to zero? We leave that to the IDisposable pattern
                        // to zero eventually?
                        await zeroAction.Invoke(h).FastPath().ConfigureAwait(false);
                    }
                    catch { }
                }
            }
            
            _buffer.Clear();
            return;
        }

        /// <summary>
        /// Checks the heap for a free item and returns it. If no free items exists make a new one if we have capacity for it.
        /// </summary>
        /// <exception cref="InternalBufferOverflowException">Thrown when the max heap size is breached</exception>
        /// <returns>True if the item required malloc, false if popped from the heap otherwise<see cref="T"/></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
                    Prep?.Invoke(item, userData);
                    return false;
                }
            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e);
            }
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
        /// <param name="destroy">Whether to destroy this object</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual ValueTask ReturnAsync(T item, bool destroy = false)
        {
#if DEBUG
             if (item == null)
                 throw new ArgumentException($"{nameof(item)} cannot be null");
#endif
            try
            {
                //IoFpsCounter.TickAsync().ConfigureAwait(false).GetAwaiter();
                if (!destroy)
                    _buffer.Add(item);
                else
                    Interlocked.Decrement(ref CurrentHeapSize);
                
                Interlocked.Decrement(ref ReferenceCount);
            }
            catch (NullReferenceException)
            {
                _logger.Fatal($"Unexpected error while returning item to the heap! ");
            }

            return ValueTask.CompletedTask;
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
        public long FreeCapacity() =>_maxSize - Interlocked.Read(ref CurrentHeapSize) ;

        /// <summary>
        /// Cache size.
        /// </summary>
        /// <returns>The number of unused heap items</returns>
        public long CacheSize() => _buffer.Count;

        /// <summary>
        /// Clear
        /// </summary>
        public void Clear()
        {
            _buffer.Clear();
        }
    }
}
