using System;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.bushes;
using zero.core.patterns.misc;

namespace zero.core.patterns.heap
{
    /// <summary>
    /// A heap construct that works with Iot types
    /// </summary>
    /// <typeparam name="T">The item type</typeparam>
    public class IoHeapIo<T>: IoHeap<T> where T: class, IIoHeapItem, IIoZeroable
    {
        /// <summary>
        /// Construct
        /// </summary>
        /// <param name="maxSize"></param>
        public IoHeapIo(long maxSize) : base(maxSize)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        private readonly Logger _logger;
        /// <summary>
        /// Take an item but call the constructor first
        /// </summary>
        /// <returns>The constructed heap item</returns>
        public async ValueTask<T> TakeAsync(Func<T, object, T> parms = null, object userData = null)
        {
            object next = null;
            try
            {
                //Allocate memory
                if ((next = await base.TakeAsync(userData).ConfigureAwait(false)) == null)
                    return null;

                //Construct
                next = ((T) next).Constructor();

                //Custom constructor
                parms?.Invoke((T) next, userData);

                //The constructor signals a flush by returning null
                while (next == null)
                {
                    Interlocked.Increment(ref CurrentHeapSize);
                    _logger.Trace($"Flushing `{GetType()}'");

                    //Return another item from the heap
                    if ((next = (T)await base.TakeAsync(userData).ConfigureAwait(false)) == null)
                    {
                        _logger.Error($"`{GetType()}', unable to allocate memory");
                        return null;
                    }

                    //Try the next one
                    next = ((T) next).Constructor();
                }
            }
            catch (NullReferenceException)
            {
                return null;
            }
            catch (Exception e)
            {
                if (next != null)
                {
                    _logger.Error(e, $"Heap `{this}' item construction returned with errors:");
                    await ReturnAsync((T)next).ConfigureAwait(false);
                    return null;
                }                    
                else
                {
                    _logger.Warn($"Heap `{this}' ran out of capacity, Free = {FreeCapacity()}/{MaxSize}");
                    return null;
                }              
            }                        

            return (T)next;
        }
    }
}
