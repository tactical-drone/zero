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
    public class IoHeapIo<T>: IoHeap<T> where T: class, IIoHeapItem, IIoNanite
    {
        /// <summary>
        /// ConstructAsync
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
        public async ValueTask<T> TakeAsync(Func<T, object, ValueTask<T>> parms = null, object userData = null)
        {
            T next = null;
            try
            {
                if (Take(out next, userData) && next != null && !await next.ConstructAsync())
                    return null;

                //Take from heap
                if (next == null)
                    return null;

                //ConstructAsync
                next = (T) await next.ConstructorAsync().FastPath().ConfigureAwait(false);

                //Custom constructor
                parms?.Invoke(next, userData);

                //The constructor signals a flush by returning null
                while (next == null)
                {
                    Interlocked.Increment(ref CurrentHeapSize);
                    _logger.Trace($"Flushing `{GetType()}'");

                    Take(out next, userData);
                    //Return another item from the heap
                    if (next == null)
                    {
                        _logger.Error($"`{GetType()}', unable to allocate memory");
                        return null;
                    }

                    //Try the next one
                    next = (T) await next.ConstructorAsync().FastPath().ConfigureAwait(false);
                }
            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e);
            }
            catch (Exception e)
            {
                if (next != null)
                {
                    _logger.Error(e, $"Heap `{this}' item construction returned with errors:");
                        await ReturnAsync(next).FastPath().ConfigureAwait(false);
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

        public override async ValueTask ReturnAsync(T item, bool destroy = false)
        {
            await base.ReturnAsync(item, destroy).FastPath().ConfigureAwait(false);

            if (destroy)
                await item.ZeroAsync(new IoNanoprobe($"{GetType()}")).FastPath().ConfigureAwait(false);
        }
    }
}
