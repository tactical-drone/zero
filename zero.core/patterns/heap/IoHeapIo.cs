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
        public IoHeapIo(uint maxSize) : base(maxSize)
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
                //Take from heap
                
                if ((next = await TakeAsync(userData).FastPath().ConfigureAwait(false)) != null && !await next.ConstructAsync())
                    return null;
                
                //fail
                if (next == null)
                    return null;

                //ConstructAsync
                next = (T) await next.ConstructorAsync().FastPath().ConfigureAwait(false);

                //Custom constructor
                parms?.Invoke(next, userData);

                //The constructor signals a flush by returning null
                while (next == null)
                {
                    Interlocked.Increment(ref _count);
                    _logger.Trace($"Flushing `{GetType()}'");

                    next = await TakeAsync(userData).FastPath().ConfigureAwait(false);
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

            return next;
        }

        /// <summary>
        /// Return item to the heap
        /// </summary>
        /// <param name="item">The item to return</param>
        /// <param name="zero">If the item is to be zeroed</param>
        public override async ValueTask<T> ReturnAsync(T item, bool zero = false)
        {
            if (item == null)
                return null;

            await base.ReturnAsync(item, zero).FastPath().ConfigureAwait(false);

            if (zero)
                await item.ZeroAsync(new IoNanoprobe($"{GetType()}")).FastPath().ConfigureAwait(false);
            return item;
        }
    }
}
