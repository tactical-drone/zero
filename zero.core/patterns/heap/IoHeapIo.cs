using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.misc;
using Logger = NLog.Logger;

namespace zero.core.patterns.heap
{
    /// <summary>
    /// A heap construct that works with Iot types
    /// </summary>
    /// <typeparam name="TItem">The item type</typeparam>
    /// <typeparam name="TContext">Heap context type</typeparam>
    public class IoHeapIo<TItem,TContext>: IoHeap<TItem, TContext> where TItem: class, IIoHeapItem, IIoNanite where TContext : class
    {
        /// <summary>
        /// ConstructAsync
        /// </summary>
        /// <param name="description"></param>
        /// <param name="maxSize"></param>
        /// <param name="context"></param>
        /// <param name="enablePerf"></param>
        public IoHeapIo(string description, int maxSize, TContext context = null, bool enablePerf = false) : base(description, maxSize, context, enablePerf)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        private readonly Logger _logger;
        /// <summary>
        /// Take an item but call the constructor first
        /// </summary>
        /// <returns>The constructed heap item</returns>
        public async ValueTask<TItem> TakeAsync<TLocalContext>(Func<TItem, TLocalContext, ValueTask<TItem>> constructor = null, TLocalContext context = default)
        {
            TItem next = null;
            try
            {
                //Take from heap
                
                if ((next = Take(context)) != null && !await next.ConstructAsync())
                    return null;
                
                //fail
                if (next == null)
                    return null;

                //ConstructAsync
                next = (TItem) await next.ConstructorAsync().FastPath().ConfigureAwait(Zc);

                //Custom constructor
                constructor?.Invoke(next, context);

                //The constructor signals a flush by returning null
                while (next == null)
                {
                    Interlocked.Increment(ref _count);
                    _logger.Trace($"Flushing `{GetType()}'");

                    next = Take(context);
                    //Return another item from the heap
                    if (next == null)
                    {
                        _logger.Error($"`{GetType()}', unable to allocate memory");
                        return null;
                    }

                    //Try the next one
                    next = (TItem) await next.ConstructorAsync().FastPath().ConfigureAwait(Zc);
                }

                return next;
            }
            catch when(Zeroed){}
            catch (Exception e)when(!Zeroed)
            {
                if (next != null)
                {
                    _logger.Error(e, $"Heap `{this}' item construction returned with errors:");
                        Return(next);
                }                    
                else
                {
                    _logger.Warn($"Heap `{this}' ran out of capacity, Free = {FreeCapacity()}/{MaxSize}");
                }              
            }

            return null;
        }

        /// <summary>
        /// Return item to the heap
        /// </summary>
        /// <param name="item">The item to return</param>
        /// <param name="zero">If the item is to be zeroed</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void Return(TItem item, bool zero = false)
        {
            if (item == null)
                return;

            base.Return(item, zero);

            if (zero)
                item.Zero(new IoNanoprobe($"{GetType()}"));
        }
    }

    public class IoHeapIo<TItem>: IoHeapIo<TItem, IIoNanite> where TItem : class, IIoHeapItem, IIoNanite
    {
        public IoHeapIo(string description, int maxSize, bool enablePerf = false) : base(description, maxSize, null, enablePerf)
        {
        }
    }
}
