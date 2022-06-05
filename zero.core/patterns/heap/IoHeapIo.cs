using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.misc;
using zero.core.runtime.scheduler;
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
        /// Constructs a new Heap that manages types of <see cref="IIoNanite"/>.
        /// 
        /// For POCO types use the more performant <see cref="IoHeap{TItem,TContext}"/>
        /// </summary>
        /// <param name="description"></param>
        /// <param name="context">dev setup context</param>
        /// <param name="capacity">Total capacity</param>
        /// <param name="malloc">allocate items callback</param>
        /// <param name="autoScale">Whether to ramp capacity</param>
        public IoHeapIo(string description, int capacity, Func<object, TContext, TItem> malloc, bool autoScale = false, TContext context = null) : base(description, capacity, malloc, autoScale, context)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        private readonly Logger _logger;
        /// <summary>
        /// Take an item but call the constructor first
        /// </summary>
        /// <returns>The constructed heap item</returns>
        public async ValueTask<TItem> TakeAsync<TLocalContext>(Func<TItem, TLocalContext, ValueTask<TItem>> localReuse = null, TLocalContext userData = default)
        {
            TItem next = null;
            var heapItem = Make(userData);
            try
            {
                if ((next = heapItem.item) == null) 
                    return null;

                if (heapItem.malloc && await next.HeapConstructAsync(userData).FastPath() == null)
                {
                    throw new InvalidOperationException($"{nameof(next.HeapConstructAsync)} FAILED: ");
                }

#if TRACE
                if (heapItem.malloc)
                {
                    await heapItem.item.ZeroSubAsync<object>((t, u) =>
                    {
                        LogManager.GetCurrentClassLogger().Fatal($"SAFE_RELEASE -> {u.ToString()}");
                        return default;
                    }, heapItem.item);
                }
#endif

                //init for use
                if (await next.HeapPopAsync(userData).FastPath() == null)
                {
                    Return(next);
                    return null;
                }

                //Custom reuse
                if (localReuse != null)
                {
                    if (await localReuse.Invoke(next, userData).FastPath() == null)
                    {
                        Return(next);
                        return null;
                    }
                }
                
                return next;
            }
            catch when(Zeroed){}
            catch (Exception e)when(!Zeroed)
            {
                _logger.Error(e, $"Heap `{this}' item construction returned with errors:");
                if (next != null)
                    Return(next);
            }
            
            return null;
        }

        /// <summary>
        /// Takes item from the heap
        /// </summary>
        /// <returns>A new item</returns>
        public ValueTask<TItem> TakeAsync()
        {
            return TakeAsync<object>();
        }

        /// <summary>
        /// Return item to the heap
        /// </summary>
        /// <param name="item">The item to return</param>
        /// <param name="zero">If the item is to be zeroed</param>
        /// <param name="deDup"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void Return(TItem item, bool zero = false, bool deDup = false)
        {
            if (item == null)
                return;

            base.Return(item, zero, deDup);

            if (zero && !deDup || Zeroed) //hack
            {
                IoZeroScheduler.Zero.LoadAsyncContext(static async state =>
                {
                    var item = (TItem)state;
                    await item.DisposeAsync(null, $"{nameof(IoHeapIo<TItem, TContext>)}: teardown direct!");
                },item);
            }
        }
    }

    public class IoHeapIo<TItem>: IoHeapIo<TItem, IIoNanite> where TItem : class, IIoHeapItem, IIoNanite
    {
        public IoHeapIo(string description, int capacity, Func<object, IIoNanite, TItem> malloc, bool autoScale = false) : base(description, capacity, malloc, autoScale: autoScale)
        {
        }
    }
}
