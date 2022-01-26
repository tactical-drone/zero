using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace zero.core.patterns.heap
{
    /// <summary>
    /// Provides access to lower level heap processes
    /// </summary>
    public interface IIoHeapItem
    {
        /// <summary>
        /// Initializes this instance for reuse from the heap. 
        /// If null is returned the instance will be flushed
        /// and a new malloc will be done
        /// </summary>
        /// <param name="context"></param>
        /// <returns>This instance</returns>
        ValueTask<IIoHeapItem> HeapPopAsync(object context);

        ///// <summary>
        ///// Constructs a new item from the heap
        ///// </summary>
        ///// <param name="context"></param>
        ///// <returns>A task</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IIoHeapItem HeapConstructAsync(object context);
    }
}
