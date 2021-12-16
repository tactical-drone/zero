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
        /// <returns>This instance</returns>
        ValueTask<IIoHeapItem> ReuseAsync();
    }
}
