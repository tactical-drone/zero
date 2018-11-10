namespace zero.core.patterns.heap
{
    /// <summary>
    /// Provides access to lower level heap processes
    /// </summary>
    public interface IOHeapItem
    {
        /// <summary>
        /// Initializes this instance for reuse from the heap. 
        /// If null is returned the instance will be flushed
        /// and a new malloc will be done
        /// </summary>
        /// <returns>This instance</returns>
        IOHeapItem Constructor();
    }
}
