using System;
using System.Threading;
using System.Threading.Tasks;

namespace zero.core.patterns.misc
{
    public interface IIoZeroable : IEquatable<IIoZeroable>
    {
        /// <summary>
        /// returns an identity
        /// </summary>
        ulong NpId { get; }
        
        /// <summary>
        /// The source of zero
        /// </summary>
        IIoZeroable ZeroedFrom { get; }
        
        /// <summary>
        /// ZeroAsync pattern
        /// </summary>
        ValueTask ZeroAsync(IIoZeroable from);
        
        /// <summary>
        /// A description of this object
        /// </summary>
        string Description { get; }

        /// <summary>
        /// Indicate zero status
        /// </summary>
        /// <returns>True if zeroed out, false otherwise</returns>
        bool Zeroed();

        /// <summary>
        /// Cascade zeroed object
        /// </summary>
        /// <param name="target">The object to be zeroed out</param>
        /// <param name="twoWay">If the zeroing is both ways</param>
        (TBase target, bool success) ZeroOnCascade<TBase>(TBase target, bool twoWay = false) where TBase : IIoZeroable;
        
        /// <summary>
        /// Subscribe to disposed event
        /// </summary>
        /// <param name="sub">The handler</param>
        /// <returns>The handler</returns>
        IoZeroSub ZeroEvent(Func<IIoZeroable, Task> sub);

        /// <summary>
        /// Unsubscribe
        /// </summary>
        /// <param name="sub">The original subscription</param>
        void Unsubscribe(IoZeroSub sub);

        /// <summary>
        /// Ensures that this action is synchronized 
        /// </summary>
        /// <param name="ownershipAction">The ownership transfer</param>
        /// <param name="disposing">If disposing</param>
        /// <param name="force">Forces the action regardless of zero state</param>
        /// <returns>true on success, false otherwise</returns>
        ValueTask<bool> ZeroEnsureAsync(Func<IIoZeroable, bool, Task<bool>> ownershipAction, bool disposing = false, bool force = false);
        
        void ZeroUnmanaged();
        ValueTask ZeroManagedAsync();
    }
}