using System;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.bushes.contracts;

namespace zero.core.patterns.misc
{
    public interface IIoZeroable
    {
        /// <summary>
        /// A description of this object
        /// </summary>
        string Description { get; }

        /// <summary>
        /// The source of zero
        /// </summary>
        IIoZeroable ZeroedFrom { get; }

        /// <summary>
        /// Zero pattern
        /// </summary>
        void Zero(IIoZeroable @from);

        /// <summary>
        /// Subscribe to disposed event
        /// </summary>
        /// <param name="sub">The handler</param>
        /// <returns>The handler</returns>
        IoZeroable.ZeroSub ZeroEvent(Action<IIoZeroable> sub);

        /// <summary>
        /// Unsubscribe
        /// </summary>
        /// <param name="sub">The original subscription</param>
        void Unsubscribe(IoZeroable.ZeroSub sub);

        /// <summary>
        /// Cascade zeroed object
        /// </summary>
        /// <param name="target">The object to be zeroed out</param>
        /// <param name="twoWay">If the zeroing is both ways</param>
        T ZeroOnCascade<T>(T target, bool twoWay = false) where T : class, IIoZeroable;

        /// <summary>
        /// Indicate zero status
        /// </summary>
        /// <returns>True if zeroed out, false otherwise</returns>
        bool Zeroed();

        /// <summary>
        /// Ensures that this action is synchronized 
        /// </summary>
        /// <param name="ownershipAction">The ownership transfer</param>
        /// <param name="force">Forces the action regardless of zero state</param>
        /// <returns>true on success, false otherwise</returns>
        bool ZeroEnsure(Func<bool> ownershipAction, bool force = false);
    }
}