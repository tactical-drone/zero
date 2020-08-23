using System;
using System.Threading.Tasks;

namespace zero.core.patterns.misc
{
    public interface IIoZeroable
    {
        /// <summary>
        /// Zero pattern
        /// </summary>
        Task Zero();

        /// <summary>
        /// Subscribe to disposed event
        /// </summary>
        /// <param name="sub">The handler</param>
        /// <returns>The handler</returns>
        Func<IIoZeroable, Task> ZeroEvent(Func<IIoZeroable, Task> sub);

        /// <summary>
        /// Unsubscribe
        /// </summary>
        /// <param name="sub">The original subscription</param>
        Func<IIoZeroable, Task> Unsubscribe(Func<IIoZeroable, Task> sub);

        /// <summary>
        /// Cascade zeroed object
        /// </summary>
        /// <param name="target">The object to be zeroed out</param>
        /// <param name="twoWay">If the zeroing is both ways</param>
        T ZeroOnCascade<T>(T target, bool twoWay = false) where T : IIoZeroable;

        /// <summary>
        /// Indicate zero status
        /// </summary>
        /// <returns>True if zeroed out, false otherwise</returns>
        bool Zeroed();
    }
}