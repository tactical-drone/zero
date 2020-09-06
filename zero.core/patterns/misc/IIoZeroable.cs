﻿using System;
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
        Action<IIoZeroable> ZeroEvent(Action<IIoZeroable> sub);

        /// <summary>
        /// Unsubscribe
        /// </summary>
        /// <param name="sub">The original subscription</param>
        void Unsubscribe(Action<IIoZeroable> sub);

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