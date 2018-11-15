using System;
using System.Globalization;
using System.Linq;
using zero.core.patterns.misc;
using zero.core.patterns.heap;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Represents a state transition while processing work on a concurrent process
    /// </summary>
    /// <typeparam name="TSource">The concurrent process type</typeparam>
    public class IoWorkStateTransition<TSource> : IOHeapItem
    where TSource : IoJobSource
    {
        /// <summary>
        /// The previous state
        /// </summary>
        public volatile IoWorkStateTransition<TSource> Previous;

        /// <summary>
        /// The next state
        /// </summary>
        public volatile IoWorkStateTransition<TSource> Next;

        /// <summary>
        /// The represented state
        /// </summary>
        public volatile IoProducable<TSource>.State State;

        /// <summary>
        /// Timestamp when this state was entered
        /// </summary>
        public DateTime EnterTime;

        /// <summary>
        /// Timestamp when this state was exited
        /// </summary>
        public DateTime ExitTime;

        /// <summary>
        /// The absolute time it took to mechanically transition from the previous state to this state. <see cref="EnterTime"/> - <see cref="Previous"/>.<see cref="EnterTime"/>
        /// </summary>
        public TimeSpan Lambda => Previous == null ? TimeSpan.Zero : EnterTime - Previous.EnterTime;

        /// <summary>
        /// The time it took between entering this state and exiting it
        /// </summary>
        public TimeSpan Mu => ExitTime - EnterTime;

        /// <summary>
        /// The absolute time this job took so far
        /// </summary>
        public TimeSpan Delta => Previous == null? Mu: Previous.Delta + Mu;

        /// <summary>
        /// Prepares this item for use after popped from the heap
        /// </summary>
        /// <returns>The intsance</returns>
        public IOHeapItem Constructor()
        {
            ExitTime = EnterTime = DateTime.Now;
            Previous = Next = null;
            State = IoProducable<TSource>.State.Undefined;
            return this;
        }

        /// <summary>
        /// Calcylates the max state string length used for log formatting purposes
        /// </summary>
        public static readonly int StateStrPadding = Enum.GetNames(typeof(IoProducable<>.State)).ToList().Select(s => s.Length).Max();

        /// <summary>
        /// Pads the current state string and returns it
        /// </summary>
        /// <returns>Returns the padded string representation of this state</returns>
        public string PaddedStr()
        {
            return State.ToString().PadLeft(StateStrPadding);
        }

        /// <summary>
        /// The default state string padded
        /// </summary>
        public string DefaultPadded => IoProducable<TSource>.State.Undefined.ToString().PadLeft(StateStrPadding);
    }
}
