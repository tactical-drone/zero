using System;
using System.Linq;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.heap;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Represents a state transition while processing work on a concurrent process
    /// </summary>
    /// <typeparam name="TJob">The type of job produced</typeparam>
    public class IoWorkStateTransition<TJob> : IIoHeapItem
        where TJob : IIoJob
        
    {
        /// <summary>
        /// The previous state
        /// </summary>
        public volatile IoWorkStateTransition<TJob> Previous;

        /// <summary>
        /// The next state
        /// </summary>
        public volatile IoWorkStateTransition<TJob> Next;

        /// <summary>
        /// The represented state
        /// </summary>
        public volatile IoJob<TJob>.State State;

        /// <summary>
        /// Timestamped when this state was entered
        /// </summary>
        public DateTime EnterTime;

        /// <summary>
        /// Timestamped when this state was exited
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
        public TimeSpan Delta => Previous == null ? Mu : Previous.Delta + Mu;

        /// <summary>
        /// Prepares this item for use after popped from the heap
        /// </summary>
        /// <returns>The instance</returns>
        public IIoHeapItem Constructor()
        {
            ExitTime = EnterTime = DateTime.Now;
            Previous = Next = null;
            State = IoJob<TJob>.State.Undefined;
            return this;
        }

        /// <summary>
        /// Calculates the max state string length used for log formatting purposes
        /// </summary>
        public static readonly int StateStrPadding = Enum.GetNames(typeof(IoJob<>.State)).ToList().Select(s => s.Length).Max();

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
        public string DefaultPadded => IoJob<TJob>.State.Undefined.ToString().PadLeft(StateStrPadding);

        public override string ToString()
        {
            return $"[{Previous?.State}] => ({State}) => [{Next?.State}]";
        }
    }
}
