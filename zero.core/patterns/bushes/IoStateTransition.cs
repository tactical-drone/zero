using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Represents a state transition while processing work on a concurrent process
    /// </summary>
    /// <typeparam name="TState">The state enum</typeparam>
    public class IoStateTransition<TState> : IoNanoprobe, IIoHeapItem
        where TState : Enum {
        public IoStateTransition()
        {
            ConstructorAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        /// <summary>
        /// A null state
        /// </summary>
        public static IoStateTransition<TState> NullState = new IoStateTransition<TState>();

        /// <summary>
        /// The previous state
        /// </summary>
        public volatile IoStateTransition<TState> Previous;

        /// <summary>
        /// The next state
        /// </summary>
        public volatile IoStateTransition<TState> Next;

        /// <summary>
        /// A repeat state
        /// </summary>
        public volatile IoStateTransition<TState> Repeat;

        /// <summary>
        /// The represented state
        /// </summary>
        public TState Value;

        /// <summary>
        /// The represented state
        /// </summary>
        protected TState UndefinedState => NullState.Value;

        /// <summary>
        /// The represented state
        /// </summary>
        public TState FinalState;

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
        public ValueTask<IIoHeapItem> ConstructorAsync()
        {
            ExitTime = EnterTime = DateTime.Now;
            Previous = Next = null;
            Repeat = null;
            Value = default(TState);
            return new ValueTask<IIoHeapItem>(this);
        }

        /// <summary>
        /// Calculates the max state string length used for log formatting purposes
        /// </summary>
        public static readonly int StateStrPadding = Enum.GetNames(typeof(TState)).ToList().Select(s => s.Length).Max();

        /// <summary>
        /// Pads the current state string and returns it
        /// </summary>
        /// <returns>Returns the padded string representation of this state</returns>
        public string PaddedStr()
        {
            return Value.ToString()!.PadLeft(StateStrPadding);
        }

        /// <summary>
        /// Used in debugger
        /// </summary>
        /// <returns>PreviousJob -> Current -> Next</returns>
        public override string ToString()
        {
            return $"{Previous.Value}/> {Value} />{Next.Value}";
        }

        /// <summary>
        /// Enter a state
        /// </summary>
        /// <param name="state"></param>
        public void Enter(TState state)
        {
#if DEBUG
            //Enter only once
            if(!Value.Equals(UndefinedState))
                throw new ApplicationException($"Current state must be {UndefinedState}, was {Value}");
#endif
            EnterTime = ExitTime = DateTime.Now;
            Value = state;
        }

        /// <summary>
        /// The default state string padded
        /// </summary>
        public string DefaultPadded => Value.ToString().PadLeft(StateStrPadding);

        /// <summary>
        /// Exit state
        /// </summary>
        /// <param name="nextState">The state we are exiting to</param>
        public void Exit(IoStateTransition<TState> nextState)
        {
            if (Zeroed())
            {
                Value = FinalState;
                return;
            }

            if (Value.Equals(FinalState))
            {
                throw new ApplicationException($"Cannot transition from `{FinalState}' to `{nextState}'");
            }

            ExitTime = DateTime.Now;
            Next = nextState;
            nextState.Previous = this;
        }
    }
}
