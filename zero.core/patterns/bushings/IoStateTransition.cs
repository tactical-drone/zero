using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.queue;
using zero.core.patterns.queue.variant;

namespace zero.core.patterns.bushings
{
    /// <summary>
    /// Represents a state transition while processing work on a concurrent process
    /// </summary>
    /// <typeparam name="TState">The state enum</typeparam>
    public class IoStateTransition<TState> : IoIntQueue.IoZNode,  IIoHeapItem
        where TState : struct, Enum
    {
        static IoStateTransition()
        {
            States = Enum.GetValues(typeof(TState)).Cast<TState>().ToArray();
        }

        public IoStateTransition()
        {
            EnterTime = Environment.TickCount;
        }

        public IoStateTransition(int initState = 0)
        {
            base.Value = initState;
            EnterTime = Environment.TickCount;
        }

        //Release all memory held
        public void ZeroManaged()
        {
            base.Prev = null;
            base.Next = null;
            FinalState = default;
        }

        #region Core
        
        /// <summary>
        /// The represented state
        /// </summary>
        public TState FinalState;

        /// <summary>
        /// Timestamped when this state was entered
        /// </summary>
        public volatile int EnterTime;

        /// <summary>
        /// Timestamped when this state was exited
        /// </summary>
        public volatile int ExitTime;
        #endregion

        public new IoStateTransition<TState> Next
        {
            get => (IoStateTransition<TState>)base.Next;
            set => base.Next = value;
        }
        public new IoStateTransition<TState> Prev
        {
            get => (IoStateTransition<TState>)base.Prev;
            set => base.Prev = value;
        }

        /// <summary>
        /// Current state enum value
        /// </summary>
        public new TState Value => States[Volatile.Read(ref base.Value)];

        /// <summary>
        /// The absolute time it took to mechanically transition from the previous state to this state. <see cref="EnterTime"/> - <see cref="IoQueue{T}.IoZNode.Prev"/>. <see cref="EnterTime"/>
        /// </summary>
        public long Lambda => EnterTime - Prev?.EnterTime?? 0;

        /// <summary>
        /// The time it took between entering this state and exiting it
        /// </summary>
        public long Mu => ExitTime - EnterTime;

        /// <summary>
        /// The absolute time this job took so far
        /// </summary>
        //public long Delta => Prev?.Delta + Mu?? Mu;

        /// <summary>
        /// default constructor
        /// </summary>
        /// <returns></returns>
        public ValueTask<IIoHeapItem> ConstructorAsync(int initState)
        {
            ExitTime = EnterTime = Environment.TickCount;
            base.Next = null;
            base.Prev = null;
            base.Value = initState;
            return new ValueTask<IIoHeapItem>(this);
        }

        /// <summary>
        /// Calculates the max state string length used for log formatting purposes
        /// </summary>
        public static readonly int StateStrPadding = Enum.GetNames(typeof(TState)).ToList().Select(s => s.Length).Max();

        /// <summary>
        /// An array of all the states
        /// </summary>
        private static readonly TState[] States;

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
            var prevStr = string.Empty;
            if (Prev != null)
                prevStr = $"{Enum.GetName(typeof(TState), Prev.Value)} ~> ";

            var nextStr = string.Empty;
            if (Next != null)
                nextStr = $" ~> {Enum.GetName(typeof(TState), Next.Value)}";

            return $"{prevStr}[{Enum.GetName(typeof(TState),Value)}]{nextStr}";
        }

        /// <summary>
        /// The default state string padded
        /// </summary>
        public string DefaultPadded => Value.ToString().PadLeft(StateStrPadding);

        /// <summary>
        /// Set the state
        /// </summary>
        /// <param name="newState">The </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(int newState)
        {
            base.Value = newState;
        }

        /// <summary>
        /// Exit state
        /// </summary>
        /// <param name="nextState">The state we are exiting to</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IoStateTransition<TState> Exit(IoStateTransition<TState> nextState)
        {
            if (Value.Equals(FinalState))
                throw new ApplicationException($"Cannot transition from `{FinalState}' to `{nextState}'");

            ExitTime = Environment.TickCount;
            base.Next = nextState;
            if(nextState != null)
                nextState.Prev = this;

            return nextState;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IoStateTransition<TState> GetStartState()
        {
            var c = this;
            while (c.Prev != null)
            {
                c = c.Prev;
            }

            return c;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TState CompareAndEnterState(int state, int cmp)
        {
            return (TState)Enum.ToObject(typeof(TState), Interlocked.CompareExchange(ref base.Value, state, cmp));
        }

        public ValueTask<IIoHeapItem> HeapPopAsync(object context)
        {
            return new ValueTask<IIoHeapItem>(this);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IIoHeapItem> HeapConstructAsync(object context)
        {
            ExitTime = EnterTime = Environment.TickCount;
            base.Next = null;
            base.Prev = null;
            return new ValueTask<IIoHeapItem>(this);
        }
    }
}
