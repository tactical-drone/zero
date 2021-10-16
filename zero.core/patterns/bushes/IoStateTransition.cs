using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.heap;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Represents a state transition while processing work on a concurrent process
    /// </summary>
    /// <typeparam name="TState">The state enum</typeparam>
    public class IoStateTransition<TState> : IIoHeapItem
        where TState : struct, Enum {
        public IoStateTransition(int initState = 0)
        {
            var list = Enum.GetValues<TState>();

            ConstructorAsync().AsTask().GetAwaiter().GetResult();
            _value = initState;
        }

        //Release all memory held
        public void ZeroManaged()
        {
            _previous = null;
            _next = null;
            
            if (Repeat != null)
                Repeat.ZeroManaged();
            else
                _repeat = null;

            FinalState = default;
        }
        

        /// <summary>
        /// A null state
        /// </summary>
        public static IoStateTransition<TState> NullState = new IoStateTransition<TState>();

        volatile IoStateTransition<TState> _previous;
        /// <summary>
        /// The previous state
        /// </summary>
        public IoStateTransition<TState> Previous => _previous;


        private volatile IoStateTransition<TState> _next;
        /// <summary>
        /// The next state
        /// </summary>
        public IoStateTransition<TState> Next
        {
            get => _next;
            set => _next = value;
        }

        private volatile IoStateTransition<TState> _repeat;
        /// <summary>
        /// A repeat state
        /// </summary>
        public IoStateTransition<TState> Repeat
        {
            get => _repeat;
            set => _repeat = value;
        }

        private volatile int _value;

        /// <summary>
        /// The represented state
        /// </summary>
        public int Value => _value;

        /// <summary>
        /// The represented state
        /// </summary>
        protected int UndefinedState => NullState._value;

        /// <summary>
        /// The represented state
        /// </summary>
        public int FinalState;

        /// <summary>
        /// Timestamped when this state was entered
        /// </summary>
        public long EnterTime;

        /// <summary>
        /// Timestamped when this state was exited
        /// </summary>
        public long ExitTime;

        /// <summary>
        /// The absolute time it took to mechanically transition from the previous state to this state. <see cref="EnterTime"/> - <see cref="Previous"/>.<see cref="EnterTime"/>
        /// </summary>
        public long Lambda => EnterTime - Previous?.EnterTime ?? 0;

        /// <summary>
        /// The time it took between entering this state and exiting it
        /// </summary>
        public long Mu => ExitTime - EnterTime;

        /// <summary>
        /// The absolute time this job took so far
        /// </summary>
        public long Delta => Previous == null ? Mu : Previous.Delta + Mu;

        /// <summary>
        /// Prepares this item for use after popped from the heap
        /// </summary>
        /// <returns>The instance</returns>
        public ValueTask<IIoHeapItem> ConstructorAsync(IoStateTransition<TState> prev, int value = 0)
        {
            ExitTime = EnterTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _next = null;
            _previous = prev;
            if(_previous != null)
                _previous.Next = this;
            _repeat = null;
            _value = value;
            return new ValueTask<IIoHeapItem>(this);
        }

        /// <summary>
        /// default constructor
        /// </summary>
        /// <returns></returns>
        public ValueTask<IIoHeapItem> ConstructorAsync()
        {
            ExitTime = EnterTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _next = null;
            _previous = null;
            _repeat = null;
            _value = 0;
            return new ValueTask<IIoHeapItem>(this);
        }

        /// <summary>
        /// default constructor
        /// </summary>
        /// <returns></returns>
        public ValueTask<IIoHeapItem> ConstructorAsync(int value)
        {
            ExitTime = EnterTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _next = null;
            _previous = null;
            _repeat = null;
            _value = value;
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
            return $"{Previous?.Value}/> {Value} />{Next?.Value}";
        }

        /// <summary>
        /// Enter a state
        /// </summary>
        /// <param name="state"></param>
        /// <param name="cmp"></param>
        /// <param name="prevState"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareAndEnterState(int state, int cmp, IoStateTransition<TState> prevState)
        {
            EnterTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _previous = prevState;
            if (_previous != null)
            {
                _previous._next = this;
                return prevState._value;
            }
            else
                return Interlocked.CompareExchange(ref _value, state, cmp);
        }

        /// <summary>
        /// The default state string padded
        /// </summary>
        public string DefaultPadded => Value.ToString().PadLeft(StateStrPadding);

        /// <summary>
        /// Set the state
        /// </summary>
        /// <param name="value">The </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(int value)
        {
            _value = value;
        }

        /// <summary>
        /// Exit state
        /// </summary>
        /// <param name="nextState">The state we are exiting to</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IoStateTransition<TState> Exit(IoStateTransition<TState> nextState)
        {
            if (Value == FinalState)
                throw new ApplicationException($"Cannot transition from `{FinalState}' to `{nextState}'");

            ExitTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _next = nextState;
            if(nextState != null)
                nextState._previous = this;

            return nextState;
        }
    }
}
