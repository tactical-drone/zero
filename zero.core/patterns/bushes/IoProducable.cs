using System;
using System.ComponentModel;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.misc;
using NLog;
using zero.core.conf;
using zero.core.patterns.heap;

namespace zero.core.patterns.bushes
{
    /// <inheritdoc />
    /// <summary>
    /// Produces work that needs to be done
    /// </summary>
    /// <typeparam name="TSource"></typeparam>
    public abstract class IoProducable<TSource> : IoConfigurable, IOHeapItem
    where TSource : IoJobSource
    {
        /// <summary>
        /// Constructor
        /// </summary>
        protected IoProducable()
        {
            _stateTransitionHeap.Make = () => new IoWorkStateTransition<TSource>();
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// A unique id for this work
        /// </summary>
        public long Id { get; private set; }

        /// <summary>
        /// Respective States as the work goes through the producer consumer pattern
        /// </summary>
        public enum State
        {
            Undefined,
            Producing,
            Produced,
            Fragmented,
            Queued,
            Consuming,
            ConsumerSkipped,            
            Consumed,
            Accept,
            Reject,            
            Finished,            
            Error,
            ProduceErr,
            ConsumeErr,
            ConsumeCancelled,
            ProduceCancelled,            
            ConsumeTo,
            ProduceTo,
            Cancelled,
            Timeout,
            Oom
        }

        /// <summary>
        /// A description of this kind of work
        /// </summary>
        public string WorkDescription;

        /// <summary>
        /// A description of the job and work
        /// </summary>
        public virtual string Description => WorkDescription;

        /// <summary>
        /// The ultimate source of workload
        /// </summary>
        public TSource Source;

        /// <summary>
        /// Callback that processes the next job in the queue
        /// </summary>
        public abstract Task<State> ConsumeAsync();

        /// <summary>
        /// Callback the generates the next job
        /// </summary>
        /// <returns>The state to indicated failure or success</returns>
        public abstract Task<State> ProduceAsync(IoProducable<TSource> fragment);

        /// <summary>
        /// The heap containing our state transition items
        /// </summary>
        private readonly IoHeap<IoWorkStateTransition<TSource>> _stateTransitionHeap = new IoHeapIo<IoWorkStateTransition<TSource>>(Enum.GetNames(typeof(State)).Length);
        
        /// <summary>
        /// The state transtition history, sourced from <see  cref="IoProducerConsumer{TConsumer,TSource}"/>
        /// </summary>      
        public readonly IoWorkStateTransition<TSource>[] StateTransitionHistory = new IoWorkStateTransition<TSource>[Enum.GetNames(typeof(State)).Length];

        /// <summary>
        /// The current state
        /// </summary>
        public volatile IoWorkStateTransition<TSource> CurrentState;

        /// <summary>
        /// Update state history
        /// </summary>
        /// <param name="value">State state</param>
        private void UpdateStateTransitionHistory(State value)
        {
            //Update the previous state's exit time
            if (CurrentState != null)
            {
                CurrentState.ExitTime = DateTime.Now;

                Interlocked.Increment(ref Source.Counters[(int)CurrentState.State]);
                Interlocked.Add(ref Source.ServiceTimes[(int)CurrentState.State], (long)(CurrentState.Mu.TotalMilliseconds));
            }

            //Allocate memory for a new current state
            var prevState = CurrentState;
            CurrentState = _stateTransitionHeap.Take();

            //TODO 
            if (CurrentState == null)
            {
                if (prevState != null)
                {
                    prevState.State = State.Oom;
                    CurrentState = prevState;
                }
                    
                return;
            }
                
            //Configure the current state
            CurrentState.Previous = prevState;

            if (prevState != null)
                prevState.Next = CurrentState;

            CurrentState.State = value;

            //Timestamps
            CurrentState.EnterTime = CurrentState.ExitTime = DateTime.Now;

            //Update the state transition history
            StateTransitionHistory[(int)ProcessState] = CurrentState;
        }

        /// <summary>
        /// Initializes this instance for reuse from the heap
        /// </summary>
        /// <returns>This instance</returns>
        public virtual IOHeapItem Constructor()
        {            
            //clear the states from the previous run
            var cur = StateTransitionHistory[0];
            while (cur != null)
            {
                _stateTransitionHeap.Return(cur);
                cur = cur.Next;
            }

            CurrentState = null;

            ProcessState = State.Undefined;

            return this;
        }

        /// <summary>
        /// Print the current state
        /// </summary>
        public void PrintCurrentState()
        {
            PrintState(CurrentState);
        }

        /// <summary>
        /// Print the state transition history for this work
        /// </summary>
        /// <param name="reverse"></param>
        public void PrintStateHistory()
        {
            var curState = StateTransitionHistory[0];

            while (curState != null)
            {
                PrintState(curState);
                curState = curState.Next;
            }
        }

        /// <summary>
        /// Log formatting parm that pads job ID strings
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_id_pad_size = 14;

        /// <summary>
        /// Log the state
        /// </summary>
        /// <param name="currentState">The instance to be printed</param>
        private void PrintState(IoWorkStateTransition<TSource> currentState)
        {
            _logger.Info("Work`{0}',[{1} {2}], [{3} ||{4}||], [{5} ({6})]",
                Description,
                (currentState.Previous == null ? CurrentState.DefaultPadded : currentState.Previous.PaddedStr()),
                (currentState.Lambda.TotalMilliseconds.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),
                currentState.PaddedStr(),
                (currentState.Mu.TotalMilliseconds.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),
                (currentState.Next == null ? CurrentState.DefaultPadded : currentState.Next.PaddedStr()),
                (currentState.Delta.TotalMilliseconds.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size));
        }

        /// <summary>
        /// The total amount of states
        /// </summary>
        public static readonly int StateMapSize = Enum.GetNames(typeof(State)).Length;
        
        /// <summary>
        /// Gets and sets the state of the work
        /// </summary>
        public State ProcessState
        {
            get => CurrentState.State;
            set
            {
                //Update timestamps
                UpdateStateTransitionHistory(value);

                //generate a uinique id
                if (value == State.Undefined)
                {
                    Id = Interlocked.Read(ref Source.Counters[(int)State.Undefined]); //TODO why do we need this again?
                }

                if (value == State.Accept || value == State.Reject)
                {
                    ProcessState = State.Finished;
                }
            }
        }
    }
}
