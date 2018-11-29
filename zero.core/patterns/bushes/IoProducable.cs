using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.heap;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Produces work that needs to be done
    /// </summary>
    /// <typeparam name="TJob">The job type</typeparam>
    /// <typeparam name="TFJob">The job forward type</typeparam>
    public abstract class IoProducable<TJob> : IoConfigurable, IIoWorker
        where TJob : IIoWorker
        
    {
        /// <summary>
        /// Constructor
        /// </summary>
        protected IoProducable()
        {
            _stateTransitionHeap.Make = (jobData) => new IoWorkStateTransition<TJob>();
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
            ProduceSkipped,
            Queued,
            Consuming,
            Forwarded,
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
        public virtual string ProductionDescription => WorkDescription;

        /// <summary>
        /// The ultimate source of workload
        /// </summary>
        public virtual IoProducer<TJob> ProducerHandle { get; protected set; }

        /// <summary>
        /// The heap containing our state transition items
        /// </summary>
        private readonly IoHeap<IoWorkStateTransition<TJob>> _stateTransitionHeap = new IoHeapIo<IoWorkStateTransition<TJob>>(Enum.GetNames(typeof(State)).Length);

        /// <summary>
        /// The state transition history, sourced from <see  cref="IoProducerConsumer{TConsumer,TProducer}"/>
        /// </summary>      
        public readonly IoWorkStateTransition<TJob>[] StateTransitionHistory = new IoWorkStateTransition<TJob>[Enum.GetNames(typeof(State)).Length];

        /// <summary>
        /// The current state
        /// </summary>
        public volatile IoWorkStateTransition<TJob> CurrentState;

        /// <summary>
        /// Indicates that this job contains unprocessed fragments
        /// </summary>
        public bool StillHasUnprocessedFragments { get; set; }

        /// <summary>
        /// Callback the generates the next job
        /// </summary>
        /// <returns>The state to indicated failure or success</returns>
        public abstract Task<State> ProduceAsync(IoProducable<TJob> fragment);

        /// <summary>
        /// Update state history
        /// </summary>
        /// <param name="value">The new state value</param>
        public void UpdateStateTransitionHistory(State value)
        {
            //Update the previous state's exit time
            if (CurrentState != null)
            {
                CurrentState.ExitTime = DateTime.Now;

                Interlocked.Increment(ref ProducerHandle.Counters[(int)CurrentState.State]);
                Interlocked.Add(ref ProducerHandle.ServiceTimes[(int)CurrentState.State], (long)(CurrentState.Mu.TotalMilliseconds));
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

            // We need a lock if this job contained fragments because there is a race condition of ownership between 
            // the producer and the consumer as to who returns this job to the heap. If the consumer has consumed this
            // job it is the producers responsibility to return this job to the heap because it still needs to
            // move the fragments to a future job. If the consumer still needs to  process this job then the consumer
            // needs to return this job to the heap. Who exactly has control is determined by the StillHasUnprocessedFragments
            // state. The producer will set this value to false when it has moved the datum fragments to another job so that
            // it can be processed
            if (StillHasUnprocessedFragments && value == State.Consumed)
            {
                lock (this)
                    CurrentState.State = value;
            }
            else
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
        public virtual IIoHeapItem Constructor()
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
            StillHasUnprocessedFragments = false;

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
        public void PrintState(IoWorkStateTransition<TJob> currentState)
        {
            _logger.Info("Work`{0}',[{1} {2}], [{3} ||{4}||], [{5} ({6})]",
                ProductionDescription,
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

                //generate a unique id
                if (value == State.Undefined)
                {
                    Id = Interlocked.Read(ref ProducerHandle.Counters[(int)State.Undefined]); //TODO why do we need this again?
                }

                if (value == State.Accept || value == State.Reject)
                {
                    ProcessState = State.Finished;
                }
            }
        }

        /// <summary>
        /// Set unprocessed data as more fragments.
        /// </summary>
        public abstract void MoveUnprocessedToFragment();

        public bool Reconfigure { get; }
    }
}
