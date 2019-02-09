﻿using System;
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
    /// Meta data about produced work that needs to be done
    /// </summary>
    /// <typeparam name="TJob">The job type</typeparam>
    public abstract class IoProducible<TJob> : IoConfigurable, IIoWorker
        where TJob : IIoWorker
        
    {
        /// <summary>
        /// Constructor
        /// </summary>
        protected IoProducible(string workDescription, IoProducer<TJob> producer)
        {            
            _logger = LogManager.GetCurrentClassLogger();
            Producer = producer;
            WorkDescription = workDescription;
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// A unique id for this work
        /// </summary>
        public long Id { get; private set; }

        public volatile IoProducible<TJob> Previous;

        /// <summary>
        /// Respective States as the work goes through the producer consumer pattern
        /// </summary>
        public enum State
        {
            Undefined,
            Producing,
            Produced,
            ProStarting,
            Queued,
            Consuming,
            Forwarded,            
            Consumed,
            ConInlined,
            Accept,
            Reject,
            Finished,            
            Error,
            Syncing,
            ProduceErr,
            ConsumeErr,
            DbError,
            ConInvalid,
            NoPow,
            FastDup,
            SlowDup,
            ConCancel,
            ProdCancel,
            ConsumeTo,
            ProduceTo,
            Cancelled,
            Timeout,
            Oom
        }

        /// <summary>
        /// A description of this kind of work
        /// </summary>
        public string WorkDescription { get; protected set; }

        /// <summary>
        /// A description of the job and work
        /// </summary>
        public virtual string ProductionDescription => WorkDescription;

        /// <summary>
        /// The ultimate source of workload
        /// </summary>
        public IoProducer<TJob> Producer { get; }

        /// <summary>
        /// The state transition history, sourced from <see  cref="IoProducerConsumer{TJob}"/>
        /// </summary>      
        public readonly IoWorkStateTransition<TJob>[] StateTransitionHistory = new IoWorkStateTransition<TJob>[Enum.GetNames(typeof(State)).Length];//TODO what should this size be?

        /// <summary>
        /// The current state
        /// </summary>
        public volatile IoWorkStateTransition<TJob> CurrentState;

        /// <summary>
        /// Indicates that this job contains unprocessed fragments
        /// </summary>
        public volatile bool StillHasUnprocessedFragments;

        /// <summary>
        /// Callback the generates the next job
        /// </summary>
        /// <returns>The state to indicated failure or success</returns>
        public abstract Task<State> ProduceAsync();

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

                Interlocked.Increment(ref Producer.Counters[(int)CurrentState.State]);
                Interlocked.Add(ref Producer.ServiceTimes[(int)CurrentState.State], (long)(CurrentState.Mu.TotalMilliseconds));                
            }
            else
            {
                if (value != State.Producing && value != State.Undefined)
                {
                    _logger.Fatal($"({Id}) Fatal error: First state transition history's first transition should be `{State.Producing}', but is `{value}'");
                    PrintStateHistory();
                }
            }

            //Allocate memory for a new current state
            var prevState = CurrentState;
            var newState = new IoWorkStateTransition<TJob> { Previous = prevState };
            CurrentState = newState;

            //Configure the current state
            if (prevState != null)
                prevState.Next = CurrentState;
            
            CurrentState.State = value;

            //Timestamps
            CurrentState.EnterTime = CurrentState.ExitTime = DateTime.Now;

            //Update the state transition history
            if(prevState != null)
                StateTransitionHistory[(int)prevState.State] = newState;
        }

        /// <summary>
        /// Initializes this instance for reuse from the heap
        /// </summary>
        /// <returns>This instance</returns>
        public virtual IIoHeapItem Constructor()
        {            
            CurrentState = null;

            ProcessState = State.Undefined;
            StillHasUnprocessedFragments = false;

            var curState = 0;
            while (StateTransitionHistory[curState] != null)
            {
                var prevState = curState;
                curState = (int) StateTransitionHistory[curState].State;
                StateTransitionHistory[prevState] = null;
            }

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
        /// Log formatting param that pads job ID strings
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_id_pad_size = 12;        

        /// <summary>
        /// Log the state
        /// </summary>
        /// <param name="currentState">The instance to be printed</param>
        public void PrintState(IoWorkStateTransition<TJob> currentState)
        {
            _logger.Info("Production: `{0}',[{1} {2}], [{3} ||{4}||], [{5} ({6})]",
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
                if (CurrentState?.State == value)
                {
                    Interlocked.Increment(ref Producer.Counters[(int)CurrentState.State]);
                    return;
                }
                    
                //Update timestamps
                UpdateStateTransitionHistory(value);

                //generate a unique id
                if (value == State.Undefined)
                {
                    Id = Interlocked.Read(ref Producer.Counters[(int)State.Undefined]);
                }

                if (value == State.Accept || value == State.Reject)
                {
                    ProcessState = State.Finished;
                }
            }
        }        
    }
}