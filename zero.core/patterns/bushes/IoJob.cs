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
    /// Meta data about produced work that needs to be done
    /// </summary>
    /// <typeparam name="TJob">The job type</typeparam>
    public abstract class IoJob<TJob> : IoConfigurable, IIoJob
        where TJob : IIoJob
    {
        /// <summary>
        /// Constructor
        /// </summary>
        protected IoJob(string description, IoSource<TJob> source)
        {            
            _logger = LogManager.GetCurrentClassLogger();
            source.ZeroOnCascade(this);
            Source = source;
            _jobDescription = description;
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
        /// Work spanning multiple jobs
        /// </summary>
        public IIoJob Previous { get; set; }

        /// <summary>
        /// Respective States as the work goes through the source consumer pattern
        /// </summary>
        public enum State
        {
            Undefined,
            Producing,
            Produced,
            ProStarting,
            Queued,
            Dequeued,
            Consuming,            
            Consumed,
            ConInlined,
            Error,
            Accept,
            Reject,
            Finished,
            Syncing,
            RSync,
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
            Oom,
            Zeroed
        }

        private string _description;
        /// <summary>
        /// A description of this kind of work
        /// </summary>
        public override string Description
        {
            get
            {
                if(_description == null) 
                    return _description = $"{Source.Description} | {_jobDescription}";
                return _description;
            }
        }

        /// <summary>
        /// A description of the job and work
        /// </summary>
        public virtual string TraceDescription => $"{Description}|#{Id} -";

        /// <summary>
        /// The ultimate source of workload
        /// </summary>
        public IIoSource Source { get; protected set; }

        /// <summary>
        /// The state transition history, sourced from <see  cref="IoZero{TJob}"/>
        /// </summary>      
        public IoWorkStateTransition<TJob>[] StateTransitionHistory = new IoWorkStateTransition<TJob>[Enum.GetNames(typeof(State)).Length];//TODO what should this size be?

        /// <summary>
        /// The current state
        /// </summary>
        public volatile IoWorkStateTransition<TJob> CurrentState;

        /// <summary>
        /// Indicates that this job contains unprocessed fragments
        /// </summary>
        public bool StillHasUnprocessedFragments { get; protected set; }

        /// <summary>
        /// Callback the generates the next job
        /// </summary>
        /// <returns>The state to indicated failure or success</returns>
        public abstract Task<State> ProduceAsync();
        
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
        /// zero unmanaged
        /// </summary>
        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            //CurrentState = null;
            //StateTransitionHistory = null;
            Source = null;
            Previous = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override void ZeroManaged()
        {
            base.ZeroManaged();
            Previous?.Zero(this);
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
        /// A description of this job
        /// </summary>
        private readonly string _jobDescription;

        /// <summary>
        /// Gets and sets the state of the work
        /// </summary>
        public State ProcessState
        {
            get => CurrentState.State;
            set
            {
                //Update the previous state's exit time
                if (CurrentState != null)
                {
                    if (CurrentState.State == State.Finished)
                    {
                        //PrintStateHistory();
                        CurrentState.State = State.Reject;
                        throw new ApplicationException($"{TraceDescription} Cannot transition from `{State.Finished}' to `{value}'");
                    }

                    if (CurrentState.State == value)
                    {
                        Interlocked.Increment(ref Source.Counters[(int)CurrentState.State]);
                        return;
                    }
                    
                    CurrentState.ExitTime = DateTime.Now;
                    
                    Interlocked.Increment(ref Source.Counters[(int)CurrentState.State]);
                    Interlocked.Add(ref Source.ServiceTimes[(int)CurrentState.State], (long)(CurrentState.Mu.TotalMilliseconds));
                }
                else
                {
                    if (value != State.Undefined)
                    {
                        //PrintStateHistory();
                        throw new Exception($"{TraceDescription} First state transition history's first transition should be `{State.Undefined}', but is `{value}'");                        
                    }
                }                                
                
                //Allocate memory for a new current state
                var prevState = CurrentState;
                var newState = new IoWorkStateTransition<TJob>
                {
                    Previous = prevState,
                    State = value,
                    EnterTime = DateTime.Now,
                    ExitTime = DateTime.Now
                };

                CurrentState = newState;

                //Configure the current state
                if (prevState != null)
                {                    
                    prevState.Next = CurrentState;                    
                    StateTransitionHistory[(int)prevState.State] = CurrentState;
                }
                
                //generate a unique id
                if (value == State.Undefined)
                {
                    Id = Interlocked.Read(ref Source.Counters[(int)State.Undefined]);
                }

                //terminate
                if (value == State.Accept || value == State.Reject)
                {                    
                    ProcessState = State.Finished;
                }                
            }
        }        
    }
}
