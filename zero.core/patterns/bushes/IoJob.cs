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
            var p = Description;
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
        public IIoJob PreviousJob { get; set; }

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
#if DEBUG
        public IoWorkStateTransition<TJob>[] StateTransitionHistory = new IoWorkStateTransition<TJob>[Enum.GetNames(typeof(IoJobMeta.JobState)).Length];//TODO what should this size be?
#else 
        public IoWorkStateTransition<TJob>[] StateTransitionHistory;
#endif


        /// <summary>
        /// The current state
        /// </summary>
#if DEBUG
        private volatile IoWorkStateTransition<TJob> _stateMeta;
#else
        private volatile IoWorkStateTransition<TJob> _stateMeta = new IoWorkStateTransition<TJob>();
#endif


        /// <summary>
        /// Indicates that this job contains unprocessed fragments
        /// </summary>
        public bool StillHasUnprocessedFragments { get; protected set; }

        /// <summary>
        /// Uses <see cref="Source"/> to produce a job
        /// </summary>
        /// <param name="barrier">The normalized barrier that we pass to the source for quick release</param>
        /// <param name="zeroClosure">Adds closure manually</param>
        /// <returns>The current state of the job</returns>
        public abstract Task<IoJobMeta.JobState> ProduceAsync(Func<IIoJob, IIoZero, ValueTask<bool>> barrier, IIoZero zeroClosure);
        
        /// <summary>
        /// Initializes this instance for reuse from the heap
        /// </summary>
        /// <returns>This instance</returns>
        public virtual IIoHeapItem Constructor()
        {
#if DEBUG
            if (_stateMeta?.Previous != null)
                _stateHeap.ReturnAsync(_stateMeta.Previous).ConfigureAwait(false).GetAwaiter().GetResult();

            _stateMeta = null;
#else
            _stateMeta.JobState = IoJobMeta.JobState.Undefined;
            Id = Interlocked.Read(ref Source.Counters[(int)IoJobMeta.JobState.Undefined]);
#endif

            State = IoJobMeta.JobState.Undefined;
            StillHasUnprocessedFragments = false;

            //var curState = 0;
#if DEBUG
            Array.Clear(StateTransitionHistory, 0, StateTransitionHistory.Length);
#endif
            //while (StateTransitionHistory[curState] != null)
            //{
            //    var prevState = curState;
            //    curState = (int) StateTransitionHistory[curState].JobState;
            //    StateTransitionHistory[prevState] = null;
            //}

            return this;
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        protected override void ZeroUnmanaged()
        {
#if DEBUG
            _stateHeap.Dispose();
#endif

            base.ZeroUnmanaged();

#if SAFE_RELEASE
            //_stateMeta = null;
            //StateTransitionHistory = null;
            Source = null;
            PreviousJob = null;
#if DEBUG
            _stateHeap = null;
#endif
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override async Task ZeroManagedAsync()
        {
#if DEBUG
            await _stateHeap.ReturnAsync(_stateMeta);
            await _stateHeap.ZeroAsync(this); //TODO
#endif
            await base.ZeroManagedAsync();
            if(PreviousJob != null)
                await PreviousJob.ZeroAsync(this).ConfigureAwait(false);
        }

        /// <summary>
        /// Print the current state
        /// </summary>
        public void Print_stateMeta()
        {
            PrintState(_stateMeta);
        }

        /// <summary>
        /// Print the state transition history for this work
        /// </summary>
        public void PrintStateHistory()
        {
#if !DEBUG
            return;
#else
            var curState = StateTransitionHistory[0];

            while (curState != null)
            {
                PrintState(curState);
                curState = curState.Next;
            }
#endif
        }

        /// <summary>
        /// Log formatting param that pads job ID strings
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_id_pad_size = 12;


        /// <summary>
        /// How long to wait for the consumer before timing out
        /// </summary>
        public virtual int WaitForConsumerTimeout { get; } = 2500;

        /// <summary>
        /// Log the state
        /// </summary>
        /// <param name="_stateMeta">The instance to be printed</param>
        public void PrintState(IoWorkStateTransition<TJob> _stateMeta)
        {
            _logger.Info("Production: `{0}',[{1} {2}], [{3} ||{4}||], [{5} ({6})]",
                Description,
                (_stateMeta.Previous == null ? _stateMeta.DefaultPadded : _stateMeta.Previous.PaddedStr()),
                (_stateMeta.Lambda.TotalMilliseconds.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),
                _stateMeta.PaddedStr(),
                (_stateMeta.Mu.TotalMilliseconds.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),
                (_stateMeta.Next == null ? _stateMeta.DefaultPadded : _stateMeta.Next.PaddedStr()),
                (_stateMeta.Delta.TotalMilliseconds.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size));
        }

        /// <summary>
        /// The total amount of states
        /// </summary>
        public static readonly int StateMapSize = Enum.GetNames(typeof(IoJobMeta.JobState)).Length;

        /// <summary>
        /// A description of this job
        /// </summary>
        private readonly string _jobDescription;


        /// <summary>
        /// state heap
        /// </summary>
#if DEBUG        
        private IoHeapIo<IoWorkStateTransition<TJob>> _stateHeap = new IoHeapIo<IoWorkStateTransition<TJob>>(64) { Make = o => new IoWorkStateTransition<TJob>() };
#endif

        /// <summary>
        /// Gets and sets the state of the work
        /// </summary>
        public IoJobMeta.JobState State
        {
            get => _stateMeta.JobState;
            set
            {
                if(Source?.Zeroed()??true)
                    return;

                //Update the previous state's exit time
                if (_stateMeta != null)
                {
                    if (_stateMeta.JobState == IoJobMeta.JobState.Finished)
                    {
                        //PrintStateHistory();
                        _stateMeta.JobState = IoJobMeta.JobState.Race; //TODO
                        throw new ApplicationException($"{TraceDescription} Cannot transition from `{IoJobMeta.JobState.Finished}' to `{value}'");
                    }

                    if (_stateMeta.JobState == value)
                    {
                        Interlocked.Increment(ref Source.Counters[(int)_stateMeta.JobState]);
                        return;
                    }
                    
                    _stateMeta.ExitTime = DateTime.Now;
                    
                    Interlocked.Increment(ref Source.Counters[(int)_stateMeta.JobState]);
                    Interlocked.Add(ref Source.ServiceTimes[(int)_stateMeta.JobState], (long)(_stateMeta.Mu.TotalMilliseconds));
                }
                else
                {
                    if (value != IoJobMeta.JobState.Undefined)
                    {
                        //PrintStateHistory();
                        throw new Exception($"{TraceDescription} First state transition history's first transition should be `{IoJobMeta.JobState.Undefined}', but is `{value}'");                        
                    }
                }

#if DEBUG
                //Allocate memory for a new current state
                var prevState = _stateMeta;

                var newState = _stateHeap.TakeAsync((transition, closure) =>
                {
                    transition.Previous = ((IoJob<TJob>) closure)._stateMeta;
                    transition.EnterTime = DateTime.Now;
                    transition.ExitTime = DateTime.Now;

                    return transition;
                }, this).GetAwaiter().GetResult();

                newState.JobState = value;

                //var newState = new IoWorkStateTransition<TJob>
                //{
                //    Previous = prevState,
                //    JobState = value,
                //    EnterTime = DateTime.Now,
                //    ExitTime = DateTime.Now
                //};

                _stateMeta = newState;
                
                //Configure the current state
                if (prevState != null)
                {
                    prevState.Next = _stateMeta;

                    StateTransitionHistory[(int)prevState.JobState] = _stateMeta;
                }
#else
                _stateMeta.JobState = value;
                _stateMeta.EnterTime = DateTime.Now;
                _stateMeta.ExitTime = DateTime.Now;
#endif
                //generate a unique id
                if (value == IoJobMeta.JobState.Undefined)
                {
                    Id = Interlocked.Read(ref Source.Counters[(int)IoJobMeta.JobState.Undefined]);
                }

                //terminate
                if (value == IoJobMeta.JobState.Accept || value == IoJobMeta.JobState.Reject)
                {                    
                    State = IoJobMeta.JobState.Finished;
                }                
            }
        }        
    }
}
