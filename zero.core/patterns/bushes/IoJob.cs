using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.Threading;
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
        public IoStateTransition<IoJobMeta.JobState>[] StateTransitionHistory = new IoStateTransition<IoJobMeta.JobState>[Enum.GetNames(typeof(IoJobMeta.JobState)).Length];//TODO what should this size be?
#else
        public IoStateTransition<IoJobMeta.JobState>[] StateTransitionHistory;
#endif


        /// <summary>
        /// The current state
        /// </summary>
#if DEBUG
        private volatile IoStateTransition<IoJobMeta.JobState> _stateMeta;
#else
        private volatile IoStateTransition<IoJobMeta.JobState> _stateMeta = new IoStateTransition<IoJobMeta.JobState>();
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
        public virtual async ValueTask<IIoHeapItem> ConstructorAsync()
        {
#if DEBUG
            foreach (var ioWorkStateTransition in StateTransitionHistory)
            {
                var c = ioWorkStateTransition;
                while (c != null)
                {
                    var r = c;
                    c = c.Repeat;
                    await _stateHeap.ReturnAsync(r).ConfigureAwait(false);
                }
            }


            if (_stateMeta != null)
            {
                await _stateHeap.ReturnAsync(_stateMeta).ConfigureAwait(false);
                _stateMeta = null;
            }

#else
            _stateMeta.Value = IoJobMeta.JobState.Undefined;
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
            //    curState = (int) StateTransitionHistory[curState].CurrentState;
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
            await _stateHeap.ReturnAsync(_stateMeta).ConfigureAwait(false);
            Array.Clear(StateTransitionHistory, 0, StateTransitionHistory.Length);
            await _stateHeap.ZeroAsync(this).ConfigureAwait(false); //TODO
#endif
            await base.ZeroManagedAsync().ConfigureAwait(false);
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
        public void PrintState(IoStateTransition<IoJobMeta.JobState> _stateMeta)
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
        //TODO
        private IoHeapIo<IoStateTransition<IoJobMeta.JobState>> _stateHeap = new IoHeapIo<IoStateTransition<IoJobMeta.JobState>>(Enum.GetNames(typeof(IoJobMeta.JobState)).Length) { Make = o => new IoStateTransition<IoJobMeta.JobState>(){FinalState = IoJobMeta.JobState.Finished} };
#endif

        /// <summary>
        /// Gets and sets the state of the work
        /// </summary>
        public IoJobMeta.JobState State
        {
            get => _stateMeta.Value;
            set
            {
                if(Source?.Zeroed()??true)
                    return;

                //Update the previous state's exit time
                if (_stateMeta != null)
                {
                    if (_stateMeta.Value == IoJobMeta.JobState.Finished)
                    {
                        //PrintStateHistory();
                        _stateMeta.Value = IoJobMeta.JobState.Race; //TODO
                        throw new ApplicationException($"{TraceDescription} Cannot transition from `{IoJobMeta.JobState.Finished}' to `{value}'");
                    }

                    if (_stateMeta.Value == value)
                    {
                        Interlocked.Increment(ref Source.Counters[(int)_stateMeta.Value]);
                        return;
                    }
                    
                    _stateMeta.ExitTime = DateTime.Now;
                    
                    Interlocked.Increment(ref Source.Counters[(int)_stateMeta.Value]);
                    Interlocked.Add(ref Source.ServiceTimes[(int)_stateMeta.Value], (long)(_stateMeta.Mu.TotalMilliseconds));
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

                    return new ValueTask<IoStateTransition<IoJobMeta.JobState>>(transition);
                }, this).ConfigureAwait(false).GetAwaiter().GetResult();

                if (newState == null)
                {
                    if(!Zeroed())
                        throw new OutOfMemoryException();

                    return;
                }
                
                newState.Value = value;

                _stateMeta = newState;
                
                //Configure the current state
                if (prevState != null)
                {
                    prevState.Next = _stateMeta;
                    if (StateTransitionHistory[(int) prevState.Value] != null)
                    {
                        StateTransitionHistory[(int)prevState.Value].Repeat = prevState;
                    }
                    else
                        StateTransitionHistory[(int)prevState.Value] = prevState;
                }
#else
                _stateMeta.Value = value;
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
