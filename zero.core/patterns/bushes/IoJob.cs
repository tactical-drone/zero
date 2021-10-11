using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Meta data about produced work that needs to be done
    /// </summary>
    /// <typeparam name="TJob">The job type</typeparam>
    public abstract class IoJob<TJob> : IoNanoprobe, IIoJob
        where TJob : IIoJob
    {
        /// <summary>
        /// static constructor
        /// </summary>
        static IoJob()
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        protected IoJob()
        {
            
        }
        
        /// <summary>
        /// Constructor
        /// </summary>
        protected IoJob(string desc, IoSource<TJob> source, int concurrencyLevel = 1) : base($"{nameof(IoJob<TJob>)}", concurrencyLevel)
        {
            Source = source;
            _jobDesc = desc;
        }

        /// <summary>
        /// logger
        /// </summary>
        protected static readonly Logger _logger;

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
                    return _description = $"{Source?.Description} | {_jobDesc}";
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
        public bool Syncing { get; protected set; }

        /// <summary>
        /// Uses <see cref="Source"/> to produce a job
        /// </summary>
        /// <param name="barrier">The normalized barrier that we pass to the source for quick release</param>
        /// <param name="nanite">Adds closure manually</param>
        /// <returns>The current state of the job</returns>
        public abstract ValueTask<IoJobMeta.JobState> ProduceAsync<T>(Func<IIoJob, T, ValueTask<bool>> barrier,T nanite);
        
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
                    await _stateHeap.ReturnAsync(r).FastPath().ConfigureAwait(false);
                }
            }

            if (_stateMeta != null)
            {
                await _stateHeap.ReturnAsync(_stateMeta).FastPath().ConfigureAwait(false);
                _stateMeta = null;
            }
            Array.Clear(StateTransitionHistory, 0, StateTransitionHistory.Length);
#else
            _stateMeta.Value = IoJobMeta.JobState.Undefined;
#endif
            FinalState = State = IoJobMeta.JobState.Undefined;
            Syncing = false;
            Id = Interlocked.Read(ref Source.Counters[(int)IoJobMeta.JobState.Undefined]);

            //var curState = 0;
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
        public override void ZeroUnmanaged()
        {
#if DEBUG
            _stateHeap.ZeroUnmanaged();
#endif

            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _stateMeta = null;
            StateTransitionHistory = null;
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
        public override async ValueTask ZeroManagedAsync()
        {
#if DEBUG
            if(_stateMeta != null)
                await _stateHeap.ReturnAsync(_stateMeta).FastPath().ConfigureAwait(false);
            Array.Clear(StateTransitionHistory, 0, StateTransitionHistory.Length);
            await _stateHeap.ZeroManagedAsync<object>().FastPath().ConfigureAwait(false);
#endif
            if (PreviousJob != null)
                await PreviousJob.ZeroAsync(this).FastPath().ConfigureAwait(false);

            await base.ZeroManagedAsync().FastPath().ConfigureAwait(false);
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
                var c = curState;
                while (c.Repeat != null)
                {
                    c = c.Repeat;
                    PrintState(c, true);
                }
                
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
        public void PrintState(IoStateTransition<IoJobMeta.JobState> _stateMeta, bool isRepeat = false)
        {
            if (isRepeat)
            {
                _logger.Fatal("Production:{0} `{1}',[{2} {3}], [{4} ||{5}||], [{6} ({7})]",
                    _stateMeta.EnterTime.Ticks,
                    Description,
                    
                    _stateMeta.Previous == null ? _stateMeta.DefaultPadded : _stateMeta.Previous.PaddedStr(),
                    (_stateMeta.Lambda.TotalMilliseconds.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),
                    
                    _stateMeta.PaddedStr(),(_stateMeta.Mu.TotalMilliseconds.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),
                    
                    _stateMeta.Next == null ? _stateMeta.DefaultPadded : _stateMeta.Next.PaddedStr(),
                    (_stateMeta.Delta.TotalMilliseconds.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size));
            }
            else
            {
                _logger.Error("Production:{0} `{1}',[{2} {3}], [{4} ||{5}||], [{6} ({7})]",
                    _stateMeta.EnterTime.Ticks,
                    Description,
                    
                    _stateMeta.Previous == null ? _stateMeta.DefaultPadded : _stateMeta.Previous.PaddedStr(),
                    (_stateMeta.Lambda.TotalMilliseconds.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),
                    
                    _stateMeta.PaddedStr(),(_stateMeta.Mu.TotalMilliseconds.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),
                    
                    _stateMeta.Next == null ? _stateMeta.DefaultPadded : _stateMeta.Next.PaddedStr(),
                    (_stateMeta.Delta.TotalMilliseconds.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size));
            }
        }

        /// <summary>
        /// The total amount of states
        /// </summary>
        public static readonly int StateMapSize = Enum.GetNames(typeof(IoJobMeta.JobState)).Length;

        /// <summary>
        /// A description of this job
        /// </summary>
        private readonly string _jobDesc;


        /// <summary>
        /// state heap
        /// </summary>
#if DEBUG
        //TODO
        private IoHeap<IoStateTransition<IoJobMeta.JobState>> _stateHeap = new((uint)(Enum.GetNames(typeof(IoJobMeta.JobState)).Length  * 2),5) { Make = o => new IoStateTransition<IoJobMeta.JobState>(){FinalState = IoJobMeta.JobState.Halted} };
#endif
        /// <summary>
        /// Final state
        /// </summary>
        public IoJobMeta.JobState FinalState { get; set; }

        /// <summary>
        /// Gets and sets the state of the work
        /// </summary>
        public IoJobMeta.JobState State
        {
            get
            {
                try
                {
                    return _stateMeta.Value;
                }
                catch (Exception)
                {
                    return IoJobMeta.JobState.Zeroed;
                }   
            }
            set
            {
                if(Zeroed())
                    return;
                
                //Update the previous state's exit time
                if (_stateMeta != null)
                {
                    _stateMeta.ExitTime = DateTime.UtcNow;
                    
                    if (_stateMeta.Value == IoJobMeta.JobState.Halted)
                    {
                        PrintStateHistory();
                        _stateMeta.Value = IoJobMeta.JobState.Race; //TODO
                        throw new ApplicationException($"{TraceDescription} Cannot transition from `{IoJobMeta.JobState.Halted}' to `{value}'");
                    }

                    if (_stateMeta.Value == value)
                    {
                        Interlocked.Increment(ref Source.Counters[(int)_stateMeta.Value]);
                        return;
                    }
                    
                    Interlocked.Increment(ref Source.Counters[(int)_stateMeta.Value]);
                    Interlocked.Add(ref Source.ServiceTimes[(int)_stateMeta.Value], (long)(_stateMeta.Mu.TotalMilliseconds));
                }
#if DEBUG
                else
                {
                    if (value != IoJobMeta.JobState.Undefined && !Zeroed())
                    {
                        PrintStateHistory();
                        throw new Exception($"{TraceDescription} First state transition history's first transition should be `{IoJobMeta.JobState.Undefined}', but is `{value}'");                        
                    }
                }
#endif

#if DEBUG
                //Allocate memory for a new current state
                var prevState = _stateMeta;
                var newState = _stateHeap.TakeAsync().AsTask().GetAwaiter().GetResult();
                if (newState == null)
                {
                    if (!Zeroed())
                        throw new OutOfMemoryException($"{Description}");

                    return;
                }

                newState.Previous = _stateMeta;
                newState.EnterTime = DateTime.UtcNow;
                newState.ExitTime = DateTime.UtcNow;
                newState.Value = value;
                
                _stateMeta = newState;
                
                //Configure the current state
                if (prevState != null)
                {
                    prevState.Next = _stateMeta;
                    if (StateTransitionHistory[(int)prevState.Value] != null)
                    {
                        var c = StateTransitionHistory[(int)prevState.Value];
                        while (c.Repeat != null)
                            c = c.Repeat;
                        
                        c.Repeat = prevState;
                    }
                    else
                        StateTransitionHistory[(int)prevState.Value] = prevState;
                }
#else
                _stateMeta.Value = value;
                _stateMeta.EnterTime = DateTime.UtcNow;
                _stateMeta.ExitTime = DateTime.UtcNow;
#endif
                //generate a unique id
                if (value == IoJobMeta.JobState.Undefined)
                    Id = Interlocked.Read(ref Source.Counters[(int)IoJobMeta.JobState.Undefined]);
                
                //terminate
                if (value == IoJobMeta.JobState.Accept || value == IoJobMeta.JobState.Reject)
                {
                    FinalState = State;
                    State = IoJobMeta.JobState.Halted;
                }                
            }
        }        
    }
}