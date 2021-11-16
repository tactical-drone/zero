using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushings
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
#if DEBUG
            _stateHeap = new($"{nameof(_stateHeap)}: {desc}", (Enum.GetNames(typeof(IoJobMeta.JobState)).Length * 2)) { Make = static (o, s) => new IoStateTransition<IoJobMeta.JobState>() { FinalState = (int)IoJobMeta.JobState.Halted } };
#endif
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
        private readonly IoStateTransition<IoJobMeta.JobState> _stateMeta = new();
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

#if DEBUG
        public virtual async ValueTask<IIoHeapItem> ConstructorAsync()
#else
        public virtual ValueTask<IIoHeapItem> ConstructorAsync()
#endif
        {
#if DEBUG
            for (var i = 0; i < StateTransitionHistory.Length; i++)
            {
                var c = StateTransitionHistory[i];
                while (c != null)
                {
                    var r = c;
                    c = c.Repeat;
                    //TODO, is this the right place
#pragma warning disable CS4014
                    r.ConstructorAsync();
#pragma warning restore CS4014
                    await _stateHeap.ReturnAsync(r).FastPath().ConfigureAwait(Zc);
                }

                StateTransitionHistory[i] = null;
            }

            if (_stateMeta != null)
            {
                await _stateHeap.ReturnAsync(_stateMeta).FastPath().ConfigureAwait(Zc);
                _stateMeta = null;
            }
            Array.Clear(StateTransitionHistory, 0, StateTransitionHistory.Length);
#else
            _stateMeta.Set((int)IoJobMeta.JobState.Undefined);
#endif
            FinalState = State = IoJobMeta.JobState.Undefined;
            Syncing = false;
            Id = Interlocked.Increment(ref Source.Counters[(int)IoJobMeta.JobState.Undefined]);

            //var curState = 0;
            //while (StateTransitionHistory[curState] != null)
            //{
            //    var prevState = curState;
            //    curState = (int) StateTransitionHistory[curState].CurrentState;
            //    StateTransitionHistory[prevState] = null;
            //}

#if DEBUG
            return (IIoHeapItem)this;
#else
            return new ValueTask<IIoHeapItem>((IIoHeapItem)this);
#endif
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
                await _stateHeap.ReturnAsync(_stateMeta).FastPath().ConfigureAwait(Zc);
            Array.Clear(StateTransitionHistory, 0, StateTransitionHistory.Length);
            await _stateHeap.ZeroManagedAsync((ioHeapItem, _) =>
            {
                ioHeapItem.ZeroManaged();
                return default;
            }, this).FastPath().ConfigureAwait(Zc);
#endif
            if (PreviousJob != null)
                await PreviousJob.ZeroAsync(this).FastPath().ConfigureAwait(Zc);

            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);
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
                var c = curState.Repeat;
                while (c != null)
                {
                    PrintState(c, true);
                    c = c.Repeat;
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
                    DateTimeOffset.FromUnixTimeMilliseconds(_stateMeta.EnterTime),
                    Description,
                    
                    _stateMeta.Previous == null ? _stateMeta.DefaultPadded : _stateMeta.Previous.PaddedStr(),
                    (_stateMeta.Lambda.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),
                    
                    _stateMeta.PaddedStr(),(_stateMeta.Mu.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),
                    
                    _stateMeta.Next == null ? _stateMeta.DefaultPadded : _stateMeta.Next.PaddedStr(),
                    (_stateMeta.Delta.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size));
            }
            else
            {
                _logger.Error("Production:{0} `{1}',[{2} {3}], [{4} ||{5}||], [{6} ({7})]",
                    DateTimeOffset.FromUnixTimeMilliseconds(_stateMeta.EnterTime),
                    Description,
                    
                    _stateMeta.Previous == null ? _stateMeta.DefaultPadded : _stateMeta.Previous.PaddedStr(),
                    (_stateMeta.Lambda.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),
                    
                    _stateMeta.PaddedStr(),(_stateMeta.Mu.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),
                    
                    _stateMeta.Next == null ? _stateMeta.DefaultPadded : _stateMeta.Next.PaddedStr(),
                    (_stateMeta.Delta.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size));
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
        private IoHeap<IoStateTransition<IoJobMeta.JobState>> _stateHeap;
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
                    return (IoJobMeta.JobState)_stateMeta.Value;
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
                    _stateMeta.ExitTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    
                    if (_stateMeta.Value == (int)IoJobMeta.JobState.Halted)
                    {
                        PrintStateHistory();
                        _stateMeta.Set((int)IoJobMeta.JobState.Race);
                        throw new ApplicationException($"{TraceDescription} Cannot transition from `{IoJobMeta.JobState.Halted}' to `{value}'");
                    }
                    
                    Interlocked.Increment(ref Source.Counters[_stateMeta.Value]);
                    if (_stateMeta.Value == (int)value)
                        return;
                    
                    Interlocked.Add(ref Source.ServiceTimes[_stateMeta.Value], _stateMeta.Mu);
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
                newState.ConstructorAsync(_stateMeta, (int)value).FastPath().ConfigureAwait(Zc);

                _stateMeta = newState;
                
                //Configure the current state
                if (prevState != null)
                {
                    IoStateTransition<IoJobMeta.JobState> c;
                    if ((c = StateTransitionHistory[prevState.Value]) != null)
                    {
                        while (c.Repeat != null)
                            c = c.Repeat;
                        
                        c.Repeat = prevState;
                    }
                    else
                        StateTransitionHistory[prevState.Value] = prevState;
                }
#else
                _stateMeta!.Set((int)value);
                _stateMeta.EnterTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                _stateMeta.ExitTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
#endif
                //generate a unique id
                if (value == IoJobMeta.JobState.Undefined)
                    Id = Interlocked.Read(ref Source.Counters[(int)IoJobMeta.JobState.Undefined]);
                
                //terminate
                if (value is IoJobMeta.JobState.Accept or IoJobMeta.JobState.Reject)
                {
                    FinalState = value;
                    State = IoJobMeta.JobState.Halted;
                }                
            }
        }        
    }
}